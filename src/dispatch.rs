//! JSON-RPC method dispatch.
//!
//! Routes incoming JSON-RPC method calls to the appropriate StorageD1 method.
//! Phase 1: makeAvailable, migrate, findOrInsertUser, internalizeAction.
//! Phase 2: listOutputs, listActions.
//! Phase 3: abortAction, createAction, processAction, updateTransactionStatusAfterBroadcast.
//! Phase 4: reviewStatus (monitor sync).
//!
//! Params format: The toolbox StorageClient sends positional params (JSON arrays)
//! while the x402 skill sends named params (JSON objects). Handlers accept both.

use serde_json::Value;

use crate::error::Error;
use crate::json_rpc::{JsonRpcError, JsonRpcResponse};
use crate::storage::certificates::{InsertCertificateArgs, RelinquishCertificateArgs};
use crate::storage::readers::{
    GetAnalyticsSummaryArgs, GetBalanceArgs, ListActionsArgs, ListOutputsArgs,
};
use crate::storage::relinquish_output::RelinquishOutputArgs;
use crate::storage::StorageD1;
use crate::types::{AuthId, FindCertificatesArgs, RequestSyncChunkArgs, SyncChunk};

use bsv_sdk::wallet::{AbortActionArgs, CreateActionArgs, InternalizeActionArgs};

use crate::types::StorageProcessActionArgs;

/// Extract the actual args from params, handling both positional and named formats.
///
/// - Toolbox StorageClient sends: `[auth, args]` for auth'd methods, `[arg]` for others
/// - x402 skill / direct callers send: `{field: value}` (object)
///
/// For auth'd methods (listOutputs, listActions, internalizeAction):
///   - If array with 2+ elements: return index 1 (index 0 is auth, ignored — we use BRC-31)
///   - If array with 1 element: return index 0
///   - If object: return as-is
///
/// For non-auth'd methods (findOrInsertUser, migrate):
///   - If array with 1+ elements: return index 0
///   - If object: return as-is
fn extract_args(params: &Value, auth_method: bool) -> Value {
    match params {
        Value::Array(arr) => {
            if auth_method && arr.len() >= 2 {
                arr[1].clone()
            } else if !arr.is_empty() {
                arr[0].clone()
            } else {
                Value::Null
            }
        }
        _ => params.clone(),
    }
}

/// Dispatch a JSON-RPC method call.
///
/// The `auth` parameter is the authenticated identity from BRC-31.
/// Some methods require auth (most do), some don't (makeAvailable, migrate).
pub async fn dispatch<B: crate::services::BroadcastService + crate::services::ProofService>(
    storage: &mut StorageD1<'_, B>,
    method: &str,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Value {
    let result = match method {
        // Phase 1: Core methods
        "makeAvailable" => handle_make_available(storage, id.clone()).await,
        "migrate" => handle_migrate(storage, params, id.clone()).await,
        "findOrInsertUser" => handle_find_or_insert_user(storage, params, id.clone()).await,
        "internalizeAction" => handle_internalize_action(storage, params, id.clone(), auth).await,

        // Phase 2: Reader methods
        "listOutputs" => handle_list_outputs(storage, params, id.clone(), auth).await,
        "listActions" => handle_list_actions(storage, params, id.clone(), auth).await,
        "getBalance" => handle_get_balance(storage, params, id.clone(), auth).await,
        "getAnalyticsSummary" => {
            handle_get_analytics_summary(storage, params, id.clone(), auth).await
        }

        // Phase 3: Heavy writers
        "abortAction" => handle_abort_action(storage, params, id.clone(), auth).await,
        "createAction" => handle_create_action(storage, params, id.clone(), auth).await,
        "processAction" => handle_process_action(storage, params, id.clone(), auth).await,
        "updateTransactionStatusAfterBroadcast" => {
            handle_update_tx_status(storage, params, id.clone(), auth).await
        }

        // Certificate CRUD
        "listCertificates" => handle_list_certificates(storage, params, id.clone(), auth).await,
        "insertCertificate" => handle_insert_certificate(storage, params, id.clone(), auth).await,
        "relinquishCertificate" => {
            handle_relinquish_certificate(storage, params, id.clone(), auth).await
        }

        // Output management
        "relinquishOutput" => handle_relinquish_output(storage, params, id.clone(), auth).await,

        // BRC-40 chunk sync (L2 durable-funds): local↔remote wallet state sync
        "getSyncChunk" => handle_get_sync_chunk(storage, params, id.clone(), auth).await,
        "processSyncChunk" => handle_process_sync_chunk(storage, params, id.clone(), auth).await,

        // BRC-38 durable backup (release blocker #1): channel-independent
        // encrypted BRC-38 blob store/fetch in R2, keyed by authenticated identity.
        // MULTI-DEVICE (Codex 5b4fe9d6): putBackup writes the caller's per-device
        // object (`backup/{identity}/{deviceId}`); listBackups returns EVERY blob
        // (all per-device objects + the legacy single object) so restore unions
        // every device's change. getBackup stays for single-object back-compat.
        "putBackup" => handle_put_backup(storage, params, id.clone(), auth).await,
        "getBackup" => handle_get_backup(storage, params, id.clone(), auth).await,
        "listBackups" => handle_list_backups(storage, params, id.clone(), auth).await,
        // Q4 (Codex `ed6581ae`): metadata-only proof that a backup EXISTS.
        // Deliberately a SEPARATE RPC rather than a flag on listBackups — that
        // path is funds-restore and must not be touched for a UI feature.
        "statBackups" => handle_stat_backups(storage, params, id.clone(), auth).await,

        // Phase 4: Monitor
        "reviewStatus" => handle_review_status(storage, id.clone(), auth).await,

        // Transaction token stubs (D1 doesn't use real transactions)
        "beginStorageTransaction" => Ok(serde_json::to_value(JsonRpcResponse::success(
            id.clone(),
            serde_json::json!({ "token": 0 }),
        ))
        .unwrap()),
        "commitStorageTransaction" => {
            Ok(serde_json::to_value(JsonRpcResponse::success(id.clone(), Value::Null)).unwrap())
        }
        "rollbackStorageTransaction" => {
            Ok(serde_json::to_value(JsonRpcResponse::success(id.clone(), Value::Null)).unwrap())
        }

        // Not yet implemented
        _ => {
            return serde_json::to_value(JsonRpcError::method_not_found(id, method)).unwrap();
        }
    };

    match result {
        Ok(val) => val,
        Err(e) => {
            let (code, msg) = match &e {
                Error::ValidationError(m) => (-32602, m.clone()),
                Error::NotFound(m) => (-32001, m.clone()),
                Error::DatabaseError(m) => (-32603, m.clone()),
                Error::InternalError(m) => (-32603, m.clone()),
            };
            serde_json::to_value(JsonRpcError::new(id, code, msg)).unwrap()
        }
    }
}

// =============================================================================
// Handlers
// =============================================================================

async fn handle_make_available<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &mut StorageD1<'_, B>,
    id: Value,
) -> Result<Value, Error> {
    let settings = storage.make_available().await?;
    let result = serde_json::to_value(&settings)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

async fn handle_migrate<B: crate::services::BroadcastService + crate::services::ProofService>(
    storage: &mut StorageD1<'_, B>,
    params: Value,
    id: Value,
) -> Result<Value, Error> {
    let args = extract_args(&params, false);

    // Positional: ["storage_name"] → just a string
    // Named: {"storageName": "...", "storageIdentityKey": "..."}
    let (storage_name, storage_identity_key) = if let Some(s) = args.as_str() {
        (s.to_string(), String::new())
    } else {
        let name = args
            .get("storageName")
            .and_then(|v| v.as_str())
            .unwrap_or("wallet-infra")
            .to_string();
        let key = args
            .get("storageIdentityKey")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        (name, key)
    };

    let chain = storage
        .migrate(&storage_name, &storage_identity_key)
        .await?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, serde_json::json!(chain))).unwrap())
}

async fn handle_find_or_insert_user<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
) -> Result<Value, Error> {
    let args = extract_args(&params, false);

    // Positional: ["identity_key"] → just a string
    // Named: {"identityKey": "..."}
    let identity_key = if let Some(s) = args.as_str() {
        s.to_string()
    } else {
        args.get("identityKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ValidationError("missing identityKey".to_string()))?
            .to_string()
    };

    let (user, inserted) = storage.find_or_insert_user(&identity_key).await?;
    let result = serde_json::json!({
        "user": serde_json::to_value(&user)?,
        "isNew": inserted,
    });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

async fn handle_internalize_action<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("internalizeAction requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let mut args_val = extract_args(&params, true);

    // The bsv-sdk InternalizeActionArgs expects `tx` as a hex string (via #[serde(with = "hex_bytes")]).
    // But the bsv-auth-cloudflare payment middleware sends `tx` as a JSON array of byte values
    // (Vec<u8> serialized). Convert array → hex string so deserialization works for both formats.
    if let Some(tx_val) = args_val.get("tx") {
        if tx_val.is_array() {
            let bytes: Vec<u8> = tx_val
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|v| v.as_u64().map(|n| n as u8))
                .collect();
            args_val["tx"] = Value::String(hex::encode(&bytes));
        }
    }

    let args: InternalizeActionArgs = serde_json::from_value(args_val)?;
    let result = storage.internalize_action(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_list_outputs<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("listOutputs requires authentication".to_string()))?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: ListOutputsArgs = serde_json::from_value(args_val)?;
    let result = storage.list_outputs(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_list_actions<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("listActions requires authentication".to_string()))?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: ListActionsArgs = serde_json::from_value(args_val)?;
    let result = storage.list_actions(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_get_balance<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("getBalance requires authentication".to_string()))?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: GetBalanceArgs = serde_json::from_value(args_val)?;
    let result = storage.get_balance(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_get_analytics_summary<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("getAnalyticsSummary requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: GetAnalyticsSummaryArgs = serde_json::from_value(args_val)?;
    let result = storage.get_analytics_summary(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

// =============================================================================
// Phase 3: Heavy writer handlers
// =============================================================================

async fn handle_abort_action<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("abortAction requires authentication".to_string()))?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: AbortActionArgs = serde_json::from_value(args_val)?;
    let aborted = storage.abort_action(user_id, &args.reference).await?;
    let result = serde_json::json!({ "aborted": aborted });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

async fn handle_create_action<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("createAction requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: CreateActionArgs = serde_json::from_value(args_val)?;
    let result = storage.create_action(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_process_action<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("processAction requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: StorageProcessActionArgs = serde_json::from_value(args_val)?;
    let result = storage.process_action(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_update_tx_status<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError(
            "updateTransactionStatusAfterBroadcast requires authentication".to_string(),
        )
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;

    // This method doesn't wrap params in [auth, args] like other methods.
    // Toolbox sends: params = [txid_string, success_bool] (bare array, no auth element)
    // Named: {"txid": "...", "success": true/false}
    let (txid, success) = if let Some(arr) = params.as_array() {
        let txid = arr
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ValidationError("missing txid".to_string()))?
            .to_string();
        let success = arr.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
        (txid, success)
    } else {
        let txid = params
            .get("txid")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ValidationError("missing txid".to_string()))?
            .to_string();
        let success = params
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        (txid, success)
    };

    storage
        .update_transaction_status_after_broadcast(user_id, &txid, success)
        .await?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, Value::Null)).unwrap())
}

// =============================================================================
// Phase 4: Monitor handlers
// =============================================================================

async fn handle_review_status<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let _auth = auth.ok_or_else(|| {
        Error::ValidationError("reviewStatus requires authentication".to_string())
    })?;

    // reviewStatus runs the same logic as the cron monitor's review_status task
    let count = crate::monitor::review_status(storage.db())
        .await
        .map_err(|e| Error::InternalError(e.to_string()))?;

    let result = serde_json::json!({ "status_synced": count });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

// =============================================================================
// BRC-40 chunk sync handlers (getSyncChunk / processSyncChunk)
// =============================================================================

/// `getSyncChunk` — the wallet sends positional `[args]` (RequestSyncChunkArgs).
/// We resolve the user from the BRC-31 authenticated identity (the secure
/// equivalent of the canonical `args.identity_key` lookup; in practice they are
/// the same identity) and return a bounded chunk of that user's data.
async fn handle_get_sync_chunk<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("getSyncChunk requires authentication".to_string()))?;
    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: RequestSyncChunkArgs = serde_json::from_value(args_val)?;
    let chunk = storage.get_sync_chunk(user_id, args).await?;
    let result_val = serde_json::to_value(&chunk)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

/// `processSyncChunk` — the wallet sends positional `[args, chunk]`. Unlike the
/// single-arg auth'd methods, `extract_args` can't serve both, so split the
/// array manually (named `{args, chunk}` accepted as a fallback).
async fn handle_process_sync_chunk<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("processSyncChunk requires authentication".to_string())
    })?;
    let (user_id, _auth) = storage.resolve_auth(auth).await?;

    let (args_val, chunk_val) = match &params {
        Value::Array(arr) => {
            let args_val = arr
                .first()
                .cloned()
                .ok_or_else(|| Error::ValidationError("processSyncChunk: missing args".to_string()))?;
            let chunk_val = arr
                .get(1)
                .cloned()
                .ok_or_else(|| Error::ValidationError("processSyncChunk: missing chunk".to_string()))?;
            (args_val, chunk_val)
        }
        Value::Object(_) => {
            let args_val = params
                .get("args")
                .cloned()
                .ok_or_else(|| Error::ValidationError("processSyncChunk: missing args".to_string()))?;
            let chunk_val = params
                .get("chunk")
                .cloned()
                .ok_or_else(|| Error::ValidationError("processSyncChunk: missing chunk".to_string()))?;
            (args_val, chunk_val)
        }
        _ => {
            return Err(Error::ValidationError(
                "processSyncChunk: expected [args, chunk] or {args, chunk}".to_string(),
            ))
        }
    };

    let args: RequestSyncChunkArgs = serde_json::from_value(args_val)?;
    let chunk: SyncChunk = serde_json::from_value(chunk_val)?;
    let result = storage.process_sync_chunk(user_id, args, chunk).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

// =============================================================================
// BRC-38 durable backup (release blocker #1 — the channel-INDEPENDENT recovery
// path). The blob is CIPHERTEXT: encrypted client-side via BRC-2 before it ever
// reaches here, so the worker stores an opaque object it cannot read. The R2
// object key is derived from the AUTHENTICATED identity (never a client param),
// so a caller can only read/write their OWN backup.
//
// MULTI-DEVICE (Codex 5b4fe9d6): each device writes its OWN object at
// `backup/{identityKey}/{deviceId}`, so a second device can NEVER clobber
// another's blob (the total-silent-loss vector). Restore LISTs the prefix +
// unions every blob. The LEGACY single object (`backup/{identityKey}`,
// no device segment) is still WRITTEN by old clients and still READ on restore
// (dual-read, never migrated/deleted) so nobody already deployed loses a backup.
// =============================================================================

/// Legacy single-object key (pre-multi-device). Still read on restore.
fn backup_object_key(identity_key: &str) -> String {
    format!("backup/{}", identity_key)
}

/// Per-device object key. The deviceId partitions within the identity's own
/// namespace (NOT a security boundary — the BRC-103 identity auth is).
fn backup_device_object_key(identity_key: &str, device_id: &str) -> String {
    format!("backup/{}/{}", identity_key, device_id)
}

/// The R2 LIST prefix that matches every PER-DEVICE object for an identity (the
/// trailing slash excludes the legacy `backup/{identityKey}` object, which is
/// read separately on restore).
fn backup_device_prefix(identity_key: &str) -> String {
    format!("backup/{}/", identity_key)
}

/// Validate a client-supplied deviceId server-side (Codex 5b4fe9d6): a bounded
/// URL/key-safe token. NEVER trust a blob-mirrored id as authority; this is the
/// only accepted source, and it only ever partitions the caller's OWN identity
/// namespace. Rejects anything that could escape the prefix (slashes, dots) or
/// bloat the key.
fn validate_device_id(device_id: &str) -> Result<(), Error> {
    let ok = (8..=64).contains(&device_id.len())
        && device_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(Error::ValidationError(
            "putBackup: invalid deviceId (want 8-64 chars of [A-Za-z0-9_-])".to_string(),
        ))
    }
}

async fn handle_put_backup<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    use base64::Engine as _;
    let auth = auth
        .ok_or_else(|| Error::ValidationError("putBackup requires authentication".to_string()))?;
    let (_user_id, auth) = storage.resolve_auth(auth).await?;

    // Accept { blob: "<base64>" } or ["<base64>"].
    let blob_b64 = match &params {
        Value::Array(arr) => arr.first().and_then(|v| v.as_str()),
        Value::Object(_) => params.get("blob").and_then(|v| v.as_str()),
        _ => None,
    }
    .ok_or_else(|| {
        Error::ValidationError("putBackup: missing 'blob' (base64 string)".to_string())
    })?;

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(blob_b64.as_bytes())
        .map_err(|e| Error::ValidationError(format!("putBackup: bad base64 blob: {}", e)))?;

    // MULTI-DEVICE: a client that supplies a valid `deviceId` writes its OWN
    // per-device object (can't clobber another device). A legacy client with no
    // deviceId keeps writing the single object (back-compat). Both call shapes
    // carry it: the engine's array-based RPC sends `[blob, deviceId]`, and the
    // object form `{ blob, deviceId }` is accepted for direct/manual callers.
    let device_id = match &params {
        Value::Object(_) => params.get("deviceId").and_then(|v| v.as_str()),
        Value::Array(arr) => arr.get(1).and_then(|v| v.as_str()),
        _ => None,
    };
    let key = match device_id {
        Some(d) => {
            validate_device_id(d)?;
            backup_device_object_key(&auth.identity_key, d)
        }
        None => backup_object_key(&auth.identity_key),
    };
    storage
        .blobs()
        .put(&key, bytes.clone())
        .execute()
        .await
        .map_err(|e| Error::InternalError(format!("putBackup: R2 put failed: {}", e)))?;

    Ok(serde_json::to_value(JsonRpcResponse::success(
        id,
        serde_json::json!({ "ok": true, "bytes": bytes.len() }),
    ))
    .unwrap())
}

async fn handle_get_backup<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    _params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    use base64::Engine as _;
    let auth = auth
        .ok_or_else(|| Error::ValidationError("getBackup requires authentication".to_string()))?;
    let (_user_id, auth) = storage.resolve_auth(auth).await?;

    let key = backup_object_key(&auth.identity_key);
    let obj = storage
        .blobs()
        .get(&key)
        .execute()
        .await
        .map_err(|e| Error::InternalError(format!("getBackup: R2 get failed: {}", e)))?;

    let blob_b64 = match obj {
        Some(obj) => {
            let body = obj.body().ok_or_else(|| {
                Error::InternalError("getBackup: R2 object has no body".to_string())
            })?;
            let bytes = body
                .bytes()
                .await
                .map_err(|e| Error::InternalError(format!("getBackup: R2 read failed: {}", e)))?;
            Some(base64::engine::general_purpose::STANDARD.encode(&bytes))
        }
        None => None,
    };

    Ok(serde_json::to_value(JsonRpcResponse::success(
        id,
        serde_json::json!({ "blob": blob_b64 }),
    ))
    .unwrap())
}

// MULTI-DEVICE restore (Codex 5b4fe9d6): return EVERY backup blob for the
// authenticated identity — all per-device objects (LIST prefix) PLUS the legacy
// single object (dual-read, never migrated). The client decrypts + imports each
// through the funds-monotonic merge (sequential import = union of every device's
// change). Per-blob decrypt/import failures are the client's to isolate (a
// corrupt orphan must not abort the whole restore).
async fn handle_list_backups<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    _params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    use base64::Engine as _;
    let auth = auth
        .ok_or_else(|| Error::ValidationError("listBackups requires authentication".to_string()))?;
    let (_user_id, auth) = storage.resolve_auth(auth).await?;

    let read_blob = |bytes: Vec<u8>| base64::engine::general_purpose::STANDARD.encode(&bytes);
    let mut blobs: Vec<String> = Vec::new();

    // 1) Per-device objects under `backup/{identity}/`.
    //
    // PAGINATION (bughunt Finding #5): R2 LIST returns at most 1000 keys per
    // call. Device objects are immortal (never migrated/deleted) and a fresh
    // deviceId is minted on every full storage wipe, so an identity can exceed
    // 1000 objects over time. Without following the cursor, everything past the
    // first 1000 (by lexicographic key) is silently dropped from the restore
    // union — a blob holding unique un-synced change could be omitted. Loop on
    // `truncated()`/`cursor()` so EVERY device object is returned.
    let prefix = backup_device_prefix(&auth.identity_key);
    let mut cursor: Option<String> = None;
    loop {
        let mut builder = storage.blobs().list().prefix(prefix.clone());
        if let Some(c) = cursor.take() {
            builder = builder.cursor(c);
        }
        let listed = builder
            .execute()
            .await
            .map_err(|e| Error::InternalError(format!("listBackups: R2 list failed: {}", e)))?;
        for obj in listed.objects() {
            let key = obj.key();
            let got = storage
                .blobs()
                .get(&key)
                .execute()
                .await
                .map_err(|e| Error::InternalError(format!("listBackups: R2 get failed: {}", e)))?;
            if let Some(o) = got {
                if let Some(body) = o.body() {
                    let bytes = body.bytes().await.map_err(|e| {
                        Error::InternalError(format!("listBackups: R2 read failed: {}", e))
                    })?;
                    blobs.push(read_blob(bytes));
                }
            }
        }
        if listed.truncated() {
            cursor = listed.cursor();
            // Defensive: truncated but no cursor → stop rather than loop forever.
            if cursor.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    // 2) Legacy single object `backup/{identity}` (dual-read; never migrated).
    let legacy_key = backup_object_key(&auth.identity_key);
    let legacy = storage
        .blobs()
        .get(&legacy_key)
        .execute()
        .await
        .map_err(|e| Error::InternalError(format!("listBackups: legacy R2 get failed: {}", e)))?;
    if let Some(o) = legacy {
        if let Some(body) = o.body() {
            let bytes = body.bytes().await.map_err(|e| {
                Error::InternalError(format!("listBackups: legacy R2 read failed: {}", e))
            })?;
            blobs.push(read_blob(bytes));
        }
    }

    Ok(serde_json::to_value(JsonRpcResponse::success(
        id,
        serde_json::json!({ "blobs": blobs }),
    ))
    .unwrap())
}

/// Q4 · `statBackups` — METADATA-ONLY proof that a backup object exists.
///
/// Answers "is my encrypted backup actually in R2?" WITHOUT downloading a single
/// blob body. `listBackups` (the restore path) walks the same objects but reads
/// every body — using it to answer an existence question would download every
/// backup an identity owns to render a UI pill.
///
/// Canonical note: `wallet-toolbox` has NO remote-durability-proof idiom (its
/// `isAvailable`/`makeAvailable` is LIVENESS — "can I reach the store" — not
/// durability, and `TableSyncState` is a LOCAL record of what we believe we
/// pushed). This is a deliberate necessary-novel addition: canonical assumes a
/// trusted server-side store, whereas our funds recovery depends on an R2 blob
/// the user may be encouraged to wipe local state against.
///
/// ⚠️ THE LEGACY TRAP (Codex `ed6581ae`): the pre-multi-device object
/// `backup/{identity}` has NO device segment, so it can belong to ANY device.
/// It is reported (so the caller can reason about it) but is flagged
/// `deviceScoped: false` and carries NO deviceId — the client MUST NOT let it
/// satisfy a current-device "verified" claim. Change is randomly derived
/// per-device: another device's blob is NOT proof that THIS device's change is
/// recoverable, and saying so would be exactly the false comfort I-3 bans.
///
/// Shares the restore path's pagination contract (bughunt Finding #5): R2 LIST
/// caps at 1000 keys and device objects are immortal, so the cursor MUST be
/// followed or the proof would silently disagree with what restore can see.
async fn handle_stat_backups<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    _params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth
        .ok_or_else(|| Error::ValidationError("statBackups requires authentication".to_string()))?;
    let (_user_id, auth) = storage.resolve_auth(auth).await?;

    let mut objects: Vec<Value> = Vec::new();

    // 1) Per-device objects under `backup/{identity}/` — the ONLY ones that may
    //    prove a specific device. Same cursor-following walk as the restore path.
    let prefix = backup_device_prefix(&auth.identity_key);
    let mut cursor: Option<String> = None;
    loop {
        let mut builder = storage.blobs().list().prefix(prefix.clone());
        if let Some(c) = cursor.take() {
            builder = builder.cursor(c);
        }
        let listed = builder
            .execute()
            .await
            .map_err(|e| Error::InternalError(format!("statBackups: R2 list failed: {}", e)))?;
        for obj in listed.objects() {
            let key = obj.key();
            // `backup/{identity}/{deviceId}` → take the trailing segment. A key
            // without one cannot prove a device; skip rather than guess.
            let device_id = match key.rsplit('/').next() {
                Some(d) if !d.is_empty() => d.to_string(),
                _ => continue,
            };
            objects.push(serde_json::json!({
                "deviceId": device_id,
                "deviceScoped": true,
                "size": obj.size(),
                "uploaded": obj.uploaded().as_millis(),
                "etag": obj.etag(),
            }));
        }
        if listed.truncated() {
            cursor = listed.cursor();
            // Defensive: truncated but no cursor → stop rather than loop forever.
            if cursor.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    // 2) Legacy single object `backup/{identity}` — reported, but NEVER
    //    device-scoped (see THE LEGACY TRAP above). HEAD, not GET: we need
    //    existence + metadata, not the payload.
    let legacy_key = backup_object_key(&auth.identity_key);
    let legacy = storage
        .blobs()
        .head(&legacy_key)
        .await
        .map_err(|e| Error::InternalError(format!("statBackups: legacy R2 head failed: {}", e)))?;
    if let Some(o) = legacy {
        objects.push(serde_json::json!({
            "deviceId": Value::Null,
            "deviceScoped": false,
            "size": o.size(),
            "uploaded": o.uploaded().as_millis(),
            "etag": o.etag(),
        }));
    }

    Ok(serde_json::to_value(JsonRpcResponse::success(
        id,
        serde_json::json!({ "objects": objects }),
    ))
    .unwrap())
}

// =============================================================================
// Certificate CRUD handlers
// =============================================================================

async fn handle_list_certificates<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("listCertificates requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: FindCertificatesArgs = serde_json::from_value(args_val)?;
    let result = storage.list_certificates(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_insert_certificate<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("insertCertificate requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: InsertCertificateArgs = serde_json::from_value(args_val)?;
    let result = storage.insert_certificate(user_id, args).await?;
    let result_val = serde_json::to_value(&result)?;
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result_val)).unwrap())
}

async fn handle_relinquish_certificate<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("relinquishCertificate requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: RelinquishCertificateArgs = serde_json::from_value(args_val)?;
    let relinquished = storage.relinquish_certificate(user_id, args).await?;
    let result = serde_json::json!({ "relinquished": relinquished });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

// =============================================================================
// Output management handlers
// =============================================================================

async fn handle_relinquish_output<
    B: crate::services::BroadcastService + crate::services::ProofService,
>(
    storage: &StorageD1<'_, B>,
    params: Value,
    id: Value,
    auth: Option<&AuthId>,
) -> Result<Value, Error> {
    let auth = auth.ok_or_else(|| {
        Error::ValidationError("relinquishOutput requires authentication".to_string())
    })?;

    let (user_id, _auth) = storage.resolve_auth(auth).await?;
    let args_val = extract_args(&params, true);
    let args: RelinquishOutputArgs = serde_json::from_value(args_val)?;
    let relinquished = storage.relinquish_output(user_id, args).await?;
    let result = serde_json::json!({ "relinquished": relinquished });
    Ok(serde_json::to_value(JsonRpcResponse::success(id, result)).unwrap())
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    // Re-import the function under test. It's private, so we test via super.
    use super::extract_args;

    // =========================================================================
    // Auth method = true (listOutputs, listActions, internalizeAction, etc.)
    // =========================================================================

    #[test]
    fn auth_positional_array_two_elements_returns_second() {
        // Toolbox sends [auth_obj, args_obj] for authenticated methods.
        let params = json!([{"identityKey": "abc"}, {"basket": "default", "limit": 10}]);
        let result = extract_args(&params, true);
        assert_eq!(result, json!({"basket": "default", "limit": 10}));
    }

    #[test]
    fn auth_positional_array_three_elements_returns_second() {
        // Extra elements beyond index 1 are ignored.
        let params = json!(["auth", {"args": true}, "extra"]);
        let result = extract_args(&params, true);
        assert_eq!(result, json!({"args": true}));
    }

    #[test]
    fn auth_positional_array_one_element_returns_first() {
        // Array with only 1 element: no auth prefix, so return index 0.
        let params = json!([{"basket": "default"}]);
        let result = extract_args(&params, true);
        assert_eq!(result, json!({"basket": "default"}));
    }

    #[test]
    fn auth_empty_array_returns_null() {
        let params = json!([]);
        let result = extract_args(&params, true);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn auth_object_params_returned_as_is() {
        // Direct callers / x402 skill send named objects.
        let params = json!({"basket": "default", "limit": 5});
        let result = extract_args(&params, true);
        assert_eq!(result, json!({"basket": "default", "limit": 5}));
    }

    // =========================================================================
    // Auth method = false (findOrInsertUser, migrate)
    // =========================================================================

    #[test]
    fn non_auth_positional_array_two_elements_returns_first() {
        // Non-auth methods always take index 0, even with 2 elements.
        let params = json!(["identity_key_123", "ignored"]);
        let result = extract_args(&params, false);
        assert_eq!(result, json!("identity_key_123"));
    }

    #[test]
    fn non_auth_positional_array_one_element_returns_first() {
        let params = json!(["identity_key_123"]);
        let result = extract_args(&params, false);
        assert_eq!(result, json!("identity_key_123"));
    }

    #[test]
    fn non_auth_empty_array_returns_null() {
        let params = json!([]);
        let result = extract_args(&params, false);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn non_auth_object_params_returned_as_is() {
        let params = json!({"identityKey": "abc123"});
        let result = extract_args(&params, false);
        assert_eq!(result, json!({"identityKey": "abc123"}));
    }

    // =========================================================================
    // Edge cases: non-array, non-object param types
    // =========================================================================

    #[test]
    fn null_params_returned_as_is() {
        let params = Value::Null;
        let result = extract_args(&params, true);
        assert_eq!(result, Value::Null);

        let result = extract_args(&params, false);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn string_params_returned_as_is() {
        // A raw string is not an array, so the match falls through to the default.
        let params = json!("just_a_string");
        let result = extract_args(&params, true);
        assert_eq!(result, json!("just_a_string"));
    }

    #[test]
    fn number_params_returned_as_is() {
        let params = json!(42);
        let result = extract_args(&params, false);
        assert_eq!(result, json!(42));
    }

    #[test]
    fn bool_params_returned_as_is() {
        let params = json!(true);
        let result = extract_args(&params, true);
        assert_eq!(result, json!(true));
    }

    // =========================================================================
    // Verify auth flag makes a difference with 2-element arrays
    // =========================================================================

    #[test]
    fn auth_flag_selects_different_index_for_two_element_array() {
        let params = json!(["first", "second"]);

        // auth=true -> index 1
        let auth_result = extract_args(&params, true);
        assert_eq!(auth_result, json!("second"));

        // auth=false -> index 0
        let non_auth_result = extract_args(&params, false);
        assert_eq!(non_auth_result, json!("first"));
    }

    #[test]
    fn nested_objects_in_array_preserved() {
        let params = json!([
            {"identityKey": "auth_key"},
            {"basket": "default", "tags": ["tag1", "tag2"], "nested": {"deep": true}}
        ]);
        let result = extract_args(&params, true);
        assert_eq!(
            result,
            json!({"basket": "default", "tags": ["tag1", "tag2"], "nested": {"deep": true}})
        );
    }
}
