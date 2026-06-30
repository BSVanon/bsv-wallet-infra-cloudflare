//! BRC-40 chunk sync RPCs: `getSyncChunk` and `processSyncChunk`.
//!
//! This is the L2 durable-funds layer the wallet's `syncToRemoteStorage`
//! drives. The wallet's canonical sync orchestrator (`storage::sync_chunks` in
//! bsv-wallet-toolbox-rs) pushes local→remote by calling `processSyncChunk` on
//! us, and pulls remote→local by calling `getSyncChunk` on us. Without these
//! two methods the push fails `-32601 Method not found` and L2 sync is inert.
//!
//! Ported from canonical `bsv-wallet-toolbox-rs/src/storage/sqlx/sync.rs`
//! (`get_sync_chunk_internal` / `process_sync_chunk_internal`), adapted from
//! sqlx to the D1 query idiom used elsewhere in this crate:
//!   - reads go through dedicated snake_case row structs (D1 returns numbers as
//!     f64, dates as TEXT) and BLOBs are read hex-wrapped (`hex(col) AS col_hex`)
//!     then decoded, falling back to R2 for >4KB overflow blobs;
//!   - writes use the two-phase NULL→put→UPDATE pattern for nullable overflow
//!     blobs and single-phase inline binding for NOT-NULL blobs;
//!   - upserts are idempotent by natural key (name/txid/reference/…), exactly
//!     as the canonical server does, so a fresh per-call id map suffices
//!     (the canonical server-side `process_sync_chunk` also uses a fresh
//!     `SyncMap::default()` per call — see `StorageSqlx::process_sync_chunk`).
//!
//! Entity dependency order (parents before children) is preserved so that
//! within one chunk a child row (output → transaction, map → its endpoints)
//! resolves its parent via the in-call id map.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::d1::Query;
use crate::entities::*;
use crate::error::{Error, Result};
use crate::types::{ProcessSyncChunkResult, RequestSyncChunkArgs, SyncChunk, SyncOffset};

use super::writers::parse_datetime_pub;
use super::StorageD1;

// =============================================================================
// Entity offset names — MUST match the wallet's ENTITY_NAMES byte-for-byte
// (bsv-wallet-toolbox-rs sync_orchestrator.rs) so chunks interoperate.
// =============================================================================

mod entity_names {
    pub const OUTPUT_BASKET: &str = "outputBasket";
    pub const PROVEN_TX: &str = "provenTx";
    pub const PROVEN_TX_REQ: &str = "provenTxReq";
    pub const TX_LABEL: &str = "txLabel";
    pub const OUTPUT_TAG: &str = "outputTag";
    pub const TRANSACTION: &str = "transaction";
    pub const OUTPUT: &str = "output";
    pub const TX_LABEL_MAP: &str = "txLabelMap";
    pub const OUTPUT_TAG_MAP: &str = "outputTagMap";
    pub const CERTIFICATE: &str = "certificate";
    pub const CERTIFICATE_FIELD: &str = "certificateField";
    pub const COMMISSION: &str = "commission";
}

// =============================================================================
// Chunking state (caps tracking) — mirrors canonical ChunkingState.
// =============================================================================

struct ChunkingState {
    items_count: u32,
    rough_size: u32,
    max_items: u32,
    max_rough_size: u32,
}

impl ChunkingState {
    fn new(max_items: u32, max_rough_size: u32) -> Self {
        Self {
            items_count: 0,
            rough_size: 0,
            max_items,
            max_rough_size,
        }
    }
    fn can_add(&self) -> bool {
        self.items_count < self.max_items && self.rough_size < self.max_rough_size
    }
    fn add_items(&mut self, count: u32, size: u32) {
        self.items_count += count;
        self.rough_size += size;
    }
    fn remaining_items(&self) -> u32 {
        self.max_items.saturating_sub(self.items_count)
    }
}

fn make_offsets_lookup(offsets: &[SyncOffset]) -> HashMap<String, u32> {
    offsets.iter().map(|o| (o.name.clone(), o.offset)).collect()
}

fn estimate_size<T: serde::Serialize>(items: &[T]) -> u32 {
    serde_json::to_string(items).map(|s| s.len() as u32).unwrap_or(0)
}

fn update_max_updated_at(max: &mut Option<DateTime<Utc>>, updated_at: DateTime<Utc>) {
    match max {
        Some(current) if updated_at > *current => *max = Some(updated_at),
        None => *max = Some(updated_at),
        _ => {}
    }
}

/// Decode a `hex(col)` D1 value, falling back to R2 for overflow blobs (column
/// NULL). Mirrors `create_action::decode_blob_with_r2`.
async fn decode_blob_with_r2(
    blobs: &worker::Bucket,
    table: &str,
    id: i64,
    column: &str,
    hex_from_d1: Option<&str>,
) -> Result<Option<Vec<u8>>> {
    if let Some(hex_str) = hex_from_d1 {
        if !hex_str.is_empty() {
            return hex::decode(hex_str).map(Some).map_err(|e| {
                Error::InternalError(format!("Bad hex in {}.{} (id={}): {}", table, column, id, e))
            });
        }
    }
    if id > 0 {
        let store = crate::r2::BlobStore::new(blobs);
        return store.get(table, id, column, None).await;
    }
    Ok(None)
}

fn req_status_from_str(s: &str) -> ProvenTxReqStatus {
    serde_json::from_value(serde_json::Value::String(s.to_string())).unwrap_or_default()
}

fn vec_is_empty<T>(v: &Option<Vec<T>>) -> bool {
    v.as_ref().is_none_or(|x| x.is_empty())
}

// =============================================================================
// D1 read row structs (snake_case; numbers as f64, dates/blobs as text)
// =============================================================================

#[derive(Debug, Deserialize)]
struct ExistsRow {
    id: Option<f64>,
    updated_at: Option<String>,
}

/// One nullable column read back as `hex(...)` — used by the fill-if-empty blob
/// guard to detect an inline-populated D1 blob.
#[derive(Debug, Deserialize)]
struct BlobHexRow {
    v: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UserMergeRow {
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BasketSyncRow {
    basket_id: Option<f64>,
    user_id: Option<f64>,
    name: Option<String>,
    number_of_desired_utxos: Option<f64>,
    minimum_desired_utxo_value: Option<f64>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProvenTxSyncRow {
    proven_tx_id: Option<f64>,
    txid: Option<String>,
    height: Option<f64>,
    idx: Option<f64>,
    block_hash: Option<String>,
    merkle_root: Option<String>,
    merkle_path_hex: Option<String>,
    raw_tx_hex: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProvenTxReqSyncRow {
    proven_tx_req_id: Option<f64>,
    proven_tx_id: Option<f64>,
    txid: Option<String>,
    status: Option<String>,
    attempts: Option<f64>,
    history: Option<String>,
    notified: Option<f64>,
    notify: Option<String>,
    raw_tx_hex: Option<String>,
    input_beef_hex: Option<String>,
    batch: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LabelSyncRow {
    tx_label_id: Option<f64>,
    user_id: Option<f64>,
    label: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TagSyncRow {
    output_tag_id: Option<f64>,
    user_id: Option<f64>,
    tag: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TransactionSyncRow {
    transaction_id: Option<f64>,
    user_id: Option<f64>,
    txid: Option<String>,
    status: Option<String>,
    reference: Option<String>,
    description: Option<String>,
    satoshis: Option<f64>,
    version: Option<f64>,
    lock_time: Option<f64>,
    raw_tx_hex: Option<String>,
    input_beef_hex: Option<String>,
    is_outgoing: Option<f64>,
    // The proof linkage travels on the wire as `proofTxid` (a txid string,
    // canonical `TableTransaction.proof_txid`). Our schema models it as an
    // INTEGER FK `transactions.proven_tx_id` → `proven_txs.proven_tx_id`, so we
    // surface the linked proof's txid via a LEFT JOIN (NULL when unproven).
    proof_txid: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OutputSyncRow {
    output_id: Option<f64>,
    user_id: Option<f64>,
    transaction_id: Option<f64>,
    basket_id: Option<f64>,
    txid: Option<String>,
    vout: Option<f64>,
    satoshis: Option<f64>,
    locking_script_hex: Option<String>,
    script_length: Option<f64>,
    script_offset: Option<f64>,
    #[serde(rename = "type")]
    output_type: Option<String>,
    provided_by: Option<String>,
    purpose: Option<String>,
    output_description: Option<String>,
    spent_by: Option<f64>,
    spent_by_reference: Option<String>,
    sequence_number: Option<f64>,
    spending_description: Option<String>,
    spendable: Option<f64>,
    change: Option<f64>,
    derivation_prefix: Option<String>,
    derivation_suffix: Option<String>,
    sender_identity_key: Option<String>,
    custom_instructions: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TxLabelMapSyncRow {
    tx_label_map_id: Option<f64>,
    transaction_id: Option<f64>,
    tx_label_id: Option<f64>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OutputTagMapSyncRow {
    output_tag_map_id: Option<f64>,
    output_id: Option<f64>,
    output_tag_id: Option<f64>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CertificateSyncRow {
    certificate_id: Option<f64>,
    user_id: Option<f64>,
    #[serde(rename = "type")]
    cert_type: Option<String>,
    serial_number: Option<String>,
    certifier: Option<String>,
    subject: Option<String>,
    verifier: Option<String>,
    revocation_outpoint: Option<String>,
    signature: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CertificateFieldSyncRow {
    certificate_field_id: Option<f64>,
    certificate_id: Option<f64>,
    user_id: Option<f64>,
    field_name: Option<String>,
    field_value: Option<String>,
    master_key: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CommissionSyncRow {
    commission_id: Option<f64>,
    user_id: Option<f64>,
    transaction_id: Option<f64>,
    satoshis: Option<f64>,
    locking_script_hex: Option<String>,
    key_offset: Option<String>,
    is_redeemed: Option<f64>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

struct UpsertResult {
    local_id: i64,
    is_new: bool,
}

// =============================================================================
// Public RPC methods
// =============================================================================

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// BRC-40 `getSyncChunk`: build a bounded chunk of this user's data for the
    /// caller to apply. Entities are emitted in dependency order, each limited
    /// by its continuation offset + the remaining per-chunk item budget.
    pub async fn get_sync_chunk(
        &self,
        user_id: i64,
        args: RequestSyncChunkArgs,
    ) -> Result<SyncChunk> {
        let mut chunk = SyncChunk {
            from_storage_identity_key: args.from_storage_identity_key.clone(),
            to_storage_identity_key: args.to_storage_identity_key.clone(),
            user_identity_key: args.identity_key.clone(),
            ..Default::default()
        };

        // Include the user record (the wallet keys on identity_key).
        let user_row: Option<UserRowLite> =
            Query::new("SELECT user_id, identity_key, active_storage, created_at, updated_at FROM users WHERE user_id = ?")
                .bind(user_id)
                .fetch_optional(self.db)
                .await?;
        if let Some(u) = user_row {
            let table_user = TableUser {
                user_id: u.user_id.map(|v| v as i64).unwrap_or(user_id),
                identity_key: u.identity_key.unwrap_or_else(|| args.identity_key.clone()),
                active_storage: u.active_storage,
                created_at: parse_datetime_pub(&u.created_at),
                updated_at: parse_datetime_pub(&u.updated_at),
            };
            if args.since.is_none() || table_user.updated_at > args.since.unwrap() {
                chunk.user = Some(table_user);
            }
        }

        let offsets = make_offsets_lookup(&args.offsets);
        // Empty offsets → user-only chunk (canonical contract; writer marks done).
        if offsets.is_empty() {
            return Ok(chunk);
        }

        let mut state = ChunkingState::new(args.max_items, args.max_rough_size);
        let since = args.since;

        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::OUTPUT_BASKET) {
                let v = self
                    .fetch_baskets_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.output_baskets = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::PROVEN_TX) {
                let v = self
                    .fetch_proven_txs_for_sync(since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.proven_txs = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::PROVEN_TX_REQ) {
                let v = self
                    .fetch_proven_tx_reqs_for_sync(since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.proven_tx_reqs = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::TX_LABEL) {
                let v = self
                    .fetch_tx_labels_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.tx_labels = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::OUTPUT_TAG) {
                let v = self
                    .fetch_output_tags_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.output_tags = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::TRANSACTION) {
                let v = self
                    .fetch_transactions_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.transactions = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::OUTPUT) {
                let v = self
                    .fetch_outputs_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.outputs = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::TX_LABEL_MAP) {
                let v = self
                    .fetch_tx_label_maps_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.tx_label_maps = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::OUTPUT_TAG_MAP) {
                let v = self
                    .fetch_output_tag_maps_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.output_tag_maps = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::CERTIFICATE) {
                let v = self
                    .fetch_certificates_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.certificates = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::CERTIFICATE_FIELD) {
                let v = self
                    .fetch_certificate_fields_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.certificate_fields = Some(v);
                }
            }
        }
        if state.can_add() {
            if let Some(&off) = offsets.get(entity_names::COMMISSION) {
                let v = self
                    .fetch_commissions_for_sync(user_id, since, off, state.remaining_items())
                    .await?;
                if !v.is_empty() {
                    state.add_items(v.len() as u32, estimate_size(&v));
                    chunk.commissions = Some(v);
                }
            }
        }

        Ok(chunk)
    }

    /// BRC-40 `processSyncChunk`: apply a received chunk to this user's data
    /// (upsert by natural key; newer-wins on `updated_at`). Returns counts +
    /// `done=true` when the chunk carries no entity rows beyond the user record
    /// (the loop-termination signal the wallet's orchestrator waits for).
    #[allow(dead_code)] // superseded by storage::sync_apply (F4 batched path); kept for reference until F4 is verified in prod.
    async fn process_sync_chunk_legacy(
        &self,
        user_id: i64,
        args: RequestSyncChunkArgs,
        chunk: SyncChunk,
    ) -> Result<ProcessSyncChunkResult> {
        let mut result = ProcessSyncChunkResult {
            done: false,
            max_updated_at: None,
            updates: 0,
            inserts: 0,
            error: None,
        };

        if chunk.user_identity_key != args.identity_key {
            return Err(Error::ValidationError(format!(
                "processSyncChunk: chunk user identity key {} does not match args identity key {}",
                chunk.user_identity_key, args.identity_key
            )));
        }

        // Fresh per-call foreign→local id maps (canonical server-side semantics:
        // StorageSqlx::process_sync_chunk also uses a fresh SyncMap per call).
        let mut basket_id_map: HashMap<i64, i64> = HashMap::new();
        let mut label_id_map: HashMap<i64, i64> = HashMap::new();
        let mut tag_id_map: HashMap<i64, i64> = HashMap::new();
        let mut transaction_id_map: HashMap<i64, i64> = HashMap::new();
        let mut output_id_map: HashMap<i64, i64> = HashMap::new();
        let mut certificate_id_map: HashMap<i64, i64> = HashMap::new();

        let chunk_is_empty = vec_is_empty(&chunk.output_baskets)
            && vec_is_empty(&chunk.proven_txs)
            && vec_is_empty(&chunk.proven_tx_reqs)
            && vec_is_empty(&chunk.transactions)
            && vec_is_empty(&chunk.outputs)
            && vec_is_empty(&chunk.tx_labels)
            && vec_is_empty(&chunk.tx_label_maps)
            && vec_is_empty(&chunk.output_tags)
            && vec_is_empty(&chunk.output_tag_maps)
            && vec_is_empty(&chunk.certificates)
            && vec_is_empty(&chunk.certificate_fields)
            && vec_is_empty(&chunk.commissions);

        // Merge the user record if present (active_storage, newer-wins).
        if let Some(ref chunk_user) = chunk.user {
            if self.merge_user(user_id, chunk_user).await? {
                result.updates += 1;
            }
            update_max_updated_at(&mut result.max_updated_at, chunk_user.updated_at);
        }

        if chunk_is_empty {
            result.done = true;
            return Ok(result);
        }

        if let Some(baskets) = &chunk.output_baskets {
            for b in baskets {
                let r = self.upsert_basket(user_id, b).await?;
                basket_id_map.insert(b.basket_id, r.local_id);
                if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                update_max_updated_at(&mut result.max_updated_at, b.updated_at);
            }
        }
        if let Some(reqs) = &chunk.proven_tx_reqs {
            for req in reqs {
                let r = self.upsert_proven_tx_req(req).await?;
                if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                update_max_updated_at(&mut result.max_updated_at, req.updated_at);
            }
        }
        if let Some(ptxs) = &chunk.proven_txs {
            for p in ptxs {
                let r = self.upsert_proven_tx(p).await?;
                if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                update_max_updated_at(&mut result.max_updated_at, p.updated_at);
            }
        }
        if let Some(labels) = &chunk.tx_labels {
            for l in labels {
                let r = self.upsert_tx_label(user_id, l).await?;
                label_id_map.insert(l.label_id, r.local_id);
                if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                update_max_updated_at(&mut result.max_updated_at, l.updated_at);
            }
        }
        if let Some(tags) = &chunk.output_tags {
            for t in tags {
                let r = self.upsert_output_tag(user_id, t).await?;
                tag_id_map.insert(t.tag_id, r.local_id);
                if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                update_max_updated_at(&mut result.max_updated_at, t.updated_at);
            }
        }
        if let Some(txs) = &chunk.transactions {
            for tx in txs {
                let r = self.upsert_transaction(user_id, tx).await?;
                transaction_id_map.insert(tx.transaction_id, r.local_id);
                if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                update_max_updated_at(&mut result.max_updated_at, tx.updated_at);
            }
        }
        if let Some(outputs) = &chunk.outputs {
            // F9: resolve a basket by NAME when the per-chunk id-map misses (see
            // process_sync_chunk for the full rationale — the stateless apply
            // otherwise stored outputs after the basket chunk basket-less).
            let basket_name_map = self.load_basket_name_map(user_id).await?;
            for o in outputs {
                let local_tx_id = transaction_id_map.get(&o.transaction_id).copied();
                let local_basket_id = o
                    .basket_id
                    .and_then(|bid| basket_id_map.get(&bid).copied())
                    .or_else(|| {
                        o.basket_name
                            .as_deref()
                            .and_then(|n| basket_name_map.get(n).copied())
                    });
                let r = self
                    .upsert_output(user_id, o, local_tx_id, local_basket_id)
                    .await?;
                output_id_map.insert(o.output_id, r.local_id);
                if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                update_max_updated_at(&mut result.max_updated_at, o.updated_at);
            }
        }
        if let Some(maps) = &chunk.tx_label_maps {
            for m in maps {
                let local_tx = transaction_id_map.get(&m.transaction_id).copied();
                let local_label = label_id_map.get(&m.label_id).copied();
                if let (Some(tx_id), Some(label_id)) = (local_tx, local_label) {
                    let r = self.upsert_tx_label_map(tx_id, label_id, m.updated_at, m.created_at).await?;
                    if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                    update_max_updated_at(&mut result.max_updated_at, m.updated_at);
                }
            }
        }
        if let Some(maps) = &chunk.output_tag_maps {
            for m in maps {
                let local_out = output_id_map.get(&m.output_id).copied();
                let local_tag = tag_id_map.get(&m.tag_id).copied();
                if let (Some(output_id), Some(tag_id)) = (local_out, local_tag) {
                    let r = self.upsert_output_tag_map(output_id, tag_id, m.updated_at, m.created_at).await?;
                    if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                    update_max_updated_at(&mut result.max_updated_at, m.updated_at);
                }
            }
        }
        if let Some(certs) = &chunk.certificates {
            for c in certs {
                let r = self.upsert_certificate(user_id, c).await?;
                certificate_id_map.insert(c.certificate_id, r.local_id);
                if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                update_max_updated_at(&mut result.max_updated_at, c.updated_at);
            }
        }
        if let Some(fields) = &chunk.certificate_fields {
            for f in fields {
                if let Some(cert_id) = certificate_id_map.get(&f.certificate_id).copied() {
                    let r = self.upsert_certificate_field(user_id, f, cert_id).await?;
                    if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                    update_max_updated_at(&mut result.max_updated_at, f.updated_at);
                }
            }
        }
        if let Some(comms) = &chunk.commissions {
            for c in comms {
                if let Some(tx_id) = transaction_id_map.get(&c.transaction_id).copied() {
                    let r = self.upsert_commission(user_id, c, tx_id).await?;
                    if r.is_new { result.inserts += 1 } else { result.updates += 1 }
                    update_max_updated_at(&mut result.max_updated_at, c.updated_at);
                }
            }
        }

        Ok(result)
    }

    // =========================================================================
    // Fetch helpers (get_sync_chunk)
    // =========================================================================

    async fn fetch_baskets_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableOutputBasket>> {
        let mut sql = String::from(
            "SELECT basket_id, user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, \
             created_at, updated_at FROM output_baskets WHERE user_id = ? AND is_deleted = 0",
        );
        if since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }
        sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<BasketSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        Ok(rows
            .into_iter()
            .map(|r| TableOutputBasket {
                basket_id: r.basket_id.map(|v| v as i64).unwrap_or(0),
                user_id: r.user_id.map(|v| v as i64).unwrap_or(user_id),
                name: r.name.unwrap_or_default(),
                number_of_desired_utxos: r.number_of_desired_utxos.map(|v| v as i32).unwrap_or(6),
                minimum_desired_utxo_value: r.minimum_desired_utxo_value.map(|v| v as i64).unwrap_or(10000),
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            })
            .collect())
    }

    async fn fetch_proven_txs_for_sync(
        &self,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableProvenTx>> {
        let mut sql = String::from(
            "SELECT proven_tx_id, txid, height, idx, block_hash, merkle_root, \
             hex(merkle_path) AS merkle_path_hex, hex(raw_tx) AS raw_tx_hex, created_at, updated_at \
             FROM proven_txs WHERE 1=1",
        );
        if since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }
        sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<ProvenTxSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let id = r.proven_tx_id.map(|v| v as i64).unwrap_or(0);
            let merkle_path = decode_blob_with_r2(self.blobs, "proven_txs", id, "merkle_path", r.merkle_path_hex.as_deref())
                .await?
                .unwrap_or_default();
            let raw_tx = decode_blob_with_r2(self.blobs, "proven_txs", id, "raw_tx", r.raw_tx_hex.as_deref())
                .await?
                .unwrap_or_default();
            out.push(TableProvenTx {
                proven_tx_id: id,
                txid: r.txid.unwrap_or_default(),
                height: r.height.map(|v| v as i64).unwrap_or(0),
                index: r.idx.map(|v| v as i64).unwrap_or(0),
                block_hash: r.block_hash.unwrap_or_default(),
                merkle_root: r.merkle_root.unwrap_or_default(),
                merkle_path,
                raw_tx,
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            });
        }
        Ok(out)
    }

    async fn fetch_proven_tx_reqs_for_sync(
        &self,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableProvenTxReq>> {
        let mut sql = String::from(
            "SELECT proven_tx_req_id, proven_tx_id, txid, status, attempts, history, notified, notify, \
             hex(raw_tx) AS raw_tx_hex, hex(input_beef) AS input_beef_hex, batch, created_at, updated_at \
             FROM proven_tx_reqs WHERE 1=1",
        );
        if since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }
        sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<ProvenTxReqSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let id = r.proven_tx_req_id.map(|v| v as i64).unwrap_or(0);
            let raw_tx = decode_blob_with_r2(self.blobs, "proven_tx_reqs", id, "raw_tx", r.raw_tx_hex.as_deref()).await?;
            let input_beef = decode_blob_with_r2(self.blobs, "proven_tx_reqs", id, "input_beef", r.input_beef_hex.as_deref()).await?;
            out.push(TableProvenTxReq {
                proven_tx_req_id: id,
                txid: r.txid.unwrap_or_default(),
                status: r.status.as_deref().map(req_status_from_str).unwrap_or_default(),
                attempts: r.attempts.map(|v| v as i32).unwrap_or(0),
                history: r.history.unwrap_or_else(|| "{}".to_string()),
                notified: r.notified.map(|v| v != 0.0).unwrap_or(false),
                notify: r.notify.unwrap_or_else(|| "{}".to_string()),
                raw_tx,
                input_beef,
                proven_tx_id: r.proven_tx_id.map(|v| v as i64),
                batch: r.batch,
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            });
        }
        Ok(out)
    }

    async fn fetch_tx_labels_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableTxLabel>> {
        let mut sql = String::from(
            "SELECT tx_label_id, user_id, label, created_at, updated_at FROM tx_labels \
             WHERE user_id = ? AND is_deleted = 0",
        );
        if since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }
        sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<LabelSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        Ok(rows
            .into_iter()
            .map(|r| TableTxLabel {
                label_id: r.tx_label_id.map(|v| v as i64).unwrap_or(0),
                user_id: r.user_id.map(|v| v as i64).unwrap_or(user_id),
                label: r.label.unwrap_or_default(),
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            })
            .collect())
    }

    async fn fetch_output_tags_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableOutputTag>> {
        let mut sql = String::from(
            "SELECT output_tag_id, user_id, tag, created_at, updated_at FROM output_tags \
             WHERE user_id = ? AND is_deleted = 0",
        );
        if since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }
        sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<TagSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        Ok(rows
            .into_iter()
            .map(|r| TableOutputTag {
                tag_id: r.output_tag_id.map(|v| v as i64).unwrap_or(0),
                user_id: r.user_id.map(|v| v as i64).unwrap_or(user_id),
                tag: r.tag.unwrap_or_default(),
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            })
            .collect())
    }

    async fn fetch_transactions_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableTransaction>> {
        // LEFT JOIN proven_txs so the proof linkage round-trips as the wire's
        // `proofTxid` string (canonical fidelity). Columns are qualified with
        // the `t.` alias because both tables carry txid/created_at/updated_at.
        let mut sql = String::from(
            "SELECT t.transaction_id, t.user_id, t.txid, t.status, t.reference, t.description, \
             t.satoshis, t.version, t.lock_time, hex(t.raw_tx) AS raw_tx_hex, \
             hex(t.input_beef) AS input_beef_hex, t.is_outgoing, p.txid AS proof_txid, \
             t.created_at, t.updated_at \
             FROM transactions t LEFT JOIN proven_txs p ON t.proven_tx_id = p.proven_tx_id \
             WHERE t.user_id = ?",
        );
        if since.is_some() {
            sql.push_str(" AND t.updated_at > ?");
        }
        sql.push_str(" ORDER BY t.updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<TransactionSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let id = r.transaction_id.map(|v| v as i64).unwrap_or(0);
            let raw_tx = decode_blob_with_r2(self.blobs, "transactions", id, "raw_tx", r.raw_tx_hex.as_deref()).await?;
            let input_beef = decode_blob_with_r2(self.blobs, "transactions", id, "input_beef", r.input_beef_hex.as_deref()).await?;
            out.push(TableTransaction {
                transaction_id: id,
                user_id: r.user_id.map(|v| v as i64).unwrap_or(user_id),
                txid: r.txid,
                status: r.status.as_deref().map(TransactionStatus::parse_status).unwrap_or_default(),
                reference: r.reference.unwrap_or_default(),
                description: r.description.unwrap_or_default(),
                satoshis: r.satoshis.map(|v| v as i64).unwrap_or(0),
                version: r.version.map(|v| v as i32).unwrap_or(0),
                lock_time: r.lock_time.map(|v| v as i64).unwrap_or(0),
                raw_tx,
                input_beef,
                is_outgoing: r.is_outgoing.map(|v| v != 0.0).unwrap_or(false),
                proof_txid: r.proof_txid,
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            });
        }
        Ok(out)
    }

    /// Resolve a wire `proofTxid` (a proof's txid string) to the LOCAL
    /// `proven_txs.proven_tx_id` FK, or `None` if absent / not present locally.
    /// Same lookup `internalize_action` uses to link a proof to a transaction.
    async fn resolve_proof_fk(&self, proof_txid: Option<&str>) -> Result<Option<i64>> {
        let Some(txid) = proof_txid else { return Ok(None) };
        #[derive(Deserialize)]
        struct PtRow {
            proven_tx_id: Option<f64>,
        }
        let row: Option<PtRow> = Query::new("SELECT proven_tx_id FROM proven_txs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(self.db)
            .await?;
        Ok(row.and_then(|r| r.proven_tx_id.map(|v| v as i64)))
    }

    async fn fetch_outputs_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableOutput>> {
        // A2-plus: alias outputs as `o` + LEFT JOIN transactions to carry the
        // spending tx's stable `reference` (spent_by_reference) on each pulled
        // output, so the client engine's FORWARD-ONLY apply can resolve the
        // local spending tx and reflect spent state (the numeric spent_by FK is
        // meaningless across stores).
        let mut sql = String::from(
            "SELECT o.output_id, o.user_id, o.transaction_id, o.basket_id, o.txid, o.vout, o.satoshis, \
             hex(o.locking_script) AS locking_script_hex, o.script_length, o.script_offset, o.type, o.provided_by, \
             o.purpose, o.output_description, o.spent_by, o.sequence_number, o.spending_description, o.spendable, o.change, \
             o.derivation_prefix, o.derivation_suffix, o.sender_identity_key, o.custom_instructions, o.created_at, o.updated_at, \
             st.reference AS spent_by_reference \
             FROM outputs o \
             LEFT JOIN transactions st ON st.transaction_id = o.spent_by AND st.user_id = o.user_id \
             WHERE o.user_id = ?",
        );
        if since.is_some() {
            sql.push_str(" AND o.updated_at > ?");
        }
        sql.push_str(" ORDER BY o.updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<OutputSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let id = r.output_id.map(|v| v as i64).unwrap_or(0);
            let locking_script = decode_blob_with_r2(self.blobs, "outputs", id, "locking_script", r.locking_script_hex.as_deref()).await?;
            out.push(TableOutput {
                output_id: id,
                user_id: r.user_id.map(|v| v as i64).unwrap_or(user_id),
                transaction_id: r.transaction_id.map(|v| v as i64).unwrap_or(0),
                basket_id: r.basket_id.map(|v| v as i64),
                basket_name: None,
                txid: r.txid.unwrap_or_default(),
                vout: r.vout.map(|v| v as i32).unwrap_or(0),
                satoshis: r.satoshis.map(|v| v as i64).unwrap_or(0),
                locking_script,
                script_length: r.script_length.map(|v| v as i32).unwrap_or(0),
                script_offset: r.script_offset.map(|v| v as i32).unwrap_or(0),
                output_type: r.output_type.unwrap_or_default(),
                provided_by: r.provided_by.unwrap_or_default(),
                purpose: r.purpose,
                output_description: r.output_description,
                spent_by: r.spent_by.map(|v| v as i64),
                spent_by_reference: r.spent_by_reference,
                sequence_number: r.sequence_number.map(|v| v as u32),
                spending_description: r.spending_description,
                spendable: r.spendable.map(|v| v != 0.0).unwrap_or(false),
                change: r.change.map(|v| v != 0.0).unwrap_or(false),
                derivation_prefix: r.derivation_prefix,
                derivation_suffix: r.derivation_suffix,
                sender_identity_key: r.sender_identity_key,
                custom_instructions: r.custom_instructions,
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            });
        }
        Ok(out)
    }

    async fn fetch_tx_label_maps_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableTxLabelMap>> {
        let mut sql = String::from(
            "SELECT m.tx_label_map_id, m.transaction_id, m.tx_label_id, m.created_at, m.updated_at \
             FROM tx_labels_map m JOIN transactions t ON m.transaction_id = t.transaction_id \
             WHERE t.user_id = ? AND m.is_deleted = 0",
        );
        if since.is_some() {
            sql.push_str(" AND m.updated_at > ?");
        }
        sql.push_str(" ORDER BY m.updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<TxLabelMapSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        Ok(rows
            .into_iter()
            .map(|r| TableTxLabelMap {
                tx_label_map_id: r.tx_label_map_id.map(|v| v as i64).unwrap_or(0),
                transaction_id: r.transaction_id.map(|v| v as i64).unwrap_or(0),
                label_id: r.tx_label_id.map(|v| v as i64).unwrap_or(0),
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            })
            .collect())
    }

    async fn fetch_output_tag_maps_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableOutputTagMap>> {
        let mut sql = String::from(
            "SELECT m.output_tag_map_id, m.output_id, m.output_tag_id, m.created_at, m.updated_at \
             FROM output_tags_map m JOIN outputs o ON m.output_id = o.output_id \
             WHERE o.user_id = ? AND m.is_deleted = 0",
        );
        if since.is_some() {
            sql.push_str(" AND m.updated_at > ?");
        }
        sql.push_str(" ORDER BY m.updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<OutputTagMapSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        Ok(rows
            .into_iter()
            .map(|r| TableOutputTagMap {
                output_tag_map_id: r.output_tag_map_id.map(|v| v as i64).unwrap_or(0),
                output_id: r.output_id.map(|v| v as i64).unwrap_or(0),
                tag_id: r.output_tag_id.map(|v| v as i64).unwrap_or(0),
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            })
            .collect())
    }

    async fn fetch_certificates_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableCertificate>> {
        let mut sql = String::from(
            "SELECT certificate_id, user_id, type, serial_number, certifier, subject, verifier, \
             revocation_outpoint, signature, created_at, updated_at FROM certificates \
             WHERE user_id = ? AND is_deleted = 0",
        );
        if since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }
        sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<CertificateSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        Ok(rows
            .into_iter()
            .map(|r| TableCertificate {
                certificate_id: r.certificate_id.map(|v| v as i64).unwrap_or(0),
                user_id: r.user_id.map(|v| v as i64).unwrap_or(user_id),
                cert_type: r.cert_type.unwrap_or_default(),
                serial_number: r.serial_number.unwrap_or_default(),
                certifier: r.certifier.unwrap_or_default(),
                subject: r.subject.unwrap_or_default(),
                verifier: r.verifier,
                revocation_outpoint: r.revocation_outpoint.unwrap_or_default(),
                signature: r.signature.unwrap_or_default(),
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            })
            .collect())
    }

    async fn fetch_certificate_fields_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableCertificateField>> {
        let mut sql = String::from(
            "SELECT certificate_field_id, certificate_id, user_id, field_name, field_value, master_key, \
             created_at, updated_at FROM certificate_fields WHERE user_id = ?",
        );
        if since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }
        sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<CertificateFieldSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        Ok(rows
            .into_iter()
            .map(|r| TableCertificateField {
                certificate_field_id: r.certificate_field_id.map(|v| v as i64).unwrap_or(0),
                certificate_id: r.certificate_id.map(|v| v as i64).unwrap_or(0),
                user_id: r.user_id.map(|v| v as i64).unwrap_or(user_id),
                field_name: r.field_name.unwrap_or_default(),
                field_value: r.field_value.unwrap_or_default(),
                master_key: r.master_key.unwrap_or_default(),
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            })
            .collect())
    }

    async fn fetch_commissions_for_sync(
        &self,
        user_id: i64,
        since: Option<DateTime<Utc>>,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TableCommission>> {
        let mut sql = String::from(
            "SELECT commission_id, user_id, transaction_id, satoshis, key_offset, is_redeemed, \
             hex(locking_script) AS locking_script_hex, created_at, updated_at FROM commissions \
             WHERE user_id = ?",
        );
        if since.is_some() {
            sql.push_str(" AND updated_at > ?");
        }
        sql.push_str(" ORDER BY updated_at ASC LIMIT ? OFFSET ?");
        let mut q = Query::new(sql).bind(user_id);
        if let Some(s) = since {
            q = q.bind(s);
        }
        let rows: Vec<CommissionSyncRow> = q.bind(limit as i64).bind(offset as i64).fetch_all(self.db).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let id = r.commission_id.map(|v| v as i64).unwrap_or(0);
            let locking_script = decode_blob_with_r2(self.blobs, "commissions", id, "locking_script", r.locking_script_hex.as_deref())
                .await?
                .unwrap_or_default();
            out.push(TableCommission {
                commission_id: id,
                user_id: r.user_id.map(|v| v as i64).unwrap_or(user_id),
                transaction_id: r.transaction_id.map(|v| v as i64).unwrap_or(0),
                satoshis: r.satoshis.map(|v| v as i64).unwrap_or(0),
                payer_locking_script: locking_script,
                key_offset: r.key_offset.unwrap_or_default(),
                is_redeemed: r.is_redeemed.map(|v| v != 0.0).unwrap_or(false),
                created_at: parse_datetime_pub(&r.created_at),
                updated_at: parse_datetime_pub(&r.updated_at),
            });
        }
        Ok(out)
    }

    // =========================================================================
    // Upsert helpers (process_sync_chunk) — newer-wins by natural key
    // =========================================================================

    pub(crate) async fn merge_user(&self, user_id: i64, chunk_user: &TableUser) -> Result<bool> {
        let current: Option<UserMergeRow> =
            Query::new("SELECT updated_at FROM users WHERE user_id = ?")
                .bind(user_id)
                .fetch_optional(self.db)
                .await?;
        let local_updated = current
            .and_then(|r| r.updated_at)
            .map(|s| parse_datetime_pub(&Some(s)));
        let is_newer = local_updated.map(|lu| chunk_user.updated_at > lu).unwrap_or(true);
        if is_newer {
            if let Some(ref active) = chunk_user.active_storage {
                Query::new("UPDATE users SET active_storage = ?, updated_at = ? WHERE user_id = ?")
                    .bind(active.as_str())
                    .bind(chunk_user.updated_at)
                    .bind(user_id)
                    .execute(self.db)
                    .await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn upsert_basket(&self, user_id: i64, b: &TableOutputBasket) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> = Query::new(
            "SELECT basket_id AS id, updated_at FROM output_baskets WHERE user_id = ? AND name = ?",
        )
        .bind(user_id)
        .bind(b.name.as_str())
        .fetch_optional(self.db)
        .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if b.updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE output_baskets SET number_of_desired_utxos = ?, minimum_desired_utxo_value = ?, updated_at = ? WHERE basket_id = ?")
                    .bind(b.number_of_desired_utxos as i64)
                    .bind(b.minimum_desired_utxo_value)
                    .bind(b.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            let meta = Query::new("INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)")
                .bind(user_id)
                .bind(b.name.as_str())
                .bind(b.number_of_desired_utxos as i64)
                .bind(b.minimum_desired_utxo_value)
                .bind(b.created_at)
                .bind(b.updated_at)
                .execute(self.db)
                .await?;
            Ok(UpsertResult { local_id: meta.last_row_id, is_new: true })
        }
    }

    async fn upsert_proven_tx(&self, p: &TableProvenTx) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> =
            Query::new("SELECT proven_tx_id AS id, updated_at FROM proven_txs WHERE txid = ?")
                .bind(p.txid.as_str())
                .fetch_optional(self.db)
                .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if p.updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE proven_txs SET height = ?, idx = ?, block_hash = ?, merkle_root = ?, merkle_path = ?, raw_tx = ?, updated_at = ? WHERE proven_tx_id = ?")
                    .bind(p.height)
                    .bind(p.index)
                    .bind(p.block_hash.as_str())
                    .bind(p.merkle_root.as_str())
                    .bind(p.merkle_path.as_slice())
                    .bind(p.raw_tx.as_slice())
                    .bind(p.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            // merkle_path + raw_tx are NOT NULL — bind inline (single-phase).
            let meta = Query::new("INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(p.txid.as_str())
                .bind(p.height)
                .bind(p.index)
                .bind(p.block_hash.as_str())
                .bind(p.merkle_root.as_str())
                .bind(p.merkle_path.as_slice())
                .bind(p.raw_tx.as_slice())
                .bind(p.created_at)
                .bind(p.updated_at)
                .execute(self.db)
                .await?;
            Ok(UpsertResult { local_id: meta.last_row_id, is_new: true })
        }
    }

    async fn upsert_proven_tx_req(&self, req: &TableProvenTxReq) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> =
            Query::new("SELECT proven_tx_req_id AS id, updated_at FROM proven_tx_reqs WHERE txid = ?")
                .bind(req.txid.as_str())
                .fetch_optional(self.db)
                .await?;
        let status = req.status.as_str();
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if req.updated_at > parse_datetime_pub(&row.updated_at) {
                // raw_tx is BLOB NOT NULL — canonical process_action can
                // mutate it post-creation (the wire only carries the final
                // signed payload; an existing local row may pre-date the
                // signed form). Refresh it on UPDATE too so a newer-wins
                // replacement carries the right bytes. Coalesce a missing
                // raw_tx to empty (real reqs always carry it).
                let raw_tx_update = req.raw_tx.clone().unwrap_or_default();
                Query::new("UPDATE proven_tx_reqs SET status = ?, attempts = ?, history = ?, notified = ?, notify = ?, raw_tx = ?, proven_tx_id = ?, batch = ?, updated_at = ? WHERE proven_tx_req_id = ?")
                    .bind(status)
                    .bind(req.attempts as i64)
                    .bind(req.history.as_str())
                    .bind(if req.notified { 1i64 } else { 0 })
                    .bind(req.notify.as_str())
                    .bind(raw_tx_update.as_slice())
                    .bind(req.proven_tx_id)
                    .bind(req.batch.clone())
                    .bind(req.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
                // input_beef (nullable, possibly large) via two-phase R2.
                self.put_blob_column("proven_tx_reqs", local_id, "input_beef", req.input_beef.as_deref(), req.updated_at, true).await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            // raw_tx is NOT NULL — bind inline; input_beef two-phase.
            let raw_tx = req.raw_tx.clone().unwrap_or_default();
            let meta = Query::new("INSERT INTO proven_tx_reqs (txid, status, attempts, history, notify, notified, raw_tx, input_beef, proven_tx_id, batch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)")
                .bind(req.txid.as_str())
                .bind(status)
                .bind(req.attempts as i64)
                .bind(req.history.as_str())
                .bind(req.notify.as_str())
                .bind(if req.notified { 1i64 } else { 0 })
                .bind(raw_tx.as_slice())
                .bind(req.proven_tx_id)
                .bind(req.batch.clone())
                .bind(req.created_at)
                .bind(req.updated_at)
                .execute(self.db)
                .await?;
            let local_id = meta.last_row_id;
            self.put_blob_column("proven_tx_reqs", local_id, "input_beef", req.input_beef.as_deref(), req.updated_at, true).await?;
            Ok(UpsertResult { local_id, is_new: true })
        }
    }

    async fn upsert_tx_label(&self, user_id: i64, l: &TableTxLabel) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> =
            Query::new("SELECT tx_label_id AS id, updated_at FROM tx_labels WHERE user_id = ? AND label = ?")
                .bind(user_id)
                .bind(l.label.as_str())
                .fetch_optional(self.db)
                .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if l.updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE tx_labels SET updated_at = ? WHERE tx_label_id = ?")
                    .bind(l.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            let meta = Query::new("INSERT INTO tx_labels (user_id, label, created_at, updated_at) VALUES (?, ?, ?, ?)")
                .bind(user_id)
                .bind(l.label.as_str())
                .bind(l.created_at)
                .bind(l.updated_at)
                .execute(self.db)
                .await?;
            Ok(UpsertResult { local_id: meta.last_row_id, is_new: true })
        }
    }

    async fn upsert_output_tag(&self, user_id: i64, t: &TableOutputTag) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> =
            Query::new("SELECT output_tag_id AS id, updated_at FROM output_tags WHERE user_id = ? AND tag = ?")
                .bind(user_id)
                .bind(t.tag.as_str())
                .fetch_optional(self.db)
                .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if t.updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE output_tags SET updated_at = ? WHERE output_tag_id = ?")
                    .bind(t.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            let meta = Query::new("INSERT INTO output_tags (user_id, tag, created_at, updated_at) VALUES (?, ?, ?, ?)")
                .bind(user_id)
                .bind(t.tag.as_str())
                .bind(t.created_at)
                .bind(t.updated_at)
                .execute(self.db)
                .await?;
            Ok(UpsertResult { local_id: meta.last_row_id, is_new: true })
        }
    }

    async fn upsert_transaction(&self, user_id: i64, tx: &TableTransaction) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> =
            Query::new("SELECT transaction_id AS id, updated_at FROM transactions WHERE user_id = ? AND reference = ?")
                .bind(user_id)
                .bind(tx.reference.as_str())
                .fetch_optional(self.db)
                .await?;
        let status = tx.status.as_str();
        // Resolve the wire proofTxid → local proven_txs FK (proven_txs sync
        // BEFORE transactions in process_sync_chunk, so the link is present).
        let proof_fk = self.resolve_proof_fk(tx.proof_txid.as_deref()).await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if tx.updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE transactions SET txid = ?, status = ?, description = ?, satoshis = ?, version = ?, lock_time = ?, is_outgoing = ?, updated_at = ? WHERE transaction_id = ?")
                    .bind(tx.txid.clone())
                    .bind(status)
                    .bind(tx.description.as_str())
                    .bind(tx.satoshis)
                    .bind(tx.version as i64)
                    .bind(tx.lock_time)
                    .bind(if tx.is_outgoing { 1i64 } else { 0 })
                    .bind(tx.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
                self.put_blob_column("transactions", local_id, "raw_tx", tx.raw_tx.as_deref(), tx.updated_at, true).await?;
                self.put_blob_column("transactions", local_id, "input_beef", tx.input_beef.as_deref(), tx.updated_at, true).await?;
                // Link the proof FK only when resolvable — never NULL out an
                // existing local link (matches internalize_action's link step).
                if let Some(fk) = proof_fk {
                    Query::new("UPDATE transactions SET proven_tx_id = ? WHERE transaction_id = ?")
                        .bind(fk)
                        .bind(local_id)
                        .execute(self.db)
                        .await?;
                }
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            let meta = Query::new("INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, is_outgoing, proven_tx_id, raw_tx, input_beef, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)")
                .bind(user_id)
                .bind(tx.txid.clone())
                .bind(status)
                .bind(tx.reference.as_str())
                .bind(tx.description.as_str())
                .bind(tx.satoshis)
                .bind(tx.version as i64)
                .bind(tx.lock_time)
                .bind(if tx.is_outgoing { 1i64 } else { 0 })
                .bind(proof_fk)
                .bind(tx.created_at)
                .bind(tx.updated_at)
                .execute(self.db)
                .await?;
            let local_id = meta.last_row_id;
            self.put_blob_column("transactions", local_id, "raw_tx", tx.raw_tx.as_deref(), tx.updated_at, false).await?;
            self.put_blob_column("transactions", local_id, "input_beef", tx.input_beef.as_deref(), tx.updated_at, false).await?;
            Ok(UpsertResult { local_id, is_new: true })
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn upsert_output(
        &self,
        user_id: i64,
        o: &TableOutput,
        local_tx_id: Option<i64>,
        local_basket_id: Option<i64>,
    ) -> Result<UpsertResult> {
        let tx_id = local_tx_id.unwrap_or(o.transaction_id);
        let existing: Option<ExistsRow> =
            Query::new("SELECT output_id AS id, updated_at FROM outputs WHERE user_id = ? AND txid = ? AND vout = ?")
                .bind(user_id)
                .bind(o.txid.as_str())
                .bind(o.vout as i64)
                .fetch_optional(self.db)
                .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if o.updated_at > parse_datetime_pub(&row.updated_at) {
                // FUNDS-SAFE PULL GUARD (2026-06-22, Codex 23bf18dd) — mirror of
                // the wasm client guard. A push must be funds-monotonic on the
                // backup: never demote an existing output's fundability
                // (spendable 1→0, change 1→0, basket out of `default`) nor
                // overwrite a populated immutable scalar; only ADD/PROMOTE. Each
                // CASE reads the PRE-update row. (locking_script is written via
                // put_blob_column below and is immutable per (txid,vout) — a
                // stale overwrite of the same outpoint is a practical no-op; the
                // client side fully guards the funds-loss-facing direction.)
                Query::new("UPDATE outputs SET transaction_id = ?, basket_id = CASE WHEN basket_id = (SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = 'default') THEN basket_id ELSE ? END, satoshis = CASE WHEN satoshis = 0 THEN ? ELSE satoshis END, script_length = CASE WHEN locking_script IS NULL OR length(locking_script) = 0 THEN ? ELSE script_length END, script_offset = CASE WHEN locking_script IS NULL OR length(locking_script) = 0 THEN ? ELSE script_offset END, type = ?, spendable = CASE WHEN spendable = 1 THEN 1 ELSE ? END, change = CASE WHEN change = 1 THEN 1 ELSE ? END, derivation_prefix = CASE WHEN derivation_prefix IS NULL THEN ? ELSE derivation_prefix END, derivation_suffix = CASE WHEN derivation_suffix IS NULL THEN ? ELSE derivation_suffix END, sender_identity_key = ?, custom_instructions = ?, updated_at = ? WHERE output_id = ?")
                    .bind(tx_id)
                    .bind(user_id)
                    .bind(local_basket_id)
                    .bind(o.satoshis)
                    .bind(o.script_length as i64)
                    .bind(o.script_offset as i64)
                    .bind(o.output_type.as_str())
                    .bind(if o.spendable { 1i64 } else { 0 })
                    .bind(if o.change { 1i64 } else { 0 })
                    .bind(o.derivation_prefix.clone())
                    .bind(o.derivation_suffix.clone())
                    .bind(o.sender_identity_key.clone())
                    .bind(o.custom_instructions.clone())
                    .bind(o.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
                self.put_blob_column("outputs", local_id, "locking_script", o.locking_script.as_deref(), o.updated_at, true).await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            let purpose = o.purpose.clone().unwrap_or_else(|| "change".to_string());
            let provided_by = if o.provided_by.is_empty() { "you".to_string() } else { o.provided_by.clone() };
            let meta = Query::new("INSERT INTO outputs (user_id, transaction_id, basket_id, txid, vout, satoshis, locking_script, script_length, script_offset, type, provided_by, purpose, spendable, change, derivation_prefix, derivation_suffix, sender_identity_key, custom_instructions, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(user_id)
                .bind(tx_id)
                .bind(local_basket_id)
                .bind(o.txid.as_str())
                .bind(o.vout as i64)
                .bind(o.satoshis)
                .bind(o.script_length as i64)
                .bind(o.script_offset as i64)
                .bind(o.output_type.as_str())
                .bind(provided_by.as_str())
                .bind(purpose.as_str())
                .bind(if o.spendable { 1i64 } else { 0 })
                .bind(if o.change { 1i64 } else { 0 })
                .bind(o.derivation_prefix.clone())
                .bind(o.derivation_suffix.clone())
                .bind(o.sender_identity_key.clone())
                .bind(o.custom_instructions.clone())
                .bind(o.created_at)
                .bind(o.updated_at)
                .execute(self.db)
                .await?;
            let local_id = meta.last_row_id;
            self.put_blob_column("outputs", local_id, "locking_script", o.locking_script.as_deref(), o.updated_at, false).await?;
            Ok(UpsertResult { local_id, is_new: true })
        }
    }

    async fn upsert_tx_label_map(
        &self,
        tx_id: i64,
        label_id: i64,
        updated_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
    ) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> =
            Query::new("SELECT tx_label_map_id AS id, updated_at FROM tx_labels_map WHERE transaction_id = ? AND tx_label_id = ?")
                .bind(tx_id)
                .bind(label_id)
                .fetch_optional(self.db)
                .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE tx_labels_map SET is_deleted = 0, updated_at = ? WHERE tx_label_map_id = ?")
                    .bind(updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            let meta = Query::new("INSERT INTO tx_labels_map (tx_label_id, transaction_id, created_at, updated_at) VALUES (?, ?, ?, ?)")
                .bind(label_id)
                .bind(tx_id)
                .bind(created_at)
                .bind(updated_at)
                .execute(self.db)
                .await?;
            Ok(UpsertResult { local_id: meta.last_row_id, is_new: true })
        }
    }

    async fn upsert_output_tag_map(
        &self,
        output_id: i64,
        tag_id: i64,
        updated_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
    ) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> =
            Query::new("SELECT output_tag_map_id AS id, updated_at FROM output_tags_map WHERE output_tag_id = ? AND output_id = ?")
                .bind(tag_id)
                .bind(output_id)
                .fetch_optional(self.db)
                .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE output_tags_map SET is_deleted = 0, updated_at = ? WHERE output_tag_map_id = ?")
                    .bind(updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            let meta = Query::new("INSERT INTO output_tags_map (output_tag_id, output_id, created_at, updated_at) VALUES (?, ?, ?, ?)")
                .bind(tag_id)
                .bind(output_id)
                .bind(created_at)
                .bind(updated_at)
                .execute(self.db)
                .await?;
            Ok(UpsertResult { local_id: meta.last_row_id, is_new: true })
        }
    }

    async fn upsert_certificate(&self, user_id: i64, c: &TableCertificate) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> = Query::new(
            "SELECT certificate_id AS id, updated_at FROM certificates WHERE user_id = ? AND type = ? AND certifier = ? AND serial_number = ?",
        )
        .bind(user_id)
        .bind(c.cert_type.as_str())
        .bind(c.certifier.as_str())
        .bind(c.serial_number.as_str())
        .fetch_optional(self.db)
        .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if c.updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE certificates SET subject = ?, verifier = ?, revocation_outpoint = ?, signature = ?, updated_at = ? WHERE certificate_id = ?")
                    .bind(c.subject.as_str())
                    .bind(c.verifier.clone())
                    .bind(c.revocation_outpoint.as_str())
                    .bind(c.signature.as_str())
                    .bind(c.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            let meta = Query::new("INSERT INTO certificates (user_id, serial_number, type, certifier, subject, verifier, revocation_outpoint, signature, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(user_id)
                .bind(c.serial_number.as_str())
                .bind(c.cert_type.as_str())
                .bind(c.certifier.as_str())
                .bind(c.subject.as_str())
                .bind(c.verifier.clone())
                .bind(c.revocation_outpoint.as_str())
                .bind(c.signature.as_str())
                .bind(c.created_at)
                .bind(c.updated_at)
                .execute(self.db)
                .await?;
            Ok(UpsertResult { local_id: meta.last_row_id, is_new: true })
        }
    }

    async fn upsert_certificate_field(
        &self,
        user_id: i64,
        f: &TableCertificateField,
        cert_id: i64,
    ) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> = Query::new(
            "SELECT certificate_field_id AS id, updated_at FROM certificate_fields WHERE certificate_id = ? AND field_name = ?",
        )
        .bind(cert_id)
        .bind(f.field_name.as_str())
        .fetch_optional(self.db)
        .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if f.updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE certificate_fields SET field_value = ?, master_key = ?, updated_at = ? WHERE certificate_field_id = ?")
                    .bind(f.field_value.as_str())
                    .bind(f.master_key.as_str())
                    .bind(f.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            let meta = Query::new("INSERT INTO certificate_fields (user_id, certificate_id, field_name, field_value, master_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
                .bind(user_id)
                .bind(cert_id)
                .bind(f.field_name.as_str())
                .bind(f.field_value.as_str())
                .bind(f.master_key.as_str())
                .bind(f.created_at)
                .bind(f.updated_at)
                .execute(self.db)
                .await?;
            Ok(UpsertResult { local_id: meta.last_row_id, is_new: true })
        }
    }

    async fn upsert_commission(
        &self,
        user_id: i64,
        c: &TableCommission,
        tx_id: i64,
    ) -> Result<UpsertResult> {
        let existing: Option<ExistsRow> =
            Query::new("SELECT commission_id AS id, updated_at FROM commissions WHERE transaction_id = ?")
                .bind(tx_id)
                .fetch_optional(self.db)
                .await?;
        if let Some(row) = existing {
            let local_id = row.id.map(|v| v as i64).unwrap_or(0);
            if c.updated_at > parse_datetime_pub(&row.updated_at) {
                Query::new("UPDATE commissions SET satoshis = ?, key_offset = ?, is_redeemed = ?, locking_script = ?, updated_at = ? WHERE commission_id = ?")
                    .bind(c.satoshis)
                    .bind(c.key_offset.as_str())
                    .bind(if c.is_redeemed { 1i64 } else { 0 })
                    .bind(c.payer_locking_script.as_slice())
                    .bind(c.updated_at)
                    .bind(local_id)
                    .execute(self.db)
                    .await?;
            }
            Ok(UpsertResult { local_id, is_new: false })
        } else {
            // locking_script is NOT NULL — bind inline.
            let meta = Query::new("INSERT INTO commissions (user_id, transaction_id, satoshis, key_offset, is_redeemed, locking_script, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(user_id)
                .bind(tx_id)
                .bind(c.satoshis)
                .bind(c.key_offset.as_str())
                .bind(if c.is_redeemed { 1i64 } else { 0 })
                .bind(c.payer_locking_script.as_slice())
                .bind(c.created_at)
                .bind(c.updated_at)
                .execute(self.db)
                .await?;
            Ok(UpsertResult { local_id: meta.last_row_id, is_new: true })
        }
    }

    /// Store a nullable overflow blob column via the two-phase pattern: small
    /// blobs go inline in D1, large (>4KB) overflow to R2 with the column NULL.
    /// `also_updated` controls whether to also stamp `updated_at` in the UPDATE.
    pub(crate) async fn put_blob_column(
        &self,
        table: &str,
        id: i64,
        column: &str,
        data: Option<&[u8]>,
        updated_at: DateTime<Utc>,
        also_updated: bool,
    ) -> Result<()> {
        let Some(bytes) = data else { return Ok(()) };
        let pk = table_pk_prefix(table);

        // FUNDS-SAFE FILL-IF-EMPTY (2026-06-22, Codex 1df06d5f + re-review) —
        // every blob routed through here (locking_script, raw_tx, input_beef) is
        // IMMUTABLE per row/outpoint. A stale-but-newer push must never poison a
        // populated blob: fetch_*_for_sync rehydrates it into outbound sync
        // chunks, and a fresh restore would then ingest the corruption.
        //
        // We decide populated-ness across BOTH storage backends BEFORE writing,
        // and skip the write entirely (R2 put included) when already populated:
        //   - INLINE (<= r2::THRESHOLD): the value lives in the D1 column;
        //     `hex(col)` returns non-empty.
        //   - R2-BACKED (> THRESHOLD): the D1 column is NULL and the bytes live
        //     in R2 under the DETERMINISTIC key `{table}/{id}/{column}`. A plain
        //     `store.put` would overwrite that object IN PLACE, and the D1
        //     `col IS NULL` guard can't catch it (NULL is the normal R2-backed
        //     state). So we must probe R2 directly.
        // The trailing `AND (col IS NULL OR length(col)=0)` on the D1 UPDATE is
        // kept as belt-and-suspenders against a concurrent fill.
        let already_populated = {
            let hex_row: Option<BlobHexRow> = Query::new(format!(
                "SELECT hex({c}) AS v FROM {t} WHERE {pk}_id = ?",
                t = table,
                c = column,
                pk = pk
            ))
            .bind(id)
            .fetch_optional(self.db)
            .await?;
            match hex_row.and_then(|r| r.v) {
                Some(h) if !h.is_empty() => true, // inline, populated
                _ => crate::r2::BlobStore::new(self.blobs)
                    .exists(table, id, column)
                    .await?, // D1 NULL → probe R2
            }
        };
        if already_populated {
            return Ok(());
        }

        let store = crate::r2::BlobStore::new(self.blobs);
        let (d1_value, _in_r2) = store.put(table, id, column, bytes).await?;
        let sql = if also_updated {
            format!(
                "UPDATE {t} SET {c} = ?, updated_at = ? WHERE {pk}_id = ? \
                 AND ({c} IS NULL OR length({c}) = 0)",
                t = table, c = column, pk = pk
            )
        } else {
            format!(
                "UPDATE {t} SET {c} = ? WHERE {pk}_id = ? \
                 AND ({c} IS NULL OR length({c}) = 0)",
                t = table, c = column, pk = pk
            )
        };
        let mut q = Query::new(sql).bind(d1_value);
        if also_updated {
            q = q.bind(updated_at);
        }
        q.bind(id).execute(self.db).await?;
        Ok(())
    }
}

/// Primary-key column prefix for the tables that carry overflow blobs
/// (`transactions` → `transaction_id`, `outputs` → `output_id`,
/// `proven_tx_reqs` → `proven_tx_req_id`).
fn table_pk_prefix(table: &str) -> &'static str {
    match table {
        "transactions" => "transaction",
        "outputs" => "output",
        "proven_tx_reqs" => "proven_tx_req",
        "proven_txs" => "proven_tx",
        _ => "",
    }
}

/// Lightweight user row for `get_sync_chunk` (snake_case D1 columns).
#[derive(Debug, Deserialize)]
struct UserRowLite {
    user_id: Option<f64>,
    identity_key: Option<String>,
    active_storage: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}
