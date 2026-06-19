//! bsv-wallet-infra-cloudflare: BSV Wallet Storage Server on Cloudflare Workers.
//!
//! Self-hosted replacement for storage.babbage.systems.
//! Backed by D1 (SQLite) + R2 (blob storage).
//!
//! Endpoints:
//! - GET  /                   -> health check (no auth)
//! - POST /.well-known/auth   -> BRC-31 handshake (middleware)
//! - POST /                   -> JSON-RPC 2.0 dispatch (authenticated)

pub mod audit;
pub mod bench;
pub mod d1;
pub mod dispatch;
pub mod entities;
pub mod error;
pub mod json_rpc;
pub mod monitor;
pub mod r2;
pub mod services;
pub mod storage;
pub mod types;

use bsv_auth_cloudflare::{
    add_cors_headers, init_panic_hook,
    middleware::auth::{
        handle_cors_preflight, process_auth_with_storage, sign_json_response,
        AuthMiddlewareOptions, AuthResult,
    },
    storage::D1SessionStorage,
};
use worker::*;

use crate::json_rpc::{JsonRpcError, JsonRpcRequest};
use crate::storage::StorageD1;
use crate::types::AuthId;

#[event(fetch)]
pub async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    init_panic_hook();

    // CORS preflight
    if req.method() == Method::Options {
        return handle_cors_preflight();
    }

    // Health check (no auth)
    if req.path() == "/" && req.method() == Method::Get {
        let response = Response::from_json(&serde_json::json!({
            "status": "ok",
            "service": "wallet-infra"
        }))?;
        return Ok(add_cors_headers(response));
    }

    // Monitor status — aggregate counts only, no PII
    if req.path() == "/monitor/status" && req.method() == Method::Get {
        let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
        let response = Response::from_json(&monitor::get_status(&db).await)?;
        return Ok(add_cors_headers(response));
    }

    // Manual monitor trigger — for debugging. Gated by ?key=<MONITOR_TRIGGER_KEY>
    // so rando HTTP callers can't cause WoC rate-limit bursts. Runs the full
    // run_monitor() pipeline synchronously and returns the MonitorResult as
    // JSON so you can see checked/found/errors without waiting for the
    // next cron cycle.
    if req.path() == "/monitor/run" && req.method() == Method::Post {
        let url = req.url().map_err(|e| Error::from(e.to_string()))?;
        let provided = url
            .query_pairs()
            .find(|(k, _)| k == "key")
            .map(|(_, v)| v.to_string())
            .unwrap_or_default();
        let expected = env
            .secret("MONITOR_TRIGGER_KEY")
            .ok()
            .map(|s| s.to_string())
            .or_else(|| env.var("MONITOR_TRIGGER_KEY").ok().map(|v| v.to_string()))
            .unwrap_or_default();
        if expected.is_empty() || provided != expected {
            let response = Response::from_json(&serde_json::json!({
                "error": "unauthorized — set MONITOR_TRIGGER_KEY secret and pass ?key=<value>"
            }))?
            .with_status(401);
            return Ok(add_cors_headers(response));
        }

        let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
        let blobs = env.bucket("BLOBS").map_err(|e| Error::from(e.to_string()))?;
        let arc_api_key = env
            .secret("ARC_API_KEY")
            .ok()
            .map(|s| s.to_string())
            .or_else(|| env.var("ARC_API_KEY").ok().map(|v| v.to_string()));
        let woc_api_key = env
            .secret("WOC_API_KEY")
            .ok()
            .map(|s| s.to_string())
            .or_else(|| env.var("WOC_API_KEY").ok().map(|v| v.to_string()));
        let chaintracks_url = env
            .var("CHAINTRACKS_URL")
            .ok()
            .map(|v| v.to_string())
            .filter(|s| !s.is_empty());
        let provider = crate::services::multi::MultiProvider::with_chaintracks(
            arc_api_key,
            woc_api_key,
            chaintracks_url,
        );
        let result = monitor::run_monitor(&db, &blobs, &provider, &provider).await;

        let response = Response::from_json(&serde_json::json!({
            "sent": result.sent,
            "send_errors": result.send_errors,
            "proofs_found": result.proofs_found,
            "proofs_checked": result.proofs_checked,
            "abandoned_failed": result.abandoned_failed,
            "status_synced": result.status_synced,
            "beef_compacted": result.beef_compacted,
            "unfail_recovered": result.unfail_recovered,
            "purged": result.purged,
            "nosend_found": result.nosend_found,
            "reorg_detected": result.reorg_detected,
            "reorg_depth": result.reorg_depth,
            "proofs_reverified": result.proofs_reverified,
            "errors": result.errors,
        }))?;
        return Ok(add_cors_headers(response));
    }

    // Debug: raw WoC TSC proof probe — GET /monitor/probe-woc?txid=<txid>&key=<MONITOR_TRIGGER_KEY>
    // Calls WoC exactly as the monitor would and returns the raw status + body.
    if req.path() == "/monitor/probe-woc" && req.method() == Method::Get {
        let url = req.url().map_err(|e| Error::from(e.to_string()))?;
        let provided = url
            .query_pairs()
            .find(|(k, _)| k == "key")
            .map(|(_, v)| v.to_string())
            .unwrap_or_default();
        let expected = env.secret("MONITOR_TRIGGER_KEY").ok().map(|s| s.to_string()).unwrap_or_default();
        if expected.is_empty() || provided != expected {
            return Ok(add_cors_headers(
                Response::from_json(&serde_json::json!({"error":"unauthorized"}))?.with_status(401),
            ));
        }
        let txid = url.query_pairs().find(|(k, _)| k == "txid").map(|(_, v)| v.to_string()).unwrap_or_default();
        if txid.len() != 64 {
            return Ok(add_cors_headers(
                Response::from_json(&serde_json::json!({"error":"txid must be 64-char hex"}))?.with_status(400),
            ));
        }
        let woc_key = env.secret("WOC_API_KEY").ok().map(|s| s.to_string());
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);
        if let Some(ref key) = woc_key {
            let headers = worker::Headers::new();
            let _ = headers.set("woc-api-key", key);
            init.with_headers(headers);
        }
        let url_str = format!("https://api.whatsonchain.com/v1/bsv/main/tx/{}/proof/tsc", txid);
        let request = worker::Request::new_with_init(&url_str, &init).map_err(|e| Error::from(e.to_string()))?;
        let mut response = worker::Fetch::Request(request).send().await.map_err(|e| Error::from(e.to_string()))?;
        let status = response.status_code();
        let body = response.text().await.unwrap_or_default();
        return Ok(add_cors_headers(Response::from_json(&serde_json::json!({
            "url": url_str,
            "has_api_key": woc_key.is_some(),
            "api_key_len": woc_key.as_ref().map(|k| k.len()),
            "status": status,
            "body_len": body.len(),
            "body_preview": &body[..body.len().min(500)],
        }))?));
    }

    // UTXO audit — integrity checks and optional deep validation
    if req.path().starts_with("/monitor/audit") && req.method() == Method::Get {
        let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
        let blobs = env
            .bucket("BLOBS")
            .map_err(|e| Error::from(e.to_string()))?;

        // Parse ?level= query parameter (default: 2)
        let url = req.url().map_err(|e| Error::from(e.to_string()))?;
        let level: u8 = url
            .query_pairs()
            .find(|(k, _)| k == "level")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(2);

        let report = audit::run_audit(&db, &blobs, level).await;
        let response = Response::from_json(&report)?;
        return Ok(add_cors_headers(response));
    }

    // Get server key
    let server_key = env
        .secret("SERVER_PRIVATE_KEY")
        .map_err(|e| Error::from(format!("SERVER_PRIVATE_KEY not set: {}", e)))?
        .to_string();

    // Auth options — all requests require authentication. The 30s debounce
    // on the per-message update_session write is the middleware default —
    // explicit here so the trade-off is documented at the call site.
    let auth_options = AuthMiddlewareOptions {
        server_private_key: server_key,
        allow_unauthenticated: false,
        session_ttl_seconds: 3600,
        session_touch_debounce_seconds: 30,
        ..Default::default()
    };

    // Process auth (handles BRC-31 handshake + session validation). Sessions
    // ride in the same D1 database as the wallet-infra data — no Workers KV
    // namespace required (the `auth_sessions` table lives alongside the
    // existing schema; migration `0002_auth_sessions.sql` provisions it).
    let auth_db = env
        .d1("DB")
        .map_err(|e| Error::from(format!("D1 binding `DB` not bound: {}", e)))?;
    let session_storage = D1SessionStorage::new(&auth_db, auth_options.session_ttl_seconds);
    // Bound the auth phase (body read + D1 session lookup/touch). A stalled D1
    // subrequest here is the captured cpuTime=0 "hung" 1101; the deadline turns
    // it into a clean retryable 503 instead of a Worker hang. See with_deadline.
    let auth_result = match with_deadline(process_auth_with_storage(
        req,
        &session_storage,
        &auth_options,
    ))
    .await
    {
        Some(r) => r.map_err(|e| Error::from(e.to_string()))?,
        None => return Ok(timeout_response()),
    };

    let (auth_context, req, session, request_body) = match auth_result {
        AuthResult::Authenticated {
            context,
            request,
            session,
            body,
        } => (context, request, session, body),
        AuthResult::Response(response) => return Ok(response),
    };

    // Require session for response signing
    let session = match session {
        Some(s) => s,
        None => {
            let resp = Response::from_json(&serde_json::json!({
                "status": "error",
                "code": "ERR_NO_SESSION",
                "description": "Authentication required"
            }))?
            .with_status(401);
            return Ok(add_cors_headers(resp));
        }
    };

    // Only POST / is valid for JSON-RPC
    if req.path() != "/" || req.method() != Method::Post {
        let body = serde_json::json!({
            "status": "error",
            "code": "NOT_FOUND",
            "description": "Unknown endpoint. Use POST / for JSON-RPC."
        });
        return sign_json_response(&body, 404, &[], &session)
            .map_err(|e| Error::from(e.to_string()));
    }

    // Parse JSON-RPC request
    let rpc_request: JsonRpcRequest = match serde_json::from_slice(&request_body) {
        Ok(r) => r,
        Err(_) => {
            let error = JsonRpcError::parse_error();
            return sign_json_response(&error, 200, &[], &session)
                .map_err(|e| Error::from(e.to_string()));
        }
    };

    // Get D1 and R2 bindings
    let db = env.d1("DB").map_err(|e| Error::from(e.to_string()))?;
    let blobs = env
        .bucket("BLOBS")
        .map_err(|e| Error::from(e.to_string()))?;

    // Read ARC API key (optional — if not set, ARC calls will fail and WoC is used as fallback)
    let arc_api_key = env
        .secret("ARC_API_KEY")
        .ok()
        .map(|s| s.to_string())
        .or_else(|| env.var("ARC_API_KEY").ok().map(|v| v.to_string()));

    // Read WoC API key (optional — if set, sent as `woc-api-key` header on all
    // WoC requests to bypass anonymous IP-based rate limiting).
    let woc_api_key = env
        .secret("WOC_API_KEY")
        .ok()
        .map(|s| s.to_string())
        .or_else(|| env.var("WOC_API_KEY").ok().map(|v| v.to_string()));

    // Read BEEF verification mode (default: "strict" — verifies merkle roots via ChainTracks/WoC)
    let beef_mode = env
        .var("BEEF_VERIFICATION")
        .ok()
        .map(|v| crate::types::BeefVerificationMode::from_env_str(&v.to_string()))
        .unwrap_or_default();

    // Read ChainTracks URL (optional — if set, uses ChainTracks with WoC fallback)
    let chaintracks_url = env
        .var("CHAINTRACKS_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty());

    // Build header provider for BEEF verification
    let header_provider = crate::services::chaintracker::build_header_provider(
        chaintracks_url.clone(),
        woc_api_key.clone(),
    );

    // Build broadcast/proof provider (ARC → WoC → Bitails failover; ChainTracks
    // used as the canonical-chain authority for TSC proof filtering)
    let provider = crate::services::multi::MultiProvider::with_chaintracks(
        arc_api_key,
        woc_api_key,
        chaintracks_url,
    );
    let mut storage =
        StorageD1::new(&db, &blobs, &provider).with_beef_verification(beef_mode, &header_provider);

    // Build auth ID from BRC-31 context
    let auth = AuthId::new(&auth_context.identity_key);

    // Dispatch — bounded by the same request deadline so a stalled method-level
    // D1 op also degrades to a clean retryable error rather than a Worker hang.
    let result = match with_deadline(dispatch::dispatch(
        &mut storage,
        &rpc_request.method,
        rpc_request.params,
        rpc_request.id,
        Some(&auth),
    ))
    .await
    {
        Some(r) => r,
        None => return Ok(timeout_response()),
    };

    // Return signed JSON-RPC response
    sign_json_response(&result, 200, &[], &session).map_err(|e| Error::from(e.to_string()))
}

/// Wall-clock ceiling for a single authenticated request's auth + dispatch
/// phases. A stalled D1 subrequest (or any never-resolving await) past this is
/// abandoned and turned into a clean, retryable HTTP error instead of hanging
/// the Worker until Cloudflare kills it as "hung" (error 1101 / HTTP 500). It
/// sits far above normal D1 latency (~50 ms) yet below Cloudflare's hang
/// cutoff, so it only fires on a genuine stall. The pending timer also keeps
/// the event loop non-empty, so CF never trips its "would never generate a
/// response" detector (the exact signature captured on the live tail).
const REQUEST_DEADLINE_SECS: u64 = 12;

/// Race `fut` against the request deadline. Returns `Some(out)` if it finished
/// in time, `None` if the deadline fired first (the stalled future is dropped).
async fn with_deadline<F: std::future::Future>(fut: F) -> Option<F::Output> {
    let timeout = async {
        Delay::from(std::time::Duration::from_secs(REQUEST_DEADLINE_SECS)).await;
    };
    futures_util::pin_mut!(fut);
    futures_util::pin_mut!(timeout);
    match futures_util::future::select(fut, timeout).await {
        futures_util::future::Either::Left((out, _)) => Some(out),
        futures_util::future::Either::Right(_) => None,
    }
}

/// Transient error returned when a request exceeds the deadline. Mirrors the
/// plain (unsigned) HTTP error the canonical auth-express-middleware returns on
/// its failure paths (401/500), which the wallet's AuthFetch transport retries.
/// HTTP 503 signals "transient — retry".
fn timeout_response() -> Response {
    let body = serde_json::json!({
        "status": "error",
        "code": "ERR_TIMEOUT",
        "description": "Storage request timed out; please retry."
    });
    match Response::from_json(&body) {
        Ok(resp) => add_cors_headers(resp.with_status(503)),
        Err(_) => Response::error("timeout", 503)
            .unwrap_or_else(|_| Response::empty().unwrap()),
    }
}

#[event(scheduled)]
pub async fn scheduled(_event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    let db = match env.d1("DB") {
        Ok(db) => db,
        Err(e) => {
            console_error!("Monitor: failed to get DB binding: {}", e);
            return;
        }
    };
    let blobs = match env.bucket("BLOBS") {
        Ok(b) => b,
        Err(e) => {
            console_error!("Monitor: failed to get BLOBS binding: {}", e);
            return;
        }
    };

    let arc_api_key = env
        .secret("ARC_API_KEY")
        .ok()
        .map(|s| s.to_string())
        .or_else(|| env.var("ARC_API_KEY").ok().map(|v| v.to_string()));
    let woc_api_key = env
        .secret("WOC_API_KEY")
        .ok()
        .map(|s| s.to_string())
        .or_else(|| env.var("WOC_API_KEY").ok().map(|v| v.to_string()));
    let chaintracks_url = env
        .var("CHAINTRACKS_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty());
    let provider = crate::services::multi::MultiProvider::with_chaintracks(
        arc_api_key,
        woc_api_key,
        chaintracks_url,
    );
    let result = monitor::run_monitor(&db, &blobs, &provider, &provider).await;

    console_log!(
        "Monitor: {} sent, {} send errors, {} proofs found, {} checked, {} abandoned failed, {} status synced, {} beef compacted, {} unfail recovered, {} purged, {} nosend found, reorg={} depth={} reverified={}, {} errors",
        result.sent,
        result.send_errors,
        result.proofs_found,
        result.proofs_checked,
        result.abandoned_failed,
        result.status_synced,
        result.beef_compacted,
        result.unfail_recovered,
        result.purged,
        result.nosend_found,
        result.reorg_detected,
        result.reorg_depth,
        result.proofs_reverified,
        result.errors.len()
    );
    for err in &result.errors {
        console_error!("Monitor error: {}", err);
    }
}
