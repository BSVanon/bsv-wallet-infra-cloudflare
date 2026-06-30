//! F4 — batched `process_sync_chunk`.
//!
//! D1 has no interactive transaction, only `batch()`. The original port
//! (sync.rs) reproduced the canonical merge LOGIC but executed it as ~2N
//! sequential round-trips + per-blob R2 ops, which on a large chunk exceeds CF's
//! 1000-subrequest-per-invocation budget → HTTP 503 "Too many API requests by a
//! single Worker invocation" (proven live 2026-06-29: one output-heavy chunk
//! made ~1500 sequential D1/R2 calls). This module restores canonical's "one
//! atomic unit" using D1
//! `batch()`: each entity is an `INSERT … ON CONFLICT(natural_key) DO UPDATE …
//! RETURNING <pk>` collected into phase batches, in the SAME dependency order
//! as the engine (`bsv-wallet-toolbox-rs/src/storage/sqlx/sync.rs`).
//!
//! Funds-safety (proven offline against SQLite — see tests/f4_*_proof.py):
//!  - newer-wins encoded per-column as CASE WHEN excluded.updated_at >
//!    <table>.updated_at THEN excluded.col ELSE <table>.col END (NOT a WHERE on
//!    the conflict — so RETURNING ALWAYS yields the pk, even on a no-op, and the
//!    SyncMap never misses a parent id);
//!  - the outputs monotonic guard (never demote spendable/change, default-basket
//!    pin, fill-if-empty) is translated 1:1 from the original UPDATE at
//!    sync.rs:1499;
//!  - the never-NULL proof-link guard on transactions is preserved;
//!  - F7-2 blob handling: INLINE blobs (<= r2 THRESHOLD — the common case, e.g.
//!    P2PKH locking_script) are FOLDED into the batched upsert via a fill-if-empty
//!    + newer-wins CASE (`nw_blob`/`inline_blob_bind`), so an output-heavy chunk
//!    costs O(1) subrequests instead of ~3/row (the 503). Only OVER-threshold
//!    blobs still use `put_blob_column` (R2 put is an irreducible subrequest), and
//!    those are bounded per chunk by the producer's rough-size cap. The inline
//!    CASE preserves the same fail-closed immutability as `put_blob_column`
//!    (proven in tests/f7_inline_blob_proof.py; routing in f7_routing_tests).

use std::collections::HashMap;

use serde::Deserialize;
use worker::D1Result;

use crate::d1::batch::BatchCollector;
use crate::d1::Query;
use crate::error::{Error, Result};
use crate::storage::StorageD1;
use crate::types::{ProcessSyncChunkResult, RequestSyncChunkArgs, SyncChunk};

/// A `RETURNING <pk> AS id` row.
#[derive(Debug, Deserialize)]
struct IdRow {
    id: Option<f64>,
}

impl<'a, B: crate::services::BroadcastService + crate::services::ProofService> StorageD1<'a, B> {
    /// BRC-40 `processSyncChunk` — batched. Same merge semantics as the
    /// canonical sequential path, executed as phase batches.
    pub async fn process_sync_chunk(
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

        // Foreign→local id maps (the SyncMap), fresh per call.
        let mut basket_id_map: HashMap<i64, i64> = HashMap::new();
        let mut label_id_map: HashMap<i64, i64> = HashMap::new();
        let mut tag_id_map: HashMap<i64, i64> = HashMap::new();
        let mut transaction_id_map: HashMap<i64, i64> = HashMap::new();
        let mut output_id_map: HashMap<i64, i64> = HashMap::new();
        let mut certificate_id_map: HashMap<i64, i64> = HashMap::new();

        let chunk_is_empty = chunk.output_baskets.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.proven_txs.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.proven_tx_reqs.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.transactions.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.outputs.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.tx_labels.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.tx_label_maps.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.output_tags.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.output_tag_maps.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.certificates.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.certificate_fields.as_ref().as_ref().map_or(true, |v| v.is_empty())
            && chunk.commissions.as_ref().as_ref().map_or(true, |v| v.is_empty());

        // User record (active_storage, newer-wins) — single row, runs first.
        if let Some(ref chunk_user) = chunk.user {
            if self.merge_user(user_id, chunk_user).await? {
                result.updates += 1;
            }
            update_max(&mut result.max_updated_at, chunk_user.updated_at);
        }

        if chunk_is_empty {
            result.done = true;
            return Ok(result);
        }

        // ── PHASE 1: independent parents (no cross-FK among them) ────────────
        // baskets, proven_tx_reqs, proven_txs, tx_labels, output_tags, certificates.
        if let Some(baskets) = &chunk.output_baskets {
            let ids = self
                .run_upserts(baskets.iter().map(|b| build_upsert_basket(user_id, b)).collect())
                .await?;
            for (b, id) in baskets.iter().zip(ids) {
                basket_id_map.insert(b.basket_id, id);
                update_max(&mut result.max_updated_at, b.updated_at);
                result.inserts += 1;
            }
        }
        if let Some(reqs) = &chunk.proven_tx_reqs {
            let ids = self
                .run_upserts(reqs.iter().map(build_upsert_proven_tx_req).collect())
                .await?;
            // input_beef blob via the proven put_blob_column (fill-if-empty).
            for (req, id) in reqs.iter().zip(ids) {
                if needs_r2_fill(req.input_beef.as_deref()) {
                    self.put_blob_column("proven_tx_reqs", id, "input_beef", req.input_beef.as_deref(), req.updated_at, false).await?;
                }
                update_max(&mut result.max_updated_at, req.updated_at);
                result.inserts += 1;
            }
        }
        if let Some(ptxs) = &chunk.proven_txs {
            let n = self
                .run_upserts(ptxs.iter().map(build_upsert_proven_tx).collect())
                .await?
                .len();
            result.inserts += n as u32;
            for p in ptxs {
                update_max(&mut result.max_updated_at, p.updated_at);
            }
        }
        if let Some(labels) = &chunk.tx_labels {
            let ids = self
                .run_upserts(labels.iter().map(|l| build_upsert_tx_label(user_id, l)).collect())
                .await?;
            for (l, id) in labels.iter().zip(ids) {
                label_id_map.insert(l.label_id, id);
                update_max(&mut result.max_updated_at, l.updated_at);
                result.inserts += 1;
            }
        }
        if let Some(tags) = &chunk.output_tags {
            let ids = self
                .run_upserts(tags.iter().map(|t| build_upsert_output_tag(user_id, t)).collect())
                .await?;
            for (t, id) in tags.iter().zip(ids) {
                tag_id_map.insert(t.tag_id, id);
                update_max(&mut result.max_updated_at, t.updated_at);
                result.inserts += 1;
            }
        }
        if let Some(certs) = &chunk.certificates {
            let ids = self
                .run_upserts(certs.iter().map(|c| build_upsert_certificate(user_id, c)).collect())
                .await?;
            for (c, id) in certs.iter().zip(ids) {
                certificate_id_map.insert(c.certificate_id, id);
                update_max(&mut result.max_updated_at, c.updated_at);
                result.inserts += 1;
            }
        }

        // ── PHASE 2: transactions (proof FK resolved from proven_txs map) ────
        if let Some(txs) = &chunk.transactions {
            // proof_fk via LOCAL proven_txs lookup by txid — covers same-chunk
            // proofs (committed in phase 1) AND prior-chunk proofs already in
            // local storage. Matches legacy resolve_proof_fk; a same-chunk-only
            // map would drop the link for a tx whose proof landed in an earlier
            // chunk (Codex f05cc25a).
            let proof_txids: Vec<&str> = txs.iter().filter_map(|tx| tx.proof_txid.as_deref()).collect();
            let proof_map = self
                .load_id_map_by_txid("proven_txs", "proven_tx_id", None, &proof_txids)
                .await?;
            let queries: Vec<Query> = txs
                .iter()
                .map(|tx| {
                    let proof_fk = tx
                        .proof_txid
                        .as_deref()
                        .and_then(|txid| proof_map.get(txid).copied());
                    build_upsert_transaction(user_id, tx, proof_fk)
                })
                .collect();
            let ids = self.run_upserts(queries).await?;
            for (tx, id) in txs.iter().zip(ids) {
                transaction_id_map.insert(tx.transaction_id, id);
                if needs_r2_fill(tx.raw_tx.as_deref()) {
                    self.put_blob_column("transactions", id, "raw_tx", tx.raw_tx.as_deref(), tx.updated_at, false).await?;
                }
                if needs_r2_fill(tx.input_beef.as_deref()) {
                    self.put_blob_column("transactions", id, "input_beef", tx.input_beef.as_deref(), tx.updated_at, false).await?;
                }
                update_max(&mut result.max_updated_at, tx.updated_at);
                result.inserts += 1;
            }
        }

        // ── PHASE 3: outputs (tx + basket FKs) and certificate_fields ────────
        if let Some(outputs) = &chunk.outputs {
            // local_tx_id (engine v1.0.1 3-step): same-chunk SyncMap → LOCAL DB
            // lookup by (user_id, txid) → SKIP orphan. NEVER fall back to the
            // remote foreign id — that collides on UNIQUE(transaction_id,vout,
            // user_id) (Robert's v1.0.1-rc rc=19; Codex f05cc25a).
            let out_txids: Vec<&str> = outputs.iter().map(|o| o.txid.as_str()).collect();
            let txid_map = self
                .load_id_map_by_txid("transactions", "transaction_id", Some(user_id), &out_txids)
                .await?;
            // F9: resolve a basket by NAME when this chunk's per-chunk id-map
            // misses. The apply is STATELESS across chunk POSTs, so an output
            // whose basket rode an EARLIER chunk has a foreign basket_id absent
            // from THIS chunk's map → it was being stored basket-less, and a
            // restored wallet under-counted its change. The basket row already
            // exists in D1, so resolve it by the stable name the producer now
            // carries on each output (basketName).
            let basket_name_map = self.load_basket_name_map(user_id).await?;
            // A2-plus: resolve each output's spending tx by its stable reference
            // (forward-only spent_by) when this chunk's per-chunk transaction map
            // misses — the spending tx may have ridden an earlier chunk / pre-exist.
            let spend_refs: Vec<&str> = outputs
                .iter()
                .filter_map(|o| o.spent_by_reference.as_deref())
                .collect();
            let spent_by_ref_map = self.load_tx_id_by_reference(user_id, &spend_refs).await?;
            let mut included: Vec<&TableOutput> = Vec::new();
            let mut queries: Vec<Query> = Vec::new();
            for o in outputs {
                let local_tx_id = transaction_id_map
                    .get(&o.transaction_id)
                    .copied()
                    .or_else(|| txid_map.get(o.txid.as_str()).copied());
                let Some(tx_id) = local_tx_id else { continue };
                let local_basket_id = o
                    .basket_id
                    .and_then(|bid| basket_id_map.get(&bid).copied())
                    .or_else(|| {
                        o.basket_name
                            .as_deref()
                            .and_then(|n| basket_name_map.get(n).copied())
                    });
                // Foreign spent_by → local via the per-chunk tx map, else the
                // stable spent_by_reference. None → never stamp spent.
                let local_spent_by = o
                    .spent_by
                    .and_then(|sid| transaction_id_map.get(&sid).copied())
                    .or_else(|| {
                        o.spent_by_reference
                            .as_deref()
                            .and_then(|r| spent_by_ref_map.get(r).copied())
                    });
                queries.push(build_upsert_output(user_id, o, tx_id, local_basket_id, local_spent_by));
                included.push(o);
            }
            let ids = self.run_upserts(queries).await?;
            for (o, id) in included.into_iter().zip(ids) {
                output_id_map.insert(o.output_id, id);
                if needs_r2_fill(o.locking_script.as_deref()) {
                    self.put_blob_column("outputs", id, "locking_script", o.locking_script.as_deref(), o.updated_at, false).await?;
                }
                update_max(&mut result.max_updated_at, o.updated_at);
                result.inserts += 1;
            }
        }
        if let Some(fields) = &chunk.certificate_fields {
            let queries: Vec<Query> = fields
                .iter()
                .filter_map(|f| {
                    certificate_id_map
                        .get(&f.certificate_id)
                        .copied()
                        .map(|cert_id| build_upsert_certificate_field(user_id, f, cert_id))
                })
                .collect();
            let n = queries.len();
            self.run_upserts(queries).await?;
            result.inserts += n as u32;
            for f in fields {
                update_max(&mut result.max_updated_at, f.updated_at);
            }
        }

        // ── PHASE 4: join/leaf rows (no children depend on their ids) ────────
        if let Some(maps) = &chunk.tx_label_maps {
            let queries: Vec<Query> = maps
                .iter()
                .filter_map(|m| {
                    let tx = transaction_id_map.get(&m.transaction_id).copied();
                    let label = label_id_map.get(&m.label_id).copied();
                    match (tx, label) {
                        (Some(tx_id), Some(label_id)) => Some(build_upsert_tx_label_map(tx_id, label_id, m.updated_at, m.created_at)),
                        _ => None,
                    }
                })
                .collect();
            let n = queries.len();
            self.run_upserts(queries).await?;
            result.inserts += n as u32;
        }
        if let Some(maps) = &chunk.output_tag_maps {
            let queries: Vec<Query> = maps
                .iter()
                .filter_map(|m| {
                    let out = output_id_map.get(&m.output_id).copied();
                    let tag = tag_id_map.get(&m.tag_id).copied();
                    match (out, tag) {
                        (Some(output_id), Some(tag_id)) => Some(build_upsert_output_tag_map(output_id, tag_id, m.updated_at, m.created_at)),
                        _ => None,
                    }
                })
                .collect();
            let n = queries.len();
            self.run_upserts(queries).await?;
            result.inserts += n as u32;
        }
        if let Some(comms) = &chunk.commissions {
            let queries: Vec<Query> = comms
                .iter()
                .filter_map(|c| {
                    transaction_id_map
                        .get(&c.transaction_id)
                        .copied()
                        .map(|tx_id| build_upsert_commission(user_id, c, tx_id))
                })
                .collect();
            let n = queries.len();
            self.run_upserts(queries).await?;
            result.inserts += n as u32;
        }

        Ok(result)
    }

    /// Execute a batch of `… RETURNING <pk> AS id` upserts and return the local
    /// id for each, in input order. RETURNING (not last_row_id) is correct for
    /// both the insert and the DO-UPDATE path.
    async fn run_upserts(&self, queries: Vec<Query>) -> Result<Vec<i64>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let mut batch = BatchCollector::new(self.db);
        for q in queries {
            let (sql, params) = q.into_parts();
            batch.add(&sql, params)?;
        }
        let results: Vec<D1Result> = batch.execute().await?;
        let mut ids = Vec::with_capacity(results.len());
        for r in &results {
            let rows = r
                .results::<IdRow>()
                .map_err(|e| Error::DatabaseError(e.to_string()))?;
            let id = rows
                .into_iter()
                .next()
                .and_then(|x| x.id)
                .map(|v| v as i64)
                .ok_or_else(|| Error::DatabaseError("upsert RETURNING produced no id".to_string()))?;
            ids.push(id);
        }
        Ok(ids)
    }

    /// Batched `txid → local id` resolver against LOCAL storage, for restoring a
    /// parent FK when the same-chunk SyncMap misses (the parent was synced in a
    /// prior chunk). One IN-list read per ≤500 txids (under the SQLite variable
    /// bound). `user_id = Some(_)` adds the `user_id = ?` filter (transactions);
    /// `None` omits it (proven_txs, txid globally unique).
    async fn load_id_map_by_txid(
        &self,
        table: &str,
        id_col: &str,
        user_id: Option<i64>,
        txids: &[&str],
    ) -> Result<HashMap<String, i64>> {
        #[derive(Deserialize)]
        struct Row {
            k: Option<String>,
            id: Option<f64>,
        }
        let mut map: HashMap<String, i64> = HashMap::new();
        let mut uniq: Vec<&str> = txids.to_vec();
        uniq.sort_unstable();
        uniq.dedup();
        for group in uniq.chunks(500) {
            if group.is_empty() {
                continue;
            }
            let placeholders = group.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let user_clause = if user_id.is_some() { "user_id = ? AND " } else { "" };
            let sql = format!(
                "SELECT txid AS k, {id_col} AS id FROM {table} WHERE {user_clause}txid IN ({placeholders})"
            );
            let mut q = Query::new(sql);
            if let Some(uid) = user_id {
                q = q.bind(uid);
            }
            for t in group {
                q = q.bind(*t);
            }
            let rows: Vec<Row> = q.fetch_all(self.db).await?;
            for r in rows {
                if let (Some(k), Some(id)) = (r.k, r.id) {
                    map.insert(k, id as i64);
                }
            }
        }
        Ok(map)
    }

    /// F9: load the user's output baskets as a name→local_id map. Used by the
    /// stateless chunk-by-chunk apply to resolve an output's basket by name
    /// when its foreign basket_id isn't in the current chunk's id-map.
    pub(crate) async fn load_basket_name_map(&self, user_id: i64) -> Result<HashMap<String, i64>> {
        #[derive(Deserialize)]
        struct Row {
            name: Option<String>,
            id: Option<f64>,
        }
        let mut map: HashMap<String, i64> = HashMap::new();
        let rows: Vec<Row> = Query::new(
            "SELECT name, basket_id AS id FROM output_baskets WHERE user_id = ?".to_string(),
        )
        .bind(user_id)
        .fetch_all(self.db)
        .await?;
        for r in rows {
            if let (Some(name), Some(id)) = (r.name, r.id) {
                map.insert(name, id as i64);
            }
        }
        Ok(map)
    }

    /// A2-plus: load a (transaction `reference` → local transaction_id) map for
    /// the given references — the stable key used to resolve a spent output's
    /// spending tx across chunks (mirrors `load_id_map_by_txid`, keyed by the
    /// `transactions.reference` natural key). Forward-only spent_by relies on it
    /// when the per-chunk transaction id-map misses.
    async fn load_tx_id_by_reference(
        &self,
        user_id: i64,
        references: &[&str],
    ) -> Result<HashMap<String, i64>> {
        #[derive(Deserialize)]
        struct Row {
            k: Option<String>,
            id: Option<f64>,
        }
        let mut map: HashMap<String, i64> = HashMap::new();
        let mut uniq: Vec<&str> = references.to_vec();
        uniq.sort_unstable();
        uniq.dedup();
        for group in uniq.chunks(500) {
            if group.is_empty() {
                continue;
            }
            let placeholders = group.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "SELECT reference AS k, transaction_id AS id FROM transactions \
                 WHERE user_id = ? AND reference IN ({placeholders})"
            );
            let mut q = Query::new(sql).bind(user_id);
            for r in group {
                q = q.bind(*r);
            }
            let rows: Vec<Row> = q.fetch_all(self.db).await?;
            for r in rows {
                if let (Some(k), Some(id)) = (r.k, r.id) {
                    map.insert(k, id as i64);
                }
            }
        }
        Ok(map)
    }
}

fn update_max(acc: &mut Option<chrono::DateTime<chrono::Utc>>, v: chrono::DateTime<chrono::Utc>) {
    match acc {
        Some(cur) if *cur >= v => {}
        _ => *acc = Some(v),
    }
}

// =============================================================================
// Per-entity ON CONFLICT statement builders. Each mirrors the column handling
// of the original sequential helper in sync.rs, re-expressed as a single
// newer-wins CASE-per-column upsert with `RETURNING <pk> AS id`.
// =============================================================================

use crate::entities::{
    TableCertificate, TableCertificateField, TableCommission, TableOutput, TableOutputBasket,
    TableOutputTag, TableProvenTx, TableProvenTxReq, TableTransaction, TableTxLabel,
};

/// `CASE WHEN excluded.updated_at > {t}.updated_at THEN excluded.{c} ELSE {t}.{c} END`
fn nw(t: &str, c: &str) -> String {
    format!("{c} = CASE WHEN excluded.updated_at > {t}.updated_at THEN excluded.{c} ELSE {t}.{c} END")
}

/// F7-2: fill-if-empty + newer-wins CASE for an IMMUTABLE blob column. Only
/// overwrites when the existing value is NULL/empty AND the incoming row is
/// strictly newer — the exact fail-closed immutability `put_blob_column`
/// enforces for the inline (D1) case, so a stale-but-newer push can never poison
/// a populated blob.
fn nw_blob(t: &str, c: &str) -> String {
    format!(
        "{c} = CASE WHEN ({t}.{c} IS NULL OR length({t}.{c})=0) AND excluded.updated_at > {t}.updated_at \
         THEN excluded.{c} ELSE {t}.{c} END"
    )
}

/// F7-2: the value to bind for a blob column in the BATCHED upsert. An
/// inline-sized blob (<= r2 THRESHOLD) is folded into the batch (`Some`) so it
/// costs ZERO extra subrequests; an over-threshold blob binds NULL here and is
/// written to R2 afterward by `put_blob_column`. A missing blob binds NULL.
/// This is what keeps an output-heavy chunk to O(1) subrequests instead of
/// ~3 per row (the CF 1000-subrequest-limit 503).
fn inline_blob_bind(data: Option<&[u8]>) -> Option<Vec<u8>> {
    match data {
        Some(d) if !crate::r2::should_use_r2(d) => Some(d.to_vec()),
        _ => None,
    }
}

/// F7-2: whether this blob still needs the post-batch R2 fill — i.e. it is large
/// enough (> THRESHOLD) to live in R2. Inline blobs are handled entirely by the
/// batch via `inline_blob_bind`, so `put_blob_column` is skipped for them.
fn needs_r2_fill(data: Option<&[u8]>) -> bool {
    matches!(data, Some(d) if crate::r2::should_use_r2(d))
}

fn build_upsert_basket(user_id: i64, b: &TableOutputBasket) -> Query {
    let sql = format!(
        "INSERT INTO output_baskets (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?) \
         ON CONFLICT(name, user_id) DO UPDATE SET {nd}, {mv}, {ua} \
         RETURNING basket_id AS id",
        nd = nw("output_baskets", "number_of_desired_utxos"),
        mv = nw("output_baskets", "minimum_desired_utxo_value"),
        ua = nw("output_baskets", "updated_at"),
    );
    Query::new(sql)
        .bind(user_id)
        .bind(b.name.as_str())
        .bind(b.number_of_desired_utxos as i64)
        .bind(b.minimum_desired_utxo_value)
        .bind(b.created_at)
        .bind(b.updated_at)
}

fn build_upsert_proven_tx(p: &TableProvenTx) -> Query {
    // merkle_path + raw_tx are NOT NULL inline (no R2 for proven_txs).
    let sql = format!(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(txid) DO UPDATE SET {h}, {i}, {bh}, {mr}, {mp}, {rt}, {ua} \
         RETURNING proven_tx_id AS id",
        h = nw("proven_txs", "height"),
        i = nw("proven_txs", "idx"),
        bh = nw("proven_txs", "block_hash"),
        mr = nw("proven_txs", "merkle_root"),
        mp = nw("proven_txs", "merkle_path"),
        rt = nw("proven_txs", "raw_tx"),
        ua = nw("proven_txs", "updated_at"),
    );
    Query::new(sql)
        .bind(p.txid.as_str())
        .bind(p.height)
        .bind(p.index)
        .bind(p.block_hash.as_str())
        .bind(p.merkle_root.as_str())
        .bind(p.merkle_path.as_slice())
        .bind(p.raw_tx.as_slice())
        .bind(p.created_at)
        .bind(p.updated_at)
}

fn build_upsert_proven_tx_req(req: &TableProvenTxReq) -> Query {
    // raw_tx NOT NULL inline; input_beef via put_blob_column after. proven_tx_id
    // bound as-is (not SyncMap-remapped — matches the original helper).
    let raw_tx = req.raw_tx.clone().unwrap_or_default();
    let sql = format!(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notify, notified, raw_tx, input_beef, proven_tx_id, batch, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(txid) DO UPDATE SET {st}, {at}, {hi}, {no}, {ny}, {rt}, {ib}, {pt}, {ba}, {ua} \
         RETURNING proven_tx_req_id AS id",
        st = nw("proven_tx_reqs", "status"),
        at = nw("proven_tx_reqs", "attempts"),
        hi = nw("proven_tx_reqs", "history"),
        no = nw("proven_tx_reqs", "notified"),
        ny = nw("proven_tx_reqs", "notify"),
        rt = nw("proven_tx_reqs", "raw_tx"),
        ib = nw_blob("proven_tx_reqs", "input_beef"),
        pt = nw("proven_tx_reqs", "proven_tx_id"),
        ba = nw("proven_tx_reqs", "batch"),
        ua = nw("proven_tx_reqs", "updated_at"),
    );
    Query::new(sql)
        .bind(req.txid.as_str())
        .bind(req.status.as_str())
        .bind(req.attempts as i64)
        .bind(req.history.as_str())
        .bind(req.notify.as_str())
        .bind(if req.notified { 1i64 } else { 0 })
        .bind(raw_tx)
        .bind(inline_blob_bind(req.input_beef.as_deref()))
        .bind(req.proven_tx_id)
        .bind(req.batch.clone())
        .bind(req.created_at)
        .bind(req.updated_at)
}

fn build_upsert_tx_label(user_id: i64, l: &TableTxLabel) -> Query {
    let sql = format!(
        "INSERT INTO tx_labels (user_id, label, created_at, updated_at) VALUES (?, ?, ?, ?) \
         ON CONFLICT(label, user_id) DO UPDATE SET {ua} RETURNING tx_label_id AS id",
        ua = nw("tx_labels", "updated_at"),
    );
    Query::new(sql)
        .bind(user_id)
        .bind(l.label.as_str())
        .bind(l.created_at)
        .bind(l.updated_at)
}

fn build_upsert_output_tag(user_id: i64, t: &TableOutputTag) -> Query {
    let sql = format!(
        "INSERT INTO output_tags (user_id, tag, created_at, updated_at) VALUES (?, ?, ?, ?) \
         ON CONFLICT(tag, user_id) DO UPDATE SET {ua} RETURNING output_tag_id AS id",
        ua = nw("output_tags", "updated_at"),
    );
    Query::new(sql)
        .bind(user_id)
        .bind(t.tag.as_str())
        .bind(t.created_at)
        .bind(t.updated_at)
}

fn build_upsert_transaction(user_id: i64, tx: &TableTransaction, proof_fk: Option<i64>) -> Query {
    // raw_tx + input_beef via put_blob_column after. proven_tx_id: never NULL out
    // an existing link — only set when newer AND incoming carries a resolved fk.
    let sql = format!(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, version, lock_time, is_outgoing, proven_tx_id, raw_tx, input_beef, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(reference) DO UPDATE SET {tx}, {st}, {de}, {sa}, {ve}, {lt}, {io}, {rt}, {ib}, \
           proven_tx_id = CASE WHEN excluded.updated_at > transactions.updated_at AND excluded.proven_tx_id IS NOT NULL \
                               THEN excluded.proven_tx_id ELSE transactions.proven_tx_id END, \
           {ua} \
         RETURNING transaction_id AS id",
        tx = nw("transactions", "txid"),
        st = nw("transactions", "status"),
        de = nw("transactions", "description"),
        sa = nw("transactions", "satoshis"),
        ve = nw("transactions", "version"),
        lt = nw("transactions", "lock_time"),
        io = nw("transactions", "is_outgoing"),
        rt = nw_blob("transactions", "raw_tx"),
        ib = nw_blob("transactions", "input_beef"),
        ua = nw("transactions", "updated_at"),
    );
    Query::new(sql)
        .bind(user_id)
        .bind(tx.txid.clone())
        .bind(tx.status.as_str())
        .bind(tx.reference.as_str())
        .bind(tx.description.as_str())
        .bind(tx.satoshis)
        .bind(tx.version as i64)
        .bind(tx.lock_time)
        .bind(if tx.is_outgoing { 1i64 } else { 0 })
        .bind(proof_fk)
        .bind(inline_blob_bind(tx.raw_tx.as_deref()))
        .bind(inline_blob_bind(tx.input_beef.as_deref()))
        .bind(tx.created_at)
        .bind(tx.updated_at)
}

fn build_upsert_output(
    user_id: i64,
    o: &TableOutput,
    local_tx_id: i64,
    local_basket_id: Option<i64>,
    // A2-plus: the LOCAL spending transaction_id, resolved by the caller from
    // the incoming spent_by / spent_by_reference. Drives the FORWARD-ONLY
    // spent_by clause (set on a locally-NULL spent_by, never clear).
    local_spent_by: Option<i64>,
) -> Query {
    // Monotonic funds guard translated 1:1 from sync.rs:1499 + folded newer-wins.
    // A2-plus adds forward-only spent_by + demote-only-when-spend-proven.
    // Proven in tests/f4_outputs_upsert_proof.py + f9 / spentness proofs.
    // locking_script via put_blob_column.
    let purpose = o.purpose.clone().unwrap_or_else(|| "change".to_string());
    let provided_by = if o.provided_by.is_empty() { "you".to_string() } else { o.provided_by.clone() };
    let sql = "INSERT INTO outputs \
         (user_id, transaction_id, basket_id, txid, vout, satoshis, locking_script, script_length, script_offset, type, provided_by, purpose, spendable, change, derivation_prefix, derivation_suffix, sender_identity_key, custom_instructions, spent_by, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(transaction_id, vout, user_id) DO UPDATE SET \
           locking_script = CASE WHEN (outputs.locking_script IS NULL OR length(outputs.locking_script)=0) AND excluded.updated_at > outputs.updated_at THEN excluded.locking_script ELSE outputs.locking_script END, \
           transaction_id = CASE WHEN excluded.updated_at > outputs.updated_at THEN excluded.transaction_id ELSE outputs.transaction_id END, \
           basket_id = CASE \
               WHEN outputs.basket_id = (SELECT basket_id FROM output_baskets WHERE user_id = outputs.user_id AND name = 'default') THEN outputs.basket_id \
               WHEN excluded.updated_at > outputs.updated_at THEN excluded.basket_id ELSE outputs.basket_id END, \
           satoshis = CASE WHEN outputs.satoshis = 0 AND excluded.updated_at > outputs.updated_at THEN excluded.satoshis ELSE outputs.satoshis END, \
           script_length = CASE WHEN (outputs.locking_script IS NULL OR length(outputs.locking_script)=0) AND excluded.updated_at > outputs.updated_at THEN excluded.script_length ELSE outputs.script_length END, \
           script_offset = CASE WHEN (outputs.locking_script IS NULL OR length(outputs.locking_script)=0) AND excluded.updated_at > outputs.updated_at THEN excluded.script_offset ELSE outputs.script_offset END, \
           type = CASE WHEN excluded.updated_at > outputs.updated_at THEN excluded.type ELSE outputs.type END, \
           spent_by = CASE WHEN outputs.spent_by IS NULL THEN excluded.spent_by ELSE outputs.spent_by END, \
           spendable = CASE WHEN outputs.spent_by IS NOT NULL THEN 0 WHEN excluded.spent_by IS NOT NULL THEN 0 WHEN outputs.spendable = 1 THEN 1 WHEN excluded.updated_at > outputs.updated_at THEN excluded.spendable ELSE outputs.spendable END, \
           change = CASE WHEN outputs.change = 1 THEN 1 WHEN excluded.updated_at > outputs.updated_at THEN excluded.change ELSE outputs.change END, \
           derivation_prefix = CASE WHEN outputs.derivation_prefix IS NULL AND excluded.updated_at > outputs.updated_at THEN excluded.derivation_prefix ELSE outputs.derivation_prefix END, \
           derivation_suffix = CASE WHEN outputs.derivation_suffix IS NULL AND excluded.updated_at > outputs.updated_at THEN excluded.derivation_suffix ELSE outputs.derivation_suffix END, \
           sender_identity_key = CASE WHEN excluded.updated_at > outputs.updated_at THEN excluded.sender_identity_key ELSE outputs.sender_identity_key END, \
           custom_instructions = CASE WHEN excluded.updated_at > outputs.updated_at THEN excluded.custom_instructions ELSE outputs.custom_instructions END, \
           updated_at = CASE WHEN excluded.updated_at > outputs.updated_at THEN excluded.updated_at ELSE outputs.updated_at END \
         RETURNING output_id AS id";
    Query::new(sql)
        .bind(user_id)
        .bind(local_tx_id)
        .bind(local_basket_id)
        .bind(o.txid.as_str())
        .bind(o.vout as i64)
        .bind(o.satoshis)
        .bind(inline_blob_bind(o.locking_script.as_deref()))
        .bind(o.script_length as i64)
        .bind(o.script_offset as i64)
        .bind(o.output_type.as_str())
        .bind(provided_by)
        .bind(purpose)
        .bind(if o.spendable { 1i64 } else { 0 })
        .bind(if o.change { 1i64 } else { 0 })
        .bind(o.derivation_prefix.clone())
        .bind(o.derivation_suffix.clone())
        .bind(o.sender_identity_key.clone())
        .bind(o.custom_instructions.clone())
        .bind(local_spent_by)
        .bind(o.created_at)
        .bind(o.updated_at)
}

fn build_upsert_tx_label_map(
    tx_id: i64,
    label_id: i64,
    updated_at: chrono::DateTime<chrono::Utc>,
    created_at: chrono::DateTime<chrono::Utc>,
) -> Query {
    let sql = "INSERT INTO tx_labels_map (tx_label_id, transaction_id, is_deleted, created_at, updated_at) \
         VALUES (?, ?, 0, ?, ?) \
         ON CONFLICT(tx_label_id, transaction_id) DO UPDATE SET \
           is_deleted = CASE WHEN excluded.updated_at > tx_labels_map.updated_at THEN 0 ELSE tx_labels_map.is_deleted END, \
           updated_at = CASE WHEN excluded.updated_at > tx_labels_map.updated_at THEN excluded.updated_at ELSE tx_labels_map.updated_at END \
         RETURNING tx_label_map_id AS id";
    Query::new(sql)
        .bind(label_id)
        .bind(tx_id)
        .bind(created_at)
        .bind(updated_at)
}

fn build_upsert_output_tag_map(
    output_id: i64,
    tag_id: i64,
    updated_at: chrono::DateTime<chrono::Utc>,
    created_at: chrono::DateTime<chrono::Utc>,
) -> Query {
    let sql = "INSERT INTO output_tags_map (output_tag_id, output_id, is_deleted, created_at, updated_at) \
         VALUES (?, ?, 0, ?, ?) \
         ON CONFLICT(output_tag_id, output_id) DO UPDATE SET \
           is_deleted = CASE WHEN excluded.updated_at > output_tags_map.updated_at THEN 0 ELSE output_tags_map.is_deleted END, \
           updated_at = CASE WHEN excluded.updated_at > output_tags_map.updated_at THEN excluded.updated_at ELSE output_tags_map.updated_at END \
         RETURNING output_tag_map_id AS id";
    Query::new(sql)
        .bind(tag_id)
        .bind(output_id)
        .bind(created_at)
        .bind(updated_at)
}

fn build_upsert_certificate(user_id: i64, c: &TableCertificate) -> Query {
    let sql = format!(
        "INSERT INTO certificates (user_id, serial_number, type, certifier, subject, verifier, revocation_outpoint, signature, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(user_id, type, certifier, serial_number) DO UPDATE SET {su}, {ve}, {ro}, {si}, {ua} \
         RETURNING certificate_id AS id",
        su = nw("certificates", "subject"),
        ve = nw("certificates", "verifier"),
        ro = nw("certificates", "revocation_outpoint"),
        si = nw("certificates", "signature"),
        ua = nw("certificates", "updated_at"),
    );
    Query::new(sql)
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
}

fn build_upsert_certificate_field(user_id: i64, f: &TableCertificateField, cert_id: i64) -> Query {
    let sql = format!(
        "INSERT INTO certificate_fields (user_id, certificate_id, field_name, field_value, master_key, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(field_name, certificate_id) DO UPDATE SET {fv}, {mk}, {ua} \
         RETURNING certificate_field_id AS id",
        fv = nw("certificate_fields", "field_value"),
        mk = nw("certificate_fields", "master_key"),
        ua = nw("certificate_fields", "updated_at"),
    );
    Query::new(sql)
        .bind(user_id)
        .bind(cert_id)
        .bind(f.field_name.as_str())
        .bind(f.field_value.as_str())
        .bind(f.master_key.as_str())
        .bind(f.created_at)
        .bind(f.updated_at)
}

fn build_upsert_commission(user_id: i64, c: &TableCommission, tx_id: i64) -> Query {
    // locking_script NOT NULL inline.
    let sql = format!(
        "INSERT INTO commissions (user_id, transaction_id, satoshis, key_offset, is_redeemed, locking_script, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(transaction_id) DO UPDATE SET {sa}, {ko}, {ir}, {ls}, {ua} \
         RETURNING commission_id AS id",
        sa = nw("commissions", "satoshis"),
        ko = nw("commissions", "key_offset"),
        ir = nw("commissions", "is_redeemed"),
        ls = nw("commissions", "locking_script"),
        ua = nw("commissions", "updated_at"),
    );
    Query::new(sql)
        .bind(user_id)
        .bind(tx_id)
        .bind(c.satoshis)
        .bind(c.key_offset.as_str())
        .bind(if c.is_redeemed { 1i64 } else { 0 })
        .bind(c.payer_locking_script.as_slice())
        .bind(c.created_at)
        .bind(c.updated_at)
}

// =============================================================================
// F7-2 tests — inline-vs-R2 blob routing (Codex condition: no <=4096 blob to R2)
// =============================================================================
#[cfg(test)]
mod f7_routing_tests {
    use super::{inline_blob_bind, needs_r2_fill};

    // Mirrors r2::THRESHOLD (its value is asserted by r2::tests::test_threshold_value_is_4096).
    const THRESHOLD: usize = 4096;

    #[test]
    fn at_or_below_threshold_folds_inline_never_r2() {
        for n in [0usize, 1, 25, THRESHOLD - 1, THRESHOLD] {
            let data = vec![0u8; n];
            assert!(inline_blob_bind(Some(&data)).is_some(), "<= {THRESHOLD} must fold inline (n={n})");
            assert!(!needs_r2_fill(Some(&data)), "<= {THRESHOLD} must NOT route to R2 (n={n})");
        }
    }

    #[test]
    fn over_threshold_routes_to_r2_not_inline() {
        for n in [THRESHOLD + 1, THRESHOLD * 4] {
            let data = vec![0u8; n];
            assert!(inline_blob_bind(Some(&data)).is_none(), "> {THRESHOLD} must NOT fold inline (n={n})");
            assert!(needs_r2_fill(Some(&data)), "> {THRESHOLD} must route to R2 (n={n})");
        }
    }

    #[test]
    fn present_blob_is_exactly_one_of_inline_or_r2() {
        // A present blob is folded inline XOR routed to R2 — never both (an inline
        // blob can't leak to R2) and never neither (a large blob can't be dropped).
        for n in [0usize, 25, 4096, 4097, 100_000] {
            let data = vec![0u8; n];
            let inline = inline_blob_bind(Some(&data)).is_some();
            let r2 = needs_r2_fill(Some(&data));
            assert!(inline ^ r2, "present blob must be inline XOR r2 (n={n}: inline={inline} r2={r2})");
        }
    }

    #[test]
    fn missing_blob_is_neither() {
        assert!(inline_blob_bind(None).is_none());
        assert!(!needs_r2_fill(None));
    }
}
