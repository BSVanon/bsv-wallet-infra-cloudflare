#!/usr/bin/env python3
"""
F4 proof #2: parent RETURNING-id on every path, the proven_tx->transaction->output
FK-chain id propagation (the SyncMap), the never-NULL proof-link guard, and the
blob hex() populated-check that drives the R2-skip. Offline against real SQLite.

Together with f4_outputs_upsert_proof.py (the monotonic guard) this validates the
whole F4 merge before any Rust is written or anything is deployed.
"""
import sqlite3, sys

SCHEMA = """
CREATE TABLE proven_txs (
  proven_tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
  txid TEXT NOT NULL UNIQUE, height INTEGER, block_hash TEXT, merkle_root TEXT,
  merkle_path BLOB, raw_tx BLOB, created_at TEXT, updated_at TEXT);
CREATE TABLE transactions (
  transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL, proven_tx_id INTEGER REFERENCES proven_txs(proven_tx_id),
  status TEXT NOT NULL, reference TEXT NOT NULL UNIQUE, is_outgoing INTEGER NOT NULL,
  satoshis INTEGER NOT NULL DEFAULT 0, version INTEGER, lock_time INTEGER,
  description TEXT NOT NULL, txid TEXT, input_beef BLOB, raw_tx BLOB,
  created_at TEXT, updated_at TEXT);
CREATE TABLE tx_labels (
  tx_label_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
  label TEXT NOT NULL, created_at TEXT, updated_at TEXT, UNIQUE(label,user_id));
CREATE TABLE tx_labels_map (
  tx_label_map_id INTEGER PRIMARY KEY AUTOINCREMENT,
  tx_label_id INTEGER NOT NULL, transaction_id INTEGER NOT NULL,
  is_deleted INTEGER NOT NULL DEFAULT 0, created_at TEXT, updated_at TEXT,
  UNIQUE(tx_label_id, transaction_id));
"""

# proven_txs: blobs (merkle_path, raw_tx) are NOT NULL inline today; here we just
# need the id + txid back for the chain.
PROVEN = """
INSERT INTO proven_txs (txid, height, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at)
VALUES (?, ?, ?, ?, x'00', x'00', ?, ?)
ON CONFLICT(txid) DO UPDATE SET
  height = CASE WHEN excluded.updated_at > proven_txs.updated_at THEN excluded.height ELSE proven_txs.height END,
  updated_at = CASE WHEN excluded.updated_at > proven_txs.updated_at THEN excluded.updated_at ELSE proven_txs.updated_at END
RETURNING proven_tx_id, txid;
"""

# transactions: proof_fk is resolved (SyncMap) BEFORE this runs. Guard: never NULL
# out an existing proof link; only set when newer AND incoming carries one.
TXN = """
INSERT INTO transactions (user_id, txid, status, reference, description, satoshis,
  version, lock_time, is_outgoing, proven_tx_id, raw_tx, input_beef, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
ON CONFLICT(reference) DO UPDATE SET
  txid    = CASE WHEN excluded.updated_at > transactions.updated_at THEN excluded.txid ELSE transactions.txid END,
  status  = CASE WHEN excluded.updated_at > transactions.updated_at THEN excluded.status ELSE transactions.status END,
  satoshis= CASE WHEN excluded.updated_at > transactions.updated_at THEN excluded.satoshis ELSE transactions.satoshis END,
  is_outgoing = CASE WHEN excluded.updated_at > transactions.updated_at THEN excluded.is_outgoing ELSE transactions.is_outgoing END,
  proven_tx_id = CASE WHEN excluded.updated_at > transactions.updated_at AND excluded.proven_tx_id IS NOT NULL
                      THEN excluded.proven_tx_id ELSE transactions.proven_tx_id END,
  updated_at = CASE WHEN excluded.updated_at > transactions.updated_at THEN excluded.updated_at ELSE transactions.updated_at END
RETURNING transaction_id, hex(COALESCE(raw_tx, x'')) AS raw_tx_hex, hex(COALESCE(input_beef, x'')) AS beef_hex, proven_tx_id;
"""

LABEL = """
INSERT INTO tx_labels (user_id, label, created_at, updated_at) VALUES (?, ?, ?, ?)
ON CONFLICT(label, user_id) DO UPDATE SET
  updated_at = CASE WHEN excluded.updated_at > tx_labels.updated_at THEN excluded.updated_at ELSE tx_labels.updated_at END
RETURNING tx_label_id;
"""

LABELMAP = """
INSERT INTO tx_labels_map (tx_label_id, transaction_id, is_deleted, created_at, updated_at)
VALUES (?, ?, 0, ?, ?)
ON CONFLICT(tx_label_id, transaction_id) DO UPDATE SET
  is_deleted = CASE WHEN excluded.updated_at > tx_labels_map.updated_at THEN 0 ELSE tx_labels_map.is_deleted END,
  updated_at = CASE WHEN excluded.updated_at > tx_labels_map.updated_at THEN excluded.updated_at ELSE tx_labels_map.updated_at END
RETURNING tx_label_map_id;
"""

# Fill a blob via the fill-if-empty path (mirrors put_blob_column intent).
FILL_RAWTX = "UPDATE transactions SET raw_tx = ? WHERE transaction_id = ? AND (raw_tx IS NULL OR length(raw_tx)=0)"

def one(con, sql, *p):
    cur = con.execute(sql, p); row = cur.fetchone(); con.commit()
    assert row is not None, f"RETURNING produced NO row for: {sql[:40]}..."
    return row

def main():
    con = sqlite3.connect(":memory:"); con.executescript(SCHEMA)
    T0,T1,T2 = "2026-06-01T00:00:00Z","2026-06-02T00:00:00Z","2026-06-03T00:00:00Z"
    fails=[]
    def check(n,c): print(("  ok  " if c else " FAIL ")+n); (fails.append(n) if not c else None)

    # --- FK chain: proven_tx -> transaction(proof_fk) -> (output would use tx id) ---
    pt = one(con, PROVEN, "txABC", 800000, "bh", "mr", T1, T1)
    proven_local = pt[0]; check("proven_tx insert returns id+txid", pt[0] is not None and pt[1]=="txABC")

    # transaction resolves proof_fk from the proven map (proven_local)
    tx = one(con, TXN, 7, "txABC", "completed", "ref-1", "desc", 500, 1, 0, 1, proven_local, T1, T1)
    tx_local = tx[0]
    check("txn insert returns id", tx_local is not None)
    check("txn carries resolved proof_fk (chain propagated)", tx[3]==proven_local)
    check("txn raw_tx empty on insert (drives R2 fill)", tx[1]=="")

    # --- parent RETURNING id on the UPDATE (newer) path ---
    tx2 = one(con, TXN, 7, "txABC", "unproven", "ref-1", "desc", 999, 1, 0, 0, None, T2, T2)
    check("txn newer-update returns same id", tx2[0]==tx_local)
    check("txn newer-update applied status", con.execute("SELECT status FROM transactions WHERE transaction_id=?",(tx_local,)).fetchone()[0]=="unproven")
    check("txn newer-update with NULL proof_fk does NOT null out existing link",
          con.execute("SELECT proven_tx_id FROM transactions WHERE transaction_id=?",(tx_local,)).fetchone()[0]==proven_local)

    # --- parent RETURNING id on the OLDER (no-op) path: id still returned ---
    tx3 = one(con, TXN, 7, "txOLD", "completed", "ref-1", "desc", 1, 1, 0, 0, None, T0, T0)
    check("txn older-noop STILL returns id (SyncMap safe)", tx3[0]==tx_local)
    check("txn older-noop leaves status unchanged", con.execute("SELECT status FROM transactions WHERE transaction_id=?",(tx_local,)).fetchone()[0]=="unproven")

    # --- blob hex() populated-check drives the R2 skip ---
    check("RETURNING hex(raw_tx)='' means R2-fill needed", tx[1]=="")
    con.execute(FILL_RAWTX, (b"\xde\xad", tx_local)); con.commit()
    tx4 = one(con, TXN, 7, "txABC", "completed", "ref-1", "d", 1, 1, 0, 0, None, T2, T2)
    check("after fill, RETURNING hex(raw_tx) non-empty -> R2 skipped next time", tx4[1]!="")

    # --- label + composite-key map chain ---
    lb = one(con, LABEL, 7, "sent", T1, T1); label_local=lb[0]
    lm = one(con, LABELMAP, label_local, tx_local, T1, T1)
    check("label returns id", label_local is not None)
    check("label_map composite upsert returns id", lm[0] is not None)
    lm2 = one(con, LABELMAP, label_local, tx_local, T1, T1)
    check("label_map idempotent re-apply returns same id", lm2[0]==lm[0])

    print()
    if fails: print(f"PROOF FAILED: {fails}"); sys.exit(1)
    print("PROOF PASSED: parent RETURNING-id (insert/update/older), FK-chain propagation, "
          "never-NULL proof link, and blob hex() R2-skip all correct.")

if __name__=="__main__": main()
