#!/usr/bin/env python3
"""
Sync fidelity proof: the transaction→proof linkage survives an L2 round-trip.

WHY THIS EXISTS
---------------
Codex review 21b7cf8f flagged that the BRC-40 sync port
(src/storage/sync.rs) originally DROPPED `transactions.proven_tx_id` on a
remote round-trip: `fetch_transactions_for_sync` didn't select it and
`upsert_transaction` never wrote it, so a synced transaction silently lost its
link to its merkle proof — a fidelity divergence from the canonical sqlx sync
path.

The wire carries the linkage as `proofTxid` (a txid STRING, canonical
`TableTransaction.proof_txid`). Our schema models it as an INTEGER FK
`transactions.proven_tx_id` → `proven_txs.proven_tx_id`. The two id-spaces do
NOT coincide across stores (proven_txs is keyed by txid, but its autoincrement
PK differs per store), so the port maps in both directions:

  READ  (fetch_transactions_for_sync):
        SELECT ... p.txid AS proof_txid
        FROM transactions t LEFT JOIN proven_txs p ON t.proven_tx_id = p.proven_tx_id
  WRITE (upsert_transaction → resolve_proof_fk):
        proven_tx_id = (SELECT proven_tx_id FROM proven_txs WHERE txid = ?)

This script executes that exact READ + WRITE SQL against real SQLite (D1 IS
SQLite) and proves the proof txid survives a round-trip even when the FK
integer is remapped between a SOURCE store and a DEST store with an offset
id-space — the case a naive "carry the raw integer" port would corrupt.

Run:  python3 tests/proof_link_roundtrip_proof.py   (exit 0 = all proofs hold)
"""

import sqlite3
import sys
import tempfile
import os

PROOF_TXID = "a" * 64  # the proven tx's txid (the wire `proofTxid`)
OTHER_TXID = "b" * 64  # a decoy proven tx, to offset the DEST id-space

# Faithful subset of migrations/0001_initial.sql for the two tables involved.
SCHEMA = """
CREATE TABLE proven_txs (
    proven_tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
    txid TEXT NOT NULL UNIQUE,
    height INTEGER NOT NULL DEFAULT 0,
    idx INTEGER NOT NULL DEFAULT 0,
    block_hash TEXT NOT NULL DEFAULT '',
    merkle_root TEXT NOT NULL DEFAULT '',
    merkle_path BLOB NOT NULL DEFAULT x'',
    raw_tx BLOB NOT NULL DEFAULT x'',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    proven_tx_id INTEGER REFERENCES proven_txs(proven_tx_id),
    status TEXT NOT NULL DEFAULT 'completed',
    reference TEXT NOT NULL UNIQUE,
    is_outgoing INTEGER NOT NULL DEFAULT 0,
    satoshis INTEGER NOT NULL DEFAULT 0,
    version INTEGER,
    lock_time INTEGER,
    description TEXT NOT NULL DEFAULT '',
    txid TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

# The byte-for-byte READ projection used by fetch_transactions_for_sync (the
# proof_txid alias is the only part this proof cares about).
READ_SQL = (
    "SELECT t.transaction_id, t.reference, p.txid AS proof_txid "
    "FROM transactions t LEFT JOIN proven_txs p ON t.proven_tx_id = p.proven_tx_id "
    "WHERE t.user_id = ? ORDER BY t.updated_at ASC"
)

# The byte-for-byte resolve used by resolve_proof_fk on the WRITE side.
RESOLVE_SQL = "SELECT proven_tx_id FROM proven_txs WHERE txid = ?"

passed = 0
failed = 0


def check(name, cond):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}")


def new_db():
    path = os.path.join(tempfile.mkdtemp(), "proof_link.db")
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    return con


def read_proof_txid(con, user_id, reference):
    cur = con.execute(READ_SQL, (user_id,))
    for row in cur.fetchall():
        if row[1] == reference:
            return row[2]
    return "<<row-missing>>"


print("Sync proof-linkage round-trip proof (src/storage/sync.rs)")
print("=" * 64)

# --- SOURCE store: a proven tx + a transaction linked to it. ---
src = new_db()
src.execute("INSERT INTO proven_txs (txid) VALUES (?)", (PROOF_TXID,))
src_pt_id = src.execute(RESOLVE_SQL, (PROOF_TXID,)).fetchone()[0]
src.execute(
    "INSERT INTO transactions (user_id, proven_tx_id, reference) VALUES (?, ?, ?)",
    (1, src_pt_id, "ref-proven"),
)
# An unproven transaction (no proof link) in the same store.
src.execute(
    "INSERT INTO transactions (user_id, proven_tx_id, reference) VALUES (?, NULL, ?)",
    (1, "ref-unproven"),
)
src.commit()

check("SOURCE read surfaces proof_txid for the proven tx",
      read_proof_txid(src, 1, "ref-proven") == PROOF_TXID)
check("SOURCE read yields NULL proof_txid for the unproven tx",
      read_proof_txid(src, 1, "ref-unproven") is None)

# What the chunk carries on the wire for each transaction:
wire_proven = read_proof_txid(src, 1, "ref-proven")      # == PROOF_TXID
wire_unproven = read_proof_txid(src, 1, "ref-unproven")  # == None

# --- DEST store with an OFFSET id-space: a decoy proven tx is inserted FIRST
#     so the same PROOF_TXID lands on a DIFFERENT local proven_tx_id than in
#     SOURCE. A naive "carry the raw foreign integer" port would mis-link here.
dest = new_db()
dest.execute("INSERT INTO proven_txs (txid) VALUES (?)", (OTHER_TXID,))   # PK 1
dest.execute("INSERT INTO proven_txs (txid) VALUES (?)", (PROOF_TXID,))   # PK 2
dest_pt_id = dest.execute(RESOLVE_SQL, (PROOF_TXID,)).fetchone()[0]
check("DEST proven_tx_id is offset from SOURCE (id-spaces differ)",
      dest_pt_id != src_pt_id)

# --- WRITE side: upsert_transaction resolves the wire proofTxid → LOCAL FK. ---
def resolve_fk(con, proof_txid):
    if proof_txid is None:
        return None
    row = con.execute(RESOLVE_SQL, (proof_txid,)).fetchone()
    return row[0] if row else None

dest.execute(
    "INSERT INTO transactions (user_id, proven_tx_id, reference) VALUES (?, ?, ?)",
    (1, resolve_fk(dest, wire_proven), "ref-proven"),
)
dest.execute(
    "INSERT INTO transactions (user_id, proven_tx_id, reference) VALUES (?, ?, ?)",
    (1, resolve_fk(dest, wire_unproven), "ref-unproven"),
)
dest.commit()

# The stored FK must be the DEST-local one, not the foreign SOURCE one.
stored_fk = dest.execute(
    "SELECT proven_tx_id FROM transactions WHERE reference = 'ref-proven'"
).fetchone()[0]
check("DEST stored FK is the LOCAL proven_tx_id (remapped, not foreign)",
      stored_fk == dest_pt_id and stored_fk != src_pt_id)

# --- Round-trip read on DEST reproduces the SAME proof txid string. ---
check("DEST read round-trips the proof_txid string intact",
      read_proof_txid(dest, 1, "ref-proven") == PROOF_TXID)
check("DEST unproven tx still has NULL proof_txid",
      read_proof_txid(dest, 1, "ref-unproven") is None)

# --- Deferred-proof case: proof not yet present locally → FK NULL, not error,
#     and the linkage is simply absent (back-fillable later), never corrupted.
dest2 = new_db()
fk = resolve_fk(dest2, PROOF_TXID)  # proven_txs empty here
check("Missing proof resolves to NULL FK (deferred, not corrupted)", fk is None)
dest2.execute(
    "INSERT INTO transactions (user_id, proven_tx_id, reference) VALUES (?, ?, ?)",
    (1, fk, "ref-proven"),
)
dest2.commit()
check("Deferred-proof tx reads back as NULL proof_txid (no false link)",
      read_proof_txid(dest2, 1, "ref-proven") is None)

print("=" * 64)
print(f"{passed} passed, {failed} failed")
sys.exit(0 if failed == 0 else 1)
