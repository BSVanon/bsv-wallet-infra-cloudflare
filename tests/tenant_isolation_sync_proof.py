#!/usr/bin/env python3
"""
Tenant-isolation proof for getSyncChunk (src/storage/sync.rs).

WHY THIS EXISTS
---------------
`proven_txs` and `proven_tx_reqs` are GLOBAL, txid-keyed pools (no `user_id`
column) shared across all tenants of the multi-tenant D1 funds-map. An unscoped
`... WHERE 1=1` fetch (filtered only by the sync cursor) would return EVERY
tenant's proven transactions + broadcast requests to any user.

`fetch_proven_txs_for_sync` and `fetch_proven_tx_reqs_for_sync` therefore scope
both fetches to the requesting user's OWN transactions (the wallet needs exactly
the proofs its transactions reference):
  proven_txs:      WHERE proven_tx_id IN (SELECT proven_tx_id FROM transactions
                                          WHERE user_id = ? AND proven_tx_id IS NOT NULL)
  proven_tx_reqs:  WHERE txid IN         (SELECT txid FROM transactions
                                          WHERE user_id = ? AND txid IS NOT NULL)

This script runs those EXACT WHERE clauses against real SQLite (D1 IS SQLite),
proving: a fresh user pulls 0; the owning user pulls only its own; and an
unscoped `WHERE 1=1` would return the global pool.

Run:  python3 tests/tenant_isolation_sync_proof.py   (exit 0 = all proofs hold)
"""

import sqlite3
import sys
import tempfile
import os

A_TXID = "a" * 64        # user A's transaction (owns a proof + a req)
OTHER_TXID = "c" * 64    # ANOTHER tenant's tx — in the global pools, owned by neither A nor the fresh user

SCHEMA = """
CREATE TABLE proven_txs (
    proven_tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
    txid TEXT NOT NULL UNIQUE,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE proven_tx_reqs (
    proven_tx_req_id INTEGER PRIMARY KEY AUTOINCREMENT,
    proven_tx_id INTEGER REFERENCES proven_txs(proven_tx_id),
    txid TEXT NOT NULL UNIQUE,
    raw_tx BLOB NOT NULL DEFAULT x'',
    input_beef BLOB,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    proven_tx_id INTEGER REFERENCES proven_txs(proven_tx_id),
    txid TEXT,
    reference TEXT NOT NULL UNIQUE,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

# The EXACT scoping WHERE clauses from the fix (src/storage/sync.rs).
PROVEN_TXS_SCOPED = (
    "SELECT proven_tx_id FROM proven_txs WHERE proven_tx_id IN "
    "(SELECT proven_tx_id FROM transactions WHERE user_id = ? AND proven_tx_id IS NOT NULL)"
)
PROVEN_TX_REQS_SCOPED = (
    "SELECT proven_tx_req_id FROM proven_tx_reqs WHERE txid IN "
    "(SELECT txid FROM transactions WHERE user_id = ? AND txid IS NOT NULL)"
)
# Unscoped queries: the negative control for the regression guard.
PROVEN_TXS_LEAKY = "SELECT proven_tx_id FROM proven_txs WHERE 1=1"
PROVEN_TX_REQS_LEAKY = "SELECT proven_tx_req_id FROM proven_tx_reqs WHERE 1=1"

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


def rows(con, sql, *params):
    return [r[0] for r in con.execute(sql, params).fetchall()]


print("Tenant-isolation proof: getSyncChunk proven_txs / proven_tx_reqs (src/storage/sync.rs)")
print("=" * 78)

path = os.path.join(tempfile.mkdtemp(), "iso_proof.db")
con = sqlite3.connect(path)
con.executescript(SCHEMA)

# Global pools: two proofs + two reqs — A's, and ANOTHER tenant's.
con.execute("INSERT INTO proven_txs (txid) VALUES (?)", (A_TXID,))
con.execute("INSERT INTO proven_txs (txid) VALUES (?)", (OTHER_TXID,))
a_pt = con.execute("SELECT proven_tx_id FROM proven_txs WHERE txid = ?", (A_TXID,)).fetchone()[0]
other_pt = con.execute("SELECT proven_tx_id FROM proven_txs WHERE txid = ?", (OTHER_TXID,)).fetchone()[0]
con.execute("INSERT INTO proven_tx_reqs (proven_tx_id, txid, input_beef) VALUES (?, ?, x'deadbeef')", (a_pt, A_TXID))
con.execute("INSERT INTO proven_tx_reqs (proven_tx_id, txid, input_beef) VALUES (?, ?, x'cafebabe')", (other_pt, OTHER_TXID))

# user_id 1 (A) owns a transaction linked to A's proof/txid. user_id 2 (fresh) owns NOTHING.
con.execute("INSERT INTO transactions (user_id, proven_tx_id, txid, reference) VALUES (1, ?, ?, 'refA')", (a_pt, A_TXID))
# (the OTHER tenant's transaction lives under user_id 9 — present in the pool, owned by neither 1 nor 2)
con.execute("INSERT INTO transactions (user_id, proven_tx_id, txid, reference) VALUES (9, ?, ?, 'refX')", (other_pt, OTHER_TXID))
con.commit()

FRESH = 2  # a brand-new user with no transactions

# 1. THE FIX: a fresh user pulls ZERO proven_txs / proven_tx_reqs.
check("fresh user pulls 0 proven_txs (scoped)", rows(con, PROVEN_TXS_SCOPED, FRESH) == [])
check("fresh user pulls 0 proven_tx_reqs (scoped)", rows(con, PROVEN_TX_REQS_SCOPED, FRESH) == [])

# 2. CORRECTNESS: the owning user (A) pulls exactly its OWN proof/req — never the other tenant's.
a_txs = rows(con, PROVEN_TXS_SCOPED, 1)
a_reqs = rows(con, PROVEN_TX_REQS_SCOPED, 1)
check("owner A pulls exactly its own proven_tx", a_txs == [a_pt])
check("owner A does NOT pull the other tenant's proven_tx", other_pt not in a_txs)
check("owner A pulls exactly its own proven_tx_req", len(a_reqs) == 1)

# 3. REGRESSION GUARD: an unscoped WHERE 1=1 returns the whole global pool to the fresh user.
leaky_txs = rows(con, PROVEN_TXS_LEAKY, )
check("old WHERE 1=1 WOULD leak >1 tenant's proven_txs (bug the fix closes)", len(leaky_txs) >= 2 and other_pt in leaky_txs)
check("old WHERE 1=1 WOULD leak the other tenant's proven_tx_req", other_pt in [
      con.execute("SELECT proven_tx_id FROM proven_tx_reqs WHERE proven_tx_req_id = ?", (r,)).fetchone()[0]
      for r in rows(con, PROVEN_TX_REQS_LEAKY)])

print("=" * 78)
print(f"  {passed} passed, {failed} failed")
sys.exit(0 if failed == 0 else 1)
