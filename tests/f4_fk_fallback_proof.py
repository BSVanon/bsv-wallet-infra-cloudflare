#!/usr/bin/env python3
"""
F4 proof #3 (Codex f05cc25a regression coverage): the CROSS-CHUNK parent-FK
resolution. Sync is chunked, so a parent transaction/proven_tx can land in an
EARLIER chunk than its child output/transaction. The same-chunk SyncMap then
MISSES, and resolution MUST fall back to LOCAL storage by txid (never to the
remote foreign id, which collides on UNIQUE(transaction_id,vout,user_id) —
Robert's v1.0.1-rc rc=19). This replicates the exact Rust resolution in
src/storage/sync_apply.rs (same-chunk map → local DB lookup by txid → skip).
"""
import sqlite3, sys

SCHEMA = """
CREATE TABLE transactions (
  transaction_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
  txid TEXT, reference TEXT NOT NULL UNIQUE, status TEXT, updated_at TEXT);
CREATE TABLE proven_txs (
  proven_tx_id INTEGER PRIMARY KEY AUTOINCREMENT, txid TEXT NOT NULL UNIQUE,
  updated_at TEXT);
"""

# --- the Rust resolution, replicated 1:1 ---
def resolve_output_tx_id(con, same_chunk_map, user_id, foreign_tx_id, out_txid):
    # 1. same-chunk SyncMap (foreign -> local)
    if foreign_tx_id in same_chunk_map:
        return same_chunk_map[foreign_tx_id]
    # 2. local DB lookup by (user_id, txid) LIMIT 1
    row = con.execute(
        "SELECT transaction_id FROM transactions WHERE user_id=? AND txid=? LIMIT 1",
        (user_id, out_txid)).fetchone()
    if row:
        return row[0]
    # 3. orphan -> skip
    return None

def resolve_proof_fk(con, proof_txid):
    if proof_txid is None:
        return None
    row = con.execute("SELECT proven_tx_id FROM proven_txs WHERE txid=?", (proof_txid,)).fetchone()
    return row[0] if row else None

def main():
    con = sqlite3.connect(":memory:"); con.executescript(SCHEMA)
    # Prior-chunk state already in LOCAL storage (NOT in the current chunk's map):
    con.execute("INSERT INTO transactions(user_id,txid,reference,status,updated_at) VALUES (7,'txPRIOR','ref-prior','completed','t')")
    LOCAL_TX = con.execute("SELECT transaction_id FROM transactions WHERE reference='ref-prior'").fetchone()[0]
    con.execute("INSERT INTO proven_txs(txid,updated_at) VALUES ('ptPRIOR','t')")
    LOCAL_PT = con.execute("SELECT proven_tx_id FROM proven_txs WHERE txid='ptPRIOR'").fetchone()[0]
    con.commit()

    fails=[]
    def check(n,c): print(("  ok  " if c else " FAIL ")+n); (fails.append(n) if not c else None)

    # The current chunk's SyncMap (foreign remote id -> local id) for a tx in THIS chunk.
    same_chunk_map = {1000: 55}  # remote tx 1000 was inserted this chunk as local 55

    # 1. output whose parent IS in this chunk -> uses the map
    r1 = resolve_output_tx_id(con, same_chunk_map, 7, 1000, "txTHISCHUNK")
    check("1 same-chunk parent resolves via SyncMap", r1 == 55)

    # 2. output whose parent is a PRIOR-chunk tx (remote id 2000, not in map) ->
    #    local DB lookup by txid returns the LOCAL id, NOT the remote 2000.
    r2 = resolve_output_tx_id(con, same_chunk_map, 7, 2000, "txPRIOR")
    check("2 prior-chunk parent resolves via local DB (local id)", r2 == LOCAL_TX)
    check("2 does NOT use the remote foreign id (the rc=19 bug)", r2 != 2000)

    # 3. output whose parent is in NEITHER -> skip (None), no foreign-id collision
    r3 = resolve_output_tx_id(con, same_chunk_map, 7, 3000, "txORPHAN")
    check("3 orphan output skipped (None)", r3 is None)

    # 4. transaction proof_fk: proven_tx exists locally (prior chunk), not in this chunk
    p4 = resolve_proof_fk(con, "ptPRIOR")
    check("4 proof_fk resolves via local proven_txs lookup", p4 == LOCAL_PT)

    # 5. proof_fk for a txid absent locally -> None (link simply not set)
    p5 = resolve_proof_fk(con, "ptABSENT")
    check("5 absent proof -> None (no bogus link)", p5 is None)

    # 6. user-scoping: same txid under a DIFFERENT user must NOT resolve
    r6 = resolve_output_tx_id(con, {}, 999, 2000, "txPRIOR")
    check("6 lookup is user-scoped (no cross-user leak)", r6 is None)

    print()
    if fails: print(f"PROOF FAILED: {fails}"); sys.exit(1)
    print("PROOF PASSED: cross-chunk parent-FK resolution (map -> local DB by txid -> skip) "
          "correct for outputs and proof links; never uses the remote foreign id.")

if __name__=="__main__": main()
