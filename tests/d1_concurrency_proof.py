#!/usr/bin/env python3
"""
D1 concurrency / double-spend proof for the wallet-infra UTXO allocator.

WHY THIS EXISTS
---------------
The audit of this server flagged "D1 has no true ACID / fake transactions"
as a funds-safety risk. Inspection showed the begin/commit/rollback methods
ARE stubs, but UTXO allocation defends against the double-spend race with an
atomic compare-and-swap:

    UPDATE outputs SET spendable=0, spent_by=?
      WHERE output_id = (SELECT ... WHERE spent_by IS NULL AND spendable=1 LIMIT 1)
            AND spent_by IS NULL
      RETURNING ...

(src/storage/create_action.rs:653 — the auto-select-change path, and :238 —
the user-specified-input path.) The repo's Rust unit tests only assert the
SQL *shape* ("can't execute D1 in tests", create_action.rs:2309). This script
closes that gap: it executes the BYTE-FOR-BYTE allocation SQL against real
SQLite.

D1 IS SQLite. Miniflare's local D1 and production D1 are both SQLite with a
single write primary, so writes serialize and the UPDATE acquires the write
lock BEFORE evaluating its subquery — exactly the invariant the guard relies
on. Proving the guard against stdlib sqlite3 (3.46) therefore proves the
production funds-safety property: two concurrent allocations can never grab
the same UTXO.

Run:  python3 tests/d1_concurrency_proof.py    (exit 0 = all proofs hold)
"""

import sqlite3
import sys
import tempfile
import threading
import os

# --- Schema: faithful subset of migrations/0001_initial.sql (the two tables
#     the allocation SQL touches). FK enforcement off (default), matching how
#     D1/the allocator behaves — the JOIN + column filters are what matter. ---
SCHEMA = """
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    reference TEXT NOT NULL UNIQUE,
    is_outgoing INTEGER NOT NULL,
    satoshis INTEGER NOT NULL DEFAULT 0,
    description TEXT NOT NULL,
    txid TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE outputs (
    output_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    transaction_id INTEGER NOT NULL,
    basket_id INTEGER,
    spendable INTEGER NOT NULL DEFAULT 0,
    change INTEGER NOT NULL DEFAULT 0,
    vout INTEGER NOT NULL,
    satoshis INTEGER NOT NULL,
    provided_by TEXT NOT NULL,
    purpose TEXT NOT NULL,
    type TEXT NOT NULL,
    txid TEXT,
    sender_identity_key TEXT,
    derivation_prefix TEXT,
    derivation_suffix TEXT,
    spent_by INTEGER,
    locking_script BLOB,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

# --- The EXACT auto-select-change allocation SQL from create_action.rs:653 ---
ALLOC_SQL = """UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ?
   WHERE output_id = (
       SELECT o.output_id
       FROM outputs o
       JOIN transactions t ON o.transaction_id = t.transaction_id
       WHERE o.user_id = ? AND o.basket_id = ?
         AND o.spent_by IS NULL AND o.spendable = 1
         AND t.status IN ('completed', 'unproven', 'nosend', 'sending')
       ORDER BY CASE WHEN o.satoshis >= ? THEN 0 ELSE 1 END,
                ABS(o.satoshis - ?) ASC
       LIMIT 1
   ) AND spent_by IS NULL
   RETURNING output_id, satoshis, txid, vout,
             hex(locking_script) as locking_script,
             derivation_prefix, derivation_suffix, sender_identity_key"""

# --- The EXACT user-specified-input allocation SQL from create_action.rs:238
#     (locks a SPECIFIC outpoint by txid+vout; same compare-and-swap guard,
#     different selector). Both production allocators must be double-spend-safe. ---
ALLOC_BY_OUTPOINT_SQL = """UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ?
   WHERE output_id = (
       SELECT o.output_id FROM outputs o
       JOIN transactions t ON o.transaction_id = t.transaction_id
       WHERE o.user_id = ? AND o.txid = ? AND o.vout = ?
         AND o.spent_by IS NULL AND o.spendable = 1
         AND t.status IN ('completed', 'unproven', 'nosend', 'sending')
       LIMIT 1
   ) AND spent_by IS NULL
   RETURNING output_id, satoshis, txid, vout,
             hex(locking_script) as locking_script,
             derivation_prefix, derivation_suffix, sender_identity_key"""

USER_ID = 1
BASKET_ID = 1
NOW = "2026-05-29T00:00:00Z"


def init_schema(conn):
    conn.executescript(SCHEMA)
    # One change-basket transaction the outputs belong to, status='completed'.
    conn.execute(
        "INSERT INTO transactions (transaction_id, user_id, status, reference, "
        "is_outgoing, satoshis, description) VALUES (1, ?, 'completed', 'ref-1', 0, 0, 'seed')",
        (USER_ID,),
    )
    conn.commit()


def seed_utxos(conn, sats_list):
    for i, sats in enumerate(sats_list):
        conn.execute(
            "INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, "
            "vout, satoshis, provided_by, purpose, type, txid, locking_script) "
            "VALUES (?, 1, ?, 1, 1, ?, ?, 'you', 'change', 'P2PKH', ?, ?)",
            (USER_ID, BASKET_ID, i, sats, f"seedtxid{i}", b"\x76\xa9\x14" + bytes(20) + b"\x88\xac"),
        )
    conn.commit()


def allocate(conn, spend_by_tx, target):
    """Auto-select-change allocator (create_action.rs:653). Returns the
    allocated output_id, or None if nothing was allocatable."""
    cur = conn.execute(ALLOC_SQL, (spend_by_tx, NOW, USER_ID, BASKET_ID, target, target))
    rows = cur.fetchall()
    conn.commit()
    return rows[0][0] if rows else None


def allocate_by_outpoint(conn, spend_by_tx, txid, vout):
    """User-specified-input allocator (create_action.rs:238). Locks a SPECIFIC
    outpoint. Returns the allocated output_id, or None if already spent/absent."""
    cur = conn.execute(
        ALLOC_BY_OUTPOINT_SQL, (spend_by_tx, NOW, USER_ID, txid, vout)
    )
    rows = cur.fetchall()
    conn.commit()
    return rows[0][0] if rows else None


FAILURES = []


def check(name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
    if not ok:
        FAILURES.append(name)


# === Proof 1: sequential double-spend guard (1 UTXO, 2 allocations) ===
def proof_sequential_double_spend():
    print("Proof 1: sequential — two allocations contend for ONE UTXO")
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    seed_utxos(conn, [5000])

    a = allocate(conn, spend_by_tx=10, target=1000)
    b = allocate(conn, spend_by_tx=11, target=1000)

    check("first allocation grabs the UTXO", a is not None, f"output_id={a}")
    check("second allocation gets NOTHING (no double-spend)", b is None, f"got={b}")
    spent_by = conn.execute(
        "SELECT spent_by FROM outputs WHERE output_id = ?", (a,)
    ).fetchone()[0]
    check("UTXO is owned by exactly the first tx", spent_by == 10, f"spent_by={spent_by}")
    conn.close()


# === Proof 2: distinct allocation (2 UTXOs, 2 allocations → no overlap) ===
def proof_distinct_allocation():
    print("Proof 2: sequential — two allocations, two UTXOs → distinct, no overlap")
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    seed_utxos(conn, [5000, 5000])

    a = allocate(conn, spend_by_tx=10, target=1000)
    b = allocate(conn, spend_by_tx=11, target=1000)
    c = allocate(conn, spend_by_tx=12, target=1000)

    check("both allocations succeed", a is not None and b is not None, f"a={a} b={b}")
    check("they grab DISTINCT outputs", a != b, f"a={a} b={b}")
    check("third allocation (no UTXOs left) gets nothing", c is None, f"got={c}")
    conn.close()


# === Proof 3: TRUE concurrency — N threads race for ONE UTXO ===
def proof_concurrent_race_single():
    print("Proof 3: concurrent — 16 threads race for ONE UTXO (real write-lock)")
    N = 16
    db_path = tempfile.mktemp(suffix=".db")
    try:
        c0 = sqlite3.connect(db_path)
        init_schema(c0)
        seed_utxos(c0, [5000])
        c0.close()

        barrier = threading.Barrier(N)
        results = [None] * N

        def worker(idx):
            conn = sqlite3.connect(db_path, timeout=30)
            conn.execute("PRAGMA busy_timeout=30000")
            barrier.wait()  # maximize contention — all fire together
            results[idx] = allocate(conn, spend_by_tx=100 + idx, target=1000)
            conn.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [r for r in results if r is not None]
        check("exactly ONE thread allocated the UTXO", len(winners) == 1, f"winners={winners}")
        # And the DB agrees: the single output has exactly one spent_by.
        c1 = sqlite3.connect(db_path)
        spent_rows = c1.execute(
            "SELECT spent_by FROM outputs WHERE spent_by IS NOT NULL"
        ).fetchall()
        c1.close()
        check("DB shows the UTXO spent exactly once", len(spent_rows) == 1, f"spent_rows={spent_rows}")
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


# === Proof 4: TRUE concurrency — N threads race for N UTXOs, none double-allocated ===
def proof_concurrent_race_many():
    print("Proof 4: concurrent — 16 threads race for 16 UTXOs → all distinct, none reused")
    N = 16
    db_path = tempfile.mktemp(suffix=".db")
    try:
        c0 = sqlite3.connect(db_path)
        init_schema(c0)
        seed_utxos(c0, [5000] * N)
        c0.close()

        barrier = threading.Barrier(N)
        results = [None] * N

        def worker(idx):
            conn = sqlite3.connect(db_path, timeout=30)
            conn.execute("PRAGMA busy_timeout=30000")
            barrier.wait()
            results[idx] = allocate(conn, spend_by_tx=200 + idx, target=1000)
            conn.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        allocated = [r for r in results if r is not None]
        check("all 16 threads allocated a UTXO", len(allocated) == N, f"count={len(allocated)}")
        check("every allocated UTXO is unique (no double-allocation)",
              len(set(allocated)) == len(allocated), f"distinct={len(set(allocated))}")
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


# === Proof 5: user-specified-input allocator (create_action.rs:238) ===
def proof_user_specified_input():
    print("Proof 5: user-specified input (create_action.rs:238) — lock a SPECIFIC outpoint twice")
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    seed_utxos(conn, [5000, 5000])  # seedtxid0/vout0, seedtxid1/vout1

    a = allocate_by_outpoint(conn, spend_by_tx=10, txid="seedtxid0", vout=0)
    b = allocate_by_outpoint(conn, spend_by_tx=11, txid="seedtxid0", vout=0)

    check("first lock of the specified outpoint succeeds", a is not None, f"output_id={a}")
    check("second lock of the SAME outpoint gets nothing (no double-spend)", b is None, f"got={b}")
    spent_by = conn.execute(
        "SELECT spent_by FROM outputs WHERE output_id = ?", (a,)
    ).fetchone()[0]
    check("outpoint owned by exactly the first tx", spent_by == 10, f"spent_by={spent_by}")
    conn.close()


# === Proof 6: TRUE concurrency — N threads race for the SAME specified outpoint ===
def proof_user_specified_concurrent():
    print("Proof 6: concurrent — 16 threads race to lock the SAME specified outpoint")
    N = 16
    db_path = tempfile.mktemp(suffix=".db")
    try:
        c0 = sqlite3.connect(db_path)
        init_schema(c0)
        seed_utxos(c0, [5000])  # seedtxid0 / vout 0
        c0.close()

        barrier = threading.Barrier(N)
        results = [None] * N

        def worker(idx):
            conn = sqlite3.connect(db_path, timeout=30)
            conn.execute("PRAGMA busy_timeout=30000")
            barrier.wait()
            results[idx] = allocate_by_outpoint(conn, 300 + idx, "seedtxid0", 0)
            conn.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [r for r in results if r is not None]
        check("exactly ONE thread locked the specified outpoint", len(winners) == 1, f"winners={winners}")
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


if __name__ == "__main__":
    print(f"sqlite engine: {sqlite3.sqlite_version}\n")
    proof_sequential_double_spend()
    proof_distinct_allocation()
    proof_concurrent_race_single()
    proof_concurrent_race_many()
    proof_user_specified_input()
    proof_user_specified_concurrent()
    print()
    if FAILURES:
        print(f"RESULT: FAIL — {len(FAILURES)} check(s) failed: {FAILURES}")
        sys.exit(1)
    print("RESULT: PASS — the atomic UPDATE...RETURNING allocator prevents "
          "double-spend under sequential AND concurrent contention.")
    sys.exit(0)
