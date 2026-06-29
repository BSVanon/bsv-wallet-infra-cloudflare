#!/usr/bin/env python3
"""
F7-2 proof: folding an INLINE blob (<= r2 THRESHOLD) into the batched
INSERT ... ON CONFLICT ... upsert preserves the SAME fail-closed immutability
that put_blob_column enforced for the inline (D1) case.

Before F7-2 the output upsert bound locking_script = NULL and filled it via a
separate put_blob_column (SELECT hex + R2 exists + UPDATE = ~3 subrequests/row),
which on an output-heavy chunk blew CF's 1000-subrequest/invocation limit (503).
F7-2 binds the inline value directly and guards the ON CONFLICT update with the
fill-if-empty + newer-wins CASE below. D1 IS SQLite, so this proves the merge
SQL offline before any prod deploy.

INVARIANTS PROVEN:
  - round-trip: a new row's inline locking_script is stored as given.
  - IMMUTABILITY (the funds-safety guard): a stale-but-newer push with a
    DIFFERENT script must NOT overwrite a populated locking_script.
  - fill-if-empty: a newer push fills a NULL/empty locking_script.
  - newer-gated: an OLDER push never fills, even into an empty column.
  - idempotent: re-applying the same row is a no-op.
"""
import sqlite3, sys

SCHEMA = """
CREATE TABLE outputs (
  output_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  transaction_id INTEGER NOT NULL,
  vout INTEGER NOT NULL,
  satoshis INTEGER NOT NULL DEFAULT 0,
  locking_script BLOB,
  updated_at TEXT NOT NULL,
  UNIQUE(transaction_id, vout, user_id));
"""

# The exact locking_script fold from build_upsert_output (sync_apply.rs):
# bind the inline value in VALUES, guard the update with fill-if-empty + newer.
UPSERT = """
INSERT INTO outputs (user_id, transaction_id, vout, satoshis, locking_script, updated_at)
VALUES (1, 10, ?, 0, ?, ?)
ON CONFLICT(transaction_id, vout, user_id) DO UPDATE SET
  locking_script = CASE
    WHEN (outputs.locking_script IS NULL OR length(outputs.locking_script)=0)
         AND excluded.updated_at > outputs.updated_at
    THEN excluded.locking_script ELSE outputs.locking_script END,
  updated_at = CASE WHEN excluded.updated_at > outputs.updated_at
    THEN excluded.updated_at ELSE outputs.updated_at END
RETURNING output_id
"""

def script(conn, vout):
    return conn.execute(
        "SELECT locking_script FROM outputs WHERE transaction_id=10 AND vout=? AND user_id=1",
        (vout,)).fetchone()[0]

def check(name, cond):
    print(("  ok  " if cond else "  FAIL ") + name)
    if not cond:
        check.failed = True
check.failed = False

def main():
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA)

    S1 = b"\x76\xa9\x14" + b"\x11" * 20 + b"\x88\xac"   # ~25B P2PKH, inline
    S2 = b"\x76\xa9\x14" + b"\x22" * 20 + b"\x88\xac"   # different script

    # 1) round-trip: new row stores the inline script.
    conn.execute(UPSERT, (0, S1, "2026-06-29T00:00:01Z"))
    check("new inline locking_script round-trips", script(conn, 0) == S1)

    # 2) IMMUTABILITY: a strictly-newer push with a DIFFERENT script must NOT
    #    overwrite a populated locking_script (immutable per outpoint).
    conn.execute(UPSERT, (0, S2, "2026-06-29T00:00:09Z"))
    check("newer push does NOT overwrite a populated inline blob", script(conn, 0) == S1)

    # 3) fill-if-empty: a newer push fills a NULL locking_script.
    conn.execute(UPSERT, (1, None, "2026-06-29T00:00:01Z"))         # insert empty
    check("row starts with NULL locking_script", script(conn, 1) is None)
    conn.execute(UPSERT, (1, S2, "2026-06-29T00:00:05Z"))           # newer, fill
    check("newer push fills an empty locking_script", script(conn, 1) == S2)

    # 4) newer-gated: an OLDER push never fills even an empty column.
    conn.execute(UPSERT, (2, None, "2026-06-29T00:00:05Z"))        # insert empty @ t5
    conn.execute(UPSERT, (2, S1, "2026-06-29T00:00:01Z"))         # older @ t1
    check("older push does not fill an empty locking_script", script(conn, 2) is None)

    # 5) idempotent: re-applying the same row changes nothing.
    before = script(conn, 0)
    conn.execute(UPSERT, (0, S1, "2026-06-29T00:00:01Z"))
    check("re-apply is idempotent", script(conn, 0) == before == S1)

    print("FAILED" if check.failed else "ALL F7-2 INLINE-BLOB INVARIANTS PROVEN")
    sys.exit(1 if check.failed else 0)

if __name__ == "__main__":
    main()
