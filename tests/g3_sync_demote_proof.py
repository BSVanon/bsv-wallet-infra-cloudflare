#!/usr/bin/env python3
"""
G3 proof: the LIVE CF worker apply path (src/storage/sync_apply.rs
build_upsert_output) accepts a producer-signalled legit demote WITHOUT
weakening the funds-monotonic guard.

D1 IS SQLite, so this proves the new spendable CASE arm offline before deploy.
The CF sync_apply SQL is a SEPARATE code path from the engine wasm/sqlx guard
(Codex 847e59c0 asked it be pinned independently).

The G3 arm (mirrors sync_apply.rs exactly):
    WHEN ? = 1 AND outputs.spent_by IS NULL AND excluded.updated_at > outputs.updated_at THEN 0
The `?` is the Rust-computed demote flag = (sync_demote AND NOT incoming.spendable),
bound AFTER the INSERT columns (a positional `?` in the ON CONFLICT clause).

INVARIANTS PROVEN:
  - POSITIVE: a NEWER incoming with sync_demote=1 + spendable=0 + no live spend
    ref DOES demote (kills the cloud phantom).
  - CONTRADICTORY: sync_demote=1 but spendable=1 → flag is 0 → NO demote
    (a malformed row can't strand a good UTXO).
  - OLDER-STALE: sync_demote=1 + spendable=0 but OLDER updated_at → NO demote
    (newer-wins guard holds; Codex 847e59c0 blocker).
  - BARE (no signal): incoming spendable=0 without sync_demote → NO demote
    (the pre-G3 monotonic guard is unchanged).
  - NEVER override a local spend: a local spent_by row is untouched by the arm.
"""
import sqlite3, sys

SCHEMA = """
CREATE TABLE outputs (
  output_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  transaction_id INTEGER NOT NULL,
  vout INTEGER NOT NULL,
  spent_by INTEGER, spendable INTEGER NOT NULL DEFAULT 0, change INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  UNIQUE(transaction_id, vout, user_id));
"""

# The spendable CASE mirrors sync_apply.rs::build_upsert_output 1:1 (the arms
# that matter for G3). The sync_demote `?` appears AFTER the 7 INSERT params.
UPSERT = """
INSERT INTO outputs (user_id, transaction_id, vout, spent_by, spendable, change, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(transaction_id, vout, user_id) DO UPDATE SET
  spent_by = CASE WHEN outputs.spent_by IS NULL THEN excluded.spent_by ELSE outputs.spent_by END,
  spendable = CASE
      WHEN outputs.spent_by IS NOT NULL THEN 0
      WHEN excluded.spent_by IS NOT NULL THEN 0
      WHEN ? = 1 AND outputs.spent_by IS NULL AND excluded.updated_at > outputs.updated_at THEN 0
      WHEN outputs.spendable = 1 THEN 1
      WHEN excluded.updated_at > outputs.updated_at THEN excluded.spendable
      ELSE outputs.spendable END,
  change = CASE WHEN outputs.change = 1 THEN 1 WHEN excluded.updated_at > outputs.updated_at THEN excluded.change ELSE outputs.change END,
  updated_at = CASE WHEN excluded.updated_at > outputs.updated_at THEN excluded.updated_at ELSE outputs.updated_at END
"""

U = 7
T1, T2 = "2026-06-02T00:00:00Z", "2026-06-03T00:00:00Z"


def apply(con, tx_id, vout, spent_by, spendable, change, updated, sync_demote=False):
    # Mirror the Rust bind: flag = (sync_demote AND NOT incoming.spendable).
    flag = 1 if (sync_demote and not spendable) else 0
    con.execute(UPSERT, (U, tx_id, vout, spent_by,
                         1 if spendable else 0, 1 if change else 0, updated, flag))
    con.commit()


def get(con, tx_id, vout):
    return con.execute(
        "SELECT spent_by, spendable FROM outputs WHERE user_id=? AND transaction_id=? AND vout=?",
        (U, tx_id, vout)).fetchone()


def main():
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA)
    fails = []

    def check(name, cond):
        print(("  ok  " if cond else " FAIL ") + name)
        if not cond:
            fails.append(name)

    # 1) POSITIVE: local spendable, NEWER incoming demote w/ sync_demote → demotes.
    apply(con, 10, 0, None, True, True, T1)
    apply(con, 10, 0, None, False, True, T2, sync_demote=True)
    sb, sp = get(con, 10, 0)
    check("1 sync_demote legit demote applies: spendable 1->0", sb is None and sp == 0)

    # 2) CONTRADICTORY: sync_demote=1 but spendable=1 → flag 0 → NO demote.
    apply(con, 11, 0, None, True, True, T1)
    apply(con, 11, 0, None, True, True, T2, sync_demote=True)
    sb, sp = get(con, 11, 0)
    check("2 contradictory sync_demote+spendable=true: stays spendable", sp == 1)

    # 3) OLDER-STALE: sync_demote=1 + spendable=0 but OLDER → NO demote.
    apply(con, 12, 0, None, True, True, T2)              # local is NEWER (T2)
    apply(con, 12, 0, None, False, True, T1, sync_demote=True)  # incoming OLDER (T1)
    sb, sp = get(con, 12, 0)
    check("3 older-stale sync_demote does NOT demote newer local", sp == 1)

    # 4) BARE (no signal): newer incoming spendable=0, no sync_demote → NO demote.
    apply(con, 13, 0, None, True, True, T1)
    apply(con, 13, 0, None, False, True, T2)
    sb, sp = get(con, 13, 0)
    check("4 bare stale-race demote still rejected (no signal)", sp == 1)

    # 5) NEVER override a local spend: local spent_by set, incoming sync_demote.
    apply(con, 14, 0, 902, False, True, T1)
    apply(con, 14, 0, None, False, True, T2, sync_demote=True)
    sb, sp = get(con, 14, 0)
    check("5 local spend untouched by the arm: spent_by kept, spendable=0", sb == 902 and sp == 0)

    print()
    if fails:
        print(f"PROOF FAILED: {len(fails)} invariant(s) broken: {fails}")
        sys.exit(1)
    print("PROOF PASSED: G3 sync_demote accepts exactly the legit newer demote, "
          "rejects contradictory/older/unsignalled ones.")


if __name__ == "__main__":
    main()
