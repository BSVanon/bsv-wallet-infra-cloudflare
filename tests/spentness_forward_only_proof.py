#!/usr/bin/env python3
"""
A2-plus proof: forward-only spent_by sync keeps the balance EXACT — it surfaces a
spent output as not-spendable on restore, NEVER hides a live output, and NEVER
un-spends a locally-reserved output via a (possibly stale) remote.

Canonical (TS+Go) tracks spent state via spent_by+spendable. Our two stores have
an unreliable updated_at, so we sync spent_by FORWARD-ONLY: a remote may SET a
locally-NULL spent_by (proving a spend by naming its tx), but the CLEAR comes
ONLY from the local review_status tx-status cascade — never a sync merge. This
replicates the exact build_upsert_output ON CONFLICT (sync_apply.rs) against
in-memory SQLite (D1 IS SQLite) before deploy. The engine upsert_output uses the
same semantics.

INVARIANTS PROVEN:
  - restore-insert of a spent output → spendable=0 / spent_by set (excluded from
    balance + allocator);
  - forward-only SET: local-unspent + incoming-proves-spend → spent_by set,
    spendable demoted 1→0;
  - NEVER clear: local-spent + bare incoming (spent_by NULL) → keeps spent_by,
    stays spendable=0 (a stale remote can't un-spend a reservation);
  - demote ONLY with a proven spend: local-unspent + bare newer incoming →
    spendable stays 1 (funds-monotonic; a stale remote can't strand funds);
  - spent stays not-spendable: a spent output is never promoted to spendable by a
    sync (the over-count bug this guards) — only review_status clears it;
  - promote works for UNSPENT: pending (spent_by NULL, spendable 0) → newer
    spendable=1 promotes;
  - idempotent.
"""
import sqlite3, sys

SCHEMA = """
CREATE TABLE outputs (
  output_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL, transaction_id INTEGER NOT NULL, vout INTEGER NOT NULL,
  spent_by INTEGER, spendable INTEGER NOT NULL DEFAULT 0, change INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL, UNIQUE(transaction_id, vout, user_id));
"""

# The exact forward-only ON CONFLICT from build_upsert_output (sync_apply.rs).
UPSERT = """
INSERT INTO outputs (user_id, transaction_id, vout, spent_by, spendable, change, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(transaction_id, vout, user_id) DO UPDATE SET
  spent_by = CASE WHEN outputs.spent_by IS NULL THEN excluded.spent_by ELSE outputs.spent_by END,
  spendable = CASE
      WHEN outputs.spent_by IS NOT NULL THEN 0
      WHEN excluded.spent_by IS NOT NULL THEN 0
      WHEN outputs.spendable = 1 THEN 1
      WHEN excluded.updated_at > outputs.updated_at THEN excluded.spendable
      ELSE outputs.spendable END,
  change = CASE WHEN outputs.change = 1 THEN 1 WHEN excluded.updated_at > outputs.updated_at THEN excluded.change ELSE outputs.change END,
  updated_at = CASE WHEN excluded.updated_at > outputs.updated_at THEN excluded.updated_at ELSE outputs.updated_at END
"""

U = 7
T0, T1, T2 = "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", "2026-06-03T00:00:00Z"


def apply(con, tx_id, vout, spent_by, spendable, change, updated):
    con.execute(UPSERT, (U, tx_id, vout, spent_by, 1 if spendable else 0, 1 if change else 0, updated))
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

    # 1) restore-insert a SPENT output (spent_by=900, spendable=0)
    apply(con, 10, 0, 900, False, True, T1)
    sb, sp = get(con, 10, 0)
    check("1 restore-insert spent output: spent_by set, spendable=0", sb == 900 and sp == 0)

    # 2) local UNSPENT (spendable=1), incoming PROVES spend (spent_by=901, newer)
    apply(con, 11, 0, None, True, True, T1)
    apply(con, 11, 0, 901, False, True, T2)
    sb, sp = get(con, 11, 0)
    check("2 forward-only SET + demote: spent_by set, spendable=0", sb == 901 and sp == 0)

    # 3) local SPENT (spent_by=902), incoming BARE newer (spent_by NULL, spendable=1)
    apply(con, 12, 0, 902, False, True, T1)
    apply(con, 12, 0, None, True, True, T2)
    sb, sp = get(con, 12, 0)
    check("3 NEVER clear a local spend via sync: spent_by kept, spendable=0", sb == 902 and sp == 0)

    # 4) local UNSPENT (spendable=1), incoming BARE newer (spent_by NULL, spendable=0)
    apply(con, 13, 0, None, True, True, T1)
    apply(con, 13, 0, None, False, True, T2)
    sb, sp = get(con, 13, 0)
    check("4 demote ONLY with a proven spend: stays spendable (no strand)", sb is None and sp == 1)

    # 5) pending UNSPENT (spent_by NULL, spendable=0), incoming PROMOTES (spendable=1, newer)
    apply(con, 14, 0, None, False, True, T1)
    apply(con, 14, 0, None, True, True, T2)
    sb, sp = get(con, 14, 0)
    check("5 promote works for an UNSPENT output: spendable 0->1", sb is None and sp == 1)

    # 6) idempotent re-apply of the spend (case 2) — no change
    before = get(con, 11, 0)
    apply(con, 11, 0, 901, False, True, T2)
    check("6 idempotent re-apply: unchanged", get(con, 11, 0) == before)

    print()
    if fails:
        print(f"PROOF FAILED: {len(fails)} invariant(s) broken: {fails}")
        sys.exit(1)
    print("PROOF PASSED: forward-only spent_by keeps the balance exact "
          "(surface spent, never hide live, never un-spend via sync).")


if __name__ == "__main__":
    main()
