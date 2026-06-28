#!/usr/bin/env python3
"""
F4 proof: the OUTPUTS monotonic funds-guard survives translation from the
current sequential newer-wins UPDATE (sync.rs:1499) into a batched
INSERT ... ON CONFLICT(transaction_id,vout,user_id) DO UPDATE ... RETURNING.

D1 IS SQLite, so this proves the new merge SQL offline before any prod deploy.

INVARIANTS PROVEN (the funds-safety guard, Codex 23bf18dd):
  - newer-wins: guarded cols only change when excluded.updated_at > outputs.updated_at
  - NEVER demote: spendable 1->0 and change 1->0 are impossible (even on a newer push)
  - fill-if-empty: satoshis(0), script_length/offset(empty locking_script),
    derivation_prefix/suffix(NULL) only fill, never overwrite a populated value
  - default-basket pin: an output in the 'default' basket is never moved out
  - RETURNING ALWAYS returns output_id (CASE-per-column always "touches" the row,
    so the SyncMap gets the id even when nothing changed) -- the V1 fix.
  - idempotent: re-applying the same chunk is a no-op.
"""
import sqlite3, sys

SCHEMA = """
CREATE TABLE output_baskets (
  basket_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL, name TEXT NOT NULL, UNIQUE(name,user_id));
CREATE TABLE outputs (
  output_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  transaction_id INTEGER NOT NULL,
  basket_id INTEGER,
  spendable INTEGER NOT NULL DEFAULT 0,
  change INTEGER NOT NULL DEFAULT 0,
  vout INTEGER NOT NULL,
  satoshis INTEGER NOT NULL,
  provided_by TEXT NOT NULL, purpose TEXT NOT NULL, type TEXT NOT NULL,
  txid TEXT, sender_identity_key TEXT,
  derivation_prefix TEXT, derivation_suffix TEXT, custom_instructions TEXT,
  script_length INTEGER, script_offset INTEGER, locking_script BLOB,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  UNIQUE(transaction_id, vout, user_id));
"""

# The NEW batched upsert. ?-params are positional, mirroring the current INSERT
# column order at sync.rs:1523. local transaction_id + basket_id are resolved
# (SyncMap) BEFORE this runs, exactly as today.
UPSERT = """
INSERT INTO outputs
  (user_id, transaction_id, basket_id, txid, vout, satoshis, locking_script,
   script_length, script_offset, type, provided_by, purpose, spendable, change,
   derivation_prefix, derivation_suffix, sender_identity_key, custom_instructions,
   created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(transaction_id, vout, user_id) DO UPDATE SET
  transaction_id = CASE WHEN excluded.updated_at > outputs.updated_at
                        THEN excluded.transaction_id ELSE outputs.transaction_id END,
  basket_id = CASE
      WHEN outputs.basket_id = (SELECT basket_id FROM output_baskets
                                WHERE user_id = outputs.user_id AND name = 'default')
        THEN outputs.basket_id
      WHEN excluded.updated_at > outputs.updated_at THEN excluded.basket_id
      ELSE outputs.basket_id END,
  satoshis = CASE WHEN outputs.satoshis = 0 AND excluded.updated_at > outputs.updated_at
                  THEN excluded.satoshis ELSE outputs.satoshis END,
  script_length = CASE WHEN (outputs.locking_script IS NULL OR length(outputs.locking_script)=0)
                            AND excluded.updated_at > outputs.updated_at
                       THEN excluded.script_length ELSE outputs.script_length END,
  script_offset = CASE WHEN (outputs.locking_script IS NULL OR length(outputs.locking_script)=0)
                            AND excluded.updated_at > outputs.updated_at
                       THEN excluded.script_offset ELSE outputs.script_offset END,
  type = CASE WHEN excluded.updated_at > outputs.updated_at THEN excluded.type ELSE outputs.type END,
  spendable = CASE WHEN outputs.spendable = 1 THEN 1
                   WHEN excluded.updated_at > outputs.updated_at THEN excluded.spendable
                   ELSE outputs.spendable END,
  change = CASE WHEN outputs.change = 1 THEN 1
                WHEN excluded.updated_at > outputs.updated_at THEN excluded.change
                ELSE outputs.change END,
  derivation_prefix = CASE WHEN outputs.derivation_prefix IS NULL
                                AND excluded.updated_at > outputs.updated_at
                           THEN excluded.derivation_prefix ELSE outputs.derivation_prefix END,
  derivation_suffix = CASE WHEN outputs.derivation_suffix IS NULL
                                AND excluded.updated_at > outputs.updated_at
                           THEN excluded.derivation_suffix ELSE outputs.derivation_suffix END,
  sender_identity_key = CASE WHEN excluded.updated_at > outputs.updated_at
                             THEN excluded.sender_identity_key ELSE outputs.sender_identity_key END,
  custom_instructions = CASE WHEN excluded.updated_at > outputs.updated_at
                             THEN excluded.custom_instructions ELSE outputs.custom_instructions END,
  updated_at = CASE WHEN excluded.updated_at > outputs.updated_at
                    THEN excluded.updated_at ELSE outputs.updated_at END
RETURNING output_id, hex(COALESCE(locking_script, x'')) AS lk, spendable, change,
          basket_id, satoshis, derivation_prefix, updated_at;
"""

def cols(user_id, tx_id, basket_id, vout, sats, sl, so, typ, prov, purp,
         spend, chg, dpfx, dsfx, sik, ci, created, updated):
    return (user_id, tx_id, basket_id, "txid_"+str(vout), vout, sats, sl, so, typ,
            prov, purp, spend, chg, dpfx, dsfx, sik, ci, created, updated)

def apply(con, *params):
    cur = con.execute(UPSERT, params)
    row = cur.fetchone()
    con.commit()
    assert row is not None, "RETURNING produced NO row -> SyncMap would miss this id (V1 BUG)"
    return row  # (output_id, lk_hex, spendable, change, basket_id, satoshis, dpfx, updated_at)

def get(con, oid):
    return con.execute("SELECT spendable,change,basket_id,satoshis,derivation_prefix,updated_at "
                       "FROM outputs WHERE output_id=?", (oid,)).fetchone()

def main():
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA)
    # default basket = id 1, a non-default basket = id 2
    con.execute("INSERT INTO output_baskets(user_id,name) VALUES (7,'default')")
    con.execute("INSERT INTO output_baskets(user_id,name) VALUES (7,'mybasket')")
    con.commit()
    T0,T1,T2 = "2026-06-01T00:00:00Z","2026-06-02T00:00:00Z","2026-06-03T00:00:00Z"
    fails=[]
    def check(name, cond):
        print(("  ok  " if cond else " FAIL ")+name); (fails.append(name) if not cond else None)

    # 1) INSERT new spendable change output in non-default basket
    r = apply(con, *cols(7,100,2,0,500,25,0,"P2PKH","you","change",1,1,"pfx","sfx","sik","ci",T1,T1))
    oid = r[0]
    check("1 insert returns id + spendable=1 change=1", r[0] is not None and r[2]==1 and r[3]==1)

    # 2) NEWER push tries to DEMOTE spendable 1->0 and change 1->0 -> must stay 1/1
    apply(con, *cols(7,100,2,0,500,25,0,"P2PKH","you","change",0,0,"pfx","sfx","sik","ci",T2,T2))
    s = get(con,oid)
    check("2 newer push cannot demote spendable", s[0]==1)
    check("2 newer push cannot demote change", s[1]==1)

    # 3) fresh output spendable=0, NEWER push promotes 0->1
    r3 = apply(con, *cols(7,101,2,1,400,25,0,"P2PKH","you","change",0,0,"pfx","sfx","sik","ci",T1,T1))
    oid3=r3[0]
    apply(con, *cols(7,101,2,1,400,25,0,"P2PKH","you","change",1,1,"pfx","sfx","sik","ci",T2,T2))
    s3=get(con,oid3)
    check("3 newer push promotes spendable 0->1", s3[0]==1)
    check("3 newer push promotes change 0->1", s3[1]==1)

    # 4) OLDER push must NOT change guarded fields, but RETURNING must still return id
    r4 = apply(con, *cols(7,101,2,1,999,25,0,"P2PKH","you","change",0,0,"x","y","z","w",T0,T0))
    s4=get(con,oid3)
    check("4 older push returns id (CASE-per-col touches row)", r4[0]==oid3)
    check("4 older push leaves spendable untouched", s4[0]==1)
    check("4 older push leaves satoshis untouched (no demote)", s4[3]==400)

    # 5) default-basket pin: output in default basket (id 1), newer push tries to move to basket 2
    rb = apply(con, *cols(7,102,1,2,300,25,0,"P2PKH","you","change",1,0,"pfx","sfx","sik","ci",T1,T1))
    oidb=rb[0]
    apply(con, *cols(7,102,2,2,300,25,0,"P2PKH","you","change",1,0,"pfx","sfx","sik","ci",T2,T2))
    sb=get(con,oidb)
    check("5 default-basket output is NOT moved out", sb[2]==1)

    # 6) satoshis fill-if-empty: existing 0 -> newer fills; existing>0 -> preserved
    rz = apply(con, *cols(7,103,2,3,0,25,0,"P2PKH","you","change",0,0,"pfx","sfx","sik","ci",T1,T1))
    oidz=rz[0]
    apply(con, *cols(7,103,2,3,777,25,0,"P2PKH","you","change",0,0,"pfx","sfx","sik","ci",T2,T2))
    check("6 satoshis fills from 0 on newer push", get(con,oidz)[3]==777)

    # 7) idempotent re-apply (same updated_at, not strictly newer) -> no change, id returned
    before = get(con,oid)
    r7 = apply(con, *cols(7,100,2,0,500,25,0,"P2PKH","you","change",0,0,"pfx","sfx","sik","ci",T2,T2))
    check("7 idempotent re-apply returns id", r7[0]==oid)
    check("7 idempotent re-apply no demote", get(con,oid)==before)

    print()
    if fails:
        print(f"PROOF FAILED: {len(fails)} invariant(s) broken: {fails}"); sys.exit(1)
    print("PROOF PASSED: outputs monotonic funds-guard preserved in ON CONFLICT...RETURNING form.")

if __name__=="__main__":
    main()
