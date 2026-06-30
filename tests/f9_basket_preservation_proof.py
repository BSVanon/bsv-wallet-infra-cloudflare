#!/usr/bin/env python3
"""
F9 proof: a synced output retains its basket across a STATELESS chunk-by-chunk
apply, so a restored wallet counts its change instead of under-reporting it.

ROOT CAUSE (verified on prod D1 + WhatsOnChain): process_sync_chunk builds its
foreign->local basket_id_map PER CHUNK from chunk.output_baskets, and the apply
is stateless across chunk POSTs. Baskets ride an early chunk; outputs span later
chunks -> every output after the basket chunk missed the (now-empty) per-chunk
map and was stored with basket_id=NULL. On restore, that change left the
'default' basket and was no longer counted as spendable (iOS user 368: 79 rows /
635,533 sats NULL-basket vs 9 rows / 902 sats in 'default' = the ~900 shown).

THE FIX (Codex-locked resolve-by-name): the engine producer now carries the
basket NAME on each output sync row; on a per-chunk map MISS, the apply resolves
the local basket by name from output_baskets (the basket row already exists in
D1 from its earlier chunk). This proof replicates that resolution against
in-memory SQLite -- D1 IS SQLite, so it proves the logic offline before deploy.

INVARIANTS PROVEN:
  - cross-chunk: an output whose foreign basket_id is NOT in this chunk's map but
    whose basket_name='default' resolves to the LOCAL default basket (not NULL).
  - regression: the OLD map-only logic would have stored NULL for that output.
  - same-chunk: an output whose foreign basket_id IS in the chunk map still
    resolves via the map (the fast path is unchanged).
  - non-default basket resolves by its own name (not collapsed into default).
  - legacy/no-name: a name-less map-miss is still NULL here (the engine restore
    safety-net files change=1 NULL-basket rows into 'default' on the client; that
    is covered by the engine unit tests, not this CF-side proof).
"""
import sqlite3, sys

SCHEMA = """
CREATE TABLE output_baskets (
  basket_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL, name TEXT NOT NULL, UNIQUE(name,user_id));
CREATE TABLE outputs (
  output_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL, transaction_id INTEGER NOT NULL,
  basket_id INTEGER, txid TEXT, vout INTEGER NOT NULL, change INTEGER NOT NULL DEFAULT 0,
  UNIQUE(transaction_id, vout, user_id));
"""


def name_to_local(con, user_id):
    """Mirror StorageD1::load_basket_name_map -- name -> local basket_id."""
    rows = con.execute(
        "SELECT name, basket_id FROM output_baskets WHERE user_id=?", (user_id,)
    ).fetchall()
    return {name: bid for (name, bid) in rows}


def resolve_new(foreign_basket_id, basket_name, chunk_map, names):
    """NEW F9 apply: per-chunk map first, then resolve-by-name fallback."""
    if foreign_basket_id is not None and foreign_basket_id in chunk_map:
        return chunk_map[foreign_basket_id]
    if basket_name is not None and basket_name in names:
        return names[basket_name]
    return None


def resolve_old(foreign_basket_id, chunk_map):
    """OLD (buggy) apply: per-chunk map ONLY -> NULL on cross-chunk miss."""
    if foreign_basket_id is not None:
        return chunk_map.get(foreign_basket_id)
    return None


def insert_output(con, user_id, tx_id, local_basket_id, vout, change):
    con.execute(
        "INSERT INTO outputs(user_id,transaction_id,basket_id,txid,vout,change) "
        "VALUES (?,?,?,?,?,?)",
        (user_id, tx_id, local_basket_id, "txid_" + str(vout), vout, change),
    )
    con.commit()
    return con.execute(
        "SELECT basket_id FROM outputs WHERE user_id=? AND vout=?", (user_id, vout)
    ).fetchone()[0]


def main():
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA)
    U = 7
    fails = []

    def check(name, cond):
        print(("  ok  " if cond else " FAIL ") + name)
        if not cond:
            fails.append(name)

    # ── CHUNK 1 (its own POST): output_baskets only. The producer's foreign ids
    # are 50 (default) and 51 (mybasket); they get fresh LOCAL autoincrement ids.
    con.execute("INSERT INTO output_baskets(user_id,name) VALUES (?, 'default')", (U,))
    default_local = con.execute(
        "SELECT basket_id FROM output_baskets WHERE user_id=? AND name='default'", (U,)
    ).fetchone()[0]
    con.execute("INSERT INTO output_baskets(user_id,name) VALUES (?, 'mybasket')", (U,))
    mybasket_local = con.execute(
        "SELECT basket_id FROM output_baskets WHERE user_id=? AND name='mybasket'", (U,)
    ).fetchone()[0]
    # The chunk-1 apply populated its per-chunk map: foreign -> local.
    chunk1_map = {50: default_local, 51: mybasket_local}

    # ── CHUNK 2 (a SEPARATE POST): outputs only. The apply is stateless, so its
    # per-chunk basket_id_map starts EMPTY -- this is the bug's trigger.
    chunk2_map = {}
    names = name_to_local(con, U)

    # 1) cross-chunk change output: foreign basket 50 absent from chunk2_map, but
    #    basket_name='default' -> resolves to the LOCAL default basket, not NULL.
    lb = resolve_new(50, "default", chunk2_map, names)
    stored = insert_output(con, U, 200, lb, 0, change=1)
    check("1 cross-chunk change resolves to default (not NULL)", stored == default_local)
    check("1 stored basket is non-NULL", stored is not None)

    # 2) regression: the OLD map-only logic would have stored NULL for that row.
    old_lb = resolve_old(50, chunk2_map)
    check("2 OLD logic would have stored NULL (the F9 bug)", old_lb is None)

    # 3) same-chunk fast path still works: foreign basket present in THIS map.
    lb3 = resolve_new(50, "default", chunk1_map, names)
    stored3 = insert_output(con, U, 201, lb3, 1, change=1)
    check("3 same-chunk map hit resolves via map", stored3 == default_local)

    # 4) a non-default basket resolves by ITS OWN name (not collapsed to default).
    lb4 = resolve_new(51, "mybasket", chunk2_map, names)
    stored4 = insert_output(con, U, 202, lb4, 2, change=0)
    check("4 non-default basket resolves by name", stored4 == mybasket_local)
    check("4 non-default not collapsed into default", stored4 != default_local)

    # 5) name-less map-miss is NULL here (engine restore safety-net handles it).
    lb5 = resolve_new(50, None, chunk2_map, names)
    check("5 name-less map-miss stays NULL on the CF side", lb5 is None)

    print()
    if fails:
        print(f"PROOF FAILED: {len(fails)} invariant(s) broken: {fails}")
        sys.exit(1)
    print("PROOF PASSED: resolve-by-name preserves the output basket across a "
          "stateless chunk-by-chunk apply.")


if __name__ == "__main__":
    main()
