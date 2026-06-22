# D1 storage restore runbook (B-6)

The wallet's durable funds metadata lives in the Cloudflare D1 database
**`wallet-infra`** (id `c0ec0482-009a-499d-8727-f44e6f8f5be2`, binding `DB`) +
the R2 bucket `wallet-infra-blobs`. For default-basket change (random
derivation), this stored metadata is the ONLY recovery path after a device wipe
— so D1 durability is load-bearing.

## What protects D1 (in order of defense)

1. **App-level non-destructiveness (live).** The sync apply path is
   funds-MONOTONIC and never deletes: the pull merge guard (`storage/sync.rs`
   `upsert_output`) can only ADD/PROMOTE, never demote spendable/change/basket;
   immutable scalars + blobs are fill-if-empty (`put_blob_column`), never
   overwritten. So a bad/stale client push cannot corrupt or delete a good row.
2. **Cloudflare D1 Time Travel (this runbook).** Automatic point-in-time
   recovery for EVERY D1 database — **no setup, no enabling, 30-day retention.**
   Every write creates an implicit restore point (a "bookmark"). This is the
   backstop for catastrophic events the app guard can't cover: a bad schema
   migration, an accidental mass operation, or a platform-level issue.

There is nothing to turn on. Time Travel is already active on `wallet-infra`.
This runbook is how you USE it.

## Verify Time Travel is healthy (do this periodically / before a risky change)

```bash
cd /home/robert/Documents/bsv-cf-stack/bsv-wallet-infra-cloudflare
npx wrangler d1 time-travel info wallet-infra
```

Prints the current bookmark + the earliest restorable timestamp (~30 days back).
If this returns a bookmark, PITR is working.

## Restore procedure (D1 corruption / bad migration / mass-delete)

> ⚠ A restore OVERWRITES the live database to the chosen point. All writes AFTER
> that point are rolled back. Time Travel automatically snapshots a bookmark
> BEFORE the restore, so a restore is itself undoable (see "Undo" below) — but
> treat it as a serious operation.

1. **Capture the current bookmark first** (so you can undo the restore):
   ```bash
   npx wrangler d1 time-travel info wallet-infra
   # → note the "current bookmark" value, save it somewhere
   ```

2. **Find the restore target.** Pick the last-known-good moment — just BEFORE
   the bad migration/deploy/incident. Use an ISO-8601 UTC timestamp:
   ```bash
   # Inspect what a candidate point looks like BEFORE committing to it:
   npx wrangler d1 time-travel info wallet-infra --timestamp="2026-06-22T17:00:00Z"
   ```

3. **Restore:**
   ```bash
   npx wrangler d1 time-travel restore wallet-infra --timestamp="2026-06-22T17:00:00Z"
   # or, if you have an exact bookmark:
   # npx wrangler d1 time-travel restore wallet-infra --bookmark="<bookmark>"
   ```

4. **Verify** the restore landed (row counts on the funds-facing tables):
   ```bash
   npx wrangler d1 execute wallet-infra --remote \
     --command "SELECT (SELECT COUNT(*) FROM outputs) AS outputs, (SELECT COUNT(*) FROM transactions) AS txs;"
   ```
   Sanity-check against what you expect for the restore point.

## Undo a restore (if you restored to the wrong point)

The restore itself created a bookmark of the pre-restore state. Restore forward
to the bookmark you captured in step 1:

```bash
npx wrangler d1 time-travel restore wallet-infra --bookmark="<bookmark-from-step-1>"
```

## R2 blobs are NOT covered by D1 Time Travel

`wallet-infra-blobs` (R2) holds the >4096-byte blobs (raw_tx, input_beef). R2
has no Time Travel. Mitigations already in place: those blobs are IMMUTABLE +
fill-if-empty (a stale push can't overwrite a populated one), and the D1 column
holds the inline copy for ≤4096-byte blobs (incl. all locking_scripts). If R2
durability ever needs a hard backstop, enable R2 bucket versioning separately —
not required for the current funds-safety model, noted here for completeness.

## When NOT to use this

Do NOT reach for Time Travel for ordinary "a client's row looks stale" cases —
the app guard + the client's own retry/idempotent push self-heal those, and a
restore would roll back OTHER users' legitimate writes. Time Travel is for
database-wide corruption, not per-row fixes.
