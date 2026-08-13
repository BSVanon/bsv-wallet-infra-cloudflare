#!/usr/bin/env python3
"""
Chunked BRC-38 backup proof — R2 native multipart (413 launch fix).

A wallet's encrypted §6-closure backup grows without bound as history grows.
`putBackup` delivers the whole blob in ONE request body, but the Worker rejects
any body over MAX_REQUEST_BODY_BYTES (8 MiB) to avoid an OOM mid-parse — so past
a point every backup 413s and the funds-map can no longer be made durable. The
fix chunks the WRITE with R2 multipart (startBackup / uploadBackupPart /
completeBackup) WITHOUT touching the read/restore path: the parts assemble into
the SAME single object at the SAME key `putBackup` would have written, so
getBackup / listBackups are byte-identical and the BRC-2 AEAD tag still verifies
the whole blob on decrypt.

Same "prove the algorithm offline before deploy" approach as the other proofs:
mirror dispatch.rs + storage_client.rs 1:1 against an in-memory R2, and assert
the funds-critical invariants. R2's native multipart storage semantics
(equal-non-final parts, atomic complete) are battle-tested and verified live
end-to-end post-deploy on the real large-history wallet.

Mirrors:
    storage_client.rs  BACKUP_SINGLE_PUT_MAX = BACKUP_PART_SIZE = 5 MiB
    storage_client.rs  split_and_encode_backup_parts: raw.chunks(part_size)
    storage_client.rs  put_backup: raw<=MAX -> single putBackup; else multipart
    dispatch.rs        backup_write_key(identity, deviceId): per-device | legacy
    dispatch.rs        handle_upload_backup_part: store part bytes by partNumber
    dispatch.rs        handle_complete_backup: sort parts ASC by partNumber, join

INVARIANTS PROVEN:
  - ROUND-TRIP BYTE-IDENTITY: a blob written via multipart reads back EXACTLY,
    across boundary sizes (sub-part, exact multiple, remainder, many parts).
  - KEY IDENTITY: the multipart path lands at the SAME object key the single
    `putBackup` writes (per-device AND legacy) — else restore would read a stale
    or empty object (silent funds-map loss).
  - ORDER-INDEPENDENCE: parts delivered/collected out of order still reassemble
    correctly (completeBackup sorts ascending by partNumber, as R2 requires).
  - EQUAL NON-FINAL PARTS: every part except the last is exactly PART_SIZE and
    >= R2's 5 MiB minimum; the final part is the (smaller) remainder.
  - THRESHOLD: a blob at/under 5 MiB takes the wire-unchanged single putBackup;
    only larger blobs use multipart (the common small-wallet path never changes).
  - SIZE FIT: each multipart part's base64 request stays under the 8 MiB cap.
"""
import base64
import re
import sys

MiB = 1024 * 1024
BACKUP_SINGLE_PUT_MAX = 5 * MiB  # storage_client.rs
BACKUP_PART_SIZE = 5 * MiB       # storage_client.rs
MAX_REQUEST_BODY_BYTES = 8 * MiB  # lib.rs

_DEVICE_RE = re.compile(r"^[A-Za-z0-9_-]+$")


# ---- mirror of dispatch.rs key construction (shared with putBackup) ---------

def backup_object_key(identity):
    return f"backup/{identity}"


def backup_device_object_key(identity, device_id):
    return f"backup/{identity}/{device_id}"


def validate_device_id(device_id):
    return 8 <= len(device_id) <= 64 and bool(_DEVICE_RE.match(device_id))


def backup_write_key(identity, device_id):
    """dispatch.rs backup_write_key — the SINGLE source of truth for the object
    key across putBackup AND completeBackup."""
    if device_id is None:
        return backup_object_key(identity)
    assert validate_device_id(device_id), f"bad deviceId {device_id!r}"
    return backup_device_object_key(identity, device_id)


# ---- in-memory R2 with a native-multipart mirror ----------------------------

class FakeR2:
    """Prefix-keyed object store + a mirror of R2 multipart: create/upload/
    complete. complete() is the ATOMIC commit — the object at `key` only changes
    when complete() runs (an interrupted upload leaves the prior object intact).
    """

    def __init__(self):
        self.objects = {}          # key -> bytes (committed)
        self._uploads = {}         # uploadId -> {"key":.., "parts": {n: bytes}}
        self._next = 1

    # single-shot put (handle_put_backup)
    def put(self, key, data: bytes):
        self.objects[key] = data

    def get(self, key):
        return self.objects.get(key)

    # multipart (handle_start_backup / upload_backup_part / complete_backup)
    def create_multipart_upload(self, key):
        uid = f"mpu-{self._next}"
        self._next += 1
        self._uploads[uid] = {"key": key, "parts": {}}
        return uid

    def upload_part(self, uid, key, part_number: int, data: bytes):
        up = self._uploads[uid]
        assert up["key"] == key, "resume key must match the created upload's key"
        up["parts"][part_number] = data
        # R2 returns an etag per part; the etag identifies the stored part.
        return f'etag-{uid}-{part_number}'

    def complete(self, uid, key, parts_meta):
        """parts_meta: list of {"partNumber": n, "etag": ..} in CLIENT order
        (possibly unsorted). R2 requires ascending order — sort defensively,
        exactly as handle_complete_backup does."""
        up = self._uploads[uid]
        assert up["key"] == key
        ordered = sorted(parts_meta, key=lambda p: p["partNumber"])
        blob = b"".join(up["parts"][p["partNumber"]] for p in ordered)
        self.objects[key] = blob        # ATOMIC commit
        del self._uploads[uid]
        return len(blob)


# ---- mirror of storage_client.rs put_backup orchestration -------------------

def split_parts(raw: bytes, part_size: int):
    """storage_client.rs raw.chunks(part_size): every part == part_size except
    the last (remainder)."""
    return [raw[i:i + part_size] for i in range(0, len(raw), part_size)] or []


def engine_put_backup(r2: FakeR2, identity, device_id, blob: bytes,
                      shuffle_parts=False):
    """Mirror of StorageClient::put_backup. Returns ('single'|'multipart', n)."""
    key = backup_write_key(identity, device_id)  # SAME key both paths
    if len(blob) <= BACKUP_SINGLE_PUT_MAX:
        r2.put(key, blob)  # single putBackup (wire-unchanged)
        return ("single", 1)

    # multipart: start -> upload each part -> complete
    uid = r2.create_multipart_upload(key)
    parts = split_parts(blob, BACKUP_PART_SIZE)
    parts_meta = []
    for i, chunk in enumerate(parts):
        part_number = i + 1  # 1-indexed
        # transport is base64 in the JSON-RPC body; assert it fits the cap.
        body_b64 = base64.b64encode(chunk)
        assert len(body_b64) < MAX_REQUEST_BODY_BYTES, (
            f"part {part_number} base64 {len(body_b64)} exceeds body cap")
        etag = r2.upload_part(uid, key, part_number, chunk)
        parts_meta.append({"partNumber": part_number, "etag": etag})

    if shuffle_parts:  # prove order-independence: client hands them back jumbled
        parts_meta = list(reversed(parts_meta))

    r2.complete(uid, key, parts_meta)
    # non-final parts are all exactly PART_SIZE and >= R2's 5 MiB minimum
    for chunk in parts[:-1]:
        assert len(chunk) == BACKUP_PART_SIZE >= 5 * MiB
    assert 0 < len(parts[-1]) <= BACKUP_PART_SIZE
    return ("multipart", len(parts))


# ---- restore read (handle_get_backup / handle_list_backups) -----------------

def read_back(r2: FakeR2, identity, device_id):
    """Byte-identical read the restore path performs — unchanged by chunking."""
    return r2.get(backup_write_key(identity, device_id))


# ---------------------------------------------------------------------------

def main():
    ID = "02" + "a" * 64  # a stand-in compressed identity key
    DEV = "device-abc123"  # 8..64 [A-Za-z0-9_-]
    fails = 0

    def check(name, cond):
        nonlocal fails
        print(f"  {'PASS' if cond else 'FAIL'}  {name}")
        if not cond:
            fails += 1

    print("Chunked BRC-38 backup — R2 multipart proof\n")

    # deterministic, position-dependent payload so any drop/reorder is caught
    def payload(n):
        return bytes((i * 131 + 7) & 0xFF for i in range(n))

    # 1) ROUND-TRIP across boundary sizes (per-device key)
    print("[1] round-trip byte-identity across sizes")
    sizes = {
        "empty": 0,
        "tiny": 10,
        "just-under-single": BACKUP_SINGLE_PUT_MAX - 1,
        "exactly-single-max": BACKUP_SINGLE_PUT_MAX,
        "one-over (first multipart)": BACKUP_SINGLE_PUT_MAX + 1,
        "1.5 parts": BACKUP_PART_SIZE + BACKUP_PART_SIZE // 2,
        "exact 2 parts": 2 * BACKUP_PART_SIZE,
        "exact 3 parts": 3 * BACKUP_PART_SIZE,
        "3 parts + remainder": 3 * BACKUP_PART_SIZE + 777,
        "gate-wallet ~6.9MiB ciphertext": 6900 * 1024,
    }
    for label, n in sizes.items():
        r2 = FakeR2()
        blob = payload(n)
        mode, nparts = engine_put_backup(r2, ID, DEV, blob)
        got = read_back(r2, ID, DEV)
        expect_mode = "single" if n <= BACKUP_SINGLE_PUT_MAX else "multipart"
        check(f"{label} ({n} B): {mode}/{nparts}p, reads back exact",
              mode == expect_mode and (got or b"") == blob)

    # 2) KEY IDENTITY: multipart lands where single putBackup / restore read
    print("[2] key identity (multipart == single putBackup key)")
    r2 = FakeR2()
    big = payload(3 * BACKUP_PART_SIZE + 123)
    engine_put_backup(r2, ID, DEV, big)
    check("per-device: object exists at backup/{id}/{dev}",
          backup_device_object_key(ID, DEV) in r2.objects)
    check("per-device: only that one key was written",
          list(r2.objects.keys()) == [backup_device_object_key(ID, DEV)])
    r2l = FakeR2()
    engine_put_backup(r2l, ID, None, big)  # legacy (no deviceId)
    check("legacy: object exists at backup/{id}",
          backup_object_key(ID) in r2l.objects and
          read_back(r2l, ID, None) == big)

    # 3) ORDER-INDEPENDENCE: client returns parts jumbled -> still exact
    print("[3] order-independence (parts completed out of order)")
    r2 = FakeR2()
    blob = payload(4 * BACKUP_PART_SIZE + 999)
    engine_put_backup(r2, ID, DEV, blob, shuffle_parts=True)
    check("reversed part order still reassembles byte-identical",
          read_back(r2, ID, DEV) == blob)

    # 4) ATOMICITY: an interrupted multipart (no complete) leaves prior object
    print("[4] atomicity (interrupted upload keeps the prior good backup)")
    r2 = FakeR2()
    good = payload(2 * BACKUP_PART_SIZE)          # first, complete a good backup
    engine_put_backup(r2, ID, DEV, good)
    key = backup_write_key(ID, DEV)
    uid = r2.create_multipart_upload(key)          # then start a new one...
    r2.upload_part(uid, key, 1, payload(BACKUP_PART_SIZE))  # ...upload 1 part...
    # ...and DON'T complete (simulated crash). The committed object is untouched.
    check("prior backup still fully readable after an interrupted upload",
          r2.get(key) == good)

    # 5) SIZE FIT: worst-case single-put + part base64 both under the 8 MiB cap
    print("[5] size fit under the 8 MiB body cap")
    single_b64 = ((BACKUP_SINGLE_PUT_MAX + 2) // 3) * 4
    part_b64 = ((BACKUP_PART_SIZE + 2) // 3) * 4
    check(f"single-put base64 {single_b64} < cap", single_b64 < MAX_REQUEST_BODY_BYTES)
    check(f"multipart part base64 {part_b64} < cap", part_b64 < MAX_REQUEST_BODY_BYTES)
    check("part size clears R2's 5 MiB minimum", BACKUP_PART_SIZE >= 5 * MiB)

    print()
    if fails:
        print(f"RESULT: {fails} FAILED")
        sys.exit(1)
    print("RESULT: all invariants proven")


if __name__ == "__main__":
    main()
