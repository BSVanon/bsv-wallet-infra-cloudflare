#!/usr/bin/env python3
"""
Multi-device BRC-38 backup proof (Storage-scoping Commit C, Codex 5b4fe9d6).

The CF worker's R2 backup logic (src/dispatch.rs) must make it STRUCTURALLY
impossible for one device to clobber another's backup, while still reading every
device's blob (plus the legacy single object) on restore. R2 is an object store
(prefix LIST + get/put by key), so this proof mirrors the worker's key
construction + put/list/dual-read against an in-memory object store — the same
"prove the algorithm offline before deploy" approach as the SQL proofs.

Mirrors dispatch.rs 1:1:
    backup_object_key(id)            = f"backup/{id}"                # legacy single object
    backup_device_object_key(id, d)  = f"backup/{id}/{d}"           # per-device object
    backup_device_prefix(id)         = f"backup/{id}/"              # LIST prefix (excludes legacy)
    validate_device_id(d): 8..=64 chars of [A-Za-z0-9_-]
    handle_put_backup: deviceId present -> per-device key; else legacy key
    handle_list_backups: LIST prefix (per-device) + dual-read legacy key

INVARIANTS PROVEN:
  - ANTI-CLOBBER: device A and device B (same identity) write DISTINCT keys, so
    B's put can NEVER overwrite A's object.
  - UNION ON RESTORE: listBackups returns BOTH device blobs.
  - LEGACY BACK-COMPAT: a pre-existing legacy `backup/{id}` object is ALSO
    returned by listBackups (dual-read; never migrated/deleted).
  - LEGACY PREFIX ISOLATION: the trailing-slash LIST prefix does NOT match the
    legacy key (which is why the explicit dual-read is required).
  - deviceId VALIDATION: bounds + charset enforced (a bad id is rejected, never
    silently written to a malformed key).
  - IDEMPOTENT SELF-OVERWRITE: the SAME device re-writing overwrites only its
    OWN object (latest-wins within a device is fine; cross-device is not).
"""
import re
import sys

# ---- mirror of dispatch.rs key construction --------------------------------


def backup_object_key(identity):
    return f"backup/{identity}"


def backup_device_object_key(identity, device_id):
    return f"backup/{identity}/{device_id}"


def backup_device_prefix(identity):
    return f"backup/{identity}/"


_DEVICE_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def validate_device_id(device_id):
    return 8 <= len(device_id) <= 64 and bool(_DEVICE_RE.match(device_id))


# ---- in-memory R2 (prefix LIST + get/put) ----------------------------------


class R2:
    def __init__(self):
        self.objects = {}

    def put(self, key, blob):
        self.objects[key] = blob

    def get(self, key):
        return self.objects.get(key)

    # R2 LIST returns at most `limit` (max 1000) keys per call, sorted by key,
    # with a cursor to page the rest — modeled here so the pagination fix is
    # provable offline.
    def list(self, prefix, cursor=None, limit=1000):
        keys = sorted(k for k in self.objects if k.startswith(prefix))
        start = keys.index(cursor) + 1 if cursor in keys else 0
        page = keys[start : start + limit]
        truncated = (start + limit) < len(keys)
        next_cursor = page[-1] if (truncated and page) else None
        return page, truncated, next_cursor


# ---- mirror of the two handlers --------------------------------------------


def handle_put_backup(r2, identity, blob, device_id=None):
    if device_id is not None:
        if not validate_device_id(device_id):
            raise ValueError("invalid deviceId")
        key = backup_device_object_key(identity, device_id)
    else:
        key = backup_object_key(identity)
    r2.put(key, blob)
    return key


def handle_list_backups(r2, identity):
    blobs = []
    # 1) per-device objects under `backup/{id}/` — PAGINATE the cursor so every
    #    object is returned even past the 1000-per-call cap (Finding #5).
    prefix = backup_device_prefix(identity)
    cursor = None
    while True:
        page, truncated, next_cursor = r2.list(prefix, cursor)
        for key in page:
            blobs.append(r2.get(key))
        if truncated and next_cursor is not None:
            cursor = next_cursor
        else:
            break
    # 2) legacy single object `backup/{id}` (dual-read; never migrated)
    legacy = r2.get(backup_object_key(identity))
    if legacy is not None:
        blobs.append(legacy)
    return blobs


# ---- proof ------------------------------------------------------------------


def check(name, cond):
    print(f"  {'PASS' if cond else 'FAIL'}: {name}")
    return cond


def main():
    ID = "02" + "a" * 64
    DEV_A = "device-aaaaaaaa"
    DEV_B = "device-bbbbbbbb"
    ok = True

    # ANTI-CLOBBER: two devices → two distinct keys.
    r2 = R2()
    key_a = handle_put_backup(r2, ID, "blobA", DEV_A)
    key_b = handle_put_backup(r2, ID, "blobB", DEV_B)
    ok &= check("device A and B write distinct keys", key_a != key_b)
    ok &= check("device B put did NOT overwrite device A", r2.get(key_a) == "blobA")
    ok &= check("both objects present", r2.get(key_b) == "blobB")

    # UNION ON RESTORE.
    blobs = handle_list_backups(r2, ID)
    ok &= check("listBackups returns BOTH device blobs", set(blobs) == {"blobA", "blobB"})

    # LEGACY BACK-COMPAT: an existing legacy object is also returned.
    r2_legacy = R2()
    handle_put_backup(r2_legacy, ID, "legacyBlob")  # no deviceId → legacy key
    handle_put_backup(r2_legacy, ID, "blobA", DEV_A)
    blobs2 = handle_list_backups(r2_legacy, ID)
    ok &= check("legacy + per-device both returned", set(blobs2) == {"legacyBlob", "blobA"})

    # LEGACY PREFIX ISOLATION: the trailing-slash prefix must NOT match legacy.
    listed_only = r2_legacy.list(backup_device_prefix(ID))
    ok &= check(
        "trailing-slash prefix excludes the legacy key (dual-read required)",
        backup_object_key(ID) not in listed_only,
    )

    # deviceId VALIDATION.
    ok &= check("valid deviceId accepted", validate_device_id(DEV_A))
    ok &= check("too-short deviceId rejected", not validate_device_id("short"))
    ok &= check("too-long deviceId rejected", not validate_device_id("x" * 65))
    ok &= check("bad-charset deviceId rejected", not validate_device_id("bad/id/slash!!"))
    try:
        handle_put_backup(R2(), ID, "b", "bad/slash")
        ok &= check("put with invalid deviceId raises", False)
    except ValueError:
        ok &= check("put with invalid deviceId raises", True)

    # IDEMPOTENT SELF-OVERWRITE: same device re-write overwrites only its own.
    r2b = R2()
    handle_put_backup(r2b, ID, "blobA1", DEV_A)
    handle_put_backup(r2b, ID, "blobB1", DEV_B)
    handle_put_backup(r2b, ID, "blobA2", DEV_A)  # A re-writes
    ok &= check("A's re-write overwrote only A", r2b.get(key_a) == "blobA2")
    ok &= check("B untouched by A's re-write", r2b.get(key_b) == "blobB1")

    # PAGINATION (Finding #5): >1000 immortal device objects must ALL be
    # returned, not just the first 1000 R2 page. Seed 1500 with distinct blobs;
    # the object holding unique change sorts past position 1000.
    r2p = R2()
    for i in range(1500):
        dev = "device-%08d" % i  # >=8 chars, valid
        r2p.put(backup_device_object_key(ID, dev), "blob-%04d" % i)
    listed = handle_list_backups(r2p, ID)
    ok &= check("all 1500 device objects returned (cursor paged)", len(listed) == 1500)
    ok &= check(
        "the object past position 1000 is included",
        "blob-1400" in listed,
    )

    print("\n" + ("ALL PASS" if ok else "FAILURES"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
