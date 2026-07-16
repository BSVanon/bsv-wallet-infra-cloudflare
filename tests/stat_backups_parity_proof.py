#!/usr/bin/env python3
"""
Q4 statBackups parity proof (Codex `ed6581ae` / `7b1f6e8e`).

`statBackups` answers "is my encrypted backup REALLY in R2?" — the question a
user implicitly asks before wiping a device. It is the ONLY source permitted to
back a positive durability claim in the wallet UI, so it must be exactly as
complete as the funds-RESTORE path: if `statBackups` can't see an object that
`listBackups` would restore, the wallet's proof is lying by omission.

Mirrors dispatch.rs 1:1 (same key helpers as multi_device_backup_proof.py):
    backup_object_key(id)            = f"backup/{id}"        # legacy single object
    backup_device_object_key(id, d)  = f"backup/{id}/{d}"    # per-device object
    backup_device_prefix(id)         = f"backup/{id}/"       # LIST prefix (excludes legacy)
    handle_list_backups: LIST prefix (cursor-followed) + GET each body + dual-read legacy
    handle_stat_backups: LIST prefix (cursor-followed) + metadata only + HEAD legacy

INVARIANTS PROVEN:
  - PAGINATION PARITY: R2 LIST caps at 1000 keys/call and device objects are
    IMMORTAL (a fresh deviceId is minted on every full storage wipe), so an
    identity CAN exceed 1000. Both handlers must follow the cursor and see the
    IDENTICAL device set. A stat that stopped at 1000 would tell device #1001
    "not backed up" (fail-closed, merely wrong) — but worse, a stat that saw MORE
    than restore would claim a proof restore cannot honour.
  - LEGACY IS REPORTED BUT NEVER DEVICE-SCOPED: the pre-multi-device
    `backup/{identity}` object has NO device segment and may belong to ANY
    device. It must appear with deviceScoped=False / deviceId=None so the client
    can NEVER let it satisfy a current-device claim (change is randomly derived
    per device — another device's blob is not proof for this one).
  - METADATA ONLY: statBackups reads ZERO bodies. Answering an existence
    question must not cost a download of every blob the identity owns.
  - LEGACY PREFIX ISOLATION: the trailing-slash prefix does not match the legacy
    key, which is why the explicit legacy HEAD is required in both handlers.
"""

R2_LIST_PAGE_CAP = 1000  # R2's hard per-call cap — the reason the cursor exists


# ── key helpers (mirror dispatch.rs) ─────────────────────────────────────────
def backup_object_key(identity):
    return f"backup/{identity}"


def backup_device_object_key(identity, device_id):
    return f"backup/{identity}/{device_id}"


def backup_device_prefix(identity):
    return f"backup/{identity}/"


class FakeR2:
    """In-memory object store with R2's LIST paging + body/metadata split."""

    def __init__(self):
        self.objects = {}  # key -> {"body": bytes, "uploaded": int, "etag": str}
        self.body_reads = 0  # how many times a BODY was actually downloaded

    def put(self, key, body, uploaded=0):
        self.objects[key] = {
            "body": body,
            "uploaded": uploaded,
            "etag": f"etag-{len(body)}",
        }

    def list(self, prefix, cursor=None):
        keys = sorted(k for k in self.objects if k.startswith(prefix))
        start = keys.index(cursor) if cursor in keys else 0
        page = keys[start : start + R2_LIST_PAGE_CAP]
        truncated = (start + R2_LIST_PAGE_CAP) < len(keys)
        next_cursor = keys[start + R2_LIST_PAGE_CAP] if truncated else None
        # LIST returns metadata; bodies are NOT downloaded here (as in R2).
        return (
            [{"key": k, **{m: self.objects[k][m] for m in ("uploaded", "etag")},
              "size": len(self.objects[k]["body"])} for k in page],
            truncated,
            next_cursor,
        )

    def get_body(self, key):
        obj = self.objects.get(key)
        if obj is None:
            return None
        self.body_reads += 1  # a real download
        return obj["body"]

    def head(self, key):
        obj = self.objects.get(key)
        if obj is None:
            return None
        return {"size": len(obj["body"]), "uploaded": obj["uploaded"], "etag": obj["etag"]}


# ── handlers (mirror dispatch.rs) ────────────────────────────────────────────
def handle_list_backups(r2, identity):
    """The funds-RESTORE path: every device blob + legacy, bodies downloaded."""
    blobs = []
    cursor = None
    while True:
        page, truncated, next_cursor = r2.list(backup_device_prefix(identity), cursor)
        for obj in page:
            body = r2.get_body(obj["key"])
            if body is not None:
                blobs.append(body)
        if truncated and next_cursor:
            cursor = next_cursor
        else:
            break
    legacy = r2.get_body(backup_object_key(identity))
    if legacy is not None:
        blobs.append(legacy)
    return blobs


def handle_stat_backups(r2, identity):
    """Q4: the same objects, METADATA ONLY, legacy never device-scoped."""
    objects = []
    cursor = None
    while True:
        page, truncated, next_cursor = r2.list(backup_device_prefix(identity), cursor)
        for obj in page:
            device_id = obj["key"].rsplit("/", 1)[-1]
            if not device_id:
                continue
            objects.append({
                "deviceId": device_id,
                "deviceScoped": True,
                "size": obj["size"],
                "uploaded": obj["uploaded"],
                "etag": obj["etag"],
            })
        if truncated and next_cursor:
            cursor = next_cursor
        else:
            break
    legacy = r2.head(backup_object_key(identity))
    if legacy is not None:
        objects.append({
            "deviceId": None,
            "deviceScoped": False,  # THE LEGACY TRAP — may belong to ANY device
            "size": legacy["size"],
            "uploaded": legacy["uploaded"],
            "etag": legacy["etag"],
        })
    return objects


# ── proofs ───────────────────────────────────────────────────────────────────
def prove_pagination_parity():
    ident = "02" + "a" * 64
    r2 = FakeR2()
    # 2,500 device objects → 3 LIST pages. Immortal objects + a fresh deviceId
    # per wipe make this reachable for a long-lived identity.
    device_ids = [f"device-{i:05d}" for i in range(2500)]
    for d in device_ids:
        r2.put(backup_device_object_key(ident, d), f"blob-{d}".encode())

    stat = handle_stat_backups(r2, ident)
    listed = handle_list_backups(r2, ident)

    stat_devices = sorted(o["deviceId"] for o in stat if o["deviceScoped"])
    assert len(stat_devices) == 2500, f"stat saw {len(stat_devices)}, want 2500 (cursor not followed?)"
    assert stat_devices == sorted(device_ids), "stat device set != the real set"
    assert len(listed) == 2500, f"restore saw {len(listed)}, want 2500"
    # THE PARITY INVARIANT: the proof must see exactly what restore can restore.
    assert len(stat_devices) == len(listed), "stat/restore disagree → the proof lies by omission"
    print("  ✓ pagination parity: 2500 objects, 3 pages, stat == restore")


def prove_legacy_is_reported_but_not_device_scoped():
    ident = "02" + "b" * 64
    r2 = FakeR2()
    r2.put(backup_object_key(ident), b"legacy-blob")  # ONLY a legacy object

    stat = handle_stat_backups(r2, ident)
    assert len(stat) == 1, "legacy object must be reported"
    entry = stat[0]
    assert entry["deviceScoped"] is False, "legacy MUST NOT be device-scoped"
    assert entry["deviceId"] is None, "legacy MUST NOT carry a deviceId"
    # And it must be invisible to a current-device claim.
    assert not [o for o in stat if o["deviceScoped"]], \
        "a legacy-only identity must yield ZERO device-scoped proofs"
    print("  ✓ legacy reported, deviceScoped=False, deviceId=None (cannot prove a device)")


def prove_legacy_prefix_isolation():
    ident = "02" + "c" * 64
    r2 = FakeR2()
    r2.put(backup_object_key(ident), b"legacy")
    r2.put(backup_device_object_key(ident, "device-0001"), b"dev")
    page, _, _ = r2.list(backup_device_prefix(ident), None)
    keys = [o["key"] for o in page]
    assert backup_object_key(ident) not in keys, \
        "the trailing-slash prefix must NOT match the legacy key"
    assert backup_device_object_key(ident, "device-0001") in keys
    print("  ✓ legacy prefix isolation (why the explicit legacy HEAD is required)")


def prove_metadata_only():
    ident = "02" + "d" * 64
    r2 = FakeR2()
    for i in range(50):
        r2.put(backup_device_object_key(ident, f"device-{i:05d}"), b"x" * 4096)
    r2.put(backup_object_key(ident), b"y" * 4096)

    r2.body_reads = 0
    handle_stat_backups(r2, ident)
    assert r2.body_reads == 0, \
        f"statBackups downloaded {r2.body_reads} bodies — a UI claim must not cost a full download"
    print("  ✓ metadata only: 0 body reads for 51 objects")

    r2.body_reads = 0
    handle_list_backups(r2, ident)
    assert r2.body_reads == 51, "restore SHOULD read every body (contrast)"
    print("  ✓ contrast: restore reads all 51 bodies (why stat is a separate RPC)")


def prove_current_device_match_is_exact():
    """The client-side rule, proven here too: only an exact device match counts."""
    ident = "02" + "e" * 64
    r2 = FakeR2()
    r2.put(backup_device_object_key(ident, "device-OTHER"), b"other")
    r2.put(backup_object_key(ident), b"legacy")
    stat = handle_stat_backups(r2, ident)

    this_device = "device-MINE"
    proven = [o for o in stat if o["deviceScoped"] and o["deviceId"] == this_device]
    assert not proven, "another device's blob + a legacy blob must NOT prove THIS device"
    print("  ✓ other-device + legacy present → still no proof for this device")


if __name__ == "__main__":
    print("Q4 statBackups parity proof")
    prove_pagination_parity()
    prove_legacy_is_reported_but_not_device_scoped()
    prove_legacy_prefix_isolation()
    prove_metadata_only()
    prove_current_device_match_is_exact()
    print("ALL PROOFS PASSED")
