#!/usr/bin/env python3
"""
Worker backup-key GUARDRAIL (Storage-scoping Commit D, Codex 6b9e6c75).

The R2 backup axis must never regress to a single-object key: every `backup/...`
R2 key MUST be built via one of the three device-scope helpers, so a future edit
can't reintroduce the two-device clobber vector (the total-loss bug Commit C
closed) by writing a raw `format!("backup/{}", identity)` at a new call site.

This mirrors the wallet-side no-raw-localStorage guardrail: it source-scans
dispatch.rs and FAILS if a `backup/` key literal appears OUTSIDE the three
allowed constructor functions.

Allowed constructors (the ONLY places a `backup/` literal may appear):
    backup_object_key        -> format!("backup/{}", identity_key)          # legacy
    backup_device_object_key -> format!("backup/{}/{}", identity_key, ...)  # per-device
    backup_device_prefix     -> format!("backup/{}/", identity_key)         # LIST prefix
"""
import re
import sys
from pathlib import Path

DISPATCH = Path(__file__).resolve().parent.parent / "src" / "dispatch.rs"
ALLOWED_FNS = {
    "backup_object_key",
    "backup_device_object_key",
    "backup_device_prefix",
}
# A `backup/` string literal (the R2 key prefix). Matches format! args + raw strings.
BACKUP_LITERAL = re.compile(r'"backup/')
FN_HEADER = re.compile(r"^\s*fn\s+([A-Za-z0-9_]+)\s*\(")


def current_fn(lines, idx):
    """Walk backwards to find the enclosing `fn name(` for line idx."""
    for j in range(idx, -1, -1):
        m = FN_HEADER.match(lines[j])
        if m:
            return m.group(1)
    return None


def main():
    src = DISPATCH.read_text()
    lines = src.splitlines()
    offenders = []
    for i, line in enumerate(lines):
        # ignore comments
        stripped = line.lstrip()
        if stripped.startswith("//"):
            continue
        if BACKUP_LITERAL.search(line):
            fn = current_fn(lines, i)
            if fn not in ALLOWED_FNS:
                offenders.append((i + 1, fn, line.strip()))

    if offenders:
        print("FAIL: raw `backup/` R2 key literal outside the device-scope helpers.")
        print("Build the key via backup_object_key / backup_device_object_key /")
        print("backup_device_prefix so the multi-device axis can't regress:")
        for ln, fn, text in offenders:
            print(f"  dispatch.rs:{ln} (in fn {fn}): {text}")
        sys.exit(1)

    # Sanity: the three helpers must still exist (a rename shouldn't silently
    # empty the guardrail).
    missing = [fn for fn in ALLOWED_FNS if f"fn {fn}(" not in src]
    if missing:
        print(f"FAIL: expected backup-key helper(s) missing: {missing}")
        sys.exit(1)

    print("PASS: every `backup/` R2 key is built via a device-scope helper.")
    sys.exit(0)


if __name__ == "__main__":
    main()
