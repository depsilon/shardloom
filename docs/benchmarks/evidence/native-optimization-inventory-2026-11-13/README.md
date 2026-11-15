# Compact ShardLoom optimization coverage inventory

This portable packet freezes the source and evidence inputs used for the
optimization coverage inventory. Inventory/evidence snapshot: 2026-11-13.

## Source snapshots

- The 253 catalog source files are the exact Git blob bytes from `cac86e8b0a5fa5fa8bf4c900dbef7a703746bfa8`.
- The 15 current validation-obligation files are the exact Git blob bytes from
  `c79f27a8222e7a1fe5009b07a2a19c1c9dd387bb` and are stored beneath a separate source prefix.
- The retained 10,000-query observations are from
  `9f4580c723f876f087d3e75fb56153b4b099ef20`, an earlier source snapshot than
  the cac86 inventory and c79 validation obligations.
- The two external design files are copied byte-for-byte and checked against
  `catalog-r2.json`'s external design SHA-256 registry.
- All files from `coverage-inventory-r1` are retained at their input-relative
  paths. Historical corrections and earlier inventories remain present as
  historical artifacts; later accepted inventories do not rewrite them.

## Evidence counts and limits

The accepted `operator-test-primary-r5.json` reports **338 unique source tests**:
337 passed and one ignored. The ignored test is not counted as passed. This is
not an exact full-test-suite claim and does not establish complete result replay.
The retained 10,000-query declaration/identity evidence remains bound to its
recorded `9f4580c` source snapshot; it is not reassigned to `cac86e8b0a5fa5fa8bf4c900dbef7a703746bfa8` or `c79f27a8222e7a1fe5009b07a2a19c1c9dd387bb`.

This archive preserves artifacts and source bytes. It does not independently
reconstruct test semantics, prove universal optimization activation, claim an
exact full test pass, or claim complete query-result replay. Source inventories,
test receipts, capability reports and retained observations retain their own
scope and historical limits. No runtime binaries, original SQLite databases or
reference ledgers are included.

## Verification

Run `python3 verify_archive.py` from this directory. The verifier uses only the
Python standard library and checks archive/member safety, manifest and member
hashes, source registry uniqueness/counts, and absence of missing/negative
source-member status. It does not run engine code or tests.
