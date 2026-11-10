<!-- SPDX-License-Identifier: Apache-2.0 -->

# Variant semantics validation repair

The original PR #1676 hosted run exposed one stale expectation: the historical
`unsupported_variant_access` fixture expected blanket rejection of Variant
functions. Native Variant expressions are now admitted, so its CSV UTF8 operand
correctly fails type admission with `SL_INVALID_INPUT`. The fixture now uses the
valid literal path `$.field` and requires that diagnostic. The historical case
ID remains stable.

This separate packet preserves the original
[Variant expression acceptance](../native-variant-expressions-2026-11-10/README.md)
unchanged and binds exactly nine validation, test and documentation changes to
the final source. No engine, provider, production Python, dependency or lockfile
input changed. The accepted release runtime and installed-tested wheel retain
their original identities; broad native validation was not repeated.

The repaired runtime matrix passed all 153 stages: 128 executable cases,
20 unsupported diagnostics, one runtime-error diagnostic, two invalid-shape
diagnostics and two support reports. Both release consumers accept the resulting
counts. The actual runtime report stays unchanged; conformance replay records an
explicit artifact-path-only projection into CI's required `target/` layout,
including the hash of every original artifact. Readiness validation checks the
semantics gate only and makes no complete release-readiness claim.

All 184 release-script and producer/consumer tests pass, including rejection of
the stale 21/1 diagnostic counts. Formatting, strict lint for the changed contract
tests, the complete repository contract suite, generated interfaces and public
documentation checks pass. The original packet retains the broader engine,
feature, supported-Rust, Python and 441 installed-package test evidence.

The exact failed hosted artifact and its report remain in the archive. Three
local repair failures also remain, with explicit dispositions: missing fields in
the new test fixture, the release consumer's fixed artifact path requirement,
and stale totals in the documentation test. Earlier passing observations retain
their original source inventories. The matrix observation differs from final
source only in the later consumer and test repairs; the release-test observation
differs only in the subsequent Rust documentation-test constants.

Two positive controls accept the source-bound and relocated packets. Nine
negative controls reject archive corruption, production-source drift, missing
hosted/local failures, fallback markers, a failed test receipt, changed runtime
identity, changed original evidence and a missing source inventory.

## Replay

For the current checkout:

```bash
python3 -I -B docs/release/evidence/native-variant-semantics-repair-2026-11-10/verify.py --require-controls --repo-root .
```

For portable replay, keep this directory and
`native-variant-expressions-2026-11-10` together as siblings, then run this
directory's `verify.py --require-controls` without `--repo-root`. The original
packet's own source-bound command remains valid only for its original source;
the repair verifier checks the final checkout and replays the original packet
portably. Replay does not execute native queries.

Hosted checks, review, merge and retirement remain separately recorded. No
benchmark, performance, publication, whole item 19/item 20, RFC 0044 or CG-1–CG-23
completion claim is made. Useful spatial support and admitted deep traversal
remain open.
