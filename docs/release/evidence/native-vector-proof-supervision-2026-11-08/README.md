<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native wheel proof supervision follow-up

This follow-up preserves the five files of the
[shared vector expression packet](../native-vector-expressions-2026-11-08/README.md)
byte-for-byte. It covers the bounded integration deadline in the installed-wheel
proof runner and its regression tests. It does not change runtime, package or
fixture inputs.

Both original PR #1671 Linux Python 3.13 attempts stopped at the existing
600-second aggregate integration deadline, after 321 completed tests and no
observed failed assertions. Both attempts and the dependent acceptance/readiness
failures remain failures. The identical Linux wheel passed all 397 tests under
Python 3.10; the original macOS Python 3.13 job also passed. These observations
establish the deadline failure, not a failed test assertion or a proven host cause.

Integration now has a finite 1,200-second budget; each setup/package command
retains its 600-second budget. Every step records its selected bound. The original
test-discovery program, zero-skip requirement and outer CI job deadline are
unchanged. All 21 focused proof/package tests pass, including work within the
larger integration budget, rejection beyond it with retained diagnostics, and
setup rejection at the original bound. A fresh isolated installation of the
byte-identical local wheel passes all 397 tests with zero failures, errors or
skips, plus load and uninstall checks.

The follow-up binds the complete new source inventory to the pinned core packet.
Only the two named proof-runner files and three named status documents may differ.
All compiled sources, Python package files, native/public tests and frozen
references must match. Earlier unit/installed checks may differ from final
documents only in those three status files; final docs, contract and formatter
checks see the final source. The original default/native workspace checks are
unchanged-input evidence, not newly repeated local runs. Fresh full hosted checks
must pass at the follow-up head before merge.

## Offline replay

Keep this directory and `native-vector-expressions-2026-11-08` beside each other.
Python 3.11 or newer can replay both packets without the original temporary files:

```sh
python3 -I verify.py --require-controls
```

Use `--core-packet PATH` if the core packet is elsewhere. From the final checkout:

```sh
python3 -I docs/release/evidence/native-vector-proof-supervision-2026-11-08/verify.py --require-controls --repo-root .
```

The original verifier runs under its original policy without a checkout comparison;
the follow-up verifies every current source input under its narrower change policy.
Replay does not build, install, execute native queries or access the network.
Original and relocated positive controls and resealed corruption controls test
missing/failed checks, omitted tests, skips, altered runtime inputs or budgets,
omitted original CI failures, modified base evidence and unsafe archive paths.
Temporary control copies are removed after each observation; originals are intact.

The first local follow-up launch used an older Python interpreter lacking
`hashlib.file_digest` and stopped before launching a child. Its observation is
retained; the unchanged supervisor then passed with the supported interpreter.

This is local acceptance of the proof-runner change. New-head hosted review,
PR checks, merge, exact-main checks and verified retirement have separate closeout
receipts. The original retirement plan remains unexecuted and historical; a fresh
inventory is required. The core packet's feature and compatibility limitations
remain. No performance claim, package publication, whole item 19/item 20,
RFC 0044 or CG-1 through CG-23 completion is claimed.
