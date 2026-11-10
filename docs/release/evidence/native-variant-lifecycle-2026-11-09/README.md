<!-- SPDX-License-Identifier: Apache-2.0 -->

# Variant lifecycle evidence

Finite local acceptance for the
[November 9 contract](../../../architecture/native-variant-lifecycle-2026-11-09.md).
Raw and supported shredded Variant payloads remain native through ordinary
SQL/DataFrame operations, bounded delivery, actual spill and typed persistence.
The public guide is [resident native results](../../../reference/resident-native-results.md).

| Check | Accepted observation |
| --- | --- |
| Default Rust workspace | 3,322 passed; zero failures or ignores |
| Native Rust workspace | 7,045 passed; zero failures; 37 existing opt-in ignores |
| Formatting and strict lint | Default/native checks passed |
| Changed Arrow provider | 408 tests and strict lint passed |
| Features and supported Rust | Seven feature configurations and both Rust 1.96 checks passed |
| Selected public Python tests | 15 passed; zero failures, errors or skips |
| Isolated installed wheel | 420 passed; zero failures, errors or skips; isolated load and uninstall passed |
| Python core | 1,280 discovered cases; 866 reported runs; 61 optional/environment skip records; zero failures/errors |
| Documentation and contracts | Final generated references, licenses, governance and Rust contracts passed; all nine package-proof runner tests passed |
| Portable evidence | Source-bound replay passed; two standalone positive controls and 18 corruption controls passed |

The packet binds 15 validation roles to complete original source inventories,
exact commands, logs and process-cleanup receipts. It requires all 19 selected
native regressions, two Arrow-provider regressions and all seven Variant plus
eight schema Python tests. The isolated installed proof covers all 420 tests
with zero failures, errors or skips, including the existing public workflows.
Its wheel contains the exact frozen native library and CLI and final production
Python source; isolated load and uninstall both passed.

Independent encoded fixtures and complete values cover SQL NULL versus Variant
null, raw/shredded objects and lists, dictionary metadata, nonzero slices,
duplicate and unmatched joins, empty/repeated values, actual spill, all native
writer interfaces and separate-process Vortex/Arrow IPC/Parquet reopening.
Cancellation, denied grants, malformed bytes, unsupported comparisons and final
exported-child ownership remain explicit checks.

All 26 original failed result records remain saved with dispositions. The original negative
self-join test omitted required aliases: replay admits only its exact two-`AS`-
token correction and preserves every fixture, value and refusal assertion.
Generated API references have a separately verified canonical-format/four-hash
refresh. Final Python core, documentation and Rust contract checks see those
final bytes. All engine/build gates bind unchanged compiled behavior; named
status prose and Cargo-independent Python tests/converter have explicit scopes.
The standalone Arrow-provider gate binds its compiled source, README, frozen
lockfile and runner. Its only later provider change is the exact three recorded
provenance-hash updates, verified against already-tested source.

The packet preserves the original archive-size and control-compression failures,
and the subsequent control-targeting review. Earlier evidence archives are not
recursively embedded; their original paths, hashes and inventories remain, and
all current and historical build/tool source bytes are included. Corrected
controls use the accepted compression and verify the intended prior-main
success requirement. Archive and storage limits are unchanged.

Two debug-cache retirements and one interrupted-fixture retirement preserve
their exact validation basis, inventories and closed-handle checks. Inputs,
retained runtimes/wheels and release cache stayed unchanged. The native retry
used four test workers without filtering cases or changing storage/deadline
limits. The earlier vector closeout and later wide-schema exact-main checks are
bound to their original observations.

## Offline replay

Use Python 3.11 or newer. After relocating this directory, run:

```sh
python3 -I verify.py --require-controls
```

From the matching checkout, also compare every final source input:

```sh
python3 -I docs/release/evidence/native-variant-lifecycle-2026-11-09/verify.py --require-controls --repo-root .
```

Replay validates saved sources, commands, logs, identities and actual subprocess
control outcomes. It does not build, install, execute native queries, access the
network or require the original scratch directories. Controls cover missing
roles/tests, failed native acceptance, installed skips, changed source/runtime,
old builds, replaced frozen expectations, omitted original failures, altered
prior-main results, unsafe archive paths and checksum corruption. Temporary
mutated payloads are removed after each observation; the original packet is
unchanged. Saved logs and mutation identities are in `corruption-controls.json`.

## Boundaries

This is finite local macOS arm64 verification for matching source builds after
1.1.0. Hosted review, PR checks, merge and exact-main acceptance for this unit
are separate; this frozen packet does not claim them. Published packages are
unchanged. No benchmark or performance comparison was run.

Logical/raw/shredded depth remains 24 and physical wrappers remain bounded at
64. Unsupported physical shredding, general JSON reconstruction, Variant keys
and field expressions, row-literal hints and standalone UUID/spatial support
remain explicit boundaries or required later work. Empty outputs preserve the
logical dtype and may use the default empty physical representation. Existing
mixed-MELT diagnostic scalar presentation remains compatible; it does not
reconstruct Variant encodings. Vortex remains native input and highest-fidelity
output, without external-engine fallback. This does not close item 19, deep
traversal or CG-1 through CG-23, and does not publish a package.
