<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native temporal-grid acceptance — October 29, 2026

Resampling, gap filling and ordered value filling pass local source-build
acceptance across SQL, DataFrame, typed native relation, embedded workflow and
CLI execution. This completes the temporal-grid capability unit within active
item 20. Installed-package, combined-workflow and the remaining item-20
capabilities retain their separate acceptance. No benchmark was rerun and no
performance claim changes.

## Shared semantics and execution

The [grid contract](../architecture/native-time-series-completion-2026-10-29.md#resampling-and-grid-contract)
adds `temporal_bucket`, DataFrame `resample`, shared `gap_fill` and ordered
`forward_fill`, `backward_fill` and `interpolate_linear` declarations. Fixed
grids use exact elapsed ticks and Euclidean division before the epoch. Calendar
grids preserve the original anchor across month-end clamping and apply explicit
gap/fold policies. Non-advancing civil boundaries fail explicitly.

Gap filling reuses native ordering and emits bounded batches. Inclusive bounds
can clip or extend observed partitions; generated non-key values are NULL.
NULL, duplicate and unaligned times are refused, including outside clipped
bounds. Forward/backward filling preserves logical payload types. Linear
interpolation uses exact widened tick differences, finite Float64 results,
checked integer/decimal conversion and NULL unbracketed edges. No second complete
partition or endpoint index is retained.

Tiny original/gap fragments pass through the existing native record-compaction
buffer, now shared with this operator. Its bounded batching prevents excessive
fragment metadata from defeating the following window's memory grant. Actual
payload, schema and indexing owners retain their existing reservations.
Vortex-native input/output and no-external-engine certificates are preserved;
no dependency or execution provider is added.

## Correctness, ownership and failure evidence

- The final public matrix makes **280 positive executions across five
  interfaces**, comparing all **8,520 rows** and exact schemas. Independent
  calendar and rational references cover 18 temporal domains, seven calendar
  populations, negative epochs, month anchors, daylight-saving gaps/folds,
  empty/clipped grids, extreme ticks, nullable groups and rich payloads.
  Floating interpolation uses explicit per-case tolerances; other values are
  exact. Fourteen invalid domain/order/policy cases also fail consistently
  through all five interfaces.
- Twenty-one real Vortex, Parquet and Arrow IPC writes reopen after original
  source deletion. Complete values and schemas are compared, and retained
  outputs remain usable after producer closure and in a subsequent native query.
- A real batch producer composes resampling, grouping, gap filling and linear
  interpolation through collect, batch delivery and Vortex write. Empty input
  and both ordinary and zero-limit late producer failures are checked. Success
  requires producer EOF/closure; a late error prevents result publication.
- A 24,013-row fixture larger than a 16 MiB grant exercises gap filling,
  interpolation and their composition. The expanded grid has 48,025 rows.
  Resident execution under that grant is refused; an ample resident control
  agrees with native spill under the original 16 MiB grant, 256 MiB disk quota
  and 1 MiB buffer. Every row is checked. Assertions require actual multiple
  runs and merge passes, bounded reported reservations/quota and complete cleanup.
- Cancellation during filling, failure/cancellation after the first output,
  quota denial and a fresh retry verify that owned state and spill files are
  released. Nested values, all-NULL edges, large epochs, opposite-sign extreme
  finite floats and the explicit decimal conversion boundary have native tests.
- The new `temporal_grid_workflow` semantics case uses two ordinary CTEs,
  calendar buckets, grouped reduction, inclusive gap-fill bounds and all three
  fill windows. Its 14 complete rows match literal independent expectations
  with `fallback_attempted=false` and `external_engine_invoked=false`.
- The literal connected-programming example executes successfully and retains
  its three-row result after the producer closes.

These are complete-result assertions, not a claim that every successful public
row envelope is persisted. The portable packet retains the complete semantics
case envelope and its source fixture. Resource evidence covers the existing
reservation and spill contracts, not a total-process RSS bound.

## Validation and source linkage

| Gate | Result |
| --- | --- |
| Native workspace, all targets, `release-user-surfaces`, `ci-native` | 6,444 passed; 35 existing ignores |
| Default workspace, all targets | 3,279 passed; no ignores |
| Native and default workspace all-target strict Clippy | Passed |
| Minimal `vortex-local-primitives` build | Passed |
| `cargo fmt --all -- --check` | Passed |
| Final rebuilt-product public checks | Four methods passed; no failures or skips |
| New executable semantics case | All 14 rows, schema and certificate checks passed |
| Python declarations, generated references and conformance consumers | 129 tests passed; 14 source/documentation commands passed; no skips |

All Rust gates above match the final changed Rust sources byte for byte.
Rust 1.99.0 builds use the unchanged dependency graph. The final public and
semantics checks verify their built-product/source identities before and after
execution. The conformance producer and consumers agree on 120 executable
fixtures, 25 diagnostic cases, 145 required runtime rows and 147 total matrix rows.

Final executable SHA-256:
`d313cca0a64364a06a66800776d1e1002e6b253e382f73039b1480237da5beb1`.
Final native Python library SHA-256:
`5c0deac2835be9329b5a05b717cdb33b325f675292ce7bc1ec2cb34e3a91ee5f`.

The [portable evidence packet](evidence/native-time-grid-2026-10-29.json.xz)
retains changed Rust/Python sources, checked reference documents, guarded
commands, raw logs, complete development/check outcomes and verification tools.
It preserves the early pressure failure that exposed fragment metadata growth,
the lightweight-feature compilation failure, fixture/reference corrections and
lint/format failures. The original zero-test filtered launch is retained and
receives no acceptance credit. The corrected feature configuration and complete
final suites pass; no failed prefix is counted as a completed gate.

Offline replay verifies every member identity, accepted source linkage, product
identity linkage, test counts and the complete saved grid result. Four corruption
controls reject changed members, public counts, source/receipt linkage and
result rows even when the altered result member has a consistent hash.
All 43 guarded attempts, including 14 failed attempts, remain represented in
178 verified members. The packet SHA-256 is
`52bf24a00e42130b5c6aab9336147e67b7204068c6ee423f695a123e5e9ee11c`.

The [cleanup receipt](evidence/native-time-grid-2026-10-29-cleanup.json) records
retirement of five superseded staged libraries after repeated identity and
closed-handle checks. The final tested products and compact outcomes remain.
The verifier extracted from the packet passes before and after cleanup without
loading repository code. No payload backup or storage-limit change is introduced.

To reproduce, use the recorded feature/profile/resource settings in a fresh
checkout and local output directory, build the CLI and Python library, then run
the recorded public and semantics helpers. The recorder's `--verify` mode reads
only the portable packet. Existing Rust/Python test modules remain CI entry
points. Do not overwrite completed attempt paths.

The 347-query checkpoint and prior Full43 observations are unchanged. Hosted CI,
installed-package acceptance and parent-item closure are not asserted by this
local packet. No package publication or competitive-gate completion is implied.
