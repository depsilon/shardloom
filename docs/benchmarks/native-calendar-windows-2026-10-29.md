<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native calendar-window acceptance — October 29, 2026

Calendar-aware RANGE windows pass local implementation and conformance acceptance
across SQL, DataFrame, typed native relation, embedded workflow and CLI execution.
This completes the calendar-window unit within active item 20. Resampling, gap
filling, interpolation, installed-package and combined item-20 acceptance remain
open. No benchmark was rerun and no performance claim changes.

## Shared semantics and execution

The [calendar contract](../architecture/native-time-series-completion-2026-10-29.md#calendar-range-contract)
extends the existing native window owner. Elapsed durations retain nanosecond
precision even when source ticks are coarser. Calendar days and months reuse the
shared temporal kernel with explicit gap/fold and month-end policies. Dates
require whole elapsed days; calendar offsets admit dates and timestamps. Zero
offsets preserve the original fold occurrence, retain their type for admission
and have the same endpoint position as `CURRENT ROW`.

Calendar endpoints can retreat across a daylight-saving fold. Resident state
uses exact membership differences and reuses its charged extrema queues and
statistical summaries. Explicit spill divides stored bounds into monotone output
ranges and reuses existing native DISTINCT intervals and extrema summaries over
the full input partition. Neither path creates an uncharged resident fallback.
Vortex-native input/output and no-external-engine certificates are preserved;
no dependency or execution provider is added.

## Correctness, ownership and failure evidence

- Independent native fixtures cover every timestamp, time and duration unit,
  exact sub-tick elapsed boundaries, date32/date64, negative epochs, leap years,
  clamped month ends, repeated local times, both directions, both NULL orders
  and all four exclusions. Fourteen reducers exercise complete resident/spilled
  results, including DISTINCT, extrema, statistics, quantiles and lists.
- The final public matrix makes **460 executions across five interfaces** and
  compares all **37,500 rows and exact schemas**, with no skipped tests. Its
  independent Python calendar reference covers spring gaps, fall folds and month
  boundaries; unsorted input, duplicate peers, NULL groups and multiple source
  batches retain decimal/list payloads. Eight invalid domain/policy/range cases
  also fail consistently through all five interfaces.
- Nine real Vortex, Parquet and Arrow IPC writes reopen after source deletion and
  compare all 1,119 persisted rows and schemas. Retained results remain usable
  after producer closure and as the source of a later native query.
- A 24,013-row fixture larger than a 16 MiB grant is refused by the resident
  path. An ample resident control agrees with native spill under the smaller
  grant. Assertions require actual runs/merges, complete output, bounded leases
  and quota, and completed owned cleanup. Cancellation during bounds, DISTINCT
  intervals and extrema summaries, output failure, quota denial and subsequent
  retry release the existing owners correctly.
- The new `calendar_window_frames` semantics-matrix case executes date-based
  calendar-day, clamped-month and elapsed-day windows against literal expected
  rows and requires no-fallback evidence.

These are complete-result assertions against independent reference logic. They
do not persist every successful public row envelope or establish a total-process
RSS bound. Resource checks cover the existing reservation and spill contracts.

## Validation and source linkage

| Gate | Result |
| --- | --- |
| Native workspace, all targets, `release-user-surfaces`, `ci-native` | 6,420 passed; 35 existing ignores |
| Default workspace, all targets | 3,277 passed; no ignores |
| Native and default workspace all-target strict Clippy | Passed |
| Minimal `vortex-local-primitives` build | Passed |
| `cargo fmt --all -- --check` | Passed |
| Final rebuilt-product public checks | Four methods passed; no failures or skips |
| New executable semantics-matrix case | Passed; complete values and certificate checks |
| Python declarations, generated references and semantics-manifest tests | 81 passed; source, documentation and public-claim gates passed |

Rust 1.99.0 builds use the unchanged dependency graph. The sole Rust difference
after the broad native suite is Clippy's equivalent nesting of three zero-value
or-patterns. The packet retains the exact before/after source, hashes and checked
whole-file replacement. Final strict lint, default tests and the rebuilt-product
zero-offset matrix validate the normalized source. This distinction is explicit
in source linkage; the broad suite is not labelled as byte-identical to that
later source.

Final executable SHA-256:
`9ad45654b034081013753c98208d7409c194f05e3080231ca9094bf065dd823f`.
Final native Python library SHA-256:
`a6c163ecac7af98a07c0ddee086ec819a4b241059a9b737e9bf1788ef2c40130`.
The public checks verify these products and their test/source identities before
and after execution.

The [portable evidence packet](evidence/native-calendar-windows-2026-10-29.json.xz)
retains complete changed Rust/Python sources, guarded commands, raw logs and all
23 development/check attempts. Six failed attempts remain visible: independent
reference corrections, strict-lint corrections, the initially missing Arrow
test environment and an omitted explicit NULL ordering in a new SQL fixture.
The concurrency guard's refused launch is retained separately. No failed prefix
is counted as a completed gate. Packet replay verifies member identities,
accepted source linkage, result counts and the zero-offset correction; three
corruption controls reject changed members, public counts and source linkage.
The packet contains 91 hash-verified members; its SHA-256 is
`12d3def6fe6ca10e2ef4a8993bfd9d8f0158d21e3f05d7226d10f17bacfec258`.

The [cleanup receipt](evidence/native-calendar-windows-2026-10-29-cleanup.json)
records retirement of three duplicate staged libraries after repeated identity
and closed-handle checks. The final tested executable/library and all compact
results remain. Packet verification passes after cleanup; no runtime backup or
storage-limit change is introduced.

To reproduce, use the recorded feature/profile/resource settings in a fresh
checkout and local output directory, build the CLI and Python library, then run
the recorded public and semantics helpers. The included recorder's `--verify`
mode reads only the portable packet. Existing Rust and Python test modules are
the CI entry points. Do not overwrite completed attempt paths.

The 347-query checkpoint and prior Full43 observations are unchanged. This unit
does not imply package publication or competitive-gate completion.
