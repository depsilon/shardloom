# Native analytic-window pressure and streaming acceptance

This unit completes the existing analytic window functions and frames under an
explicit shared native spill policy and admits one finite single-use batch
source. It reuses Window semantics, native ordering/run storage, exact keys,
credited payloads and result writers. Resident execution stays the default.

Status at local packet close: complete local runtime, independent packet
inspection and documentation acceptance; hosted integration was pending. The
[evidence index](evidence/native-window-pressure-2026-10-08.json),
[portable packet](evidence/native-window-pressure-2026-10-08.json.xz) and
[independent inspection](evidence/native-window-pressure-2026-10-08-inspection.json)
bind source, complete results and original failure history. This unit makes no
comparative performance or whole-process memory claim and does not publish a
package or alter v0.4.0.

Hosted follow-up, 2026-10-08: [PR #1533](https://github.com/depsilon/shardloom/pull/1533)
merged at `a88ec5c9` with all 39 hosted checks passing and all 1,001 accepted
runtime assets unchanged. The separate
[hosted receipt](evidence/native-window-hosted-2026-10-08.json) preserves exact
pre/post-merge evidence, preview/production search and guide checks, and the live
acceptance link. Primary source review found no blocking issue. Automated hosted
review was unavailable because the account review quota was exhausted; no
independent source-review approval is claimed. The original local packet and
its historical status remain unchanged.

The packet contains 47,979,308 compressed bytes with SHA-256
`338655c1d6878a879dfd3532483485d9b686b2c78810173380abf78f5a8c13f1`.
Complete decompression produces 3,987,841,321 bytes with SHA-256
`6667b1a77af2f812544a9a7ecbd6cacca1cb2ede07ee46946f611fd28e6244d6`.
Original source/artifact hashes identify local bytes; portable text substitutes
local paths. Complete values are reconstructed and reopened before a separate
stream inspector verifies packaged coverage, claims and no-fallback reports.

## Contract and reuse

The [design and reuse map](../architecture/native-window-pressure-2026-10-08.md)
records the Vortex-first decision. Pinned Vortex 0.85.0 supplies arrays, credited
builders, native take, Flat file writes and bounded scans. ShardLoom retains
frame/exclusion policy, exact observation order, admission, cancellation, quota
and publication. No dependency, external engine or error-triggered retry is added.

An explicit policy stores original input ordinals, grouped order, peer boundaries
and results through the shared native run store. Exact DISTINCT uses unions of
the output-position intervals containing each observed value, followed by counted
events. Extrema use native range summaries and bounded lookups. Selected-value
functions retain positions until final native gathering; output returns in input
order. No full-partition membership table or extrema queue remains resident.

Shared positional frame logic preserves ROWS/GROUPS/RANGE, exclusions, nulls,
ties, ranking/navigation/distribution and exact floating/decimal reductions.
Evaluation preserves group, partition and function error order. Private movement
retains unobserved floating bits and decimal storage; public result copies still
validate selected values and declared precision. Typed and nested values use
the existing exact logical contracts, without conversion to another engine.

One finite batch URI occurs exactly once in an admitted streamed plan. It can
compose windows with joins, aggregation, ordering, projection/filter and draining
limits alongside ordinary sources. Every private batch owner releases before
the next demand. Window evaluation follows complete input validation. Repeated
batch sources, multiple producers, dynamic schemas and unadmitted stateful
families reject before consumption. Intake types and finite transport bounds
are unchanged.

## Complete constrained workloads

Both native controls contain 24,013 rows with 768-byte strings and descending
source positions. A cumulative analytic partition computes exact COUNT DISTINCT,
MIN, MAX and SUM while retaining the wide payload. The file-backed fixture also
contains nullable measures and independently constructed physical leaves. Its
native file exceeds 16 MiB; the streamed fixture has 18,832,464 logical input
bytes. Each successful control checks every output value and original row order.

| Observation | File-backed window | Streamed window |
| --- | --- | --- |
| Constrained native grant | 16,777,216 bytes | 16,777,216 bytes |
| Resident control at that grant | Deterministic denial; reservations refunded | Deterministic denial; reservations refunded |
| Ample resident control | Complete output at 128 MiB | Complete output at 128 MiB |
| Ample peak reservation | 25,886,815 bytes | 23,474,274 bytes |
| Explicit-spill peak reservation | 11,340,461 bytes | 4,742,917 bytes |
| Native runs / merge passes | 101 / 44 | 103 / 45 |
| Peak temporary disk | 38,916,608 bytes | 38,148,620 bytes |
| Disk quota / flush threshold | 256 MiB / 1 MiB | 256 MiB / 1 MiB |
| Complete output rows | 24,013 | 24,013 |
| Final ownership | Owned runs removed; source/result credits released | Input owners detached; owned runs removed; baseline credits restored |

The file-backed observation records 72,039 stored frame-bound rows, 22,600
distinct intervals, 45,200 events, 48,036 extrema-summary rows and 116,512 lookup
blocks. The streamed observation records 24,013 detached rows across 49 internal
window input batches. These counters describe actual work, not performance gains.

Public pressure cases separately use the 1-GiB minimum grant and a 1-MiB flush
threshold. Their source is smaller than that grant; they prove public route,
composition, writer and failure behavior. The 16-MiB native tests establish the
larger-than-grant controls. Neither scope bounds all provider allocations,
Python-retained values, conversion libraries, allocator overhead or process RSS.

## Semantics, composition and failures

The additional public spill-frame cohort preserves prior frozen declarations:
1,498 checks and 1,061,210 complete row comparisons across Python/SQL, existing
frame/exclusion/null/order variants, typed/nested values, empty output, complete
large output and representable writers. Independent readback validates 290
collection results, 1,154 writer results and 54 deterministic denials. Eight
scalar writers and seven admitted typed/nested writers retain their format
contracts; CSV uses quoted JSON text cells and ORC rejects unsupported types.

Streaming adds 221 small semantic/composition checks and eight pressure/failure
checks to the earlier 213-case suite. The resulting 442 checks cover both public
frontends, ranking/navigation, all 48 frame variants, multiple order groups,
empty/singleton/all-null inputs, composed operators, late errors, iteration,
bounded collection and a new native Vortex destination. Independent readback
reconstructs all small expectations and complete pressure values; it reopens 420
envelopes, 365 small-value files, ten malformed-protocol traces and 14 complete
pressure outputs totaling 517,499 rows. The three new complete window outputs
contribute 72,039 of those rows.

Native tests additionally cover exact recursive keys and decimal metadata,
parent validity, unobserved invalid values, signed-zero selection and error
order across functions, partitions and final result validation. Failure tests
exercise reservations and quota, cancellation during real frame/distinct/extrema
work, truncated/corrupt/replaced runs and cached generations, late producer/schema
failure, slow/failing consumers, failed cleanup and protected destinations.
The dead-owner fixture kills a real child after native window runs exist,
rejects live-owner or unknown-file cleanup, then checks owned cleanup and a
fresh complete restart. It does not resume the interrupted operation.

## Verification and provenance

The accepted runtime is `80057ba60fbfb64ac9d0b9a78c209706e4d300fb`.
All 1,001 frozen runtime assets have combined identity
`496096fd701b1f38e321a8b35f0e6e667d055100a08e50a3dca693c4878e41fb`.
The locked, offline release build enables `release-user-surfaces`; its executable
SHA-256 is `a938c218b403b2827ee8979138d706c4e0e1247d26b4c1a0165f0cbce701d6b3`.

| Verification | Observed result |
| --- | --- |
| Formatting, default workspace lint/tests | Pass; 3,145 tests, zero ignored |
| Native Vortex library and examples | 2,468 library tests pass, 24 ignored; 17 example tests pass |
| Native CLI tests and CLI/Vortex lint | Pass; 1,310 CLI tests |
| No-write, write-only, lean and MSRV feature checks | Pass |
| Python tests with optional adapters | 615 pass |
| Frozen public regression | 28,871 cases; 16,881,391 complete rows |
| Additional spill-frame cohort within that regression | 1,498 cases; 1,061,210 complete rows |
| Direct unary regression | 202 cases; 131,734 complete rows |
| Completion-aware input suite | 442 cases, including 229 new window cases |
| Resident batch / format fidelity | 48 / 19 cases |
| Admitted semantics / golden workflows | 145 / 9 stages |
| Retained Full43 reference | All 43 queries, three runs each; 129 complete values pass |
| Independent packet inspector contract | 165 positive/negative checks pass |

All 15 source gates run on the same accepted runtime assets. Suites overlap;
counts are not additive workload totals. The packet retains exact commands,
source snapshots, features, supervision receipts, assertions and original logs.
Required workspace commands are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --all-targets`. Native, feature-matrix and MSRV commands
are recorded separately. Manifests, lockfile and vendored source are byte-identical
to the prior accepted join build; no dependency or license scope changes.

The previous 27,373 public declarations and 15,820,181 expected rows are preserved.
Only the new explicit-spill frame cohort extends them. Its expectations come from
the prior accepted frame declarations, with path and execution-policy changes
only. Complete driver/source checks enforce that boundary. No candidate result
generates an expected value.

### Retained-dataset observation

The retained Vortex input contains 15,682,956,489 bytes, with SHA-256
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
The unchanged native correctness-reference packet has SHA-256
`cb83c770674073f31ad0c4224f02faeb1ba557c512595a5c2d36f92bb2344b39`.
The macOS arm64 request uses `memory_gb=24` and `max_parallelism=12`; these are
admission settings, not physical capacity or exclusive-machine guarantees.

Across three runs per query, the sum of query minima is 67.511189 seconds,
the sum of query medians is 68.775499 seconds, and all 129 native process times
sum to 222.351981 seconds. Each includes startup, complete public output and
exit. The guarded query-stage wall time is 255.623873 seconds, including
supervision, value checks and log compression, excluding the separate preflight.
The highest observed native child RSS is 4,750,950,400 bytes. These are retained
dataset observations, not fresh-ingest timing, paired comparisons, a speedup
claim or enforced RSS bounds. File cache and ordinary host activity are uncontrolled.

Initial preflight stopped before queries at the unchanged 252-MiB accumulated
log threshold. The completed prior join cohort's 516 original JSON/companion
files were archived with original identities, closed-handle checks and full
readback before redundant originals were removed. This recovered 3,362,816
accounted log bytes. Summaries, failed/incomplete observations and all storage
ceilings remain intact. Finalization independently reopens all 516 members.
Evidence readback also preserves an initial failure from a Python environment
without the optional Arrow reader; the accepted continuation uses the same
adapter environment as the passing format suite. No engine result, runtime
source or expected value changed for either continuation.

The [support acceptance](evidence/native-window-support-2026-10-08.json) and
[support archive](evidence/native-window-support-2026-10-08.tar.xz) bind seven
documentation/build checks, six executed native examples and the actual rendered
desktop/mobile/search review. The window example returns every expected row and
verifies input detachment, successful completion and owned cleanup. Search for
`window` reaches the Python batch-results section. Dark desktop and light mobile
pages have no page-width overflow or console errors; navigation works and both
temporary servers are drained. Initial check receipts and browser observations
are retained alongside the second pass after two copy corrections. All 1,001
accepted runtime assets and the frozen executable remain byte-identical.

The [remaining-scope contract](../architecture/native-local-completion-scope-2026-10-07.md)
preserves pivot pressure, broader allocation and streaming/adapter coverage,
operational platform/release acceptance and all eight performance investigations.
Cleanup/restart is distinct from execution resume. This finite unit closes no
whole PERF/CG owner and does not trigger a version bump.
