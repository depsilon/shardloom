# Native join pressure and mixed-source acceptance

This unit implements all seven admitted join kinds under an explicit native
spill policy and accepts one finite single-use batch source on either side,
alongside ordinary file or resident sources. It extends the existing Join,
Ordering, query run store and result writers. The default resident strategy and
its deterministic resource denial remain available.

Status: complete local runtime acceptance and independent packet inspection.
Hosted integration completed in
[PR #1532](https://github.com/depsilon/shardloom/pull/1532) at `25906290`; the
[hosted receipt](evidence/native-join-hosted-2026-10-08.json) records all 39 checks
and the preserved accepted runtime. The [evidence index](evidence/native-join-pressure-2026-10-08.json),
[portable packet](evidence/native-join-pressure-2026-10-08.json.xz) and
[independent inspection](evidence/native-join-pressure-2026-10-08-inspection.json)
bind the implementation, complete results and original failure history.
This report preserves its original v0.4.0-era checkpoint and makes no performance
or whole-process memory claim. Subsequent window/pivot completions, publication
and still-open obligations follow the
[remaining-work contract](../architecture/native-local-completion-scope-2026-10-07.md).

The packet contains 44,936,164 compressed bytes with SHA-256
`2e939bf93341ed72fec15ccb8b04ccfae119b76c794bf0b43b1da161483634f9`.
Complete decompression produces 3,645,903,016 bytes with SHA-256
`a67766a72592b7cb7c95c32430b0876e88cb9f55249f134418d9fb12b97113d6`.
Source/artifact hashes identify original local bytes; portable text substitutes
local paths. The finalizer reconstructs complete expected values before the
separate stream inspector checks every saved report and claim boundary.

## Contract and reuse

The [design and reuse map](../architecture/native-join-pressure-2026-10-08.md)
records the Vortex-first decision. Pinned Vortex 0.85.0 arrays, credited builders,
native take and bounded file scans remain the native providers. ShardLoom owns
join admission, exact key equality, ON evaluation, output order, cancellation,
spill quota and publication. No dependency, alternate query engine or
error-triggered execution retry is introduced.

An explicit policy builds stable native records containing the existing key
hash, original ordinal and typed payload. Hash ranges narrow candidates; exact
equality still decides matches. Bounded candidate batches preserve the resident
ON evaluator's order, errors and semi/anti short-circuit boundary. Right/Full
joins record matched positions and restore unmatched build rows to their
original order through the same native Ordering. A resident match bitmap or
index proportional to the full spilled build is unnecessary.

Exactly one batch URI can occur once in a streamed plan. Ordinary sources may
occur normally, including repeated file/resident scans. The private input owner
must release before the next demand; delivered arrays retain their independent
native credits. Nested joins, aggregates, ordering and zero-output limits drain
and validate the complete source before success. Repeated batch use and multiple
batch producers fail before consumption. Existing intake types and finite
row/frame/batch limits remain explicit.

## Complete constrained workload

The native Full-join fixture has 65,537 build rows carrying 512-byte strings,
nullable permuted integer keys and three probe rows. Two matches, one null-left
extension and every unmatched right row produce 65,538 outputs in exact order.
Every output is compared independently in both successful controls.

| Observation | Result |
| --- | --- |
| Input logical bytes | 34,620,572 |
| Constrained native grant | 16,777,216 bytes (16 MiB) |
| Resident control at that grant | Deterministic reservation denial; reservations refunded |
| Ample resident control | Complete values pass under 128 MiB; peak reservation 44,482,332 bytes |
| Explicit spill at 16 MiB | Complete values pass; peak reservation 6,542,711 bytes |
| Actual native runs / merge passes | 254 / 126 |
| Peak temporary disk / quota | 71,504,016 / 268,435,456 bytes |
| Bounded lookup blocks loaded | 28 |
| Final state | Source complete, private input released, owned runs removed, reservation baseline restored |

These are native reservation and disk observations. Public pressure cases use
the existing 1-GiB minimum grant and a 1-MiB flush threshold; their source is
smaller than that grant. The two pressure boundaries are not interchangeable,
and neither controls all provider allocations, Python-retained values or RSS.

## Semantics, composition and failures

Native fixtures cover all join kinds, forced hash collisions, typed and static
nested keys/payloads, cross-integer equality, null roots, duplicate hot keys,
empty inputs, no-key ON conditions, three-valued predicates, evaluation errors
and original unmatched-right order. The existing typed/nested/ON suites also
exercise the new strategy. Eight scalar writers and seven admitted typed/nested
writers check complete output and reopening under their format contracts; CSV
represents nested cells as quoted JSON text, and ORC rejects nested output.

The public additions comprise 96 small semantic/composition cases and eight
pressure/failure cases. They exercise SQL and DataFrame surfaces, both batch
source sides, file/resident counterparts, nested joins with grouping/sorting/
limits, collection, incremental delivery and one native Vortex destination.
The full 213-case stream suite retains earlier input, ordering, aggregation
and malformed-protocol checks. Its 199 envelopes, 153 small-value files and
445,460 saved pressure rows are reopened independently; the three new complete
join outputs contribute 196,614 of those rows.

Fault tests inject late producer/schema/ON failures, exhausted reservations,
quota denial, cancellation during real merging/probing, damaged or replaced
runs, source mutation, failed consumers and failed cleanup. Existing output
destinations remain intact, and incomplete new files are not published. The
dead-owner fixture kills a real child after native join runs exist, rejects
cleanup while its owner is live or unknown files are present, then proves owned
cleanup and a fresh complete restart. This does not resume the interrupted join.

## Verification and provenance

The accepted runtime is `92ab9f203160d4add9b6cfc33396aee5d3349c6a`.
Its 981 frozen source assets have combined identity
`aff4e5bc78535360bd388262004a0c32593c0fd4038b7ab97f4fb4283cb4149a`.
The locked, offline release build enables `release-user-surfaces`; its executable
SHA-256 is `75928d82313aa656957a5b4f918ceb9e32e5c66509f47ae13da69d7a0ee489e3`.

| Verification | Observed result |
| --- | --- |
| Formatting, default workspace lint/tests | Pass; 3,145 tests, zero ignored |
| Native Vortex library and examples | 2,440 library tests pass, 24 ignored; 17 example tests pass |
| Native CLI tests and CLI/Vortex lint | Pass; 1,310 CLI tests |
| No-write, write-only, lean and MSRV feature checks | Pass |
| Python tests with optional adapters | 615 pass |
| Frozen public regression | 27,373 cases; 15,820,181 complete rows |
| Direct unary regression | 202 cases; 131,734 complete rows |
| Completion-aware input suite | 213 cases, including 104 new join cases |
| Resident batch / format fidelity | 48 / 19 cases |
| Admitted semantics / golden workflows | 145 / 9 stages |
| Retained Full43 reference | All 43 queries, three runs each; 129 complete values pass |
| Independent packet inspector contract | 165 positive/negative checks pass |

All 15 engine gates pass. Suites overlap; their counts must not be summed as
distinct workloads. The archive retains exact commands, source snapshots,
features, process supervision, assertions and original logs. Required workspace
commands are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --all-targets`. Rust gates use locked/offline resolution
where applicable, Rust 1.99.0 for the current toolchain, and the recorded MSRV
for its separate checks. Public replay uses the checked-in
`run_native_streaming_input_uat.py`, `run_native_relational_suite.py`,
`run_native_unary_uat.py` and `run_clickbench_query_uat.py` drivers with their
frozen declarations and the local storage/process guards.

The gate record preserves two failed lint/feature attempts and their exact
corrections: splitting one CLI test without changing assertions or production
code, and gating the positional-read entry point with its existing write feature.
Only unaffected passed checks are reused. Final CLI tests, formatting, native
clippy, no-write/write-only, lean and MSRV checks run on the relevant corrected
source. All function bodies in the feature-gate delta are unchanged.

The public regression's 27,373 declarations and 15,820,181 expected complete
rows are unchanged. The only prior Python adapter difference is the named
`from_batches` docstring, verified by comparing the entire module AST after
replacing that one literal. No candidate result is used to generate expectations.
The retained Full43 source/reference remains a correctness regression with no
comparative speedup claim or fresh-ingest claim.

### Retained-dataset observation

This observation ran on macOS 27.0 arm64, with 10 logical CPUs and
17,179,869,184 bytes of physical RAM. The request selected `memory_gb=24` and
`max_parallelism=12`; these are admission settings, not the machine's physical
capacity or proof of exclusive CPU use. The retained Vortex source contains
15,682,956,489 bytes, with SHA-256
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
The unchanged reference packet hash is
`cb83c770674073f31ad0c4224f02faeb1ba557c512595a5c2d36f92bb2344b39`.

Across the three runs of each query, the sum of query minima is 64.310469 seconds,
the sum of query medians is 65.415259 seconds and the sum of all 129 native
process times is 197.135572 seconds. Each native clock includes startup, complete
public output and exit. The guarded query-stage wall time is 228.437080 seconds,
including its supervision, result checks and log compression; the separate
admission preflight is excluded. These sums are not ingest-plus-query elapsed
time. The highest observed child RSS is 5,590,269,952 bytes, an OS observation
rather than an enforced bound. The source was prehashed and cache/ordinary host
activity were uncontrolled. No comparative performance claim follows.

Initial preflight stopped before queries at the unchanged 252-MiB accumulated
log threshold. One completed prior cohort's 516 JSON/companion files were
archived with original identities, closed-handle checks and byte-for-byte
readback before redundant copies were removed. This recovered 3,362,816
accounted log bytes. Its summary and all failed/incomplete observations remain
intact; no limit was raised. The final packet independently reopens all 516
members again. Evidence readback also retains an initial failure caused by
launching without the optional Arrow dependency; the accepted readback uses the
same adapter environment as the passed format suite. No query result or oracle
changed for either continuation.

The primary source review rechecked exact equality and ON evaluation order,
bounded candidate/input/output ownership, original unmatched-right order and
cached reader generation validation. No further runtime correction was needed.
The archived review is primary acceptance evidence, not an independent submitted
GitHub approval.

### Documentation and executable examples

All seven support checks pass: the user-surface reference, public status,
use-case backlinks, Astro validation, website generation, website readiness
and whitespace checks. Five exact documentation examples execute against the
accepted binary and compare their complete rows, including streamed aggregate/
order/limit composition and the new streamed left join. All 981 accepted source
assets remain byte-identical. The contribution-governance check also passes.

Rendered review covers the dark desktop guide at 1,280 pixels, the light mobile
guide at 390 pixels, resource/recovery and support-limit pages, and opening and
closing the mobile menu. The page body stays within each viewport; code and
wide tables retain their own horizontal scrolling. Searching for `joins`
returns the updated guides and follows the Python result to the batch-consumption
section. No browser console warnings or errors were observed. The temporary
tab and preview server are closed and the viewport override is reset.

The [support receipt](evidence/native-join-support-2026-10-08.json) and
[portable archive](evidence/native-join-support-2026-10-08.tar.xz) retain exact
source files, checks, executable examples, observed page text and screenshots.
This is local documentation acceptance; hosted integration remains separate.

## Remaining scope

This closes the declared join pressure and mixed-source family under
PERF-02/03/06/10/12 and CG-20/21 when local and hosted acceptance complete.
Window/pivot pressure, repeated batch-source spooling, wider batch-input types,
streamed compatibility writers/fanout, automatic preparation ownership,
remaining allocation coverage, platform acceptance and the eight conditional
investigations retain their [existing owners](../architecture/native-local-completion-scope-2026-10-07.md).
Cleanup/restart, execution resume and output-transaction recovery are distinct
promises. No whole PERF or CG gate is closed by this finite unit.
