<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native unary membership pressure acceptance

Local source acceptance passed on October 19, 2026, for the October 18 work unit.

Distinct, deduplication with first/last/all-duplicates policies, and duplicate
masks with those three policies now complete the admitted native pressure
workloads under explicit spill permission. This report covers those seven
relational unary variants. Hosted, installed-wheel and other-platform acceptance
remain separate. Published v0.6.0 artifacts are unchanged; this is unreleased
development behavior, with no version bump or publication.

The [implementation contract](../architecture/native-unary-membership-spill-2026-10-18.md),
[evidence index](evidence/native-unary-membership-spill-2026-10-18.json) and
[member manifest](evidence/native-unary-membership-spill-2026-10-18.manifest.json.xz)
bind the sources, actual compiled artifacts, complete values and failure history.
Every archive member is reopened against its recorded size and SHA-256 before
acceptance. Bulk outputs and executable files remain local, with their identities
recorded in the index and complete values and regeneration sources archived.

Only this finite scope within `PERF-06.membership`, `PERF-06.recovery` and
`PERF-06.strategy` is accepted. Sampling populations, subquery indexes and legacy
standalone `PreparedVortexUnary` retain their resident contracts. All 14 broader
owners, 58 substeps (37 required and 21 conditional), and CG-1 through CG-23
retain their independent obligations.

## Execution and ownership

The implementation extends the existing chronological replacement-run store
using pinned Vortex 0.85 providers. Exact unary keys retain their original type,
nested/null and floating-bit semantics, including distinct signed zero. These
are unary semantics; relational set equality is not substituted.

One bounded ordered buffer and immutable native runs retain each key's first
and last ordinal, exact count and only its required payload. Newer complete
state replaces older state during chronological compaction. First-occurrence
variants emit online. Future-dependent deduplication orders surviving payloads
by their selected source ordinal. Future duplicate masks retain only bounded
prefix identities and consult final membership after the producer completes.

Predicates precede key observation. Prefix limits preserve skipped suffix-key
evaluation; future policies still observe every passing key. Replacement and
duplicate-payload observation remain unchanged. Private movement preserves
scalar floating storage that may later be discarded; selected public output
still passes the existing finite-value checks. Nested selected payloads retain
their existing eager validation.

Native run blocks use at most 256 rows, or a smaller requested batch size.
The shared execution owner credits keys, payloads, schema, lookup caches,
readers, merge work and output. Small state can remain resident. Strategy
selection is distinct from actual temporary-file activity, which the pressure
checks require. Vortex remains native persistence; other formats are explicit
exports. No external engine executes runtime work.

## Constrained completion and failures

The original wide-key pressure fixture uses 8,193 rows and 2,048-byte strings.
All seven variants complete under its unchanged 16-MiB execution grant when
spill is enabled; resident controls retain explicit denial. The future-mask
identity fixture completes under its unchanged 8-MiB grant. The store has a
128-MiB quota and 1-MiB buffer threshold. A single hot key, large individual
records, unique/skewed populations, limits and exact original order have
separate regression controls. A buffer threshold is not a record-size limit.

Admission for wider workflows is recorded separately:

| Boundary | Grant |
| --- | --- |
| Original key pressure fixture | 16 MiB |
| Original future-mask identity fixture | 8 MiB |
| Direct-binding producer and publication | 32 MiB |
| Public wide fixture through all eight destinations | 32 MiB |
| Independent direct native-file readback | 128 MiB |

All eight writers and fanout complete from one producer execution per workflow.
Direct binding checks SQL/DataFrame convergence, bounded delivery and retained
Arrow values after both clients close. The independent readback grant does not
establish a smaller producer grant. Python `batch_rows` limits delivered slices;
it does not bound the native file scan or Arrow admission. Managed reservations
do not impose a whole-process RSS ceiling.

The native family checks 252 typed-key comparisons across nine key sets, seven
variants, dictionary/plain layouts and resident/spill strategies. Its generation
matrix covers 105 combinations: seven variants, five lookup/merge observation
phases and corrupt/truncated/replaced runs. Another 21 combinations cover quota,
grant and partial-merge cancellation; 28 cover late producer/schema/consumer
failure and cancellation. Seven real process-death cases preserve live and
unknown owners, clean abandoned owned files and execute a fresh query. Seven
native-file generation cases preserve the same failure boundary. These suites
overlap; their counts are not additive.

The public harness retains 63 publication-failure cases: producer faults,
corruption and quota exhaustion across seven variants and three terminals
(batches, Vortex and fanout). Each case also verifies a fresh successful execution.
Successful reports require producer and consumer completion. Early abandonment
remains provisional. Cleanup followed by fresh execution is not interrupted-query
resume, and rejecting an existing target is not generation-conditional replacement.

## Verification

| Check | Result |
| --- | --- |
| Required native unary selectors | 17 pass, including the process-death child entry |
| Native Vortex library, release features | 2,992 pass; 29 opt-in screens ignored |
| Default workspace tests | 3,235 pass; zero ignored |
| Selected native CLI suites | 1,087 pass; zero ignored |
| Formatting and strict workspace/native-feature lint | Pass |
| Required direct native binding tests | 103 pass; zero skipped |
| Public context tests | 17 pass; one installed-package check skipped |
| Public input/operator union | 5,126 cases across 37 families pass |
| New unary complete-value archives | 274 archives; all 2,183,578 rows independently reopened |
| Full43 regression | 43 queries, one execution each; all 381 result rows and schemas match |
| Modular workflows | 18 cohorts; 704 native and 704 fresh independent pandas oracle workflows match |

The public union adds 210 membership checks and five future-mask pressure cases:
4,911 + 210 + 5 = 5,126. Its focused unary set comprises 210 membership and 51
pressure cases. Independent verification checks complete values and defined
source order, all 13,944 compressed JSON members, 6,949 no-fallback envelopes
and frozen source snapshots. The 74 earlier distinct-set value archives are
also reopened as retained regressions.

The modular matrix covers eight input formats for collection and eight outputs
from CSV, with raw/prepared states and 22 workflows. It is not a complete
input/output Cartesian product. Pandas is an independent comparison oracle only.

Full43 engine time is **50.027328742 seconds**, summed from
`engine_execution_seconds` over one execution of each query. This single
regression observation uses a retained native reference, not a fresh independent
Full43 oracle, and establishes no speed improvement or superiority. The input is
`fresh-roadmap-663-a1.vortex`, 15,713,610,513 bytes, SHA-256
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`.
It was not reingested. Full pre/post hashing and generation checks passed outside
query timing. Cache state was uncontrolled and preflight hashing may warm it.
Per-query clocks, complete values/schemas, host observations and supervision
diagnostics remain in the packet.

The named context skip is
`test_process_result_chaining_preserves_empty_nested_nullable_and_exact_types`.
It requires a separately selected clean installed package. That mode was not
enabled; local source acceptance does not replace installed-package proof.

## Source identity and preserved corrections

Acceptance source is `cafa2ae7438f905b6e53e0f1837d4a93e3decea0`. Actual compiled
source is `a31bd7748dfb37d9b8550deb274eeef8a9d7a2e8`; the CLI SHA-256 is
`779bd13a06449d0cd909a6820362130dd4bf3c4e415299e925486c5e099b3444`, and the
native binding SHA-256 is
`879e5348312072f44f99004c1922af8b224d569ab8c874bd3ba8d829e43a067a`.

The intervening changes affect only `python/tests/test_native_unary_binding.py`
and `scripts/native_streaming_unary_membership_cases.py`. The explicit reuse
proof checks the complete Git diff, all 2,181 frozen source files, compiled
artifacts and 12 passed gate/build/freeze receipts with their original log hashes.
All 2,179 other source files are byte-identical. The build record is retained
verbatim. Binding, public, Full43 and modular acceptance are fresh after the
harness corrections, with actual compiled and harness commits recorded separately.

The packet preserves the native ownership-test and lint corrections, the first
failed direct-binding and public attempts, and the readback diagnosis. The
ownership test had retained a DType owner past its zero-credit assertion; it
now releases that owner first. Independent readback at 32 MiB and the wide
publication fixture at 16 MiB correctly encountered separate admission limits.
Their final grants are listed above; the original 16/8-MiB operator-pressure
fixtures are unchanged.

A later focused public run stopped after 150 passes because a truncated cached
run was rejected by its generation check before the expected byte-length check.
The assertion now accepts those two exact corruption diagnostics. All 261
focused cases then passed, and independent verification reopened every unary
value archive. That diagnostic run had a changed harness and is not counted as
complete acceptance. The final full run uses the newly frozen source identity;
all failed and superseded attempts remain available.

The first final evidence check also mistook the modular runner's 18 progress
streams for single JSON receipt documents. A separately retained verifier
correction recognizes only the exact declared stream paths and checks their
unchanged identities. All other receipt parsing, source, plan, result and cleanup
checks remain intact. The original verifier, frozen plan and failed receipt are
preserved; the corrected final check passes without rerunning runtime workloads.

Reproduction uses the packet's source/build manifests, frozen plan, guarded
commands and independent verifiers in a fresh local-only evidence directory.
Required broad gates are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`cargo test --workspace --all-targets`. Native gates use the locked, offline
`release-user-surfaces` build. Exact selectors, feature flags and Python commands
remain in the receipts. Historical output paths are immutable and must be
rebound for a new run. Storage and concurrency ceilings were not raised.
