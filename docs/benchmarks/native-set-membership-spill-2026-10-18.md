<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native distinct-set membership pressure acceptance

`UNION` (distinct), `INTERSECT` and `EXCEPT` now complete the admitted native
membership workloads under explicit spill permission. Local source acceptance
passed on October 18, 2026. Hosted, installed-wheel and other-platform acceptance
remain separate. Published v0.6.0 artifacts are unchanged; this is unreleased
development behavior, with no version bump or publication.

The [implementation contract](../architecture/native-set-membership-spill-2026-10-18.md),
[evidence index](evidence/native-set-membership-spill-2026-10-18.json) and
[member manifest](evidence/native-set-membership-spill-2026-10-18.manifest.json.xz)
bind the sources, actual compiled artifacts, complete values and failure history.
The index identifies every archive part; each part and every member is reopened
against its recorded size and SHA-256 before acceptance. Generated bulk outputs
and executable files remain local with their identities recorded in the index.

Only this finite scope within `PERF-06.membership`, `PERF-06.recovery` and
`PERF-06.strategy` is accepted. Unary distinct/deduplication/mask state, sampling
populations, subquery indexes and broader recovery remain open. No broader PERF
owner or CG-1 through CG-23 gate is closed by this result.

## Execution and ownership

The implementation extends existing ShardLoom native ordering and query-run
storage using pinned Vortex 0.85 providers. Complete row keys, source sides and
checked input ordinals are ordered together; one equal-key group is reduced at
a time. A second bounded order restores the resident first-occurrence semantics.
`UNION` retains the first combined left/right occurrence; `INTERSECT` and `EXCEPT`
retain the first qualifying left occurrence. Existing NULL, typed/nested and
signed-zero semantics remain in the shared native key comparison.

Set run blocks and fanout replay blocks are bounded at 256 rows, with smaller
requested batches respected and byte ownership charged separately. The input
replay owner retains its existing independent bound. A permission/strategy
counter is not proof of actual temporary files; the pressure checks require
actual native runs, bounded reservations, quota accounting and completed cleanup.
No-spill resident execution and `UNION ALL` keep their existing algorithms.

Ordered membership observes both complete inputs before delivering output.
Early abandonment still has no successful completion report. Resident controls
preserve their early producer termination, retained output and exact cleanup.
Vortex remains native persistence; other formats are explicit exports. No
external query engine executes residual work.

## Constrained completion and failures

The wide-key fixture uses two sources of 8,193 rows, 2,048-byte strings and
251-row input batches. Cumulative native input exceeds 32 MiB. Under the same
16-MiB operation grant, the resident membership controls refuse growth and the
explicit-spill controls complete. The temporary-storage quota is 128 MiB with
a 1-MiB buffer threshold. Reversed left order, equal/disjoint/overlapping domains,
empty output and a single hot key check exact values and order.

The native fanout regression runs all three operations through Vortex, Parquet,
Arrow IPC, Avro, ORC, JSON, JSONL and CSV from one execution per operation under
that original grant. It reopens every output, verifies actual replay, releases
owned runs and restores credits while preserving an unrelated file. Direct
binding covers SQL/DataFrame convergence, bounded delivery, retained Arrow
owners and fanout without subprocess or JSON-input substitution.

Independent native-file readback is a separate admission boundary. The direct
binding producer and writers retain the 16-MiB grant; their verification client
uses 128 MiB. Python `batch_rows` limits delivered slices, not the underlying
native file-reader or Arrow admission. Small Python batches therefore do not
prove a smaller file-reading grant. Managed reservations also do not impose a
whole-process RSS ceiling.

The native fault matrix covers 75 combinations across the three set operations:
producer/schema failures on either source, input and merge cancellation,
corrupt/truncated/replaced runs, quota exhaustion and failing/cancelled/denied
consumers. Six existing-destination controls reject before input demand. Three
real process-death cases preserve live and unknown owners, clean only abandoned
owned runs, and complete a fresh execution. Recovery does not resume the
interrupted query. Existing-target replacement remains separate PERF-07 work.

## Verification

| Check | Result |
| --- | --- |
| Focused native membership family | 16 tests pass |
| Native Vortex library, release features | 2,978 pass; 29 opt-in screens ignored |
| Default workspace tests | 3,235 pass; zero ignored |
| Selected native CLI suites | 1,087 pass; zero ignored |
| Formatting and strict workspace/native-feature lint | Pass |
| Required direct native binding tests | 102 pass; zero skipped |
| Public context tests | 17 pass; one installed-package check skipped |
| Public input/operator union | 4,911 cases across 36 families pass |
| New complete-value archives | 74 archives; all 450,606 rows independently reopened |
| Full43 regression | 43 queries, one execution each; all 381 result rows and schemas match |
| Modular workflows | 18 cohorts; 704 native and 704 fresh independent pandas oracle workflows match |

The public union adds 83 membership checks and four early-close controls while
replacing three former spill-enabled refusal cases: 4,827 − 3 + 83 + 4 = 4,911.
The new value verifier derives expected keys independently from the numeric
domains and checks every field and the defined source order. It also reopens
13,231 compressed JSON members, source snapshots and 6,496 no-fallback envelopes.
The modular matrix covers eight input formats for collection and eight outputs
from CSV input, with raw/prepared states and 22 workflows; it is not a complete
input/output Cartesian product. Pandas supplies a comparison oracle only.

Full43 engine time is **49.583021628 seconds**, summed from
`engine_execution_seconds` over one execution of each query. This is one
regression observation, with a retained native reference rather than a fresh
independent Full43 oracle; it establishes no speed improvement or superiority.
The immutable input is `fresh-roadmap-663-a1.vortex`, 15,713,610,513 bytes,
SHA-256 `06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`.
It was not reingested. Full pre/post hashing and generation checks passed outside
query timing. Cache state was uncontrolled and preflight hashing may warm it.
Per-query clocks, complete values/schemas, host and resource observations, input
identity and supervision diagnostics remain in the packet.

Suites overlap and their counts are not additive. The named context skip is
`test_process_result_chaining_preserves_empty_nested_nullable_and_exact_types`,
which requires an explicitly selected clean installed package. That mode was
not enabled; source acceptance does not substitute for its separate proof.

## Source identity and preserved corrections

Acceptance source is `7831657a5bb5c3d2504fd6aaf7f0bb8152e3f755`. The actual
compiled source is `7a55bc7fa4fea475035493eded4672c393e85b42`; its CLI SHA-256 is
`c928b13967b2f5081dcbb10d607a7fd5a3cdb7f2366e96c9c156f7aaa595e71d`, and native
binding SHA-256 is `bd7108ae509faa61916e50a7a6481de78582ff203bce2f2a39278336e38f2738`.
The sole intervening change is `scripts/native_streaming_set_boundary_cases.py`.
The explicit reuse proof checks the complete Git diff, all 2,174 frozen source
files, Cargo inputs, compiled artifacts and 13 passed gate/build receipts with
their original log hashes. All 2,173 other source files are byte-identical.
The original build manifest is retained verbatim. Runtime tests reuse that
evidence; the complete public, Full43 and modular runs are fresh. Full43 records
the actual compiled commit, and modular reports keep runtime and harness commits
separate.

The packet retains the initial run-block overlap failures, lint repair, failed
direct-binding attempt and separate file-readback diagnosis. Set blocks first
required the 256-row bound; direct binding then exposed fanout replay rebuilding
1,024-row blocks while the set reader remained live. Both handoff and replay
storage now use the bound, verified under the original producer grant.

The first public attempt stopped at case 1,082 because its early-close test
required an unfinished producer even for ordered spill. The corrected test
checks both strategies while retaining the no-final-report, child-exit, value
and cleanup assertions. All 427 focused set checks then passed. That stopped
attempt is preserved and contributes no complete-suite acceptance credit.

Reproduction uses the packet's source/build manifests, frozen acceptance plan,
guarded commands and result verifiers in a fresh local-only evidence directory.
The required broad gates are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`cargo test --workspace --all-targets`. Native gates use the locked, offline
`release-user-surfaces` build; exact selectors, feature flags and Python commands
are retained in their receipts. Original output paths are immutable and must
be rebound for a new run. Storage and concurrency ceilings were not raised.
