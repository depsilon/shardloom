<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native typed unary state acceptance — October 3, 2026

The finite typed-unary unit passes local functional acceptance on frozen source
`948551d4`. Hosted acceptance remains open, and the paired Q9 timing/RSS
observation is inconclusive. This report preserves those limits.

The existing native unary state machines now preserve Binary, Decimal128,
Date32 and timezone-free TimestampMicros. Direct file calls and relational
composition use the same admission, selected state and output ownership.
This finite unit extends the [typed unary contract](../architecture/native-typed-unary-2026-10-03.md);
broader nested, aggregate/window, adapter and resource work remains separate.

## Frozen scope

| Evidence | Scope |
| --- | --- |
| Candidate source | `948551d476855682e7693973bcf2ef00260d9fd2` |
| Candidate executable SHA-256 | `388aeac40e07fdd1cf9b29df4289897a26b6e26ba436bcfecd800bd920caea47` |
| Paired control | `cdc6b66103d00b9d34979d38dc9d91c7d11717ee`, the validated 0.4.0 source candidate |
| Control executable SHA-256 | `2849ed602194d0af0811a39f8948f283fb531dea3543b6a4f86953d5b1208d23` |
| Build | Rust 1.99.0, release profile, `release-user-surfaces`, identical Cargo lockfile |
| Source identity | Clean committed candidate, 829 source fingerprints, CLI and both fixture executables frozen |
| Complete public scope | 9,300 passed checks; 14,125,745 complete row comparisons |
| Typed subset | 5,995 checks; 6,438,220 rows |
| New unary subset | 2,700 checks; 5,412 rows from 135 independent declarations |
| Separate direct-unary scope | 202 passed checks; 131,734 rows |

All 6,600 cases and 14,120,333 expected row comparisons from the accepted
typed-expression cohort retain their names and row counts. No denial or earlier
case is removed. The added complete-value oracles are frozen before execution.
Native composed, declared-Arrow composed and direct native routes each run
equivalent SQL and DataFrame declarations, collection at parallelism one/two,
and the existing eight writer checks. ORC decimal/temporal denials remain
explicit; binary-only ORC output has positive coverage.

## Semantics and ownership

Distinct selection, first/last/remove-all duplicate policies, duplicate masks,
tail, sampling, forward-fill and typed replacement retain exact logical values.
Melt uses one lossless admitted scalar domain; selected decimal values rescale
exactly. Rolling COUNT uses validity. Pivot supports exact typed keys and
first/first-unique payloads, with lossless typed fill for missing cells. A present
NULL cell remains NULL. Unsupported decimal/temporal numeric aggregates and
incompatible typed melt domains fail at binding, including empty input.

The core `ScalarValue` model declares expanded rewrite and pivot parameters.
CLI binary/decimal/temporal forms and Python declarations preserve exact values;
Decimal validation is independent of the active Python decimal context.
Primitive JSON spellings remain compatible. The unpublished Rust request fields
have an intentional 0.4 source migration; legacy predicate values retain their
existing `StatValue` contract.

Compact retained state reserves variable bytes and container capacity before
copying. Existing native source arrays, encoded dictionary access, output
builders and resource owners remain in use. Tests cover cross-batch state,
empty/all-null schemas, full temporal storage extrema, parent validity, renamed
schemas, unselected large domains, narrow grants, cancellation, failing consumers,
and final-owner credit release. Failed writers stop after an emitted prefix and
leave no new destination or staging files; existing-destination rejection preserves
the original file. Columnar writer readback preserves logical DTypes.

All 2,526 successful new public typed-unary calls retain route-specific resource
proof under the same 1-GiB operation grant; the other 174 new checks are explicit
ORC denials. Direct writers additionally prove the owned native array stream and
source-generation validation. Missing, malformed or excessive peak reservations
fail the verifier. These reservations do not account for every provider scratch
allocation or enforce process RSS.

The 25 selected local gate categories pass. Default workspace tests report
3,469 passed; native Vortex reports 2,333 passed with 23 existing ignored tests;
native CLI reports 1,594 passed. Python runs 875 tests with 144 skips. The UAT
consumer suite includes six archive-retention tests. Counts overlap across build
configurations and must not be added as independent tests.

Broad default/native lint, tests, lean/no-write and Rust 1.96 compatibility
checks retain byte-identical compiled runtime inputs. The subsequent source
changes affect public expected-column ordering, support-reference validation,
lossless UAT log storage and selection of the direct writer's existing native
resource evidence. Their paths and retained-check rationale are explicit
in the packet; current native tests, consumer tests and public acceptance cover
the final candidate. Final documentation checks are recorded separately.

## Paired Full43 regression

All 258 initial complete-result comparisons and 18 prescribed reversed-order
calls pass. The initial aggregate crosses neither investigation threshold:

| Initial Full43 metric | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Sum of each query's fastest valid call | 57.592227 s | 57.372117 s | -0.3822% |
| Sum of per-query medians | 59.190897 s | 59.648752 s | +0.7735% |
| Sum of all 129 calls per role | 195.880075 s | 195.357152 s | -0.2670% |

These sums are process measurements, not elapsed workflow time. The supervised
initial paired workflow takes 472.451565 seconds; the three-query repeat takes
44.759412 seconds. The complete public workflow takes 799.609788 seconds, and
the separate direct-unary workflow takes 3.097713 seconds.

| Flagged observation | Initial candidate change | Reversed-order change |
| --- | ---: | ---: |
| Q9 median time | -14.8150% (-0.253463 s) | +21.3273% (+0.281972 s) |
| Q9 median RSS | +1.2634% (+48,201,728 bytes), below threshold | -12.3397% (-489,029,632 bytes), flagged |
| Q15 median time | +10.3475% (+0.142552 s) | +3.9089% (+0.055795 s), below threshold |
| Q34 median RSS | +15.5309% (+647,266,304 bytes) | +3.4976% (+159,285,248 bytes), below threshold |

Q15's timing and Q34's RSS flags do not reproduce. Q9 changes timing direction;
its reverse cohort still crosses the timing and RSS thresholds. Q9 user CPU
changes -2.1751% then +0.6909%, while system CPU changes -26.3254% then +48.5301%.
These observations do not establish a cause. A descriptive pooling of all six
calls per role gives +2.3403% Q9 time and -0.1195% RSS, but this post hoc summary
does not replace the frozen comparisons or clear their flags. Q9 performance
remains inconclusive. Local functional acceptance rests on the complete value
and resource checks; no speedup or uniformly unchanged performance is claimed.

Both binaries read the same resident 99,997,497-row Vortex source and compare
all 43 complete retained results, three calls per query and role. This is
retained-result regression evidence, distinct from the new independent unary
oracles. Source length is 15,682,956,489 bytes and SHA-256 is
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
No ingestion timing or external-engine comparison is included.

The macOS 27.0 arm64 host has 16 GiB physical RAM and 10 logical CPUs. Each
role uses parallelism 12 and a 24 GiB admission grant, which is not an enforced
RSS limit. Work remains sequential under the existing storage, process-deadline,
overlap and cleanup guards. Source hashing warms bytes; OS cache and ordinary
host activity remain uncontrolled.

Predeclared investigation thresholds are symmetric: per-query median timing
changes of at least 10% and 0.1 seconds, median RSS changes of at least 10% and
32 MiB, or aggregate fastest-call/median-sum changes of at least 5% and one
second. Flagged queries repeat with reversed role order; an aggregate flag
requires all 43 to repeat. Every observation is retained.

## Retained failures and evidence

The initial complete public attempt stops after 7,048 passing checks because
gzip files and their filesystem allocation reach the unchanged 192-MiB log
ceiling. It reports no result mismatch. Its summary, 15,214 envelope references,
source/build/scope manifests and failure log remain intact. It is not counted
as completed acceptance.

The runner adds optional `--archive-logs` batching of up to 128 closed gzip envelopes into an
xz-compressed tar archive. It verifies every original byte, source identity,
archive member and sidecar manifest before removing redundant gzip files.
Per-envelope raw and compressed hashes remain addressable. Readback failures
preserve originals; an existing archive is never replaced. The restarted scope
and complete oracle hash are identical, as are all three release executables.
Storage limits remain unchanged. The packet independently reopens all archives
and reconstructs every envelope.

The second attempt stops after 8,185 passing checks at the first new direct
typed writer. The writer succeeds, but the harness expects the composed/collect
memory-peak field. The direct writer records its peak under the existing native
array-sink field from the same admitted operation session. The corrected check
requires that route's owned native stream and source-generation proof, a valid
numeric peak and the original 1-GiB grant. Missing, malformed, negative and
over-budget evidence fails. Tests also pin incorrect route, export-kind and
source-generation rejection. This attempt is preserved and is not counted as
completed acceptance. Neither correction changes runtime or fixture binaries.

Development receipts retain earlier fixture corrections, the feature-gated
import lint failure and the missing public-status matrix row. The nullable-parent
test uses the produced native result boundary because the upstream file
statistics writer cannot serialize that fixture. Earlier extreme-value and
writer-policy fixture failures are retained with the corrected boundary tests.

The [portable evidence packet](evidence/native-typed-unary-2026-10-03.json.xz)
contains source/binary identity, all frozen declarations and complete oracles,
public/direct summaries, raw envelopes, output hashes, paired results, required
repeats, check logs and verification tools. Local paths use placeholders; replay
must bind them to resident inputs and a new output identity. Historical paths
must not be reused. The previous typed-expression packet remains unchanged.
The compressed packet is 15,499,152 bytes with SHA-256
`1a4da033ef6686c9676769ce2ee89f9ed6ad0e2fcaea833fbaf4a8f402144eca`.
It preserves 20,025 public envelopes, 1,783 supplemental envelopes and all 276
paired/repeat results. The verifier checks 190,295 report field sets for unique
names. Packaging checks corrected the frozen oracle's `{columns, rows}` wrapper
handling and the independent inspector's zero-padded `q01` filename lookup.
The corrected independent inspector passes against the first assembled packet;
that packet, its receipt and both inspection observations remain retained. The
final packet embeds the corrected verifier and this history. No oracle, public
result, acceptance scope or executable changes during these packaging repairs.

Hosted gates remain pending the existing website advisory decision. The hosted
code-review bot reports exhausted review quota and has not completed its review.
The source version is 0.4.0; this does not publish a package, tag or release.
Nested keys/state, richer aggregate/window semantics, wider adapters and general
state spill/recovery remain open. Successful native runs retain
`fallback_attempted=false` and `external_engine_invoked=false`. This evidence
does not establish general speedup, total-process memory enforcement, broad
SQL/DataFrame parity, production readiness or competitive superiority.
