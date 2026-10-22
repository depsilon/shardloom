<!-- SPDX-License-Identifier: Apache-2.0 -->

# Region-dependent investigations — October 20, 2026

Status: three explicit **missing-prerequisite decisions**, reviewed after the
finite projection-request candidate was rejected and removed. No new ordered
candidate, preparation policy or topology comparison is admitted. These records
do not complete the three conditional substeps or their broader owners.

The review binds source `730f793d4509ef09a1791e2d0a5aa45de68260e4` and the
[row-16 decision](native-region-projection-decision-2026-10-20.md). That candidate
failed three of four actual-read targets; no timing cohort ran. Its exactness
and lifetime checks do not establish a retained topology, local reduction or
preparation strategy. Its copied-executable guard limitation remains explicit.

## Current execution boundary

The selected current aggregate path in `local_primitives.rs` admits
`CountWorkers`, creates the existing native scan iterator, obtains each native
array, then submits that array to the bounded workers. The worker interface takes
an `ArrayRef`; `AggregateChunkJobs` accounts for the caller's CPU lane, queues,
memory leases and ordered completions. Those are useful retained foundations.
They do not show independently scheduled region reads with local reduction.

The current ordering path uses provider natural splits when its scratch estimate
admits them and otherwise retains its existing smaller input batches. It still
consumes native scan chunks through the existing ordering path. Native split
discovery and bounded candidate storage do not by themselves prove region-local
Top-K and a bounded global candidate merge.

This conclusion is scoped to the cited current callers and recorded experiments;
it is not a repository-wide claim that no other parallel work exists. The pinned
Vortex 0.85 provider already supplies natural split discovery and reuse. Its
provider availability is distinct from ShardLoom's admission and acceptance.
The existing [topology design](execution-aware-native-artifact-topology-2026-09-08.md)
requires the applicable provider/aggregate scheduling ownership to share one CPU
grant; adding another pool around these callers would not satisfy that contract.

## Decisions and reopening conditions

| Conditional row | Specific missing prerequisite | Work required before its experiment |
| --- | --- | --- |
| `PERF-10.ordered_candidates` | Retained independent natural-region execution and exact region-local reduction under the shared resource owner. | A materially different region mechanism must prove generation identity, complete coverage, original batch boundaries, skew progress, ordered delivery and cancellation/drain, then pass its cost gate. Admitted local reduction must preserve NULLs, overflow, key equality and floating semantics before local candidates are compared with bounded global merge. |
| `PERF-09.design` | Accepted runtime evidence connecting region geometry to pruning, encoded work and merge cost. | Freeze joint preparation/ingest/storage/query costs after runtime acceptance. Include small, wide-text, large-numeric, dictionary-heavy, unknown-row-count and constrained-memory inputs. The finite codec/text profile rejections do not supply this joint evidence. |
| `PERF-12.topology` | A newly admitted topology mechanism. | Freeze safe current/coarse/small/medium/large modes, identical immutable 100M-row input, build/resource identities and realized/pruned/materialized work. Preserve the original 129-call comparison coverage and independent held-out controls. No such comparison runs in this review. |

The earlier grouping and coalescing attempts remain unretained. The
[coalesced-job decision](../benchmarks/native-topology-coalesced-2026-09-08.md)
explicitly reports local reduction and local Top-K as inactive and leaves its
fresh held-out matrix unrun after rejection. Repeating that unchanged mechanism
cannot satisfy the new-mechanism prerequisite. The recent projection deferral
also supplies no retained replacement.

These dependencies were revisited after row 16's result rather than inferred
from the initial prospective intake. Reopening requires new evidence at the
named gate, not favorable results from another investigation. No performance
comparison or general no-benefit conclusion is made here.

## Auditable record

The [prerequisite record](../benchmarks/evidence/conditional-prerequisites-2026-10-20/region-dependent-acceptance.json)
contains each decision, missing input and reopening condition, with exact source
hashes and line excerpts. It binds the accepted region comparison/restoration
packets. The [verifier](../benchmarks/evidence/conditional-prerequisites-2026-10-20/verify_conditional_prerequisites_832.py)
checks those source versions against Git and their evidence identities; use
`--repository`, `--region` and `--pgo` with the two records in that directory.
It reports three missing prerequisites and zero completed substeps.

No runtime source, provider API, dependency or public capability changes in this
review, so no new native workload is run. The fresh restoration checks in row 16
remain their own historical execution evidence. All 58 roadmap substeps and
CG-1 through CG-23 keep their existing acceptance boundaries.
