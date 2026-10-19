<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact predicate exclusion — October 19, 2026

Status: **finite one-fact-per-source candidate rejected**. All three repeated-use
targets passed both frozen benefit gates in both cohorts, but two single-call
controls failed their predeclared regression bounds. Correctness, activation,
resource and cleanup checks passed. The experimental implementation, observation
hooks and candidate-only tests were archived and removed.

This is the sixth finite decision in the
[conditional investigation sequence](native-conditional-investigations-2026-10-19.md):
five mechanisms rejected and one retained, with 15 first decisions remaining.
`PERF-10.predicate_exclusion`, broader owners and CG-1 through CG-23 remain open.
The accepted compact local indices remain in production. There is no release,
package, API or capability expansion from this experiment.

## Opportunity and exact native boundary

The existing prepared relational path evaluates Vortex-native predicates and
WHERE masks over immutable memory sources for each operation. The attribution
protocol measured the production path with its observer off and on. All
1,252 workflows and 2,916 operations passed independent complete values,
schemas, certificates, source identity and cleanup checks; all three predefined
repeated predicate-work targets qualified in both cohorts. The
[attribution packet](../benchmarks/evidence/predicate-attribution-portable-767.json)
and [independent reopening](../benchmarks/evidence/predicate-attribution-packet-verification-768.json)
preserve that prerequisite. It was permission to evaluate a candidate, not an
accepted optimization.

The [candidate design](native-predicate-exclusion-design-2026-10-19.md) retained
at most one exact no-TRUE fact on an immutable source owner. It admitted only
fully native, bounded Int64 comparison/membership, binary UTF8 contains and
small conjunctions on 8,192–65,536-row ordinary memory sources. Full typed
predicate equality and the source owner bound expression, parameters, dtype,
nullability and payload identity. Replacement sources and changed predicates
could not reuse a fact. Generated/private-batch sources, residuals, unsafe
negation and unsupported semantics kept ordinary execution.

The first call still evaluated every native range. A provisional fact could
publish only after complete source validation, result/schema/certificate and
JSON finalization, followed by the final cancellation check. Other consumers
could use an already completed fact but did not publish new ones. Credited
keys, a bounded source slot, optional admission, replacement and pressure
eviction preserved ownership and deterministic refusal. No query answers or
result payloads were cached.

Vortex-first classification was `wrap_vortex_concept`: pinned Vortex 0.85.0
expression, Boolean, mask and native-array providers remained the execution
boundary. No new dependency, persistence format, Arrow execution substrate or
external-engine fallback was introduced.

## Frozen comparison

The [candidate protocol](../benchmarks/evidence/predicate-exclusion-candidate-protocol-768.json)
was frozen before implementation and measurement. Its 114 cells combine two
source sizes; three predicate families; no-TRUE, positive, all-null, changed
parameter, replacement-source and interleaved-parameter shapes; and one, two
or four calls, plus unfiltered controls. Calls alternate complete count and
grouped-sum consumers. Source intake, all plan preparation, execution, complete
result consumption/release and owner destruction remain in the corresponding
workflow clocks.

Both roles use one freshly compiled release executable. The baseline's
test-only switch disables the candidate; both roles include the same native
work observer. The 456-workflow/1,064-operation preflight at 0, 17, 8,192 and
65,536 rows verifies every target's actual activation before measurement.
Cohort A runs baseline then candidate; B reverses roles and cell order. Both
finish before comparative inspection. Three samples per cell/role/cohort yield
1,372 measured workflows and 3,196 operations, including four retained,
excluded warmups. No samples or thresholds were changed after observation.

Exactly three targets are 65,536-row, four-call no-TRUE workflows. Each must
save at least 20% and 2 ms of median engine time, and separately at least 15%
and 2 ms of complete workflow time, in each cohort. All 114 cells also reject
an engine or complete-workflow increase exceeding both 5% and 0.25 ms in
either cohort. The metadata overhead, exact semantics, native certificates,
activation and final zero-reservation gates are mandatory independently.

## Measured decision

All three repeated-use targets pass both benefit gates. Engine medians below
sum the four complete calls; they are not Full43 or whole-engine results.

| Predicate family | A baseline → candidate engine ms | B baseline → candidate engine ms |
| --- | ---: | ---: |
| Correlated comparisons | 11.205375 → 2.775333 | 10.948378 → 2.804790 |
| Int64 membership | 11.996998 → 3.069583 | 11.885751 → 3.134833 |
| UTF8 contains | 12.858917 → 3.210499 | 12.867709 → 3.248456 |

The retained decision is rejection because these controls fail:

| Predeclared cell | Cohort | Failed gate |
| --- | --- | --- |
| 65,536-row membership, no-TRUE, one call | B | Complete-workflow regression. Its engine increase alone does not cross the absolute bound. |
| 65,536-row membership, interleaved parameters, one call | A | Engine and complete-workflow regression. Engine time rises from 2.977583 to 3.271624 ms. |

With one call, the interleaved-parameter cell has not changed its parameter
yet; it remains a separately predeclared control. The packet retains every
sample and both clock families. These observations establish a frozen gate
failure, not its causal explanation or a universal single-call regression.
No host cause is inferred, and the strong target gains do not override controls.

All 1,828 preflight and measured workflows / 4,260 operations pass the frozen
independent result, type, source, activation and cleanup oracle. All memory
controls pass. The
[separate verification](../benchmarks/evidence/predicate-candidate-verification-770.json)
binds all 1,904 candidate source inputs and the executable, then independently
recomputes every one of the 114 cells' gates from raw samples. It confirms six
passing target/cohort combinations and exactly two failed control/cohort pairs.

## Safety tests and failed observations

All 16 focused lifecycle tests pass. They cover exact-key reuse, changed
operators/columns/literals, replacement sources and sessions, final-block TRUE,
all-null and negation boundaries, zero limits, partial/final cancellation,
late consumer/JSON failure, allocation denial, pressure eviction, owner release,
concurrent queries and admission limits. No failed or partial operation publishes
an exclusion fact. Thirteen driver/oracle helper tests also pass.

The packet preserves the initial compilation failure from two older internal
callers, the compatibility-wrapper repair, and the subsequent failed focused
test. That test assumed different negated-membership NULL semantics from the
established native API; its expectation was corrected before preflight. The
candidate still excludes negation from admission. Earlier checker/driver draft
corrections and their evidence are recorded; no candidate measurement preceded
the final source freeze or depended on changing a decision threshold.

## Restoration and remaining scope

The [candidate packet](../benchmarks/evidence/predicate-candidate-portable-770.json)
contains every raw stream and content witness, original/final source patches
and added files, failed and successful receipts, exact build/executable
identities, protocols, drivers and guard dependencies. The
[packet reopening](../benchmarks/evidence/predicate-candidate-packet-verification-770.json)
checks every archive member and original byte. Large compiled binaries remain
outside Git with hashes and regeneration commands; replays require new local
output paths and the original sequential/storage guards.

The [restoration receipt](../benchmarks/evidence/predicate-exclusion-restoration-770.json)
proves that all 1,899 Rust/Cargo inputs match accepted production commit
`896c7179`, and all 2,217 previously accepted runtime, Python, test and harness
source contents are unchanged. Formatting passes after restoration. The prior
workspace/native/CLI, installed Python, Public39, Full43 and modular acceptance
therefore remains attached to the same production bytes; it is not represented
as a new test run. The rejected candidate did not proceed to broad integration
acceptance.

The [packaging verification](../benchmarks/evidence/predicate-exclusion-packaging-771.json)
preserves both restoration patch texts as lossless `.patch.xz` files and maps
their original paths and hashes. Their significant context spaces failed the
first staged whitespace check. The original restoration and closeout receipts
remain unchanged; the packaging verification supersedes the earlier unstaged-only
whitespace claim and checks the staged evidence, links and unchanged sources.

The rejected one-slot mechanism is not rerun unchanged. A materially different
mechanism requires a new source-grounded design and frozen protocol. First-use
summaries and all other roadmap obligations retain their independent gates.
The next conditional investigation is `PERF-06.merge_cost`.
