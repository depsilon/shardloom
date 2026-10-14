<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native helper execution-context continuity

Status: implemented locally with 22 focused regressions and all six broad gates
passing, including 3,219 workspace and 4,200 native tests. All 80 direct binding
tests and 757 public cases across 19 families pass on the same frozen source.
The [final combined acceptance](../benchmarks/native-admitted-context-2026-10-14.md)
passes all 516 matched query calls without a repeated declared cost breach and
retains 2,240 verified evidence members. Hosted integration remains pending
under PERF-03/06/08/09 and CG-5/19/20/21. The subsequent
[fresh UAT](../benchmarks/native-fresh-unpaced-uat-2026-10-14.md) passes fresh
ingest, all 129 query calls and all 18 format/workflow cohorts without artificial
pacing or battery/thermal admission. The same executable totals 194.125941 query
seconds. The maintainer then resumed PR #1551, one version bump after integration
and the remaining work. Historical timing causes remain unproven; no capability
is removed and this is not general allocation coverage.

## Existing boundary and defect

The admitted native session already carries the query allocator and registered
provider kernels. `NativeExecutionContext`, prepared sources, aggregate workers
and sort/projection execution own that session. Some private helper chains still
create a legacy session while extracting dictionary codes, Boolean values,
validity, predicate values, row exports or nested scalar/row-key values. Doing so
can bypass both admission and the specialized provider registered on the caller.
Some of those helpers also convert execution failures to `None`, which makes
an admission failure look like an unsupported direct representation.

Vortex-first decision: `use_vortex_native_provider`. Reuse pinned Vortex 0.85
`VortexSession` and `ExecutionCtx`, and the existing ShardLoom query pool. Pass the
owner explicitly through private helpers; no new session registry, ambient
thread-local context, execution engine, dependency or external fallback.
The expert comparator is a native execution maintainer tracing resource identity,
provider selection, error propagation and lifetime through the complete caller.

## Reuse map and required behavior

| Existing family | Required extension |
| --- | --- |
| Aggregate accessors and scalar-DISTINCT preparation | Use the supplied context for Boolean canonicalization, numeric dictionary values, dictionary codes and encoded validity, including existing UTF-8 dictionary paths. |
| Materialized predicate evaluator and its specialized helpers | Pass the same context through count, candidate selection, conjunction refinement, dictionary/FSST text checks and validity. Preserve existing encoded and selected-row strategies. |
| Row export and scalar/stat extraction | Carry context through full and selected extraction, primitive/text/Boolean materialization and validity. Return provider errors rather than silently changing the execution route. |
| Explode and nested row keys | Carry context through list/fixed-list canonicalization, struct/list validity and recursive child extraction. Preserve existing null, order and key semantics. |
| Prepared and direct scan, aggregate and sort entry points | Create helper contexts from their existing admitted native session. Prepared scan must pass its native session along with the runtime already supplied. Worker tasks must use their owning session. |
| Materialization diagnostics | Inspect available dtype/validity metadata without a second decode merely to select a reason string. |

Use `Result<Option<T>>` where a helper distinguishes a supported direct shape
from a provider miss. `Ok(None)` remains a shape/semantics decision. An attempted
native execution that fails must return its error through the existing diagnostic
boundary. Do not catch admission errors and retry with a different session.
Pure canonical inspection can retain `Option<T>` when it executes no provider.

Remove production convenience paths that manufacture legacy contexts. Test-only
fixtures may construct a session explicitly. The ingestion writer's use of the
legacy session solely to inspect the pinned enabled-component policy is not a
decode and must not be changed into a different encoding policy as part of this
correction. Inventory counts are lexical evidence, not a count of proven public
resource bypasses.

Preserve sparse-frame avoidance, native append/fusion, the 512-KiB small-memory
readback contract and existing output-batch semantics. The separate matched
composition study decides whether its historical timing difference reproduces;
context changes cannot retrospectively establish that cause. Capillary admission,
PulseWeave scheduling and scan granularity remain unchanged unless separate
source and measured evidence requires a correction.

## Verification and limits

The implementation removes production legacy-context construction from these
helper chains. Pure aggregate shape planning carries no execution context; a
later native access requires the admitted owner explicitly. The final recursive
inventory covers all 552 Rust sources under `shardloom-vortex/src`. Its counts
include tests and owner-session factories and do not represent public bypasses.

Nested coverage also exposes two concrete cases. Pinned Vortex's list row
convenience accessors create a legacy context for encoded offsets and sizes.
The wrapper prepares those children once with the caller's context before row
access, while null rows and empty arrays avoid that work. Struct extraction now
enumerates declared fields instead of physical slots, excluding the parent's
validity child from logical scalar values and keys.

Seven of the original eight aggregate regressions fail against the preceding
helper implementation; all eight pass after context propagation. Expanded
coverage passes 22 cases for aggregate values/codes/validity, full and selected
exports, numeric and text predicates, conjunction refinement, nested lists and
structs, and diagnostic-only classification. FSST tests distinguish its encoded
DFA path from full materialization: Zstd offsets use the supplied allocator in
the former, while the latter preserves its explicit unadmitted-metadata error.
This does not expand the FSST metadata allocation contract.

Add regressions that exercise the real helper chain with an admitted session:
encoded numeric dictionary values and codes, Boolean materialization, encoded
validity, full/selected row extraction, predicate refinement and nested scalar/key
extraction. Show that a too-small grant fails through the expected resource
diagnostic, releases temporary credits and can retry when credit is available.
Check complete values, nulls and order on successful retry and retained output
ownership where the helper returns native storage. Include a diagnostic-only
case proving that classification performs no decode. Demonstrate the relevant
regressions against the previous helper behavior.

Run focused native tests during implementation, then the required formatting,
workspace Clippy/tests, native feature checks, direct bindings, public streaming
matrix and complete retained-input Full43 on the final source. Use a frozen
complete-operation paired screen for any measured overhead concern; preserve
every sample and the existing performance evidence. Bind final source, binaries,
commands, complete results, exclusions and hosted checks in the next PR.

Final query acceptance compares the complete combined correction with the
retained executable whose native source matches merged main at `22b2cde0`.
All 43 queries pass in both starting orders on the same retained input. The
correction's 258 complete results supply final Full43 acceptance; no separate
unpaired cohort is needed to prove the same result set. No query repeats the
declared elapsed/CPU/RSS breach in both orders. Single-order breaches remain
recorded. The historical composition investigation and its unproven historical
cause remain separate.

This correction preserves an existing owner wherever native execution occurs.
It does not claim that ordinary Rust collections, conversion libraries, encoding
training, all upstream providers or total process RSS are fully accounted.
Vortex remains native input/output. Unsupported work remains explicit and no
external engine is invoked.
