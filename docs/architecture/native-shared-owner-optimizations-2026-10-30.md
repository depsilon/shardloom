# Shared-owner optimization continuation — October 30

Status: maintainer-requested candidate queue inside active item 20. The
[Rust/Python application batch-function unit](native-application-batch-functions-2026-10-30.md)
is accepted and merged in #1628 as `b6883adc`, with all 57 hosted checks passing.
The two supplied October 30 source reviews extend the
existing finite performance work; they are hypotheses to verify, not measured
speedups or permission to skip semantics and resource admission.

The review baseline is PR #1627 at `7d6c02c6d2358e205fb60f6868830d905ab4b62d`,
merged as `65be06c9d7324de45f0661f8fab9546e2f7d0259` after 57 passing checks.
Its accepted 347-query total is **12.055132613 engine seconds**, with 12,565
complete result rows. Preserve its original observations and dropped mechanisms.
The subsequent [compact grouped-state candidate](../benchmarks/native-grouped-state-access-2026-10-31.md)
is retained at **11.997152207 engine seconds** for the same complete
cohort and merged in #1630 as `3b4c7d92`, with all 57 hosted checks passing.
The [closeout](../benchmarks/evidence/native-grouped-state-closeout-2026-10-31/closeout.json)
records verified retirement. This advances the accepted baseline without
rewriting the review baseline or attributing all observed movement to grouping.
The [prior follow-through closeout](../benchmarks/evidence/native-shared-followthrough-closeout-2026-10-30/closeout.json)
records hosted integration, generated-artifact retirement and saved predicate
attribution without new query execution.
The rejected adjacent-read experiment is merged in #1631 as `e56cc37d`, after
all 57 hosted checks pass. Its
[closeout](../benchmarks/evidence/native-adjacent-parquet-reads-closeout-2026-10-31/closeout.json)
links the original rejection and verified retirement; the accepted baseline remains unchanged.

The subsequent [ordered-input experiment](../benchmarks/native-ordered-inputs-2026-10-31.md)
passes both complete-cohort gates: **11.979219492 engine seconds** for the
unchanged 347 queries and **0.920503372 engine seconds** for the separately
declared 48 workflows, against 11.997152207 and 1.307041040 respectively.
The analytical comparison baseline advances to this retained candidate. These
are single-pass observations. PR #1632 merges as `6e413dcc` after all 57 hosted
checks pass, with [verified closeout](../benchmarks/evidence/native-ordered-inputs-closeout-2026-10-31/closeout.json).

This queue stays in item 20, before connector/refresh completion and item 18.
Item 18 still owns comprehensive I/O format/interface acceptance. The later
[expanded coverage campaign](native-broad-query-program-2026-10-26.md#october-30-coverage-expansion-after-item-19)
remains after item 19. Candidate-specific acceptance happens now; its mechanism
fixtures and inventory feed that later campaign. Full43 remains held until item
18 acceptance. CG-1 through CG-23 and package publication remain separate gates.

## October 31 maintainer pruning decision

The maintainer requests dropping remaining proposals whose expected complete-workflow
benefit is negligible or comparable to the small recent analytical improvement.
This changes **candidate selection**, not the original recorded gates or dispositions.
The accepted ordered-input change stays accepted. The separately tested
[typed-comparison candidate](../benchmarks/native-typed-ordering-2026-10-31.md)
is dropped under its original two-cohort rule: the 347-query total increases
from 11.979219492 to 12.086481459 engine seconds although the supplemental
48-workflow total falls from 0.920503372 to 0.808468919. The accepted baseline
does not advance.

The following are **judgment screens, not measured rejections**. They leave the
active optimization queue without implementation or another benchmark. Their
descriptions below retain the original proposals and semantic obligations; they
are not deferred work that must automatically resume later.

| Screened-out proposal | Reason for stopping now |
| --- | --- |
| Further typed comparator/view binding | The allocation-free comparison experiment already loses its required complete-cohort gate. Retaining more view metadata would target the remaining dispatch/addressing overhead while introducing lifetime and accounting costs. There is no current evidence supporting another large complete-workflow gain. |
| Monotonic-run sorting extension | The accepted owner already skips heap sorting for completely ordered, validated input. Merging partial monotonic runs would address a narrower full-sort regime; the current analytical cases use bounded selection or small aggregate results. Expected benefit to the current target is too small to prioritize. |
| Whole winning merge spans | This removes comparisons and gather bookkeeping within a merge, while leaving the run generations and rewritten bytes intact. Keep the structurally larger fan-in proposal and stop this separate fine-grained extension. |
| Monotone as-of probes | Build sorting and bounded binary search already exist. Advancing probe bounds may help dense ordered time-series inputs, but the present evidence does not identify probing as a large remaining complete-workflow cost. Drop this narrow speculative extension from the active performance work. |
| Peer-level window-bound reuse | Existing cursors already carry monotone endpoints and current-row/group peer edges. The residual opportunity is narrower, particularly repeated calendar RANGE bounds; the supplemental windows use ROW_NUMBER and do not establish it. Expected benefit to the present target is insufficient. |
| Prepared nested-key static structure | Prepared columns already bind child owners and structure once per column. Further static schema/compatibility reuse leaves dynamic validity, coordinates, leaf hashing and comparison untouched; no large repeated static cost is established. |
| Vacant index-bucket reuse | Removes a repeated bucket walk around an insertion, with reservation/rehash constraints, rather than the dominant scan/key/payload work. Treat as a small local improvement and stop. |
| Redundant accounting-operation shortcuts | Equal-size and zero-size accounting shortcuts are small per-call work with delicate peak/failure/concurrency semantics. No material complete-workflow opportunity is established. |

Two previously deferred branches are also closed for this active queue:
complete-group worker reuse has the recorded roughly three-microsecond typed-update
scope and 0.32% selective-case update share, and the guard-only dynamic-source
proposal cannot activate at its execution binder. Neither gets a new experiment.
Execution-phase source-demand design is not silently supplied by the inactive guard.

### Remaining active optimization work

The subsequent October 31 maintainer instruction combines all retained
optimizations into **one implementation batch**, one shared build/validation
cycle and one final comparison campaign. The five areas below organize work;
they no longer prescribe separate builds, benchmark rounds or PRs per candidate.
Freeze the combined semantic/resource contract and exercising cohorts before
runtime edits. Keep mechanism-specific correctness and activation controls,
but make the performance disposition for the complete batch. Reuse the accepted
347-query observations; separately declared supplemental workflows cover paths
that cohort does not exercise. No routine repeats, per-mechanism timing loops
or Full43 run are added. Previously dropped proposals stay dropped.

Retain these ten concrete candidates in five owner areas. They can remove whole
passes, copies, searches or repeated executions; **potential is not a speedup claim**.
For each, first establish that the removable work is material in an actual
complete workflow. Stop at that screen if it is absent, already optimized or
likely another marginal change. Do not implement and benchmark every proposal
just to give it a measured rejection.

| Order | Area | Candidates and reason to retain the screen |
| --- | --- | --- |
| 1 | Source reads and expression preparation/construction | Selective page/range delivery; repeated input-view preparation; encoded composed reductions; direct native string append. These may avoid physical reads, full-array preparation/decoding or repeated value allocation/copying. Require actual read/representation/preparation evidence; repeated column names or a Parquet dictionary page alone are insufficient. |
| 2 | Joins and correlated subqueries | Direct residual conditions; restricted pure correlated-plan rewriting. These may remove candidate-table construction or repeated inner executions. Preserve complete candidate validation, multiplicity, NULLs, laziness and first errors. |
| 3 | Native spill and lookup | Higher-fan-in merging; stored-run boundary directories. These may remove complete rewrite generations or block reads. The current resident 347 cases do not certify them; require separately declared pressured workflows and complete resource/cleanup evidence. |
| 4 | Owned materialization | Compact owned capture. Retain only the existing tiny-fragment opportunity; do not add another copy to suitable owned batches. Compare capture plus replay and preserve producer release and late errors. |
| 5 | Exact quantiles | Static finite exact selection. The current resident owner inserts observations into a balanced rank index; finite completion may avoid that repeated ordered maintenance. This is a distinct algorithmic opportunity, with separate workloads because neither frozen cohort declares median/quantile measures. Dynamic window state is unchanged. |

After this pruned performance work, continue the remaining connector/refresh
package and item-20 acceptance, then item 18's comprehensive I/O/interface
acceptance. Capability, correctness, format, connector and release obligations
are not removed by this screen. The under-ten-second objective remains open;
Full43 remains held until item 18 is accepted. CG-1 through CG-23 remain visible
in the phase plan, and external engines remain baselines only.

## Common admission and evidence

Use the existing native owner and check the pinned Vortex provider before adding
an abstraction. Keep one engine, Vortex-native I/O, complete results, validation
domains, first errors, cancellation, shared allocation and last-alias refunds.
No external execution, new answer cache, revival of a rejected mechanism, or
algorithmic reassociation follows from these proposals.

For the combined batch, freeze its hypotheses, supported domains, declines,
whole-workflow controls, resource limits and acceptance rule before changing
runtime behavior. Verify actual activation and the proposed removed work. Record
retained, dropped or evidence-insufficient batch disposition with source/build identity
and every failed attempt. Prefer the largest coherent owner-level implementation
that remains independently reviewable. The latest maintainer instruction
explicitly combines the retained owner mechanisms; record which controls
exercise each one without claiming isolated causal speedups from batch timings.

Keep the 347 suite and its timing history unchanged. Apply the standing binary
complete-cohort engine-time rule: lower ships after other gates pass; unchanged
or higher drops. For broader routes, predeclare additional complete workloads
that actually exercise them and retain the 347 regression observation separately;
neither selected fast cases nor an unexercised 347 family certifies a new route.
Do not relax an existing gate or invent a new clock. Diagnostic profiling and
work counters have separate runs and cannot contaminate acceptance observations.

Measure several inexpensive sizes and a few bounded larger sentinels. Exercise
both sides of admission boundaries and semantically equivalent compositions
such as direct projections, renaming, named relations and alternate interfaces.
Do not treat fallible-expression reordering, floating reassociation or changed
materialization as harmless composition. Keep guards, source validation and
cleanup intact; no routine repeats, cache manipulation or artificial pacing.

## Initial current-cohort work

The [single-predicate mask candidate](native-single-predicate-masks-2026-10-30.md)
is **dropped**: all 347 results pass, but its complete engine total is
12.199579628 seconds against 12.055132613. The candidate source is removed and
the accepted baseline is unchanged; its complete evidence remains retained.
It is merged in #1629 as `aae7ecf1`, with all 57 hosted checks passing and
[verified artifact retirement](../benchmarks/evidence/native-single-predicate-mask-closeout-2026-10-31/closeout.json).
The [48-case grouped diagnostic](../benchmarks/native-grouped-cost-2026-10-31.md)
now attributes 45.20% of its engine time to source intake, 23.51% to key
preparation/interning and 9.41% to typed updates. It selects bounded state-access
and proof-consumption work, now retained after complete acceptance; general
worker routing is deferred for this workload. These fractions do not establish
a production speedup. The October 31 screen above determines which broader candidates remain active.

| Candidate | Existing owner and work to remove | Admission and evidence |
| --- | --- | --- |
| Single-comparison and null-test masks — dropped | The experiment extended `native_relational_predicate_block` to avoid Boolean-array construction and scalar selection above scan pushdown. | Complete correctness, work and ownership controls pass, but 12.199579628 engine seconds loses to 12.055132613. Candidate source removed; retain the [complete experiment](../benchmarks/native-single-predicate-masks-2026-10-30.md). Do not revive the same mechanism from selected faster cases. |
| Grouped cost attribution — complete | Separate source intake, key preparation, group interning and aggregate updates inside the current grouped owner. | All 48 cases/1,920 rows pass; typed updates contribute 9.41% of this instrumented sample. The baseline is unchanged. See the [complete attribution](../benchmarks/native-grouped-cost-2026-10-31.md). |
| Compact grouped-state access and proof consumption — retained and merged | Bind a leased single-page state slice before typed traversal and consume the existing paired-extrema count-headroom proof once per non-null row. | All 347 results pass; 11.997152207 engine seconds is lower than 12.055132613. Boundary, overflow, cancellation, ownership, public-interface, package and compiled-code controls pass. Preserve the [complete observation and regressions](../benchmarks/native-grouped-state-access-2026-10-31.md); #1630 merges as `3b4c7d92` after all 57 checks pass. |
| Complete-group worker reuse — screened out of active queue | Reuse existing complete-key routing, persistent workers and bounded outstanding jobs for general relational aggregation. | The 48-case sample records about 3 microseconds per typed-update scope and 0.32% update share for selective cases; it does not justify this route now. A larger workload needs separate evidence that updates can repay coordination. Give each complete group one exclusive state owner and preserve row/error order, input lifetime and charged routing storage. PulseWeave/capillary control uses the current coordinator, not a second pool. |

## Broader execution and source candidates

The additional review prioritizes scoped per-source demand, direct residual join
conditions, higher-fan-in spill merging, order-aware sorting and peer-frame bounds.
The October 31 screen supersedes that priority order. The table preserves
the original hypotheses and admission constraints. Comparator reuse and order-aware algorithms are distinct
hypotheses, as are fewer spill generations and bulk winning-span processing.

| Candidate | Existing owner and work to remove | Admission and evidence |
| --- | --- | --- |
| Typed ordering beyond Top-K — measured candidate dropped; further binding screened out | Reuse typed key views/comparisons across full sorts, windows, as-of preparation and spill. | Preserve NULL placement, direction, stable ties and floating normalization. Do not blindly grow K-location metadata to N; reuse ordinals and charge any additional locations. The existing ten bounded-order cases do not cover this extension. |
| Order-aware shared sorting — retained and merged | Avoid unconditional heap sorting for already ordered validated input in `native_relational_order`; the monotonic-run extension is screened out above. | The [October 31 contract](native-ordered-inputs-2026-10-31.md) restricts comparison reordering to completely validated matching flat domains and pure group ordinals. Both complete gates pass: 347 queries at 11.979219492 engine seconds versus 11.997152207, and 48 full-sort/window/as-of/aggregate workflows at 0.920503372 versus 1.307041040. All complete results, ownership and mechanism controls pass; preserve slower cases and the [single-pass limits](../benchmarks/native-ordered-inputs-2026-10-31.md). PR #1632 merges as `6e413dcc` after all 57 hosted checks pass, with verified closeout. |
| Repeated input-view preparation | Reuse a prepared input view within one batch across distinct expressions, separately from completed-expression reuse. | Key identity includes exact array/slice, dtype, float policy, preparation purpose and selected domain. Populate lazily at first evaluation and release before producer return. Demonstrate repeated preparation/canonicalization and bound retained credit; single-use inputs keep ordinary preparation. |
| Encoded composed reductions | Route eligible Constant/RunEnd inputs to the existing encoded reduction owner before primitive preparation. | Begin with exact count, extrema and eligible distinct membership. Preserve ordered SUM/AVG and group/selection domains. Record actual physical representation and avoided primitive work; a Parquet dictionary page alone proves no activation. Keep existing provider declines. |
| Scoped dynamic-source demand — inactive guard-only proposal closed | The incomplete-analysis guard runs in static preparation; dynamic execution binders copy prepared source owners into an already-created input executor without calling that guard. | A guard-only patch cannot activate on this path. Execution-phase narrowing needs an explicit contract for consumed sources, replay, root discovery callbacks and executor ownership. Preserve reachable schemas, aliases/shared sources, outer fields and first errors. No patch or query experiment was performed; see the [source-path screen](native-adjacent-parquet-reads-2026-10-31.md#current-operation-and-selection). |
| Direct residual join conditions | Replace temporary repeated-left/gathered-right candidate tables with bound scalar/indexed operands and direct masks in `native_relational_join_condition`. | Start with pure fixed-width comparisons; validate the complete bounded candidate batch before semi/anti short-circuiting. Preserve selected values, error order and current owned join output. Measure candidate-table bytes, pairs and full join time across fanout/condition widths. This is not borrowed output handoff. |
| Higher-fan-in relational spill merging | Reuse memory-derived multi-run merge geometry from specialized sort/weighted-count owners in `native_relational_spill`. | Charge each reader/block/head/output and keep two-way merging at low memory. Preserve stable ties, quota overlap, source identity and cleanup. Initially finish a merged run before consumption. Measure generations, bytes read/written, open readers and complete pressured-query time across sorts, join builds and windows. |
| Whole winning merge spans — screened out | Admit a validated ordered span preceding other run heads through the existing owned gather. | Preserve exact tie priority and validation. Measure comparisons/gather bookkeeping independently of fan-in and rewrite-pass savings. |
| Stored-run boundary directories | Add bounded charged first/last block-key metadata to `native_relational_stored_order` for `native_relational_join_lookup`. | Start with normalized hash boundaries; exact key equality stays authoritative. Test collisions, duplicates across blocks, NULLs, short/empty blocks and changed generations. Count actual block reads per probe and construction overhead; do not retain a payload/probe cache. |
| Monotone as-of probes — screened out | Reuse verified lower/upper bounds within a partition instead of repeatedly searching the full build range. | Preserve original left order, duplicate timestamps, exact-match rules, tolerance, nearest ties and bounded interleaved-partition state. Keep binary searches for random/sparse probes. Measure searched ranges/comparisons and full execution across probe density. |
| Peer-level window bounds — screened out | Reuse completed base bounds for partition + exact frame specification + peer group, then consider identical-frame sharing across functions. | Keep exclusions row-specific. Cover DST, units/zones, NULL placement, retreating calendar boundaries and every exclusion mode. Count calendar shifts/searches per peer group versus per row; this does not duplicate already shared window sorting. |
| Restricted correlated-plan rewrite | Recognize statically bound pure equality-correlated plans that can use existing join/membership owners or a bounded parameter relation. | Freeze a separate semantic design unit. Preserve guards, laziness, scalar zero/one/many-row behavior, NULLs, outer multiplicity, completion and first errors. Exclude opaque/effectful callbacks. This is declarative rewriting, not a per-key answer cache. Measure inner scans/executions/binding over outer-size scaling. |
| Selective source page/range delivery | First avoid unused tail-page reads for an already-admitted unfiltered prefix; then evaluate predicate-first payload selections at the demanded reader. | Fetch and decoder layers must honor the same generation-bound scope. Preserve demanded-payload validation, including the ranked contract's unselected values in winning groups. Decline when finer reads would hide required corruption/errors. Prove actual ranges/bytes, not decoder selection alone. |
| Adjacent demanded-read coalescing — dropped | Combined exactly adjacent demanded compressed extents through one charged positional read while retaining original decoder slices. | All correctness/ownership/public/package controls pass. The complete 347-query total is 12.028866368 engine seconds versus 11.997152207; the runtime change is removed. Requests fall in only 16 cases, with identical selected bytes. Preserve the [complete experiment and original gate](../benchmarks/native-adjacent-parquet-reads-2026-10-31.md); selected read savings do not retain it. |
| Compact owned materialization capture | Form bounded owned capture blocks inside the recorder that already completes before downstream use. | Validate each incoming callback immediately, release source-backed owners before producer return, and seal through existing resident/spill ownership. Avoid adding a second copy after detach. Preserve failure timing, cancellation, limits and cleanup; compare large-batch controls with the recorded tiny-fragment pressure workflow. |
| Direct native string append | Write eligible transformed bytes into the existing credited variable-width result owner. | Begin with ASCII-proven case conversion and admitted concatenation; preserve exact Unicode/null/error behavior otherwise. Measure temporary allocations/copied bytes and complete long-text/multiple-expression workflows. |
| Prepared nested-key structure — screened out | Bind static logical compatibility, field layout, recursive structure and admissible schema-hash material once. | Keep values, validity, offsets, list lengths and selected domains dynamic. Attribute static work versus leaf hashing/comparison before implementation; include all-unique nested inputs. Do not revive the rejected nested value/dictionary-identity cache. |

## Smaller feasibility screens

| Candidate | Screen and stop condition |
| --- | --- |
| Reuse a vacant index bucket — screened out | Count repeated cluster visits for unique joins/distinct/sets. Reuse only if reservation did not rehash; reserve before publishing any new entry. |
| Static finite exact quantiles | Compare charged typed storage plus exact multi-selection at completion with current rank-query insertion state for few/many requested quantiles and repeated/unique values. Preserve interpolation, memory refusal and cleanup; leave dynamic window state unchanged. |
| Redundant resource-accounting operations — screened out | Measure equal-size lease resizes and zero-sized operations first. Admit a no-op only when the complete accounting, peak/failure and concurrency contract remains equivalent. |

## Mechanism inventory and completion

For every owner, record its current algorithm/representations, optimized route,
admission/declines, exercising fixtures, measured removed work and disposition.
Include previously completed and rejected experiments. Family coverage such as
"joins tested" is insufficient: distinguish unique insertion, duplicate chains,
residual conditions, unmatched rows, semi/anti behavior, spill lookup and output.

Candidate coverage includes outer parameters × inner work; candidate fanout and
predicate width; rows/run count/ties/key width; initial spill runs and memory;
probe blocks/duplicate spans; as-of density/partitions; window peers/frame count;
nested depth/field/list sizes; and read extent count/adjacency. A candidate closes
with reproducible acceptance or a documented decline/evidence-based stop, never
with an unsupported speedup assertion. Keep all original failures and decisions.
