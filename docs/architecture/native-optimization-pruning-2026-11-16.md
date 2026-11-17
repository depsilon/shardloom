<!-- SPDX-License-Identifier: Apache-2.0 -->

# Remaining optimization queue disposition

The November 16 maintainer instruction removes the open-ended “complete
optimization coverage” obligation. The five areas below are **removed from the
active implementation and benchmark queue**. Current evidence does not establish
material removable work for their remaining proposals. Unknown exposure is not
a benefit estimate. This is a scope decision, not a claim of complete coverage
or a failed performance experiment.

| Area removed | Evidence and decision | Obligations retained |
| --- | --- | --- |
| Remaining Vortex demand/encoding coverage | The [inventory](native-optimization-inventory-2026-11-13.md) closes 194 excessive Parquet/IPC column observations. Missing Vortex logical contracts and physical read/decode attribution do not establish savings. Drop the blanket per-layout/per-encoding completion campaign. | Existing source-demand connections, complete payload validation, truthful reporting and required input/type support. |
| Earlier join pruning | Existing membership filtering occurs after key validation. The [propagation observation](native-optimization-propagation-2026-11-15.md) assigns 500.383376588 engine seconds to complete join/multiway requests, not source-pruning work. Remove item 20.D's prospective source-pushdown investigation. | Accepted membership/skew paths and their semantics. The separately requested [direct join-to-aggregate](native-join-aggregate-2026-11-16.md) remains active because it targets observed intermediate construction. |
| Further scan/build reuse | Five accepted prepared lifecycles prove immutable source-segment reuse; they do not prove repeated join-build cost. Remove recommendation 13 and the general scan/build/result-reuse expansion. | Existing same-generation segment reuse and invalidation/ownership guarantees. The explicitly requested Chrysalis/Resonance program has its own bounded contracts and later queue position. |
| Further sorting optimization | The [shared Sort prefix connection](native-sort-prefix-validation-2026-11-13.md) already improves its complete 388-case cohort. Neither that result nor the invalid older shortcut establishes the benefit of normalized keys or a replacement full-sort algorithm. Remove J's unmeasured experiment. | Complete validation, stable order/ties, supported ordering/Top-K/window/ASOF behavior and accepted prefix work. |
| Further spill optimization | The archived 10k has zero observed spill bytes and no requested relational spill. Separate pressure tests prove correctness, not material workload cost. Remove recommendations 11/12 and ordered aggregate/join/Sort performance extensions. | Admitted native spill, memory refusal, cancellation, recovery, cleanup and required rich-data spill acceptance. |

Do not launch attribution, broadened encoding/scale cases or a benchmark merely
to keep one of these hypotheses alive. Reopening requires new concrete workload
evidence of material removable work or a named necessary feature, followed by a
bounded proposal under the current phase plan. Existing frozen protocols,
observations, regressions and retain/drop decisions remain unchanged. No fixed
percentage threshold is added and no accepted useful implementation is removed.

The active order is: integrate the accepted dense join work; finish the direct
join-to-aggregate decision; complete rich-data and analytical acceptance;
investigate [Chrysalis then Resonance](native-chrysalis-resonance-cells-2026-11-16.md);
continue portable serverless and item-21 Foundry integration; then item-22 AI
workflows and the planned 1.3.0 checkpoint. Required capabilities and independent
CG-1 through CG-23 gates remain. The [phase plan](phased-execution-plan.md#planned)
owns the precise sequence.
