# Remaining Performance Work

## Authorization and acceptance

The maintainer's September 6 instruction is to work through all remaining
performance suggestions. This continues RFC 0044 and PERF-01 through PERF-13;
it does not introduce replacement phase IDs or close competitive gates.
PR #1435 is the starting implementation, with measured runtime `e0264748` and
documentation head `9e43d163`. Its numeric compression and exact native
execution remain the control. Pending publication is separate from implementation.

Each item below requires source-grounded provider admission, implementation or
a concrete feasibility experiment, correctness tests, and a measured retain/drop
decision. A proposal, extra counter, or passing compilation alone does not close
an item. Independent work can proceed together; Cargo builds and large local
measurements run sequentially with existing storage and process guards.

All code remains safe Rust within the existing native provider boundaries.
Native Vortex is the execution and highest-fidelity persistence target. External
engines do not execute residual work. Temporary query runs require the existing
explicit workspace and owned cleanup. This work does not authorize package
publication or a new release.

## Execution ledger

| Existing queue | Remaining work | Current status and required evidence |
|---|---|---|
| PERF-04/05 | Compound numeric/text COUNT | Retained: exact complete-key workers and typed pressure handoff; Q15/Q17 and complete public/held-out acceptance recorded in the frozen checkpoint report |
| PERF-04/05/06 | Exact grouped distinct and broader aggregate parallelism | Native integer group/value workers implemented and checked across original widths, dictionary domains, pressure handoff and complete global selection; fresh timing and held-out acceptance pending |
| PERF-03/04/05 | Compact aggregate state, string slabs and partition ownership scheduling | Test-only compact-state and owner-scheduling experiments pass complete values, capacity, cancellation and refund checks; paired release retain/drop decisions pending |
| PERF-03/07/10 | Scan-local compressed segment reuse and consumer fusion | Narrow separate-field reuse retained for measured completed-read savings, with small observed local latency cost; prepared scan and real filtered-count reuse now pass source-invalidation and complete-result tests; broader fusion remains |
| PERF-10 | Constant/run/FoR/bit-packed numeric computation | Weighted constant/run consumers retained with measured extrema gains; dense fused additive-only RunEnd keeps the faster native typed route after a measured regression; bounded native FoR/bit-packed experiment passes exact width/null/ordered-additive tests, with release measurements pending |
| PERF-08/09 | Column-addressable logical file layout over bounded physical writes | Private paired writer seam preserves real default/fast-load/balanced/text payloads, geometry, statistics and values; ordinary writer remains unchanged pending lifecycle measurements |
| PERF-07/11 | Multi-segment memory generations and owned-buffer intake | Native column/row-group generations and owned intake pass ownership, value and durable-reopen tests; release experiment records zero owned-intake payload copies, selective leaf requests and direct/generation clocks; broader lifecycle acceptance remains |
| PERF-03/08/09 | Ingest worker scaling and bounded cohort overlap | Shared source/conversion/provider CPU grants and explicit one-worker ceilings implemented and tested; legacy requested-one control actually used two (95.539879 s, 3,127,197,696-byte RSS, identical output); matched scaling precedes overlap |
| PERF-03/09/10 | Joint codec/consumer selection and task-level controllers | Native Zstd/Dict/FSST portfolio writes real files and passes complete count/group/filter oracles; separate setup/publication/query clocks and reuse-1/10/100 release evaluation are pending before any promotion/controller |
| PERF-02 | Remaining prepared execution and native Python binding decision | Actual resident filtered counts reuse one source and reexecute native scans; 11 process-worker lifetime/invalidation tests pass. Isolated in-process binding prototype is authored separately and remains unregistered/unvalidated |
| PERF-03 | Remaining CPU/I/O/codec and retained-memory admission | Queued across the above families; progress, cancellation, shared capacity and explicit provider allocation exclusions |
| PERF-06 | Shared aggregate/distinct/join query spill and recovery | Native query-run store extracted without changing numeric sort admission; private integer-distinct runs pass exact values, overlap quota, corruption/cancellation and owned cleanup. Explicit aggregate admission and weighted COUNT adapters remain; broader join semantics are a later separate boundary |
| PERF-07/10 | Remaining materializing/multi-source/compatibility results and physical composition | Native-array IPC/Parquet export prototype is authored separately, still unregistered/unvalidated; source/result ownership, sink fidelity, full lifecycle and broader multi-source scope remain |
| PERF-11/12 | Bulk-load envelopes and broader held-out acceptance | Queued alongside implementations; completed operations, distribution/worker/resource variants, latency distributions and independent values |
| PERF-01/08/12 | Final baseline, timing attribution and lifecycle scorecards | Queued per retained batch; frozen code/artifact/settings, all-query and non-ClickBench results, preserved regressions |
| PERF-13 | Profile-guided optimization feasibility | Queued after structural measurements; representative training, independent validation, build/code-size cost and portable correctness; retain only a demonstrated benefit |

The ledger remains open while any implementation, evaluation or declared
acceptance remains. A rejected optimization must retain its evidence and explain
which scope was tested; rejection does not imply that the entire operator family
or phase is complete. No universal sub-millisecond, sub-100-second suite, RSS-bound,
or engine-superiority claim follows from this plan.

The [first retained checkpoint](../benchmarks/perf-native-continuation-2026-09-06.md)
records runtime `75fc09a0`, the unchanged native artifact, 98.831499-second full43
best sum versus 119.887782 seconds, all 129 complete reference results and all
1,360 independent held-out checks. Eighteen query bests still regress; small
held-out timings are effectively unchanged or slightly higher. Intermediate
arithmetic regressions, raw samples, cache tradeoffs and provider allocation
exclusions remain visible. Later implementation continues against this frozen
control; the score does not close the remaining ledger.
