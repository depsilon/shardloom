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
| PERF-04/05 | Compound numeric/text COUNT | Implemented with exact complete-key workers and typed pressure handoff; correctness tests pass, paired public and held-out timing acceptance pending |
| PERF-04/05/06 | Exact grouped distinct and broader aggregate parallelism | Queued; complete group/value pairs, deterministic reductions, skew and full global selection |
| PERF-03/04/05 | Compact aggregate state, string slabs and partition ownership scheduling | Queued; charged retained capacity, probes/lock waits, exact refunds, throughput and peak state |
| PERF-03/07/10 | Scan-local compressed segment reuse and consumer fusion | Generation-scoped owned reuse passes focused ownership and actual duplicate-read tests; release lifecycle measurement and broader prepared-reader integration remain |
| PERF-10 | Constant/run/FoR/bit-packed numeric computation | Constant/run scalar consumers pass complete-value and no-expansion tests; release measurements and bounded FoR/bit-packed experiments remain |
| PERF-08/09 | Column-addressable logical file layout over bounded physical writes | Test-only prototype preserves encoded payloads, footer statistics and selective reads; ordinary writer promotion and lifecycle measurements remain |
| PERF-07/11 | Multi-segment memory generations and owned-buffer intake | Native column/row-group generations and owned intake pass ownership, value and durable-reopen tests; addressable operation timing and broader lifecycle acceptance remain |
| PERF-03/08/09 | Ingest worker scaling and bounded cohort overlap | Queued; matched 1/2/4/8-worker controls before overlap, full publication, observed RSS and retained-output bounds |
| PERF-03/09/10 | Joint codec/consumer selection and task-level controllers | Queued; actual representation/scheduling decisions and complete ingest/first/repeated-query costs |
| PERF-02 | Remaining prepared execution and native Python binding decision | Queued; existing runtime reuse, invalidation, supported family coverage, native/adapter/process timing and packaging constraints |
| PERF-03 | Remaining CPU/I/O/codec and retained-memory admission | Queued across the above families; progress, cancellation, shared capacity and explicit provider allocation exclusions |
| PERF-06 | Shared aggregate/distinct/join query spill and recovery | Queued; real Vortex runs, quotas, source identity, corruption/cancellation/crash recovery and exact owned cleanup |
| PERF-07/10 | Remaining materializing/multi-source/compatibility results and physical composition | Queued; executable arrays, selections, ownership, sink fidelity and complete output |
| PERF-11/12 | Bulk-load envelopes and broader held-out acceptance | Queued alongside implementations; completed operations, distribution/worker/resource variants, latency distributions and independent values |
| PERF-01/08/12 | Final baseline, timing attribution and lifecycle scorecards | Queued per retained batch; frozen code/artifact/settings, all-query and non-ClickBench results, preserved regressions |
| PERF-13 | Profile-guided optimization feasibility | Queued after structural measurements; representative training, independent validation, build/code-size cost and portable correctness; retain only a demonstrated benefit |

The ledger remains open while any implementation, evaluation or declared
acceptance remains. A rejected optimization must retain its evidence and explain
which scope was tested; rejection does not imply that the entire operator family
or phase is complete. No universal sub-millisecond, sub-100-second suite, RSS-bound,
or engine-superiority claim follows from this plan.
