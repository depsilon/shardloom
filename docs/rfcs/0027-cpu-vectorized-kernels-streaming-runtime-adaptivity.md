# RFC 0027 — CPU Vectorized Kernels, Streaming, and Runtime Adaptivity

## Scope

This RFC defines implementation contracts for:
- CG-7 physical operators/kernels.
- CG-8 streaming/parallel/adaptive execution.
- CG-14 runtime-adaptive optimizer and execution memory.
- CG-15 CPU operator specialization.

## CPU vectorized execution model

- Commodity CPU vectorized execution is a first-class target.
- Kernel interfaces operate on encoded batches/segments.
- Selection-vector execution is first-class.
- SIMD/cache-friendly kernels are required where applicable.
- GPU/FPGA acceleration is not required for the primary competitive claim.

## Operator API requirements

- Encoded batch/segment operator API.
- Filter/projection/count/aggregate primitive kernels with encoded awareness.
- Scalar fallback is allowed only inside ShardLoom kernels and dispatch logic, never via external engines.

## Streaming and bounded parallel runtime

- Streaming encoded batches.
- Bounded parallel local execution.
- Adaptive split/coalesce behavior.
- Backpressure-aware scheduling.
- Memory/spill-aware scheduler integration.

## Runtime adaptivity and reporting

- Runtime filter reordering.
- Segment-level adaptive pruning.
- Workload profile cache.
- CPU feature detection/report.
- Deterministic diagnostics and explainability for adaptive decisions.

## Non-goals

- No external engine fallback.
- No GPU/FPGA requirement.
- No implicit conversion to Arrow-default execution.
