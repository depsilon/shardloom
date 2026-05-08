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
- CPU specialization must remain native to ShardLoom and must not delegate to Spark, DataFusion, DuckDB, Polars, Velox, or another execution engine.
- Performance or superiority claims require correctness evidence and benchmark evidence before they can be emitted.

## Operator API requirements

- Encoded batch/segment operator API.
- Filter/projection/count/aggregate primitive kernels with encoded awareness.
- Portable scalar/native baselines are allowed only inside ShardLoom kernels and dispatch logic, never via external engines.
- Architecture-specific paths require explicit CPU feature guards.
- Dispatch decisions must be deterministic and explainable.

## CG-15 CPU specialization report foundation

CG-15 starts with a report-only contract before any runtime specialization is implemented.

`CpuOperatorSpecializationReport` records:
- schema version and report identifier
- report-only status
- operator/kernel specialization entries
- candidate instruction/layout classes
- correctness evidence requirement
- benchmark evidence requirement
- CPU feature guard requirement
- portable native baseline requirement
- deterministic dispatch requirement
- host CPU probe disabled
- runtime dispatch disabled
- unsafe code requirement disabled
- GPU/FPGA requirement disabled
- runtime/data/IO side-effect fields
- external engine execution disabled
- fallback execution disabled
- production claim disabled

`CpuOperatorSpecializationEntry` records:
- operator kind
- kernel kind
- instruction/layout classes
- specialization candidate flag
- correctness evidence state
- benchmark evidence state
- CPU feature guard requirement
- portable native baseline requirement
- deterministic dispatch requirement

Candidate instruction/layout classes include:
- scalar_portable
- simd_portable
- avx2_candidate
- avx512_candidate
- neon_candidate
- cache_tiled
- branch_reduced
- dictionary_aware
- run_aware
- bit_packed
- selection_vector_aware

Initial CG-15 report-only candidates cover:
- encoded filter kernels
- encoded projection kernels
- encoded count-aggregate kernels
- partial-decode aggregate kernels
- partial-decode sort kernels
- partial-decode join kernels

The report is intentionally not a CPU profiler or runtime dispatcher. It is a
deterministic capability and evidence surface so future specialization work can
be reviewed without hiding execution behavior.

## CG-15 acceptance gates

CG-15 cannot move from report-only planning to runtime specialization until:
- every specialized path has correctness evidence
- benchmark evidence exists before any performance or superiority claim
- CPU feature guards are deterministic and testable
- a portable native baseline remains available
- unsupported CPU features fail with explicit diagnostics
- encoded, partially decoded, and selection-vector semantics are preserved
- unsafe code has an RFC-approved safety contract if it is introduced
- no external execution engine is used for specialization
- fallback execution remains disabled and explicitly reported

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
