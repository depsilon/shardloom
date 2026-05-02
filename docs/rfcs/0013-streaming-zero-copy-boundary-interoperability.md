# RFC 0013: Streaming, Zero-Copy, Zero-Decode, and Boundary Interoperability

## Status

Draft

## Summary

This RFC defines ShardLoom's streaming execution, zero-copy interoperability, zero-decode execution, sink-driven output, and boundary interoperability principles.

ShardLoom should not only be fast because it uses a modern storage format. It should be fast because it avoids unnecessary data work:

1. Do not read.
2. Do not decode.
3. Do not copy.
4. Do not materialize.
5. Do not shuffle.
6. Do not distribute unless necessary.

Streaming and zero-copy are important, but the deeper ShardLoom goal is zero-decode execution over Vortex-native encoded layouts wherever possible.

## Context

ShardLoom is a standalone Vortex-native encoded-columnar execution engine.

The project already defines or plans:

- Vortex-native input and output.
- Encoded segments.
- Segment statistics.
- Metadata-only execution.
- Segment pruning.
- Adaptive sizing and parallelism.
- Object-store runtime planning.
- Translation reports.
- Developer and agent experience.
- Modular extension points.

The next design concern is how data flows through the engine without forcing full materialization.

Many modern systems use streaming to process larger-than-memory workloads and reduce peak memory. Many columnar systems use zero-copy interoperability to move data across process/language boundaries efficiently. Vortex adds a stronger opportunity: ShardLoom can often avoid decoding in the first place.

## Goals

- Define streaming execution as a first-class ShardLoom concept.
- Define zero-decode as a higher priority than zero-copy.
- Define zero-copy interoperability boundaries.
- Define sink-driven execution.
- Define streaming source/operator/sink concepts.
- Define backpressure and bounded-memory planning concepts.
- Define Arrow-like boundary interoperability without making Arrow the default internal execution model.
- Define future Flight/FlightSQL-like surfaces without adding dependencies.
- Define diagnostics for unsupported streaming and materialization boundaries.
- Preserve no-fallback execution.

## Non-goals

- Do not implement streaming execution in this RFC.
- Do not implement Arrow C Data, Arrow C Stream, Arrow IPC, Flight, or FlightSQL in this RFC.
- Do not add Arrow dependencies in this RFC.
- Do not add Vortex dependencies in this RFC.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not define final networking APIs.
- Do not define final Python or FFI APIs.
- Do not implement actual sink-driven execution yet.

## Core principle

ShardLoom should prefer data work avoidance in this order:

### 1. Metadata-only

Answer from manifest, file, segment, or array statistics.

No data read.

### 2. Pruned

Skip segments proven irrelevant.

No segment data read.

### 3. Encoded / zero-decode

Compute directly against encoded Vortex layouts.

Data may be read, but values are not fully decoded.

### 4. Zero-copy boundary

Share already-represented buffers across interfaces without copying.

Useful for Arrow/FFI/IPC boundaries.

### 5. Partial decode

Decode only necessary buffers, rows, or columns.

### 6. Late materialization

Materialize selected columns/rows only after filters and pruning.

### 7. Full materialization

Materialize complete values only when required.

### 8. Shuffle/distribute

Move data across tasks/workers only when necessary.

This hierarchy should shape planning, execution, diagnostics, and benchmarks.

## Streaming execution

Streaming execution means work is performed in bounded chunks rather than collecting an entire dataset into memory.

ShardLoom should eventually support streaming pipelines composed of:

- StreamingSource.
- StreamingOperator.
- StreamingSink.
- PipelineStage.
- BackpressurePolicy.
- BoundedMemoryPolicy.
- StreamingDiagnostics.

Streaming should be native and explicit.

If a plan cannot stream, ShardLoom must say why.

ShardLoom must not silently switch from streaming to full in-memory execution without diagnostics.

## StreamingSource

A StreamingSource yields native chunks of work.

Potential source units:

- Vortex segments.
- Vortex splits.
- Manifest segment groups.
- Object-store byte ranges.
- Metadata-only pseudo-chunks.
- External read chunks.
- Future Arrow stream chunks at a boundary.

A StreamingSource should expose:

- Schema or DTypes.
- Segment descriptors.
- Statistics.
- Estimated encoded size.
- Estimated decoded size.
- Byte ranges.
- Streaming capability.
- Diagnostics.

## StreamingOperator

A StreamingOperator transforms or filters streaming inputs.

Potential operators:

- Metadata-only operator.
- Segment pruning operator.
- Encoded predicate operator.
- Projection operator.
- Partial decode operator.
- Aggregate partial operator.
- Aggregate final operator.
- Join build/probe operator.
- Translation operator.
- External effect operator.

Operators should declare:

- Whether they can stream.
- Whether they require state.
- Whether they require full materialization.
- Whether they preserve encoded representation.
- Whether they preserve ordering.
- Whether they may emit external effects.
- Their memory behavior.
- Their diagnostics.

## StreamingSink

A StreamingSink consumes streaming output.

Potential sinks:

- Vortex native sink.
- Arrow IPC compatibility sink.
- Parquet compatibility sink.
- Null benchmark sink.
- In-memory debug sink.
- Future Arrow C Stream sink.
- Future Flight/FlightSQL sink.
- Future external API sink.

Sinks should declare:

- Accepted input representation.
- Whether encoded input can be preserved.
- Required materialization.
- Metadata preservation.
- Output fidelity.
- Commit behavior.
- Backpressure behavior.
- Diagnostics.

Vortex sinks should prefer preserving encoded segments, statistics, layouts, and metadata.

Compatibility sinks may require materialization and must report metadata loss.

## Sink-driven execution

ShardLoom should eventually support sink-driven execution planning.

A sink should be able to tell the planner:

- I can accept encoded chunks.
- I need materialized values.
- I preserve Vortex metadata.
- I lose physical metadata.
- I require a commit plan.
- I support streaming writes.
- I require full materialization.

This lets output format choice influence upstream execution.

Example:

- Vortex output may preserve encoded representation and segment metadata.
- Parquet output may require materialization and lose Vortex physical metadata.
- Arrow IPC output may preserve logical columnar values but lose Vortex layout metadata.

## Zero-decode execution

Zero-copy is useful, but zero-decode is more important to ShardLoom.

Zero-decode means ShardLoom evaluates work against encoded Vortex representation instead of decoding values first.

Examples:

- Count rows from metadata.
- Answer min/max from statistics.
- Evaluate equality against dictionary ids.
- Prune ranges from min/max.
- Use run-length information for repeated values.
- Use constant-array metadata.
- Preserve encoded values into native Vortex output.

Zero-decode execution should be visible in explain output and benchmarks.

## Zero-copy interoperability

Zero-copy means already-represented buffers are shared across interfaces without copying.

ShardLoom should eventually support zero-copy or low-copy interoperability at boundaries such as:

- Rust APIs.
- Python/FFI APIs.
- Arrow C Data-like boundaries.
- Arrow C Stream-like boundaries.
- Arrow IPC exports.
- Future Flight/FlightSQL-like server interfaces.

Zero-copy is a boundary optimization.

It should not cause ShardLoom to abandon Vortex-native encoded execution internally.

## Arrow boundary principles

Arrow-like interoperability is valuable because many tools understand Arrow-style columnar data.

ShardLoom should eventually support Arrow-like boundaries for:

- Python integration.
- FFI integration.
- Compatibility output.
- Downstream dataframe tools.
- Decoded reference testing.
- Future server/query interfaces.

However:

- Arrow should not become ShardLoom's default internal execution representation.
- Arrow conversion must report metadata loss when relevant.
- Arrow compatibility output is not the same as Vortex native output.
- DataFusion should not be added just because Arrow interoperability exists.

## Future Flight/FlightSQL-like surface

ShardLoom may eventually expose a Flight/FlightSQL-like surface for familiar data access.

This could enable:

- SQL-like client access.
- BI/tool integration.
- Agent query workflows.
- Efficient columnar transport.

This is future work.

A Flight/FlightSQL-like surface must not imply:

- DataFusion execution.
- Spark fallback.
- Row-oriented transport.
- Hidden materialization.
- Silent metadata loss.

## Backpressure

Streaming execution requires backpressure.

ShardLoom should eventually model:

- Producer speed.
- Consumer speed.
- Sink write capacity.
- Memory limits.
- Object-store request limits.
- External effect rate limits.
- Model/API call budgets.
- Output commit constraints.

Backpressure behavior should be explicit.

If the engine cannot maintain bounded memory, it should say so.

## Bounded memory

Streaming should preserve bounded memory where possible.

A streaming plan should identify:

- Operators that can stream with bounded memory.
- Operators requiring state.
- Operators requiring full materialization.
- Operators requiring shuffle.
- Operators requiring spill.
- Operators that are unsupported.

Bounded-memory behavior should appear in explain/estimate output.

## Streaming and adaptive sizing

Streaming should interact with adaptive sizing.

Adaptive sizing should consider:

- Target task bytes.
- Available memory.
- Encoded bytes.
- Estimated decoded bytes.
- Selected columns.
- Materialization policy.
- Byte ranges.
- Sink requirements.
- Operator state.
- Object-store request budget.

The planner should be able to choose:

- Stream segment as-is.
- Split segment.
- Coalesce small segments.
- Use metadata-only pseudo-chunks.
- Use encoded stream chunks.
- Partial decode.
- Full materialization only when required.

## Streaming and effects

LLM calls, API calls, embedding generation, vector search, and external writes are effectful operations.

Streaming plans must model effectful operators explicitly.

Effectful operators should declare:

- Whether they can stream.
- Batch size.
- Rate limit.
- Cost budget.
- Retry policy.
- Idempotency.
- Dry-run behavior.
- Whether they mutate external state.

Explain and estimate must not execute effects.

## Streaming unsupported behavior

If streaming is requested but unsupported, ShardLoom should produce a structured diagnostic.

Examples:

- Operator requires full materialization.
- Sink requires full materialization.
- Join requires shuffle not implemented.
- Aggregate requires unbounded state.
- External effect cannot stream safely.
- Output commit mode prevents streaming write.
- Statistics are missing.
- Encoding is unsupported.

Unsupported streaming must not trigger fallback execution.

## Diagnostics

Streaming and zero-copy diagnostics should include:

- Requested streaming mode.
- Actual planned mode.
- Whether metadata-only was used.
- Whether encoded execution was used.
- Whether zero-copy boundary was used.
- Whether partial decode was required.
- Whether full materialization was required.
- Whether shuffle was required.
- Whether distribution was required.
- Why streaming was unsupported.
- Fallback attempted. This should be false.

## Explain and estimate

Explain output should show:

- Streaming stages.
- Source representation.
- Operator streaming capability.
- Sink requirements.
- Encoded/decoded boundaries.
- Materialization boundaries.
- Backpressure assumptions.
- Memory bounds.
- Unsupported streaming features.

Estimate output should show:

- Estimated chunks.
- Estimated bytes read.
- Estimated bytes decoded.
- Estimated rows materialized.
- Estimated peak memory.
- Estimated sink output bytes.
- Estimated object-store requests.
- Estimated effect calls when relevant.
- Known uncertainty.

## Benchmark implications

Streaming and zero-decode benchmarks should measure:

- Peak memory.
- Bytes read.
- Bytes decoded.
- Bytes copied where measurable.
- Rows materialized.
- Segment count.
- Chunk count.
- Object-store requests.
- Sink fidelity.
- Runtime.
- Correctness.

Wall-clock time alone is not enough.

## Design examples

### Vortex-native streaming write

A Vortex sink may accept encoded chunks and preserve metadata.

Expected behavior:

- Avoid full materialization.
- Preserve DTypes.
- Preserve statistics where possible.
- Preserve layout hints where possible.
- Emit native Vortex output.
- Report full-fidelity output.

### Parquet compatibility export

A Parquet sink may require materialized values.

Expected behavior:

- Materialization boundary is explicit.
- Physical Vortex metadata loss is reported.
- Output is compatibility, not native.
- No fallback execution.

### Arrow boundary export

An Arrow-like boundary may expose columnar values efficiently.

Expected behavior:

- Boundary is explicit.
- Metadata loss is reported.
- Arrow is not internal default execution.
- No DataFusion execution is implied.

## Alternatives considered

### Use Polars streaming directly

Rejected for core execution.

Polars streaming is an important inspiration, but ShardLoom must preserve Vortex-native execution and no fallback architecture.

### Use Arrow as the internal execution model

Rejected as default.

Arrow interop is useful, but ShardLoom's deeper advantage is Vortex-native zero-decode execution.

### Silently fallback to in-memory execution

Rejected.

If streaming is requested and unsupported, ShardLoom must produce diagnostics.

### Implement FlightSQL early

Rejected.

FlightSQL-like access may be valuable later, but core execution contracts come first.

## Risks

- Streaming architecture may become too complex too early.
- Zero-copy terminology may confuse users if metadata is lost.
- Some operators inherently require state or materialization.
- Compatibility sinks may force materialization.
- Future Arrow/Flight integration may introduce dependency and version challenges.
- Users may expect every query to stream.
- Agents may misinterpret planned streaming features as implemented.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Streaming execution is a first-class design goal.
- Zero-decode is more important than zero-copy internally.
- Arrow-like zero-copy boundaries are valuable but not internal default execution.
- Sink-driven execution is required for output-aware planning.
- Streaming must not silently fall back to full in-memory execution.
- Backpressure and bounded memory must be modeled.
- Explain/estimate should expose streaming and materialization boundaries.
- Compatibility outputs must report metadata loss.
- No fallback execution is permitted.

## Verification plan

Future implementation PRs should verify:

- Streaming capability can be represented.
- Source/operator/sink boundaries can be represented.
- Materialization boundaries can be represented.
- Sink requirements can influence planning.
- Unsupported streaming produces deterministic diagnostics.
- Vortex-native output can be represented as high-fidelity.
- Compatibility output can report metadata loss.
- Explain/estimate can represent streaming states.
- No Spark or DataFusion dependency is introduced.

## Open questions

- What streaming source abstraction should be implemented first?
- Should streaming chunks map one-to-one with Vortex segments initially?
- What sink should be implemented first: Vortex native, null sink, or Arrow-like boundary?
- How should backpressure be represented in early code?
- Should streaming be opt-in, default, or planner-selected?
- How should streaming interact with adaptive task sizing?
- When should Arrow C Data/C Stream be supported?
- When should Flight/FlightSQL-like access be considered?
