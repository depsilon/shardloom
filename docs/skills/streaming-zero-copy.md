# Streaming, Zero-Copy, and Zero-Decode Skill

## Purpose

Use this skill when designing or implementing streaming execution, zero-copy interoperability,
zero-decode execution, sink-driven output, materialization boundaries, Arrow-like boundaries, or
future Flight/FlightSQL-like surfaces.

ShardLoom should minimize data work in this order:

1. Do not read.
2. Do not decode.
3. Do not copy.
4. Do not materialize.
5. Do not shuffle.
6. Do not distribute unless necessary.

## When to use

Use this skill for tasks involving:

- Streaming execution.
- Streaming sources.
- Streaming operators.
- Streaming sinks.
- Sink-driven planning.
- Zero-copy boundaries.
- Zero-decode execution.
- Arrow-like interop.
- Arrow IPC export.
- Future Flight/FlightSQL-like surfaces.
- Backpressure.
- Bounded memory.
- Materialization boundaries.
- Streaming diagnostics.
- Streaming benchmarks.

## Rules

- Prefer metadata-only answers before reads.
- Prefer segment pruning before reads.
- Prefer encoded Vortex-native execution before decoding.
- Prefer zero-decode before zero-copy.
- Prefer zero-copy boundary sharing before copying.
- Prefer partial decode before full materialization.
- Prefer late materialization before eager materialization.
- Prefer shuffle avoidance before shuffle optimization.
- Streaming must be explicit and diagnosable.
- Do not silently fallback from streaming to full in-memory execution.
- Arrow-like boundaries are interoperability surfaces, not default internal execution.
- Future Flight/FlightSQL-like access must not imply DataFusion execution.
- Vortex output remains highest-fidelity.
- Compatibility outputs must report metadata loss.
- No Spark or DataFusion fallback is allowed.

## Required checks

For streaming source work:

- What unit is streamed?
- Is it a Vortex segment, split, byte range, metadata pseudo-chunk, or compatibility chunk?
- Are statistics available?
- Are byte ranges available?
- Is encoded representation preserved?
- Is bounded memory preserved?
- What diagnostics are emitted?

For streaming operator work:

- Can the operator stream?
- Does it require state?
- Does it require full materialization?
- Does it preserve encoded data?
- Does it preserve ordering?
- Does it introduce external effects?
- Does it require shuffle?
- What happens when streaming is unsupported?

For sink work:

- Can the sink accept encoded data?
- Does the sink require materialization?
- Does the sink preserve Vortex metadata?
- Does the sink lose metadata?
- Does the sink support streaming writes?
- Does the sink require a commit plan?
- Is metadata loss reported?

For Arrow-like boundary work:

- Is this a boundary, not internal default execution?
- What metadata is preserved?
- What metadata is lost?
- Is the loss reported?
- Is DataFusion avoided?
- Is Vortex-native output still available?

For diagnostics:

- Is requested streaming mode represented?
- Is actual planned mode represented?
- Are materialization boundaries visible?
- Is fallback status explicit?
- Are unsupported features deterministic?
- Are estimates honest about uncertainty?

## Red flags

- Reading whole files by default.
- Decoding all values before filtering.
- Copying buffers unnecessarily.
- Materializing everything before projection/filter.
- Treating Arrow as the internal execution substrate.
- Adding DataFusion because Arrow exists.
- Silently switching from streaming to in-memory execution.
- Hiding sink materialization requirements.
- Hiding metadata loss during compatibility export.
- Treating Flight/FlightSQL as a shortcut to external execution.
- Adding Spark or DataFusion fallback.

## Example Codex prompt fragment

"Use the Streaming, Zero-Copy, and Zero-Decode skill. Prefer metadata-only, pruning, encoded
execution, zero-copy boundaries, partial decode, and late materialization in that order. Streaming
must be explicit and diagnosable. Do not silently fallback to full in-memory execution. Arrow-like
boundaries are interop surfaces, not internal execution."
