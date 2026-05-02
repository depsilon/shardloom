# Vortex Arrow Interoperability Skill

## Purpose

Use this skill when designing or implementing boundaries between Vortex and Apache Arrow.

The goal is to use Arrow interoperability where it helps without letting decoded Arrow become ShardLoom's default execution model.

## When to use

Use this skill for tasks involving:

- Arrow arrays.
- Arrow RecordBatches.
- Arrow IPC output.
- Vortex-to-Arrow conversion.
- Arrow-to-Vortex conversion.
- Testing decoded references.
- Compatibility exports.
- Python or FFI interoperability.

## Rules

- Arrow interoperability is valuable.
- Arrow IPC is a compatibility output target.
- Arrow decoded arrays may be useful for tests, debugging, and some translation boundaries.
- Arrow decoded arrays must not become the hidden default execution representation.
- Preserve Vortex metadata before converting to Arrow.
- Document metadata loss when converting to Arrow.
- Use decoded Arrow reference behavior for correctness tests where appropriate.
- Do not use Arrow conversion to hide unsupported Vortex encodings or layouts.
- Do not introduce DataFusion execution because Arrow is available.

## Required checks

For Vortex-to-Arrow conversion:

- DType mapping.
- Nullability mapping.
- Unsupported DType diagnostics.
- Metadata loss report.
- Empty arrays.
- All-null arrays.
- Nested or struct arrays where relevant.
- Round-trip expectations.

For Arrow-to-Vortex conversion:

- Logical DType preservation.
- Statistics generation or omission.
- Encoding choice.
- Nullability preservation.
- Unsupported Arrow type diagnostics.
- Vortex output compatibility.

## Red flags

- "Convert to Arrow first, then execute everything."
- Dropping Vortex metadata without reporting it.
- Treating Arrow IPC as equivalent to Vortex output.
- Adding DataFusion because it already understands Arrow.
- Using decoded Arrow reference behavior as production execution.
- Losing nullability or type information during conversion.

## Example Codex prompt fragment

"Use the Vortex Arrow Interoperability skill. Arrow is allowed for compatibility, tests, and some boundaries, but not as hidden default execution. Preserve or report metadata loss. Do not add DataFusion."
