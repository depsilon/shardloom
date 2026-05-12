# Vortex File IO Skill

## Purpose

Use this skill when designing or implementing Vortex file reads, Vortex file writes, file metadata
inspection, byte-range planning, or Vortex persistence.

The goal is to make Vortex file IO native, metadata-aware, and object-store-friendly.

## When to use

Use this skill for tasks involving:

- Opening Vortex files.
- Validating Vortex file references.
- Reading Vortex metadata.
- Reading Vortex segments.
- Planning byte ranges.
- Writing Vortex files.
- Preserving Vortex file statistics.
- Vortex round-trip tests.

## Rules

- Prefer metadata inspection before data reads.
- Prefer byte-range reads over full-file reads when possible.
- Do not materialize full files by default.
- Preserve Vortex metadata when writing Vortex output.
- Treat Vortex output as the highest-fidelity output path.
- Fail explicitly for unsupported file versions, encodings, layouts, or metadata.
- Do not use another engine to read or write Vortex files for ShardLoom execution.
- Do not turn Vortex file IO into generic decoded Arrow IO unless explicitly required by a
  translation boundary.

## Required checks

For read behavior:

- Empty path.
- Missing file.
- Invalid file.
- Unsupported file version.
- Unsupported encoding.
- Unsupported layout.
- Missing metadata.
- Empty file or empty array.
- Null-heavy data.
- Wide table / struct dtype behavior.

For write behavior:

- Empty output.
- Simple primitive output.
- Nullable output.
- Struct/wide output.
- Metadata preservation.
- Statistics preservation or explicit omission.
- Deterministic error for unsupported output.
- Round-trip test plan.

## Red flags

- Whole-file reads as the default.
- Reading all columns before projection.
- Reading all segments before pruning.
- Writing Parquet first and treating Vortex as optional.
- Dropping Vortex metadata silently.
- Using Spark, DataFusion, or DuckDB to perform Vortex reads/writes for execution.

## Example Codex prompt fragment

"Use the Vortex File IO skill. Inspect metadata before data. Prefer byte ranges and segment-level
reads. Preserve Vortex metadata on output. Do not use Spark, DataFusion, or DuckDB as Vortex
execution helpers."
