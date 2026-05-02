# Vortex Native Output Skill

## Purpose

Use this skill when designing or implementing ShardLoom output behavior where Vortex is the target.

The goal is to make Vortex output the highest-fidelity persistence target for ShardLoom.

## When to use

Use this skill for tasks involving:

- Vortex output writers.
- Vortex result persistence.
- Vortex metadata preservation.
- Vortex statistics emission.
- Vortex layout hints.
- Vortex round trips.
- Output compatibility reports.
- Translation loss reports.

## Rules

- Vortex output is first-class and native.
- Vortex output is the highest-fidelity target.
- Do not treat Vortex output as optional or secondary.
- Preserve logical DTypes.
- Preserve or emit statistics where possible.
- Preserve layout and encoding intent where possible.
- Preserve nullability and validity information.
- If ShardLoom cannot preserve metadata, report the loss explicitly.
- If another output target loses information, compare it against the Vortex-native output path.
- Do not use Spark, DataFusion, DuckDB, or another engine to write Vortex output for ShardLoom execution.

## Required checks

For Vortex output:

- Empty result.
- Primitive columns.
- Nullable columns.
- Struct/wide outputs.
- Selection-vector materialization.
- Metadata preservation.
- Statistics preservation or explicit absence.
- Round-trip read.
- Unsupported output diagnostic.

For comparison with lower-fidelity output:

- What metadata Vortex preserves.
- What metadata Parquet/Arrow/Iceberg/Delta-compatible output drops or maps.
- Whether the loss is documented.

## Red flags

- Parquet output implemented before Vortex output.
- Vortex output missing from a translation design.
- Vortex output loses metadata silently.
- Output paths that force full materialization unnecessarily.
- Treating Vortex as only an input format.
- Using another engine to produce Vortex output.

## Example Codex prompt fragment

"Use the Vortex Native Output skill. Vortex must be the first-class highest-fidelity output. Preserve DTypes, statistics, layout intent, and validity where possible. Report metadata loss explicitly."
