# Vortex Internals Skill

## Purpose

Use this skill as the top-level entry point for Vortex-related ShardLoom work.

ShardLoom's main technical advantage depends on treating Vortex as a native execution substrate, not
just a file format.

Vortex-native execution may include upstream Vortex array, compute, scan,
source, or sink APIs when they are isolated behind approved ShardLoom
boundaries, feature-gated, version-recorded, policy-admitted, and
certificate-backed. That is distinct from using Vortex query-engine integrations
or external engines as fallback.

## Required deeper skills

For Vortex-specific work, use the detailed skill pack in `docs/skills/vortex/`:

- `docs/skills/vortex/README.md`
- `docs/skills/vortex/vortex-concepts.md`
- `docs/skills/vortex/vortex-file-io.md`
- `docs/skills/vortex/vortex-encodings-layouts.md`
- `docs/skills/vortex/vortex-stats-pruning.md`
- `docs/skills/vortex/vortex-native-output.md`
- `docs/skills/vortex/vortex-scan-api.md`
- `docs/skills/vortex/vortex-arrow-interop.md`
- `docs/skills/vortex/vortex-versioning-upstream.md`

## Core rules

- Vortex is a first-class native input target.
- Vortex is a first-class native output target.
- Upstream Vortex-native providers are allowed when admitted and certified by
  ShardLoom policy.
- Vortex output is the highest-fidelity persistence target.
- Preserve Vortex metadata, statistics, encodings, layouts, and validity information where possible.
- Avoid unnecessary decode.
- Use metadata and statistics before reading data.
- Use encoded execution when possible.
- Use partial decode only when required.
- Fail explicitly for unsupported Vortex behavior.
- Do not add Spark, DataFusion, DuckDB, Polars, or Velox as execution fallback.
- Do not use Vortex DataFusion, DuckDB, Spark, Trino, or similar integrations as
  fallback execution.
- Do not treat Arrow conversion as the default execution model.

## Required checks

For any Vortex-related implementation, confirm:

- Relevant detailed Vortex skill files were read.
- DType, encoding, layout, statistics, and validity behavior were considered.
- Vortex-native input/output remains intact.
- Unsupported behavior has deterministic diagnostics.
- Tests cover empty, null, unsupported, and metadata-preservation cases.
- No fallback execution was introduced.

## Example Codex prompt fragment

"Use `docs/skills/vortex-internals.md` and the detailed Vortex skill pack under
`docs/skills/vortex/`. Preserve Vortex as native input and output, avoid unnecessary decode,
preserve metadata where possible, and fail explicitly for unsupported behavior."
