# Expression and Kernel Registry

## Purpose

Provide design and implementation guidance for ShardLoom's native expression engine and kernel
registry so expression evaluation stays Vortex-aware, null-safe, type-aware, deterministic, and
no-fallback.

## When to use

Use this skill for:

- Expression IR design.
- Function signature metadata design.
- Kernel capability metadata design.
- Kernel registry APIs and selection logic.
- UDF metadata and integration boundaries.
- Effectful expression/kernels safety behavior.
- Expression diagnostics and unsupported-path design.

## Rules

- Keep expression and kernel behavior native to ShardLoom.
- Preserve Vortex-aware evaluation decisions (metadata, encoded, partial decode, late
  materialization, full materialization).
- Prefer metadata kernels before encoded kernels.
- Prefer encoded kernels before partial decode.
- Prefer partial decode before full materialization.
- Make kernel selection deterministic for equivalent inputs/configuration.
- Declare null behavior and type coercion explicitly.
- Fail unsupported kernels explicitly with deterministic diagnostics.
- Treat decoded reference kernels as test/reference tools or explicit native paths, not hidden
  fallback execution.
- Require UDF metadata: type signatures, null behavior, determinism, effect level, materialization
  requirement, and safety controls.
- Ensure effectful kernels do not run during explain, estimate, or dry-run-style workflows.
- Never add Spark/DataFusion fallback or fallback to other execution engines.

## Required checks

- Are expression nodes typed, null-aware, and effect-aware?
- Is function and kernel capability metadata explicit enough for deterministic selection?
- Does kernel-selection order preserve metadata/encoded-first priorities?
- Are unsupported/coercion failures explicit and diagnosable?
- Are UDF declarations complete for safety and correctness?
- Are effect boundaries enforced for non-execution flows?
- Is fallback attempted status explicitly false in diagnostics?

## Red flags

- Implicit type coercion without diagnostics.
- Silent switch to decoded execution without policy visibility.
- Missing null semantics on functions/kernels.
- Missing determinism/effect metadata.
- UDF definitions without materialization or safety metadata.
- Any fallback path to Spark, DataFusion, DuckDB, Polars, Velox, or other external engines.

## Example Codex prompt fragment

"Design a native ShardLoom expression function and kernel capability contract for `<function>`,
including deterministic kernel-selection rules with metadata/encoded-first priority, explicit
null/type behavior, explicit unsupported diagnostics, and no fallback execution."
