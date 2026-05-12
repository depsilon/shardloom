# Plan IR Interoperability

## Purpose

Provide design guidance for ShardLoom's native-first plan IR and interoperability direction so plan
import/export stays capability-checked, explicit about boundaries, and no-fallback.

## When to use

Use this skill for:

- Logical/physical/encoded plan IR design.
- Plan node identity and schema contracts.
- Plan import/export format and validation design.
- Agent-readable explain/estimate/report design.
- Effect and translation boundary modeling.
- Substrait-compatible interoperability planning.

## Rules

- Keep ShardLoom-native plan IR first and interoperability-aware second.
- Require imported plans to pass ShardLoom capability checks before execution.
- Fail unsupported imported plans explicitly with deterministic diagnostics.
- Keep effect boundaries explicit and machine-readable.
- Keep translation boundaries explicit, including Vortex-native and compatibility outputs.
- Version machine-readable plan schemas before making stability guarantees.
- Ensure plan export does not leak secrets.
- Treat Substrait-compatible thinking as an interoperability direction, not a dependency or fallback
  execution path.
- Never add Spark/DataFusion fallback or fallback to other external engines.

## Required checks

- Are plan layers and node boundaries explicit and consistent?
- Are capability checks comprehensive (types, functions, kernels, IO, memory/spill, effects,
  outputs)?
- Do import paths enforce no-fallback policy?
- Are effectful nodes blocked from explain/estimate/doctor-style flows unless explicitly safe?
- Are exported plans and reports redacted for secrets and credentials?
- Is plan versioning strategy defined before compatibility promises?

## Red flags

- Imported plans executing without validation.
- Implicit compatibility translation with unreported metadata loss.
- Missing effect boundary declarations.
- Claims of full Substrait compatibility without scoped support.
- Plan export containing secrets, tokens, or credentials.
- Any fallback path to Spark, DataFusion, DuckDB, Polars, Velox, or other external engines.

## Example Codex prompt fragment

"Propose a ShardLoom-native plan IR node contract for `<node kind>` and a capability validation path
for imported Substrait-like plans that enforces explicit unsupported diagnostics, explicit
effect/translation boundaries, versioned schema direction, and no fallback execution."
