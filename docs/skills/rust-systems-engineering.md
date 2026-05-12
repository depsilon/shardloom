# Rust Systems Engineering Skill

## Purpose

Use this skill when modifying ShardLoom's Rust workspace, crates, APIs, runtime logic, errors,
tests, or command-line interface.

The goal is to build a clean, safe, performant Rust systems project without accidentally introducing
fallback execution or unclear ownership boundaries.

## When to use

Use this skill for tasks involving:

- Rust crate structure.
- Public APIs.
- Internal traits.
- Error types.
- Runtime code.
- CLI behavior.
- Tests.
- Cargo workspace configuration.
- Rust dependency changes.

## Rules

- Keep modules small and cohesive.
- Prefer explicit types over loosely structured strings or maps.
- Use clear domain types for ShardLoom concepts such as segments, layouts, statistics, selection
  vectors, physical plans, and translation reports.
- Use explicit errors for unsupported functionality.
- Unsupported execution must not silently fall back to Spark, DataFusion, DuckDB, Polars, Velox, or
  another engine.
- Do not introduce `unsafe` code without an RFC and a clear safety contract.
- Avoid hidden global mutable state.
- Avoid broad dependencies when a small local abstraction will do.
- Keep workspace dependencies centralized where practical.
- Public APIs should have documentation comments.
- Prefer deterministic behavior and deterministic errors.
- Do not optimize prematurely without tests and benchmarks.
- Avoid mixing architecture policy changes with implementation changes in the same PR.

## Required checks

Run these before completing implementation work:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`

Also confirm:

- New public APIs have documentation.
- New errors are specific and understandable.
- No fallback engine dependency was introduced.
- No dependency with an incompatible license was introduced.
- Tests cover success and failure paths.

## Red flags

- Adding Spark, DataFusion, DuckDB, Polars, or Velox as an execution dependency.
- Returning vague errors such as "failed" or "unsupported" without context.
- Adding `unsafe` without a written safety rationale.
- Adding large dependencies for small utilities.
- Creating one large module that mixes planning, execution, storage, and translation.
- Writing performance-sensitive code without a benchmark plan.
- Writing code that assumes decoded Arrow arrays are always the execution representation.

## Example Codex prompt fragment

When modifying Rust code, include this instruction:

"Use the Rust Systems Engineering skill. Keep the change small and reviewable. Do not add Spark,
DataFusion, or fallback execution. Use explicit errors for unsupported behavior. Run fmt, clippy,
and tests before opening the PR."
