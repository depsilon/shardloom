# Extension Plugin Sandboxing

## Purpose

Use this skill when designing extension, plugin ABI, UDF runtime boundary, connector safety, or capability manifest behavior.

## When to use

Use this document when work touches extension manifests, plugin lifecycle, capability reporting, permission models, effect models, sandboxing policy, or agent-safe extension inspection.

## Rules

- Extensions must declare capabilities, permissions, effect level, determinism, materialization requirements, and license/provenance metadata.
- Untrusted code must be sandbox-aware and must not run with unrestricted host access.
- Extension inspection must not execute extension code.
- Planned capabilities must not appear as supported.
- Plugins must not hide fallback execution behavior.
- Unsupported extension behavior must fail explicitly with deterministic diagnostics.
- No Spark or DataFusion fallback is allowed.

## Required checks

- Confirm extension category and capability declarations are explicit and machine-readable.
- Confirm permission and effect declarations are explicit and conservative by default.
- Confirm determinism and materialization requirements are declared.
- Confirm license, provenance, and dependency metadata are present.
- Confirm manifest validation can run without loading executable extension code.
- Confirm unsupported behavior does not silently delegate to any fallback execution engine.

## Red flags

- Capability manifests that claim support for planned or undefined behavior.
- Extension loading paths that execute code during metadata inspection.
- Missing permission/effect declarations for external reads, writes, model calls, or API calls.
- Unbounded filesystem/network/secret access in default plugin settings.
- Hidden dependencies that imply Spark, DataFusion, DuckDB, Polars, Velox, or other fallback execution.

## Example Codex prompt fragment

"Design this extension contract as manifest-first and sandbox-aware. Require explicit capability, permission, effect, determinism, materialization, and license/provenance fields. Ensure inspection is non-executing and preserve no-fallback execution with deterministic unsupported diagnostics."
