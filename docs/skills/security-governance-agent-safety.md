# Security, Secrets, Governance, and Agent Safety Skill

## Purpose

Guide security-sensitive ShardLoom work so credentials, permissions, side effects, auditability, and agent behavior remain explicit, deterministic, and no-fallback.

## When to use

Use this skill for:

- Secret and credential handling changes.
- Permission/capability gating changes.
- Effectful operations (API/LLM/embedding/vector/external write) design.
- Agent-facing safety and policy diagnostics.
- Audit log, redaction, and governance-related work.

## Rules

- Never store raw secrets in plans, diagnostics, logs, traces, or reports.
- Use explicit capability gates for effectful operations.
- Keep explain/estimate/doctor/capabilities non-effectful.
- Ensure dry-run checks permissions/capabilities without mutating external systems.
- Emit deterministic diagnostics for denied/unsupported behavior.
- Keep fallback attempted false and do not invoke Spark/DataFusion or any external fallback engine.

## Required checks

- Are credentials represented via references/handles instead of raw values?
- Are effectful nodes explicit and approval-gated?
- Are non-execution flows protected from side effects?
- Are audit events structured and redacted?
- Are diagnostics actionable and stable?

## Red flags

- Secret values in logs or human-text diagnostics.
- Effectful operation hidden behind a non-effectful node.
- Imported plans bypassing capability checks.
- Missing denial diagnostics for disallowed operations.
- Any fallback path to Spark/DataFusion/DuckDB/Polars/Velox.

## Example Codex prompt fragment

"Design capability-gated external write behavior for `<target>` with explicit approval, dry-run-safe validation, redacted diagnostics/audit output, deterministic failure codes, and no fallback execution."
