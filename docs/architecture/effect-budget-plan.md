# Effect Budget Plan

## Purpose

This document centralizes the report-only effect budget used by future Python, UDF, plugin,
object-store, catalog, event/API/SaaS, LLM, embedding, vector, and unstructured/media surfaces.

Active queue and completion state live in `docs/architecture/phased-execution-plan.md`.

The effect budget does not authorize runtime behavior, credentials, network calls, object-store IO,
catalog probes, file probes, UDF/plugin execution, model calls, media extraction, writes,
benchmarks, or fallback execution.

`GAR-RUNTIME-IMPL-4R/5O` adds a companion effectful-operation admission matrix for the current local
fixture exceptions: local SQLite import/export smoke, typed extension-manifest inspection, and the
built-in deterministic scalar UDF fixture. SQLite admission is a named-table local fixture scan to
workspace-safe JSONL plus roundtrip SQLite replay; optional ordering happens after the scan in
ShardLoom fixture code, and BLOB schemas/values are blocked. The effect budget remains
deny-by-default for external effects; those admitted rows are local/metadata-only and keep
credentials, network probes, dynamic loading, extension-code execution, fallback, and
external-engine invocation disabled.

## Default Policy

- External effects are denied by default.
- Destructive or mutating effects are denied by default.
- Network egress is denied by default.
- Credential resolution and secret loading are not performed by default.
- Redaction and audit requirements are explicit budget fields, not hidden side behavior.
- Materialization boundaries must be declared when an effect can expose rows, decoded data, media
  payloads, or external service inputs.
- Fallback execution remains disabled regardless of effect approval.

## EffectBudgetReport Checklist

- [x] Core no-probe report contract
  - `schema_version`
  - `report_id`
  - `budget_mode`
  - `entries`
  - `external_effects_allowed=false`
  - `destructive_effects_allowed=false`
  - `network_egress_allowed=false`
  - `credentials_resolved=false`
  - `secrets_loaded=false`
  - `fallback_attempted=false`
  - `fallback_execution_allowed=false`
- [x] Default scope inventory
  - local file read/write
  - object-store read/write
  - catalog read/write
  - API read/write
  - LLM call
  - embedding generation
  - vector search
  - Python UDF
  - WASM UDF
  - external service UDF
  - plugin execution
  - media extraction
  - network egress
- [x] CLI exposure
  - `shardloom effect-budget-plan`
  - `--format json` emits stable fields for agents and tests.
- [~] Future integration
  - Python wrapper and notebook APIs consume effect budgets before external work.
  - UDF/plugin execution checks budget, sandbox, permission, and audit fields.
  - Object-store and catalog adapters connect budget approval to source/sink plans.
  - Unstructured/media extraction, LLM, embedding, vector, and event/API/SaaS adapters require
    explicit budget approval.
  - Execution certificates and native I/O certificates include consumed effect-budget references
    when effects are approved.

## Guardrails

- Do not perform probes while creating a default `EffectBudgetReport`.
- Do not resolve secrets or credentials inside the budget report.
- Do not treat benchmark baselines as effect approval.
- Do not let effect approval authorize fallback execution.
- Do not hide row reads, decode, materialization, Arrow conversion, network egress, or writes behind
  a user-facing API.
