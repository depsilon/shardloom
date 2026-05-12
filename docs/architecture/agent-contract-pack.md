# Agent Contract Pack

## Purpose

This document centralizes the machine-readable surfaces an autonomous agent should inspect before
planning, executing, benchmarking, or stopping.

Active queue and completion state live in `docs/architecture/phased-execution-plan.md`.

The agent contract pack is report-only. It does not execute commands, probe the environment, resolve
credentials, read data, call external systems, run benchmarks, publish artifacts, or authorize
fallback execution.

## Default Workflow

Agents should prefer stable JSON over human text:

1. `shardloom feature-footprint --format json`
2. `shardloom effect-budget-plan --format json`
3. `shardloom doctor --format json`
4. `shardloom capabilities certification --format json`
5. `shardloom world-class-sufficiency-plan --format json`
6. `shardloom benchmark-plan --format json`

Human text is useful for explanations, but JSON fields are the authoritative automation surface.

## AgentContractPack Checklist

- [x] Core report contract
  - `schema_version`
  - `pack_id`
  - `surfaces`
  - `recommended_sequence`
  - `deterministic_json_required=true`
  - `text_is_authoritative=false`
  - `no_probe_default=true`
  - `external_effects_default_denied=true`
  - `destructive_effects_default_denied=true`
  - `fallback_attempted=false`
  - `fallback_execution_allowed=false`
- [x] Required surface inventory
  - output envelope
  - diagnostics
  - capabilities
  - feature footprint
  - effect budget
  - doctor
  - explain/estimate
  - plan portability
  - native I/O envelope
  - execution certificate
  - benchmark evidence
  - world-class sufficiency
  - security governance
- [x] CLI exposure
  - `shardloom agent-contract-pack`
  - `--format json` emits stable fields for agents and tests.
- [~] Future integration
  - Agent-facing reports should include next-action hints when a gate blocks.
  - Future effectful commands should require `EffectBudgetReport` references.
  - Future execution commands should emit execution certificates and native I/O certificate
    references.
  - Future benchmark publication should require benchmark evidence and claim gates.
  - Future Python/API wrappers should expose the same pack fields without text scraping.

## Guardrails

- Do not make human text the source of truth for automation.
- Do not let agents infer support from planned or docs-only status.
- Do not hide fallback status, effect budgets, materialization, row reads, decode, Arrow conversion,
  object-store IO, writes, or benchmark claim gates.
- Do not execute external engines except as explicit correctness or benchmark baselines.
