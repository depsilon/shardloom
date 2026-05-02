# ShardLoom Codex Skills

These skills are required pre-implementation guidance for contributors using Codex/agents in this repository.

## How to use

Before starting implementation work, identify the touched subsystem and review the corresponding skill(s) in this folder. Apply the skill rules together with `AGENTS.md` hard requirements.

## Skill index

- `rust-systems-engineering.md` — Rust design and reliability rules for systems-level code.
- `license-provenance.md` — Apache-2.0 dependency and provenance controls.
- `vortex-internals.md` — Vortex-first data model and I/O expectations.
- `encoded-execution.md` — encoded-native execution behavior and guardrails.
- `planner-optimizer.md` — planner/optimizer constraints and invariants.
- `translation-layer.md` — external format translation boundaries.
- `benchmarking.md` — reproducible benchmarking requirements.
- `testing-correctness.md` — correctness-focused testing expectations.
- `object-store-runtime.md` — object store/runtime integration constraints.
- `documentation-rfc.md` — docs and RFC quality bar for non-trivial changes.

## Non-negotiable project constraints

- Keep ShardLoom standalone; do not add Spark or DataFusion.
- Do not add fallback execution to external engines.
- Unsupported plans must fail explicitly with clear errors.
- Treat Vortex as a first-class native input and output target.
- Maintain Apache-2.0 license hygiene and provenance traceability.
