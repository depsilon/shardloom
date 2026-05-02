# Skill: Documentation & RFC

## Purpose
Keep architecture and behavior changes reviewable with concise, decision-oriented documentation.

## When to use
Use when introducing non-trivial design, invariants, interfaces, or behavior changes.

## Rules
- Document problem, constraints, decision, alternatives, and risks.
- Explicitly state impacts on Vortex-first I/O and standalone architecture.
- Record unsupported paths and explicit failure behavior.
- Include rollout/testing/benchmark plan when claiming performance changes.
- Keep docs concise and update related RFC/docs alongside code changes.

## Validation checklist
- [ ] Decision rationale and rejected alternatives are captured.
- [ ] Constraints (no Spark/DataFusion/fallback) are reaffirmed.
- [ ] Verification plan (tests/benchmarks) is concrete.
- [ ] Linked docs remain consistent after the change.
