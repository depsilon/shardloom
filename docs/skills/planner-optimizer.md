# Skill: Planner & Optimizer

## Purpose
Guide plan construction and optimization without violating correctness or standalone constraints.

## When to use
Use for logical/physical planning rules, pruning logic, and plan validation.

## Rules
- Apply safe rewrites only; correctness outranks performance.
- Prefer metadata- and stats-driven pruning before physical work.
- Unsupported plan shapes must fail explicitly with actionable errors.
- Do not inject Spark/DataFusion or other external engine fallbacks.
- Keep rule ordering deterministic and explainable.

## Validation checklist
- [ ] Rewrites preserve semantics (filters, nulls, ordering, limits).
- [ ] Planner failures are explicit and user-actionable.
- [ ] Rule interactions are covered by focused tests.
- [ ] No hidden delegation to non-ShardLoom engines.
