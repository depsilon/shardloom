# Skill: Encoded Execution

## Purpose
Enforce encoded-native execution so the engine avoids unnecessary decoding/materialization.

## When to use
Use when implementing operators, kernels, expression evaluation, or pipeline stages.

## Rules
- Follow architecture order: metadata -> pruning -> encoded compute -> partial decode -> late materialization.
- Keep unsupported encoded paths explicit; do not silently reroute elsewhere.
- Track nullability, ordering, and type semantics across encoded operations.
- Minimize row-wise fallbacks and full-buffer materialization.
- Ensure observable results match decoded-reference semantics.

## Validation checklist
- [ ] Operator behavior is correct on encoded and edge-case inputs.
- [ ] Any decode boundary is intentional and justified.
- [ ] No hidden external-engine fallback exists.
- [ ] Planner/runtime invariants remain consistent.
