# Planner and Optimizer Skill

## Purpose

Use this skill when designing or implementing logical plans, physical plans, optimization rules,
diagnostics, or cost decisions.

The goal is to create a standalone ShardLoom planner that produces encoded-columnar physical plans
without relying on Spark or DataFusion fallback execution.

## When to use

Use this skill for tasks involving:

- Logical plan types.
- Physical plan types.
- Expression modeling.
- Optimizer rules.
- Cost estimation.
- Predicate pushdown.
- Projection pushdown.
- Segment pruning.
- Join strategy.
- Aggregate strategy.
- Plan diagnostics.
- Explain output.

## Rules

- ShardLoom must own its physical execution model.
- Do not add DataFusion or Spark as planning or execution fallback.
- Third-party libraries may be considered for parsing only if approved, but parsing must not imply
  execution delegation.
- Optimizer rewrites must preserve semantics.
- Optimizer rules must be testable in isolation.
- Plan nodes should make materialization boundaries explicit.
- Unsupported plan shapes must fail with clear diagnostics.
- The planner should prefer plans that reduce reads, decode, movement, memory pressure, and shuffle.
- The planner should distinguish native execution from translation/export.
- Cost decisions should be explainable, even if initially heuristic.

## Required checks

For planner changes:

- Add tests for logical-to-physical conversion.
- Add tests for unsupported plan diagnostics.
- Add tests for projection and predicate behavior where relevant.
- Confirm no fallback engine dependency was introduced.
- Confirm Vortex-native input/output is not weakened.
- Confirm plan explain output or diagnostics are understandable.

For optimizer rules:

- Test before and after plans.
- Test semantic equivalence with expected results or reference behavior.
- Test rule non-application when preconditions are not met.
- Test interaction with null semantics if expressions are involved.

## Red flags

- "Use DataFusion for now and replace it later."
- "Use Spark for distributed plans temporarily."
- Optimizer rules that change results.
- Optimizer rules that hide unsupported behavior.
- Plan nodes that do not expose when decoding or materialization occurs.
- Treating translation to another format as execution.

## Example Codex prompt fragment

When working on planning or optimization, include this instruction:

"Use the Planner and Optimizer skill. Produce standalone ShardLoom plan types. Rewrites must
preserve semantics. Unsupported plans must fail explicitly. Do not add Spark, DataFusion, or
fallback execution."
