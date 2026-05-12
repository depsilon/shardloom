# Correctness, Semantics, Differential Testing, and Fuzzing Skill

## Purpose

Use this skill when designing or implementing tests, kernels, pruning rules, metadata-only answers,
output translation, diagnostics, SQL semantics, UDF semantics, or reference comparisons.

ShardLoom must be correct before it is fast.

## When to use

Use this skill for tasks involving:

- Unit tests.
- Integration tests.
- Fixture tests.
- SQLLogicTest-style tests.
- Differential testing.
- Fuzzing.
- Property tests.
- Encoded kernels.
- Decoded reference behavior.
- Null semantics.
- Type semantics.
- Output translation correctness.
- Unsupported diagnostics.
- Benchmark correctness validation.

## Rules

- Fast wrong answers are unacceptable.
- Incorrect pruning is a correctness bug.
- Missing statistics must not be treated as proof.
- Encoded kernels should be compared against decoded reference behavior.
- Decoded reference behavior is allowed in tests, not hidden production fallback.
- External engines may be used as test oracles, not fallback execution.
- Null semantics must be tested explicitly.
- Empty and all-null inputs must be tested.
- Unsupported behavior must be tested.
- Diagnostics should have stable machine-readable fields.
- Benchmarks must validate correctness before reporting performance.

## Required checks

For kernel work:

- Are nulls tested?
- Are empty inputs tested?
- Are all-null inputs tested?
- Is decoded reference behavior tested?
- Are unsupported DTypes/encodings tested?
- Are selection vectors tested?
- Is materialization boundary tested?

For pruning work:

- Is pruning conservative?
- Are missing stats tested?
- Are approximate stats handled safely?
- Are nulls handled?
- Is AlwaysFalse the only prunable proof unless explicitly proven otherwise?

For differential tests:

- Is the external engine used only as oracle/comparison?
- Is engine version recorded?
- Are semantic differences documented?
- Is fallback execution avoided?

For diagnostics:

- Is code stable?
- Is category stable?
- Is fallback attempted false?
- Is suggested next step present when possible?

## Red flags

- Performance claim without correctness validation.
- Pruning based on incomplete proof.
- Ignoring nulls.
- Treating external engine output as production fallback.
- Letting unsupported behavior pass silently.
- Fuzz failure without reproducible seed.
- Golden tests that over-specify unstable human wording.
- Tests that only cover happy paths.

## Example Codex prompt fragment

"Use the Correctness Testing skill. Add edge-case tests for nulls, empty inputs, all-null inputs,
unsupported behavior, decoded reference comparison where appropriate, and deterministic diagnostics.
External engines may be test oracles only, not fallback execution."
