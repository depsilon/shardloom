# Testing and Correctness Skill

## Purpose

Use this skill when adding tests, changing behavior, implementing operators, changing errors, or validating encoded execution against decoded reference behavior.

The goal is to make ShardLoom correct before it is fast.

## When to use

Use this skill for tasks involving:

- Unit tests.
- Integration tests.
- Property tests.
- Fuzzing.
- Reference result checks.
- Operator behavior.
- Null semantics.
- Type behavior.
- Unsupported-path diagnostics.
- Deterministic errors.
- Edge cases.

## Rules

- Correctness comes before performance.
- Unsupported behavior must fail explicitly and deterministically.
- Tests should cover both success and failure paths.
- Encoded execution should be compared against decoded reference behavior where possible.
- Reference behavior is for testing correctness, not production fallback execution.
- Null semantics must be tested directly.
- Empty inputs and all-null inputs must be tested.
- Ordering requirements must be tested when order matters.
- Error messages should be stable enough to diagnose unsupported features.
- Avoid flaky tests.
- Avoid tests that depend on external services unless marked and isolated.

## Required checks

For behavior changes, consider tests for:

- Empty inputs.
- Single-row inputs.
- All-null inputs.
- Mixed-null inputs.
- Low-cardinality data.
- High-cardinality data.
- Duplicate values.
- Sorted and unsorted inputs.
- Invalid schemas.
- Unsupported encodings.
- Unsupported plan shapes.
- Deterministic error messages.
- Precision-sensitive types such as decimals or timestamps when relevant.
- UTF-8 and string edge cases when relevant.

For operator changes:

- Test encoded result.
- Test decoded reference result.
- Test selection vector behavior.
- Test materialization boundary.
- Test null behavior.
- Test unsupported diagnostics.

## Red flags

- Only testing the happy path.
- Skipping nulls.
- Skipping empty input.
- Relying on performance benchmarks as correctness tests.
- Using another execution engine as hidden production fallback.
- Non-deterministic errors.
- Tests that pass only because unsupported behavior is ignored.

## Example Codex prompt fragment

When adding or changing behavior, include this instruction:

"Use the Testing and Correctness skill. Add success and failure tests, including empty inputs, nulls, unsupported diagnostics, and decoded reference checks where appropriate. Correctness comes before performance."
