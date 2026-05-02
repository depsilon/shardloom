# Skill: Testing & Correctness

## Purpose
Maintain correctness-first behavior across planner, runtime, and I/O changes.

## When to use
Use for any behavioral change, bug fix, invariant update, or regression defense.

## Rules
- Add/update targeted tests for every non-trivial behavior change.
- Test observable semantics: results, errors, nulls, ordering, and boundaries.
- Include failure-path tests for unsupported plans/features.
- Keep tests deterministic and fast where practical.
- Run workspace formatting, lint, and test gates before review.

## Validation checklist
- [ ] New/changed behavior is covered by focused tests.
- [ ] Unsupported paths have explicit failure assertions.
- [ ] Required checks (`fmt`, `clippy -D warnings`, `test`) pass.
- [ ] Test names describe scenario and expected behavior.
