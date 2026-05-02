# ShardLoom Agent Instructions

ShardLoom is a standalone encoded-columnar execution engine designed to compute directly over Vortex-native layouts and produce Vortex-native and lakehouse-compatible outputs.

## Hard requirements

- Do not add Apache DataFusion as an execution fallback.
- Do not add Spark as an execution fallback.
- Do not silently delegate unsupported execution plans to other engines.
- Unsupported execution paths should fail explicitly with clear errors.
- Vortex must be treated as a first-class native input and output target.
- Keep the core engine standalone.
- Prefer original implementations over copying from existing engines.
- Do not copy code from GPL, AGPL, SSPL, BUSL, proprietary, or unknown-license sources.
- Use Apache-2.0 compatible dependencies only unless explicitly approved.
- Prioritize correctness before performance claims.
- Every performance claim needs a reproducible benchmark.
- Every non-trivial change should include tests.

## Architecture principles

ShardLoom should avoid work before optimizing work:

1. Answer from metadata when possible.
2. Prune segments using statistics.
3. Compute against encoded data when possible.
4. Decode partially when necessary.
5. Materialize late.
6. Distribute only when single-node execution is insufficient.
7. Avoid shuffle whenever possible.

## Current phase

The project is in early skeleton/setup mode.

Do not overbuild. Prefer small, reviewable pull requests.

# Codex Coding Defaults

## Engineering Standard

Use this for code changes, bug fixes, refactors, tests, reviews, architecture questions, debugging, build failures, frontend work, documentation tied to code, repository analysis, and implementation planning.

- Follow system, developer, user, and project instructions in priority order.
- Prefer repository conventions, local helpers, and existing architecture over generic preferences.
- Ask only blocking questions. If a safe assumption can be stated and tested, proceed.
- Implement when the request implies implementation; plan first only when asked or when risk requires it.
- Keep scope tied to the requested outcome. Avoid unrelated refactors and metadata churn.
- Inspect the repo shape, relevant docs, configs, tests, and existing patterns before editing.
- For nontrivial bugs, trace the real call path, data flow, and failure mode before patching.
- Prefer `rg` and `rg --files` for discovery.
- Preserve user work. Never revert unrelated changes.
- Make small, coherent changes that solve the stated problem.
- Use established abstractions. Add new abstractions only when they remove real complexity or match local patterns.
- Maintain public contracts unless explicitly asked to change them.
- Prefer clear names, explicit errors, simple control flow, and maintainability over cleverness.
- Add comments only where they reduce cognitive load for complex or non-obvious logic.
- Review the final diff for regressions, accidental files, dead code, formatting drift, and security issues.
- Final responses should cover what changed, where it changed, what was verified, and any residual risk.

## Test Verification Standard

Use this when adding, changing, selecting, running, or interpreting tests and verification for code changes.

- Define the behavior or invariant that must be true when done.
- Find the nearest existing test style and follow it before introducing new frameworks or patterns.
- Add or update tests when the change affects behavior, fixes a bug, protects a contract, or covers a realistic regression.
- Prefer fast focused tests first, then broader suites when the touched code is shared, cross-cutting, security-sensitive, or release-critical.
- Include static checks when relevant: typecheck, lint, formatting, schema validation, generated-code checks, or dependency audits.
- For UI, verify the affected interaction/state and responsive layout when feasible.
- Rerun the original failing command for bug fixes.
- Test observable behavior, not implementation details, unless the unit boundary is intentionally internal.
- Cover important edge cases: empty/null values, invalid inputs, permissions, time, ordering, retries, and compatibility.
- Avoid brittle waits, snapshots without intent, excessive mocking, and tests that only assert implementation calls.
- Always report exact commands run and their result. If a relevant check was skipped, state why.
