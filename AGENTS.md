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
