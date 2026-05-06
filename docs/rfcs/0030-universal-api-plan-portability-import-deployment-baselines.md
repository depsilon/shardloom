# RFC 0030 — Universal API, Plan Portability, Import/Deployment, and External Baselines

## Scope

This RFC defines implementation contracts for:
- CG-11 Python/API surface later.
- CG-12 plan portability / semantic IR.
- CG-18 universal import/deployment/baseline harness.

## Universal API posture

- Thin Python wrapper over CLI JSON first.
- Stable command schema.
- No PyO3/maturin unless explicitly approved.
- No Spark fallback.

## Plan portability contract

- ShardLoom plan export contract.
- Optional Substrait-like export/import validation.
- Residual unsupported plan reporting.
- No external engine execution in runtime paths.

## Universal runner/deployment contract

- Universal CLI JSON runner contract.
- Package/import guidance independent of Foundry.
- Foundry appears only as optional transform/deployment examples under CG-18.
- Foundry is not the primary engine target.

## External baseline harness

- Spark baseline runner, external only.
- Polars baseline runner, external only.
- DataFusion baseline runner, external only.
- Stable comparison report dataset.
- No runtime fallback.

## Non-goals

- No fallback/delegation to external engines.
- No mandatory Foundry dependency.


### Additional CG-18 reporting direction

- Foundry remains an optional example under universal import/deployment, not the primary engine target.
- Add an external baseline report dataset concept for stable, machine-readable cross-engine comparisons.
