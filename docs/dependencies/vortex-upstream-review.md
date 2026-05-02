# Vortex Upstream Dependency Review

## Purpose

This document records ShardLoom's first upstream Vortex dependency review for dependency-verification mode only. Integration remains isolated in `shardloom-vortex`, with no execution fallback and no real Vortex file IO in this PR.

## Current status

- Upstream Vortex dependency has been added to `shardloom-vortex`.
- Scope in this PR is compile/readiness only.
- Real Vortex file IO is not implemented.
- Fallback execution remains disabled.

## Dependency review

- Crate name: `vortex`
- Version requested: `0.70`
- Repository: upstream Vortex repository
- License: Apache-2.0
- Purpose: native Vortex format/toolkit integration inside `shardloom-vortex`
- Current scope: dependency compile/readiness only
- Public APIs used in this PR: none (compile marker only)
- Internal APIs used: none
- Actual IO implemented: no
- Fallback engines introduced: no
- Copied upstream code: no
- Vendored upstream code: no

## License/provenance checklist

- Upstream license identified as Apache-2.0.
- License is compatible with Apache-2.0 project policy.
- No GPL/AGPL/SSPL/BUSL/proprietary code introduced.
- No copied upstream implementation code.
- No vendored upstream code.
- Dependency usage is isolated to `shardloom-vortex`.
- No fallback execution dependency was directly added.

## Dependency addition status

- Upstream Vortex dependency has been added to `shardloom-vortex`.
- This PR does not implement actual Vortex IO.
- This PR does not add fallback execution.
- This PR does not add DataFusion/Spark/DuckDB/Polars/Velox.

## Follow-up required

- Identify minimal metadata inspection API.
- Identify DType mapping API.
- Identify encoding/layout mapping API.
- Add adapter tests.
- Add unsupported diagnostics.
- Avoid decode-to-Arrow default path.
