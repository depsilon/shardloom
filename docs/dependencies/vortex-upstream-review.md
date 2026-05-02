# Vortex Upstream Dependency Review

## Purpose

ShardLoom plans to integrate upstream Vortex in a future change, but the dependency must be reviewed before it is added. This document defines readiness checks so integration remains Apache-2.0-compatible, Vortex-native, and no-fallback.

## Current status

- Upstream Vortex dependency is not yet added.
- `shardloom-vortex` currently models adapter contracts only.
- No real Vortex file IO is implemented yet.
- No external engine fallback is allowed.

## Dependency candidates

Record candidate details at implementation time (do not guess values):

- Crate name: `<to be verified>`
- Version: `<to be verified>`
- Repository: `<to be verified>`
- License: `<to be verified>`
- Documentation URL: `<to be verified>`
- Public APIs needed: `<to be verified>`
- Internal APIs avoided: `<to be verified>`
- Feature flags needed: `<to be verified>`
- Transitive dependency concerns: `<to be verified>`
- Security concerns: `<to be verified>`
- Release cadence concerns: `<to be verified>`

## License/provenance checklist

- Confirm upstream license.
- Confirm license compatibility with Apache-2.0.
- Confirm no GPL/AGPL/SSPL/BUSL/proprietary dependency issue.
- Confirm NOTICE requirements.
- Confirm copied-code risk is avoided.
- Confirm no vendored code.
- Confirm generated code provenance if any.
- Confirm dependency is needed.
- Confirm no fallback execution dependency is introduced.

## API compatibility checklist

- Prefer public APIs.
- Avoid unstable internals.
- Keep Vortex-specific details isolated in `shardloom-vortex`.
- Add adapter tests.
- Add version assumptions.
- Document unsupported upstream features.
- Fail explicitly for unsupported features.

## Initial APIs ShardLoom likely needs

- Open/inspect Vortex file metadata.
- Inspect logical DTypes.
- Inspect arrays/encodings/layouts.
- Read segment-level metadata/statistics.
- Map Vortex DTypes into ShardLoom `LogicalDType`.
- Map Vortex encodings/layouts into ShardLoom `EncodingKind`/`LayoutKind`.
- Plan metadata-only reads.
- Plan native Vortex output.
- Eventually write Vortex output.

## Do not do yet

- Do not add upstream Vortex dependency before review.
- Do not implement real IO before adapter boundaries are tested.
- Do not convert all Vortex data into Arrow as the default execution model.
- Do not add DataFusion/Spark/DuckDB/Polars/Velox as helpers.
- Do not copy upstream implementation code.

## Approval gate

Adding upstream Vortex dependency requires a future PR that:

- Updates `Cargo.toml`.
- Records license review.
- Adds minimal adapter tests.
- Adds no fallback execution dependencies.
- Keeps actual IO minimal and side-effect safe.
