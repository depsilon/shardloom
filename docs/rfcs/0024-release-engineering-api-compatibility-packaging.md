# RFC 0024: Release Engineering, API Compatibility, and Packaging

## Status

Draft

## Summary

This RFC defines ShardLoom's release engineering, API compatibility, and packaging design.

ShardLoom is intended to become serious open-source data infrastructure. That requires reliable
releases, clear versioning, API stability rules, packaging discipline, license hygiene, security
practices, reproducible builds, and benchmark accountability.

## Context

ShardLoom may expose:

- Rust crates.
- CLI.
- Python package.
- Future TypeScript/tooling bindings.
- Docker/GHCR images.
- Documentation site.
- Machine-readable schemas.
- Benchmark artifacts.
- Release notes.
- Future extension/plugin APIs.

Release and packaging contracts must preserve ShardLoom's architecture, diagnostics, and no-fallback
guarantees while making behavior changes understandable and auditable for users and integrators.

## Goals

- Define versioning principles.
- Define API compatibility principles.
- Define crate/package release strategy.
- Define CLI compatibility principles.
- Define machine-readable schema compatibility.
- Define packaging strategy.
- Define dependency/license hygiene.
- Define security release principles.
- Define benchmark/release accountability.
- Preserve no-fallback architecture.

## Non-goals

- Publish packages.
- Create releases.
- Add dependencies.
- Define final CI/CD pipeline.
- Implement packaging.
- Add Spark/DataFusion/fallback execution.

## Core principle

ShardLoom releases should be boring, reproducible, inspectable, and safe.

Users should understand:

- What changed.
- What is stable.
- What is experimental.
- What APIs changed.
- What behavior changed.
- What dependencies changed.
- What benchmarks changed.
- Whether no-fallback architecture is preserved.

## Detailed design

### Versioning

ShardLoom should use a cautious 0.x policy while APIs evolve and adopt strict semver behavior once
stability targets are met.

Version surfaces should include:

- Rust crate versions.
- CLI version.
- Python package version.
- Docs version.
- Plan/diagnostic schema version.
- Extension manifest version.
- Benchmark result schema version.

Breaking changes should be documented clearly during both 0.x and post-1.0 phases.

### API stability tiers

Every surfaced API should be classified as:

- Internal.
- Experimental.
- Stable.
- Deprecated.
- Removed.

### Rust crate strategy

Publishing readiness should require:

- Apache-2.0 license metadata.
- README.
- Documentation.
- Tests.
- MSRV policy.
- Dependency license review.
- No forbidden fallback dependencies.
- Release notes.

### CLI compatibility

CLI compatibility should treat these surfaces explicitly:

- Human text output.
- Machine-readable JSON output.
- Exit codes.
- Command names.
- Flags.
- Diagnostic codes.

Machine-readable output should be more stable than human text.

### Machine-readable schema compatibility

Schema policy should cover:

- Diagnostics.
- Capabilities.
- Explain reports.
- Estimate reports.
- Doctor reports.
- Translation reports.
- Benchmark reports.
- Extension manifests.
- Plan IR.

Schemas should eventually include version fields before stability promises are made.

### Python package strategy

Python packaging should be introduced only when the Rust core has meaningful value and should
preserve no-fallback policy and native Vortex input/output.

### Container strategy

Container design should support CLI, benchmarks, and future server mode if introduced, while
ensuring images are versioned, avoid embedded secrets, use minimal bases where practical, include
license notices, avoid unnecessary dependencies, and do not include fallback engines.

### Documentation releases

Docs should consistently mark:

- Stable features.
- Experimental features.
- Planned features.
- Unsupported features.
- No-fallback policy.
- Vortex-native behavior.
- Compatibility output behavior.
- Performance claim caveats.

### Dependency hygiene

Dependency review should check:

- License.
- Necessity.
- Security.
- Transitive dependencies.
- Size/complexity.
- Architecture impact.
- Fallback risk.

Reject or avoid:

- GPL/AGPL/SSPL/BUSL/proprietary copied code.
- Unknown-license code.
- Dependencies that secretly introduce external execution engines.
- Dependencies that conflict with Apache-2.0 goals.
- Heavy dependencies for small utilities.

### Security release principles

Future security release process should include:

- Security contact.
- Vulnerability reporting.
- Embargo handling if needed.
- Patch release process.
- Security advisories.
- Dependency vulnerability response.
- Secret-handling issue response.

### SBOM and supply chain

Future supply-chain work should include:

- SBOM.
- Dependency lockfiles.
- Signed artifacts.
- Checksums.
- Reproducible build notes.
- Provenance attestations.
- CI dependency scanning.

### Benchmark accountability

Performance claims should include:

- Benchmark version.
- Dataset.
- Hardware.
- Engine versions.
- Correctness validation.
- Metrics beyond wall time.
- Limitations.

No broad Spark-displacement claims should be made without workload-specific evidence.

### No-fallback release check

Every release should verify:

- Spark is not an execution dependency.
- DataFusion is not an execution dependency.
- DuckDB/Polars/Velox are not fallback execution dependencies.
- Unsupported paths fail explicitly.
- Capability report shows fallback disabled.
- Docs do not imply fallback execution.

### Release checklist

Release readiness should include checks for:

- Tests, formatting, and clippy.
- Docs quality.
- License metadata and NOTICE expectations.
- Dependency licenses.
- Security scan outputs.
- No forbidden fallback dependencies.
- Version bump consistency.
- Release notes completeness.
- Benchmark claim verification.
- Package build integrity.
- Checksums and signatures if supported.

## Failure behavior

Release-blocking issues should fail explicitly and include deterministic diagnostics where relevant,
including:

- Missing license metadata.
- Incompatible dependency.
- Broken docs/tests.
- Unreviewed performance claim.
- Public schema changed without notice.
- Forbidden fallback dependency introduced.
- Package build failed.
- Security issue unresolved.

## Alternatives considered

- Release early without process: rejected.
- Stabilize APIs immediately: rejected.
- Delay packaging discussions: partially rejected.
- Allow fallback dependencies in optional packages: rejected unless a future RFC defines a
  non-execution interoperability package.

## Risks

- Overly strict early process could slow useful iteration.
- Under-specified schema/version policy could fragment tooling.
- Loose dependency review could reintroduce architecture drift.
- Documentation drift could overstate stability/performance.
- Packaging pressure could predate mature compatibility policy.

## Acceptance criteria

- Versioning and stability tiers are documented.
- Release surfaces and compatibility expectations are explicit.
- Dependency/license/security principles are explicit.
- Benchmark-accountability and no-fallback checks are explicit.
- Release-blocking failure conditions are documented.

## Verification plan

- Audit future release plans against this RFC before any publication.
- Validate that API and schema changes include compatibility notes.
- Validate dependency changes against license and fallback-risk checks.
- Validate performance claims against reproducible benchmark evidence.
- Validate release artifacts and docs for no-fallback consistency.

## Open questions

- When should ShardLoom declare first stable API tiers for CLI and schemas?
- Which release artifacts should be required for first public package publication?
- What minimum reproducibility bar should be required for benchmark artifacts?
- How should compatibility windows be defined for machine-readable schemas?
- What signing/provenance mechanisms should be mandatory at first release?
