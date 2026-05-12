# RFC 0023: Extension, Plugin ABI, and Sandboxing

## Status

Draft

## Summary

This RFC defines ShardLoom's extension, plugin ABI, and sandboxing design.

ShardLoom should be flexible enough to support UDFs, connectors, output targets, LLM/API effects,
embeddings, vector search, and enterprise-specific integrations. That flexibility must not
compromise correctness, safety, licensing, governance, or no-fallback architecture.

## Context

ShardLoom is designed to support modular use cases:

- SQL frontend.
- DataFrame-like APIs.
- Scalar UDFs.
- Aggregate UDFs.
- Table functions.
- Connectors.
- Vortex adapters.
- Translation sinks.
- LLM calls.
- API calls.
- Embedding generation.
- Vector search.
- Unstructured data extraction.
- Observability exporters.
- Catalog integrations.

Extensions will eventually shape how users integrate external systems, define custom compute
behavior, and compose effectful operations. Without explicit contracts, extension flexibility can
hide fallback execution, introduce safety gaps, or weaken deterministic diagnostics.

## Goals

- Define extension categories.
- Define plugin capability declarations.
- Define plugin lifecycle concepts.
- Define ABI/API stability principles.
- Define sandboxing requirements.
- Define UDF runtime considerations.
- Define connector/plugin permission boundaries.
- Define license/provenance requirements.
- Define agent-readable extension manifests.
- Preserve no-fallback execution.

## Non-goals

- Implement plugin loading.
- Define final ABI.
- Add WASM/Python/plugin dependencies.
- Implement UDF runtime/connectors/effects.
- Add Spark/DataFusion/fallback execution.
- Allow untrusted code execution by default.

## Core principle

Extensions should expand ShardLoom's capability surface without hiding execution, effects,
dependencies, permissions, or fallback behavior.

An extension must declare:

- What it does.
- What it requires.
- What it can access.
- Whether it is deterministic.
- Whether it has effects.
- Whether it needs materialization.
- Whether it can stream.
- Whether it can run encoded.
- Whether it can spill.
- What licenses apply.
- What safety constraints apply.

## Detailed design

### Extension categories

ShardLoom should model extension categories explicitly:

- Frontend.
- Function.
- Scalar UDF.
- Aggregate UDF.
- Table function.
- Encoded kernel.
- Translation sink.
- Connector.
- Catalog provider.
- Object-store provider.
- Effect provider.
- LLM provider.
- Embedding provider.
- Vector index provider.
- Observability exporter.
- Benchmark provider.

Category-specific contracts should remain strict about effects, determinism, and execution
boundaries.

### Extension manifest

Every extension should provide a machine-readable manifest including:

- Extension id.
- Name.
- Version.
- Provider.
- License.
- Homepage/source.
- Category.
- Capabilities.
- Required permissions.
- Effect level.
- Determinism.
- Resource requirements.
- Supported DTypes.
- Supported encodings.
- Supported output targets.
- Materialization requirements.
- Streaming capability.
- Spill capability.
- Security notes.
- Provenance notes.

Manifest parsing should be possible without executing extension code.

### Capability declaration

Capabilities should expose status values from a constrained enum:

- Supported.
- PartiallySupported.
- Planned.
- Disabled.
- RequiresConfiguration.
- RequiresExplicitEnablement.
- Unsupported.

Planned features must not appear as supported.

### Plugin lifecycle

Future lifecycle state modeling should include:

- Discovered.
- Loaded.
- Validated.
- Enabled.
- Disabled.
- Failed.
- Quarantined.
- Deprecated.
- Removed.

Transition rules should be deterministic and diagnosable.

### Sandboxing

Sandbox-aware contracts should cover:

- Filesystem access.
- Network access.
- Environment access.
- Secret access.
- Memory limits.
- CPU limits.
- Execution timeout.
- External effects.
- Credential scope.
- Output validation.
- Dependency isolation.

Untrusted plugins must not run unrestricted.

### UDF runtime options

Future UDF execution modes may include:

- Rust-native UDFs.
- WASM UDFs.
- Python UDFs.
- SQL-defined UDFs.
- External service UDFs.

Tradeoff evaluation should prioritize correctness, deterministic diagnostics, sandbox isolation,
overhead transparency, and no-fallback guarantees.

### ABI/API stability

Stability policy should distinguish:

- Internal APIs.
- Public Rust APIs.
- Plugin APIs.
- CLI APIs.
- Python APIs.
- Machine-readable schemas.

ShardLoom should avoid stabilizing plugin ABI too early.

### License and provenance

Extension compliance should include explicit review of:

- License.
- Source.
- Dependencies.
- Transitive dependencies.
- Copied code risk.
- Generated code provenance.
- NOTICE requirements.
- Apache-2.0 compatibility.

License or provenance ambiguity should block enablement.

### Permission model

Extensions should declare fine-grained permissions, including:

- Read metadata.
- Read data.
- Write output.
- Delete temporary files.
- Access network.
- Access filesystem.
- Access secrets.
- Call LLM.
- Call API.
- Generate embeddings.
- External write.
- Execute native code.

Missing permissions should fail explicitly with deterministic diagnostics.

### Effect model

Effects should be represented explicitly:

- PureDeterministic.
- PureNondeterministic.
- ExternalRead.
- ExternalWrite.
- ModelCall.
- EmbeddingCall.
- VectorSearch.
- Unknown.

Unknown effects should be treated conservatively.

### Extension diagnostics

Extension diagnostics should include:

- Extension id.
- Category.
- Version.
- Capability requested.
- Missing permission.
- Missing configuration.
- License issue.
- Sandbox issue.
- Unsupported DType/encoding.
- Materialization requirement.
- Effect disabled.
- Fallback attempted false.

### Agent safety

Agents should inspect extension manifests safely without executing extension code.

Agent-facing capability discovery should remain deterministic, machine-readable, and explicit about
unsupported behavior.

## Failure behavior

Unsupported or unsafe extension behavior must fail explicitly with clear deterministic diagnostics
and must not invoke Spark, DataFusion, DuckDB, Polars, Velox, or any fallback execution engine.

## Alternatives considered

- No plugin system: rejected long-term.
- Unlimited native plugins: rejected.
- Python UDFs first: deferred.
- WASM first: possible but undecided.
- Hide extensions behind UDFs only: rejected.

## Risks

- Premature ABI promises may lock in poor extension boundaries.
- Weak sandbox defaults could create security and governance risks.
- Capability inflation could mislead users and agents.
- Ambiguous permission/effect models could hide external behavior.
- License/provenance drift could block enterprise adoption.

## Acceptance criteria

- Extension categories and manifest expectations are documented.
- Capability, permission, effect, and lifecycle concepts are explicit.
- Sandboxing and agent-safety principles are clear.
- License/provenance policy is explicit.
- No-fallback behavior is preserved in extension contracts.

## Verification plan

- Review future extension RFCs and designs against this contract.
- Validate that proposed manifests are inspectable without code execution.
- Validate that proposed diagnostics include deterministic extension metadata.
- Validate that extension proposals do not introduce hidden fallback execution.
- Validate that permission/effect declarations are explicit before enablement.

## Open questions

- Should the first production plugin boundary be Rust-native only, WASM-first, or hybrid?
- What minimum sandbox profile should be default for untrusted extensions?
- How should extension lifecycle states map to CLI/API capability reports?
- Which manifest fields should be required vs optional at initial rollout?
- How should extension signing and provenance attestations be phased in?
