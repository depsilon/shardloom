# RFC 0012: Diagnostics, Explain, Estimate, Doctor, and Capability Discovery

## Status

Draft

## Summary

This RFC defines ShardLoom's structured diagnostics, explain, estimate, doctor, and capability discovery contract.

ShardLoom should be highly performant internally, but it must also be easy for humans and LLM agents to understand. Unsupported behavior must be explicit, deterministic, and actionable. Execution behavior should be inspectable before and after running work.

This RFC defines the design for:

- Diagnostic records.
- Diagnostic categories.
- Error codes.
- Explain reports.
- Estimate reports.
- Doctor reports.
- Capability discovery.
- Machine-readable output.
- Agent-friendly integration.

## Context

ShardLoom's architecture includes complex internal concepts:

- Vortex-native input and output.
- Encoded segments.
- Segment statistics.
- Metadata-only execution.
- Segment pruning.
- Encoded evaluation.
- Partial decode.
- Late materialization.
- Translation reports.
- Snapshot manifests.
- Object-store planning.
- Future distributed segment tasks.
- Optional modular extensions such as SQL, UDFs, LLM calls, API calls, embeddings, and vector search.

Users should not need to understand all of this to use ShardLoom.

However, when something is unsupported, expensive, lossy, or effectful, ShardLoom must explain what happened.

This is especially important for LLM agents. Agents need stable, machine-readable outputs that let them decide what to do next without guessing.

## Goals

- Define structured diagnostics.
- Define diagnostic categories and severity.
- Define deterministic error codes.
- Define explain reports.
- Define estimate reports.
- Define doctor reports.
- Define capability discovery.
- Define machine-readable output requirements.
- Make unsupported behavior actionable.
- Make fallback status explicit.
- Make Vortex-native input/output status explicit.
- Support human and LLM agent usage.
- Preserve ShardLoom's no-fallback architecture.

## Non-goals

- Do not implement Rust code in this RFC.
- Do not define final JSON schemas in full detail.
- Do not implement CLI commands in this RFC.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not define a complete observability platform.
- Do not define final benchmark reporting schemas.
- Do not make diagnostics a substitute for tests.

## Design principles

ShardLoom diagnostics should follow these principles:

1. Deterministic errors are better than vague errors.
2. Explicit unsupported behavior is better than hidden fallback.
3. Machine-readable output is required for agents.
4. Human-readable output is required for users.
5. Explain and estimate should be available before execution where possible.
6. Diagnostics should distinguish execution, translation, effects, and configuration.
7. Every unsupported path should suggest a useful next step.
8. Performance behavior should be inspectable.
9. Metadata loss should be explicit.
10. Fallback execution should normally be false and visible.

## Core concepts

### Diagnostic

A Diagnostic is a structured message describing an error, warning, information item, or planning note.

A diagnostic should include:

- Code.
- Severity.
- Category.
- Message.
- Feature.
- Reason.
- Suggested next step.
- Fallback attempted.
- Native Vortex input available.
- Native Vortex output available.
- Related output target if applicable.
- Related plan node if applicable.
- Related dataset or file if applicable.
- Related extension or effect if applicable.
- Machine-readable metadata.

### DiagnosticCode

A stable code that identifies a diagnostic.

Example codes:

- `SL_UNSUPPORTED_ENCODING`
- `SL_UNSUPPORTED_DTYPE`
- `SL_UNSUPPORTED_SQL`
- `SL_UNSUPPORTED_UDF`
- `SL_UNSUPPORTED_EFFECT`
- `SL_UNSUPPORTED_OUTPUT_FORMAT`
- `SL_MISSING_STATISTICS`
- `SL_PRUNING_INCONCLUSIVE`
- `SL_METADATA_LOSS`
- `SL_MATERIALIZATION_REQUIRED`
- `SL_EXTERNAL_EFFECT_DISABLED`
- `SL_LLM_CALL_DISABLED`
- `SL_API_CALL_DISABLED`
- `SL_EMBEDDING_MODEL_UNCONFIGURED`
- `SL_VECTOR_INDEX_UNAVAILABLE`
- `SL_OBJECT_STORE_UNSUPPORTED`
- `SL_COMMIT_NOT_ATOMIC`
- `SL_RESOURCE_BUDGET_EXCEEDED`
- `SL_NO_FALLBACK_EXECUTION`

Codes should be stable enough for LLM agents and automation to consume.

### DiagnosticSeverity

Suggested severity levels:

- `info`
- `warning`
- `error`
- `fatal`

Severity should not replace category. For example, unsupported behavior is a category and may be an error or fatal depending on context.

### DiagnosticCategory

Suggested categories:

- `unsupported_feature`
- `invalid_input`
- `configuration`
- `planning`
- `execution`
- `vortex_io`
- `statistics`
- `pruning`
- `materialization`
- `translation`
- `metadata_loss`
- `object_store`
- `resource_budget`
- `external_effect`
- `model_call`
- `api_call`
- `embedding`
- `vector_search`
- `security`
- `license_provenance`
- `no_fallback_policy`

### SuggestedNextStep

Diagnostics should include a suggested next step when possible.

Examples:

- Use a supported predicate.
- Enable explicit partial decode.
- Select Vortex output for full fidelity.
- Use a compatibility output and accept metadata loss.
- Add required statistics.
- Configure an embedding model.
- Enable dry-run safe API calls.
- Reduce query scope.
- Wait for native support.
- File an issue with the unsupported feature.

### FallbackStatus

Diagnostics should explicitly report fallback behavior.

Suggested fields:

- `fallback_attempted`
- `fallback_engine`
- `fallback_allowed`
- `reason_fallback_not_used`

For ShardLoom core execution, fallback should normally be:

```json
{
  "fallback_attempted": false,
  "fallback_engine": null,
  "fallback_allowed": false,
  "reason_fallback_not_used": "ShardLoom prohibits Spark, DataFusion, DuckDB, Polars, Velox, and other fallback execution engines."
}
```

## Explain report

An ExplainReport describes how ShardLoom plans to execute work.

Explain should be available before execution where possible.

An ExplainReport should include:

- Query or operation summary.
- Input datasets.
- Snapshot ids if available.
- Manifest ids if available.
- Output target.
- Native Vortex input status.
- Native Vortex output status.
- Plan nodes.
- Execution boundaries.
- Metadata-only decisions.
- Segment pruning decisions.
- Encoded operations.
- Partial decode operations.
- Full materialization boundaries.
- Translation boundaries.
- External effects.
- Unsupported features.
- Diagnostics.
- Estimated work if available.

## Plan node diagnostics

Each plan node should eventually be able to report:

- Node id.
- Node type.
- Input schema.
- Output schema.
- Required columns.
- Predicate.
- Execution state.
- Materialization state.
- Estimated rows.
- Estimated bytes read.
- Estimated bytes decoded.
- Diagnostics.
- Unsupported features.

## Execution states

Explain and diagnostics should use consistent execution-state labels.

Suggested states:

- `metadata_only`
- `pruned`
- `encoded_evaluation`
- `partial_decode`
- `full_materialization`
- `translation`
- `external_read`
- `external_write`
- `model_call`
- `unsupported`

## Estimate report

An EstimateReport describes expected work before execution.

An estimate should include:

- Estimated bytes read.
- Estimated bytes decoded.
- Estimated rows scanned.
- Estimated rows materialized.
- Estimated segments considered.
- Estimated segments pruned.
- Estimated output size.
- Estimated object-store requests.
- Estimated memory.
- Estimated CPU work.
- Estimated model/API calls if applicable.
- Estimated cost proxy if available.
- Known uncertainty.
- Missing statistics.
- Unsupported estimate components.

Estimates should be honest about uncertainty.

If an estimate is incomplete, the report should state why.

## Doctor report

A DoctorReport checks ShardLoom environment and project health.

Potential doctor checks:

- CLI availability.
- Version information.
- Vortex support status.
- Native Vortex output support status.
- Config file validity.
- Object-store configuration.
- Credentials presence without leaking secrets.
- Output path writeability.
- Required feature availability.
- Extension capability status.
- No-fallback policy status.
- Dependency/license status if available.
- Benchmark setup status if relevant.

Doctor checks should be safe and should not execute queries or external effects unless explicitly requested.

## Capability discovery

Capability discovery should let humans and agents ask what ShardLoom can do.

A CapabilityReport should include:

- Engine version.
- Native input formats.
- Native output formats.
- Compatibility output formats.
- Supported execution features.
- Planned execution features.
- Unsupported features.
- SQL frontend status.
- UDF status.
- Vortex feature support.
- Translation support.
- Object-store support.
- External effect support.
- LLM/model-call support.
- API-call support.
- Embedding support.
- Vector search support.
- Fallback execution status.
- Safety defaults.

## Capability status

Suggested capability status values:

- `supported`
- `partially_supported`
- `planned`
- `disabled`
- `requires_explicit_enablement`
- `requires_configuration`
- `unsupported`

These values should be stable enough for agents to consume.

## Example capability report

```json
{
  "engine": "shardloom",
  "version": "0.1.0",
  "fallback_execution": {
    "allowed": false,
    "engines": []
  },
  "native_inputs": {
    "vortex": "planned"
  },
  "native_outputs": {
    "vortex": "planned"
  },
  "compatibility_outputs": {
    "arrow_ipc": "planned",
    "parquet": "planned",
    "iceberg_compatible": "planned",
    "delta_compatible": "planned"
  },
  "frontends": {
    "sql": "planned",
    "dataframe_api": "planned",
    "cli": "partially_supported"
  },
  "extensions": {
    "udfs": "planned",
    "llm_calls": "planned",
    "api_calls": "planned",
    "embeddings": "planned",
    "vector_search": "planned"
  }
}
```

## Machine-readable output

CLI and API surfaces should eventually support machine-readable output.

Recommended formats:

- Text for humans.
- JSON for agents and automation.
- YAML only if later approved.

JSON fields should be stable and documented.

Text output may evolve more freely than JSON output.

## Agent behavior requirements

Agent-facing commands should allow LLM agents to:

- Discover capabilities.
- Check whether a query is supported.
- Explain planned execution.
- Estimate work.
- Detect unsupported features.
- Detect metadata loss.
- Detect effectful operations.
- Detect whether writes are safe.
- Avoid destructive operations.
- Avoid hidden external calls.
- Avoid fallback assumptions.

Agents should not need to scrape vague text to make decisions.

## Interaction with effectful operations

Explain, estimate, capabilities, and doctor must not accidentally execute:

- LLM calls.
- API calls.
- External writes.
- Embedding generation.
- Vector index mutations.
- Destructive file operations.

Effectful operations should be represented as planned or disabled, not executed, unless execution is explicitly requested.

## Interaction with translation

Translation diagnostics should report:

- Source representation.
- Target format.
- Metadata preserved.
- Metadata degraded.
- Metadata lost.
- Materialization required.
- Fidelity level.
- Native Vortex output availability.

Compatibility exports must not be confused with Vortex-native output.

## Interaction with benchmarks

Benchmark reports should eventually use the same diagnostic vocabulary where possible.

Benchmark outputs should include:

- Segments considered.
- Segments pruned.
- Bytes read.
- Bytes decoded.
- Rows materialized.
- Execution states.
- Unsupported features.
- Baseline comparison metadata.

## Failure behavior

When ShardLoom cannot execute a query or operation, it must fail explicitly.

Failure diagnostics should include:

- What failed.
- Why it failed.
- Whether fallback was attempted.
- Why fallback was not used.
- What the user or agent can do next.

Failure must not silently call Spark, DataFusion, DuckDB, Polars, Velox, or another engine.

## Alternatives considered

### Plain string errors only

Rejected.

Plain string errors are hard for agents and automation to consume.

### Machine-readable output only

Rejected.

Human-readable diagnostics are still required.

### Add fallback execution for better UX

Rejected.

Better UX must come from clear diagnostics, capability discovery, explain, estimate, and safe extension points, not hidden execution delegation.

### Delay diagnostics until after engine implementation

Rejected.

Diagnostics should shape implementation early.

## Risks

- Diagnostics may become too verbose.
- Stable JSON fields may constrain future changes.
- Explain output may expose too much internal complexity.
- Estimate output may be misunderstood as exact.
- Capability discovery may drift from implementation reality.
- Agents may over-rely on planned capabilities if statuses are unclear.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Structured diagnostics are required.
- Diagnostic codes should be stable.
- Explain reports should expose execution boundaries.
- Estimate reports should be honest about uncertainty.
- Doctor reports should check environment and project health safely.
- Capability discovery is required for agent usability.
- Fallback status must be explicit.
- Effectful operations must not run during explain, estimate, capabilities, or doctor.
- No fallback execution is permitted.

## Verification plan

Future implementation PRs should verify:

- Diagnostics can be represented structurally.
- Unsupported behavior produces deterministic codes.
- Fallback status is explicit and false by default.
- Capability metadata can be represented.
- Explain output can represent plan boundaries.
- Estimate output can represent uncertainty.
- Doctor checks do not trigger side effects.
- JSON output is stable enough for agents.
- No Spark or DataFusion dependency is introduced.

## Open questions

- What should the exact JSON schema be?
- Should diagnostic codes be generated from constants?
- Should explain, estimate, and capabilities share common metadata structures?
- How should diagnostics be versioned?
- How much internal plan detail should be public?
- Should JSON output be schema-versioned from the beginning?


### Future report schemas

The following report schemas are RFC-level contract targets. They define required semantics and acceptance behavior, but they do **not** yet define final JSON schema bindings.

#### PushdownProofReport

Required fields:
- `operation`
- `accepted`
- `guarantee` with enum values: `exact`, `exact_with_residual`, `conservative_may_include_false_positives`, `unsupported`
- `proof_basis`
- `residual_expression`
- `metadata_loss`
- `requires_decode`
- `fallback_attempted=false`
- `diagnostics`

Acceptance criteria:
- Unsupported pushdown must fail or continue with explicit ShardLoom-native residual behavior and must **not** become fallback execution.
- Residual work must be explicit and machine-readable.
- Exactness/guarantee class must be machine-readable.

#### LoweringTraceReport

Required fields:
- `from_layer`
- `to_layer`
- `source_node`
- `produced_nodes`
- `lowering_rule`
- `reason`
- `lost_information`
- `preserved_guarantees`
- `diagnostics`

#### TaskGranularityReport

Required fields:
- `min_encoded_bytes_per_task`
- `target_encoded_bytes_per_task`
- `max_encoded_bytes_per_task`
- `segment_count_limits`
- `coalesced_tasks`
- `split_tasks`
- `refused_distributed_execution_reason`
- `diagnostics`

#### PlannedVsActualOperatorProfile

Required fields:
- `node_id`
- `operator`
- `planned_rows`
- `actual_rows`
- `planned_encoded_bytes`
- `actual_encoded_bytes`
- `decoded_bytes`
- `materialized_rows`
- `wall_time_ms`
- `memory_peak_bytes`
- `spill_bytes`
- `avoided_bytes`
- `diagnostics`

#### RuntimeFilterReport

Required fields:
- `lifecycle_state`
- `source_node`
- `target_node_or_segment`
- `filter_kind`
- `correctness_guarantee`
- `false_positive_policy`
- `estimated_selectivity`
- `actual_work_avoided`
- `diagnostics`

Lifecycle states:
- `planned`
- `built`
- `published`
- `applied`
- `rejected`
- `expired`

#### PortabilityReport

Required fields:
- `native_only_nodes`
- `portable_nodes`
- `lossy_nodes`
- `unsupported_nodes`
- `metadata_loss`
- `fallback_attempted=false`
- `diagnostics`

#### SystemIntrospectionReport

Required fields:
- `virtual_dataset_name`
- `capability_scope`
- `query_safe`
- `side_effect_free`
- `schema_version`
- `diagnostics`

## Systems-learning conceptual report vocabulary (R5.1)

This pass adds conceptual report-schema vocabulary only.
No implementation is added.
No execution behavior is added.
No external engines are added.

Conceptual report names:
- `PushdownProofReport`
- `LoweringTraceReport`
- `TaskGranularityReport`
- `RuntimeFilterReport`
- `PlannedVsActualOperatorProfile`
- `PlanPortabilityReport`

