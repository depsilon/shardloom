# RFC 0018: Observability, Tracing, Profiling, and Runtime Introspection

## Status

Draft

## Summary

This RFC defines ShardLoom's observability, tracing, profiling, and runtime introspection design.

ShardLoom should be easy to inspect when it is fast, slow, memory-bound, IO-bound, spill-bound, effect-bound, or unsupported. Users and agents should be able to understand what the engine planned, what it actually did, what it avoided, and where time and resources went.

Observability is not an afterthought. It is part of making ShardLoom trustworthy, debuggable, benchmarkable, and usable at scale.

## Context

ShardLoom's execution model includes:

- Vortex-native input and output.
- Metadata-only execution.
- Segment pruning.
- Encoded execution.
- Partial decode.
- Late materialization.
- Streaming.
- Adaptive sizing.
- Memory budgets.
- Spill.
- Object-store reads.
- Translation outputs.
- Fault tolerance.
- Future distributed tasks.
- Future UDFs, API calls, LLM calls, embeddings, and vector search.

Without strong observability, these optimizations become invisible magic. Users need to understand why a workload was cheap, expensive, unsupported, or memory-bound.

LLM agents also need structured observability so they can diagnose integration failures and propose safe next steps.

## Goals

- Define runtime observability concepts.
- Define metrics categories.
- Define trace/span concepts.
- Define structured event concepts.
- Define operator-level profiling.
- Define object-store and Vortex IO metrics.
- Define memory/spill metrics.
- Define translation and metadata-loss metrics.
- Define effectful operation metrics.
- Define benchmark integration.
- Define privacy and redaction requirements.
- Preserve no-fallback architecture.

## Non-goals

- Do not implement tracing in this RFC.
- Do not add OpenTelemetry, tracing, metrics, or profiling dependencies in this RFC.
- Do not define final metrics schema.
- Do not define a full observability backend.
- Do not implement distributed tracing.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not expose sensitive values in logs or traces.

## Core principle

ShardLoom should make execution behavior inspectable without leaking sensitive data.

Observability should answer:

- What did ShardLoom plan?
- What did it execute?
- What work did it avoid?
- What data did it read?
- What data did it decode?
- What data did it materialize?
- What did it prune?
- What did it spill?
- What did it write?
- What was unsupported?
- Was fallback execution attempted? This should normally be false.
- Where did time and resources go?

## Observability surfaces

ShardLoom should eventually expose observability through:

- Human-readable CLI output.
- Machine-readable JSON output.
- Explain reports.
- Estimate reports.
- Doctor reports.
- Benchmark reports.
- Runtime summaries.
- Operator profiles.
- Structured events.
- Future trace spans.
- Future metrics exporters.
- Future profiling artifacts.

## Metrics categories

ShardLoom should eventually collect metrics in these categories.

### Planning metrics

- Planning duration.
- Optimization duration.
- Number of plan nodes.
- Number of optimizer rules applied.
- Number of optimizer rules skipped.
- Unsupported features detected.
- Runtime filters planned.
- Materialization boundaries planned.
- Translation boundaries planned.

### Vortex and scan metrics

- Files considered.
- Segments considered.
- Segments pruned.
- Segments metadata-answered.
- Segments read.
- Byte ranges planned.
- Byte ranges read.
- Bytes read.
- Bytes decoded.
- Rows scanned.
- Rows materialized.
- Columns projected.
- Metadata reads.
- Encoded operations.

### Object-store metrics

- Object-store requests.
- Metadata requests.
- Range requests.
- Full-object reads.
- Retries.
- Request failures.
- Request latency.
- Bytes downloaded.
- Bytes uploaded.
- Cold-cache vs warm-cache status if known.

### Execution metrics

- Operator duration.
- Rows in.
- Rows out.
- Batches/chunks in.
- Batches/chunks out.
- Execution state.
- Streaming stages.
- Materialization events.
- Shuffle events.
- Task attempts.
- Task failures.
- Task retries.

### Memory and spill metrics

- Memory budget.
- Memory reserved.
- Peak memory estimate.
- Reservation failures.
- Memory pressure events.
- Spill decisions.
- Bytes spilled.
- Spill files created.
- Spill files cleaned.
- Memory released by spill.
- Spill readback events.
- OOM-prevention diagnostics.

### Translation/output metrics

- Output target.
- Output fidelity.
- Metadata preserved.
- Metadata degraded.
- Metadata dropped.
- Materialization required.
- Output files.
- Output bytes.
- Commit status.
- Cleanup status.

### Effectful operation metrics

For future LLM/API/embedding/vector workflows:

- Effect calls planned.
- Effect calls executed.
- Effect calls skipped during dry run.
- Model/API cost estimate.
- Token estimate.
- Rate-limit events.
- Timeouts.
- Retries.
- External write attempts.
- Idempotency keys present.
- Credential errors.
- Redaction applied.

### Benchmark metrics

Benchmark reports should integrate:

- Correctness validation status.
- Baseline engine metadata.
- Metrics collected.
- Runtime diagnostics.
- Unsupported behavior.
- Fallback execution status.
- Reproducibility metadata.

## Trace model

ShardLoom should eventually support trace-like spans.

Potential span categories:

- Query.
- Planning.
- Optimization.
- Scan.
- Segment pruning.
- Encoded evaluation.
- Partial decode.
- Materialization.
- Aggregation.
- Join.
- Sort.
- Spill.
- Translation.
- Output write.
- Commit.
- External effect.
- Task execution.
- Object-store request.

Each span should eventually include:

- Span id.
- Parent id.
- Name.
- Category.
- Start/end timing.
- Status.
- Diagnostics.
- Resource metrics.
- Safe attributes.

Spans must not contain raw secrets or sensitive values.

## Structured events

ShardLoom should eventually emit structured events for important state changes.

Examples:

- Plan created.
- Segment pruned.
- Metadata-only answer used.
- Encoded evaluation used.
- Partial decode required.
- Full materialization required.
- Runtime filter applied.
- Memory pressure elevated.
- Spill planned.
- Spill completed.
- Task retried.
- Output commit ambiguous.
- External effect skipped in dry run.
- Unsupported feature encountered.

Events should be safe for logs and agent consumption.

## Profiling

ShardLoom should support profiling at multiple levels.

### Operator profiling

Operator profiles should include:

- Operator kind.
- Input/output counts.
- Time spent.
- Bytes read/decoded/materialized.
- Memory use.
- Spill use.
- Diagnostics.

### Kernel profiling

Encoded kernels should eventually report:

- DType.
- Encoding.
- Layout.
- Rows processed.
- Null count.
- Selection count.
- Time spent.
- Bytes touched.
- Decode avoided.

### End-to-end profiling

End-to-end profiles should summarize:

- Total time.
- Planning time.
- Execution time.
- IO time.
- Spill time.
- Output time.
- Effect time.
- Memory pressure.
- Fallback status.

## Introspection

ShardLoom should expose introspection commands and APIs.

Potential commands:

- `shardloom explain`
- `shardloom estimate`
- `shardloom doctor`
- `shardloom capabilities`
- `shardloom profile`
- `shardloom metrics`
- `shardloom trace`
- `shardloom benchmark-plan`

Early implementations may be skeletons, but the architecture should keep these surfaces consistent.

## Privacy and redaction

Observability must not leak secrets or sensitive data.

Logs/traces/metrics should avoid:

- API keys.
- Object-store secrets.
- Authorization headers.
- LLM provider keys.
- Raw credential paths.
- PII values.
- Raw document text unless explicitly allowed.
- Prompt contents unless explicitly allowed.
- External API payloads unless explicitly allowed.

ShardLoom should prefer:

- Dataset ids over raw credentials.
- Field names over field values.
- Counts and sizes over raw payloads.
- Redacted values.
- Configurable verbosity.

## Agent usability

LLM agents need observability that is:

- Deterministic.
- Machine-readable.
- Stable enough to parse.
- Explicit about unsupported behavior.
- Explicit about fallback status.
- Explicit about side effects.
- Explicit about suggested next steps.

Agents should not need to scrape vague logs.

## Observability and no-fallback policy

Every runtime report should make fallback status visible when relevant.

Expected default:

- `fallback_execution_allowed = false`
- `fallback_attempted = false`
- `fallback_engine = null`

This prevents accidental assumptions that Spark, DataFusion, DuckDB, Polars, or Velox were used.

## Failure behavior

If observability data cannot be collected, ShardLoom should continue safely where possible and emit diagnostics.

Examples:

- Metrics collection unsupported.
- Trace export unsupported.
- Profiling unsupported.
- Redaction required.
- Unsafe field omitted.
- Sensitive value suppressed.
- Unknown metric.

Missing observability must not trigger fallback execution.

## Alternatives considered

### Add observability after execution is complete

Rejected.

Observability should shape execution APIs, diagnostics, benchmarks, and agent integration early.

### Use only logs

Rejected.

Logs are useful, but structured diagnostics, metrics, traces, and reports are needed.

### Expose all values for debugging

Rejected.

Secrets and sensitive data must be protected.

### Add observability dependencies immediately

Rejected.

This RFC defines design only. Dependencies should be added later through implementation PRs and license review.

## Risks

- Observability can add overhead.
- Metrics may be inconsistent across early features.
- Too much logging can leak sensitive data.
- Too many fields can overwhelm users.
- Stable schemas can constrain future changes.
- Distributed tracing may be complex.
- Profiling may be noisy or expensive.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Observability is a first-class design concern.
- Metrics, traces, events, profiles, and reports should be structured.
- Operator-level and kernel-level profiling are important.
- Object-store, memory, spill, translation, and effect metrics are required.
- Sensitive data must be redacted or omitted.
- Agent-readable observability is required.
- Fallback status must be visible.
- No fallback execution is permitted.

## Verification plan

Future implementation PRs should verify:

- Metrics can be represented.
- Trace/span concepts can be represented.
- Structured events can be represented.
- Profiles can be represented.
- Sensitive fields are redacted.
- Diagnostics remain deterministic.
- Observability does not trigger side effects.
- No Spark or DataFusion dependency is introduced.

## Open questions

- What metrics should be implemented first?
- Should metrics be in core, exec, or a dedicated crate?
- What should the first profile report look like?
- When should OpenTelemetry-style integration be considered?
- How should observability schemas be versioned?
- How should redaction be configured?
