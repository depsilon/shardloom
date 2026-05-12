# RFC 0017: Fault Tolerance, Cancellation, and Recovery

## Status

Draft

## Summary

This RFC defines ShardLoom's fault tolerance, cancellation, and recovery design.

ShardLoom aims to support massive object-store and lakehouse workloads without Spark fallback. At
that scale, correctness and performance depend on predictable failure handling, cancellation,
retries, cleanup, idempotent writes, spill recovery, and commit recovery.

## Context

Large data workloads fail for many reasons:

- Object-store read failure.
- Object-store write failure.
- Network timeout.
- Task cancellation.
- User cancellation.
- Memory pressure.
- Spill file error.
- Worker crash.
- Process restart.
- Partial output write.
- Ambiguous commit.
- External API failure.
- LLM/model call failure.
- Credential expiration.
- Schema mismatch.
- Unsupported runtime behavior.

ShardLoom must not treat these as afterthoughts.

Fault tolerance is part of being Spark-displacing.

## Goals

- Define task retry concepts.
- Define cancellation concepts.
- Define recovery concepts.
- Define idempotency expectations.
- Define cleanup behavior.
- Define partial output behavior.
- Define ambiguous commit diagnostics.
- Define spill cleanup and recovery expectations.
- Define effectful operation failure behavior.
- Preserve no-fallback architecture.

## Non-goals

- Do not implement fault tolerance in this RFC.
- Do not implement distributed runtime in this RFC.
- Do not implement object-store IO in this RFC.
- Do not implement commit protocol in this RFC.
- Do not implement external effect retries in this RFC.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not guarantee exactly-once semantics in this RFC.
- Do not define a full transaction manager.

## Core principle

ShardLoom should make failure states explicit, recoverable where possible, and deterministic when
unrecoverable.

ShardLoom should avoid ambiguous side effects.

When ambiguity cannot be avoided, diagnostics must say so.

## Failure domains

ShardLoom should model failures by domain.

Suggested domains:

- Planning.
- Metadata.
- Vortex IO.
- Object-store read.
- Object-store write.
- Execution task.
- Memory reservation.
- Spill.
- Shuffle.
- Output translation.
- Commit.
- Cleanup.
- External API.
- LLM/model call.
- Embedding generation.
- Vector search.
- Credential/auth.
- User cancellation.
- System cancellation.
- Worker/process failure.

## Task identity

Tasks should have stable identities.

A task identity should include:

- Task id.
- Input dataset.
- Snapshot id.
- Segment ids.
- Attempt id.
- Operation kind.
- Output intent if applicable.

Stable identity enables:

- Retry.
- Idempotency.
- Diagnostics.
- Recovery.
- Deduplication.

## Task attempt

A task may have multiple attempts.

A task attempt should record:

- Attempt id.
- Task id.
- Start status.
- End status.
- Failure reason.
- Retry eligibility.
- Output files written.
- Spill files created.
- Cleanup status.
- Diagnostics.

## Retry policy

Retries should be explicit.

Retry policy should consider:

- Maximum attempts.
- Retryable failure kinds.
- Non-retryable failure kinds.
- Backoff.
- Idempotency.
- Object-store semantics.
- External effect safety.
- Spill cleanup.
- Partial output cleanup.

Retries must not duplicate committed outputs.

## Cancellation

ShardLoom should support cancellation at multiple levels:

- Query cancellation.
- Task cancellation.
- Scan cancellation.
- Output write cancellation.
- External effect cancellation where possible.
- Spill cleanup after cancellation.

Cancellation should be cooperative where possible.

Cancellation should produce diagnostics.

Cancellation should not leave committed partial outputs unless explicitly documented.

## Recovery

ShardLoom should eventually support recovery for:

- Failed tasks.
- Failed output writes.
- Failed commits.
- Ambiguous commits.
- Spill cleanup.
- Temporary output cleanup.
- Manifest/snapshot recovery.
- Worker restart if distributed later.

Recovery behavior should be explicit.

If recovery is unsupported, ShardLoom should fail deterministically and report what may need
cleanup.

## Idempotency

Idempotency is required for safe retries.

ShardLoom should use idempotency keys for:

- Write intents.
- Output commits.
- External writes.
- API calls that support idempotency.
- LLM/model operations if cached.
- Embedding generation if cached.
- Task outputs.

Idempotency does not guarantee exactly-once behavior unless the underlying system supports it.

## Commit states

Output commit should use explicit states.

Suggested states:

- NotStarted.
- Planned.
- WritingTemporaryFiles.
- Validating.
- Committing.
- Committed.
- Failed.
- Ambiguous.
- Aborted.
- CleanupRequired.
- Unsupported.

Ambiguous states must be reported clearly.

## Partial output behavior

Partial output can happen if a write fails midway.

ShardLoom should track:

- Temporary files.
- Output files.
- Manifest changes.
- Snapshot pointer changes.
- Commit record.
- Cleanup status.

If partial output cannot be cleaned up automatically, diagnostics must say so.

## Spill recovery

Spill files are temporary execution state.

ShardLoom should track:

- Spill file ownership.
- Spill file status.
- Spill file cleanup.
- Spill readback failure.
- Spill write failure.
- Spill ambiguity after crash.

Spill cleanup should be best-effort initially, then stronger as runtime matures.

## Object-store failure behavior

Object-store operations may fail due to:

- Missing object.
- Permission denied.
- Timeout.
- Rate limit.
- Partial read.
- Failed write.
- Consistency delay.
- Ambiguous overwrite.
- Delete failure.

ShardLoom should distinguish these where possible.

Retries must respect idempotency and write safety.

## External effect failure behavior

LLM calls, API calls, embedding generation, vector search, and external writes are effectful.

Failure handling must account for:

- Cost.
- Timeout.
- Rate limit.
- Credential failure.
- Partial success.
- External mutation.
- Idempotency key.
- Retry safety.
- Dry-run behavior.
- Human approval where required.

External writes should not be retried blindly.

## Cancellation and agents

LLM/coding agents need safe cancellation and dry-run behavior.

Agent-facing behavior should support:

- Plan before execute.
- Dry run.
- Explain failure state.
- Identify cleanup required.
- Avoid destructive retries.
- Report ambiguous commits.
- Report fallback attempted false.

## Recovery diagnostics

Recovery diagnostics should include:

- Failure domain.
- Task id.
- Attempt id.
- Snapshot id.
- Output target.
- Temporary files.
- Spill files.
- Commit state.
- Retry eligibility.
- Cleanup status.
- Suggested next step.
- Fallback attempted false.

## Fault-tolerance levels

ShardLoom may classify features by fault-tolerance level.

Suggested levels:

- None.
- DiagnosticOnly.
- Retryable.
- Recoverable.
- Idempotent.
- TransactionalIfBackendSupports.
- Unsupported.

This lets users and agents understand safety boundaries.

## Failure behavior

Unsupported recovery behavior must fail explicitly.

Examples:

- Retry unsupported.
- Cancellation unsupported.
- Commit recovery unsupported.
- Spill cleanup unsupported.
- External write retry unsafe.
- Ambiguous commit.
- Partial output cleanup failed.
- Worker recovery unsupported.
- Distributed recovery unsupported.

Failures must not invoke Spark, DataFusion, DuckDB, Polars, Velox, or another fallback engine.

## Alternatives considered

### Let failures be ordinary errors only

Rejected.

Massive workloads need structured recovery information.

### Use Spark for fault-tolerant fallback

Rejected.

This violates ShardLoom's no-fallback architecture.

### Delay fault tolerance until distributed execution

Rejected.

Fault-tolerance concepts should shape local execution, object-store writes, spill, and cancellation
early.

### Promise exactly-once semantics everywhere

Rejected.

Exactly-once semantics depend on storage systems, external APIs, idempotency, and commit protocols.

ShardLoom should be honest about guarantees.

## Risks

- Fault-tolerance design can become complex.
- Overpromising recovery semantics is dangerous.
- Object-store commit behavior varies.
- External effects may not be safely retryable.
- Cancellation can leave partial state.
- Recovery metadata adds overhead.
- Distributed recovery is difficult.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Failure states must be structured.
- Task attempts and retry policy are required.
- Cancellation is required.
- Idempotency is required for safe writes and retries.
- Partial outputs and ambiguous commits must be represented.
- Spill cleanup and recovery must be represented.
- External effects need explicit retry/cancellation semantics.
- Recovery limitations must be reported honestly.
- No fallback execution is permitted.

## Verification plan

Future implementation PRs should verify:

- Task attempt state can be represented.
- Retry policy can be represented.
- Cancellation state can be represented.
- Commit state can be represented.
- Partial output diagnostics can be represented.
- Cleanup requirements can be represented.
- Ambiguous commit diagnostics can be represented.
- External effect retry safety can be represented.
- No Spark or DataFusion dependency is introduced.

## Open questions

- What is the first cancellation point to implement?
- What commit protocol should be implemented first?
- How should temporary output paths be tracked?
- How should spill cleanup be tracked?
- Should task attempts be persisted locally?
- How should recovery metadata integrate with manifests?
- How should external effect idempotency be represented?
