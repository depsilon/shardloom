# Fault Tolerance, Cancellation, and Recovery Skill

## Purpose

Use this skill when designing or implementing task attempts, retries, cancellation, recovery, idempotency, partial output cleanup, commit states, spill cleanup, or external effect failure behavior.

ShardLoom should make failure states explicit, recoverable where possible, and deterministic when unrecoverable.

## When to use

Use this skill for tasks involving:

- Task attempts.
- Retry policy.
- Query cancellation.
- Task cancellation.
- Output commit.
- Partial output cleanup.
- Ambiguous commit diagnostics.
- Spill cleanup.
- Manifest recovery.
- Snapshot recovery.
- Object-store failure behavior.
- External API failure.
- LLM/model call failure.
- Embedding generation failure.
- Worker/process failure.
- Recovery diagnostics.

## Rules

- Failure states must be structured.
- Retry must be explicit.
- Idempotency is required for safe writes and retries.
- Cancellation should be cooperative where possible.
- Partial outputs must be tracked.
- Ambiguous commits must be diagnosable.
- Spill files need cleanup semantics.
- External writes must not be retried blindly.
- Recovery limitations must be honest.
- Fallback attempted must be false for unsupported behavior.
- No Spark or DataFusion fallback is allowed.

## Required checks

For retry behavior:

- Is the failure retryable?
- Is the operation idempotent?
- Are attempts tracked?
- Are partial outputs handled?
- Are diagnostics clear?

For cancellation:

- What can be cancelled?
- What cleanup is required?
- Are external effects protected?
- Is the cancellation state diagnosable?

For commits:

- What is the commit state?
- Could it be ambiguous?
- Are temporary outputs tracked?
- Is cleanup required?
- Is idempotency represented?

For external effects:

- Is retry safe?
- Is idempotency key available?
- Could an external mutation have occurred?
- Is dry-run behavior safe?
- Is failure reported clearly?

## Red flags

- Retrying external writes blindly.
- Partial outputs without cleanup tracking.
- Ambiguous commit without diagnostic.
- Cancellation that leaves hidden state.
- Spill files without cleanup semantics.
- Failure represented only as vague string.
- Adding Spark/DataFusion for recovery.
- Claiming exactly-once semantics without backend support.

## Example Codex prompt fragment

"Use the Fault Tolerance and Recovery skill. Represent task attempts, retries, cancellation, idempotency, commit states, partial outputs, and cleanup explicitly. Unsupported recovery must fail deterministically. Do not add Spark/DataFusion fallback."
