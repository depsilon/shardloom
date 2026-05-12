# Object Store and Runtime Skill

## Purpose

Use this skill when working on object-store IO, distributed execution, task scheduling, retries,
byte ranges, manifests, spilling, or commit behavior.

The goal is to make ShardLoom object-store-native and Spark-free for large lakehouse workloads.

## When to use

Use this skill for tasks involving:

- S3, ADLS, GCS, or local object-store abstractions.
- Range reads.
- Metadata caching.
- Manifest handling.
- Task scheduling.
- Distributed execution.
- Shuffle avoidance.
- Retry behavior.
- Spill behavior.
- Output commits.
- Fault tolerance.
- Resource limits.

## Rules

- Object storage should be treated as a primary runtime environment, not an afterthought.
- Prefer byte-range and segment-level planning over full-file reads.
- Avoid loading whole files when segment metadata or byte ranges can reduce IO.
- Reads should be bounded, observable, and retryable.
- Tasks should be idempotent where possible.
- Partial failure should be anticipated.
- Output commits should be atomic or explicitly documented as not yet atomic.
- Resource use should be bounded.
- Backpressure and spilling should be considered for large workloads.
- Distributed execution should be introduced only after single-node contracts are clear.
- Avoid shuffle whenever possible.
- Do not use Spark as a distributed runtime fallback.

## Required checks

For object-store reads:

- Test missing object behavior.
- Test invalid range behavior.
- Test empty object behavior.
- Test partial read behavior.
- Test retry/error propagation where possible.
- Confirm reads do not require whole-file loading unless justified.

For runtime tasks:

- Define task input and output clearly.
- Define retry semantics.
- Define idempotency expectations.
- Define cancellation expectations where relevant.
- Define memory bounds or known limitations.
- Emit useful diagnostics or tracing where relevant.

For output commits:

- Define temporary output behavior.
- Define final commit behavior.
- Define failure cleanup behavior.
- Define whether the commit is atomic.

## Red flags

- Reading whole files by default.
- Ignoring object-store latency and retry behavior.
- Ignoring small-file and manifest costs.
- Treating distributed runtime as "just Spark without Spark."
- Adding Spark as a temporary large-workload path.
- Unbounded memory use.
- Output writes that can leave ambiguous partial results without documentation.

## Example Codex prompt fragment

When working on object-store or runtime behavior, include this instruction:

"Use the Object Store and Runtime skill. Plan around byte ranges, retries, idempotency, bounded
resources, and atomic or documented commit behavior. Avoid shuffle and do not add Spark fallback."
