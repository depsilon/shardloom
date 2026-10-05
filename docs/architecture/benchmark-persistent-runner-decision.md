# Benchmark Runner Decision

Status: public-workflow harness interface documented; live acceptance pending.

## Decision

The traditional analytics harness has one ShardLoom candidate, `shardloom`, and sends each complete
declared SQL workload through the public native workflow. It starts a fresh executable process for
each native request. Input state (`raw` or `prepared`) and requested output format are parameters to
the same candidate path. The harness does not select benchmark-private ShardLoom commands or
different execution routes by workload name.

Independent comparison engines run separately to provide complete reference values. They cannot
complete, repair, or take over a rejected ShardLoom declaration. A missing baseline result, native
unsupported diagnostic, failed request, or result mismatch prevents a complete report.

## Timing And Integrity

The harness records process wall time for each native request and separates input preparation,
requested sink writing, output readback validation, fixture generation, source hashing, and actual
run wall time. Requested sinks are reopened through the public workflow and validated before their
values are compared. Source preparation and readback validation are outside query timing; readback,
fixture work, and hashing remain part of actual harness elapsed time.

Each run uses an exclusive, fresh run directory under a local-only workspace. It records executable,
harness-source, input, and reference-result hashes, then verifies executable and input stability
through the run. A local lock, storage budgets, process-conflict checks, and sampled process-tree RSS
guard the run. Resource samples and timing boundaries must stay attached to the report.

## Acceptance Gate

The harness is not yet live-accepted. Before its output is used for timing analysis, validate at
least one raw-input case, one prepared-input case, and one requested-output case against independent
complete values. Inspect report and process receipts for stable hashes, no-fallback evidence, source
and binary identity, timing boundaries, and resource observations. Confirm that failures and
unsupported cases produce an incomplete or failed report and nonzero exit status.

Even after acceptance, measurements alone do not authorize performance superiority, production
readiness, or Spark-replacement claims. Those require the project's separate correctness, workload,
and benchmark gates. This document defines no hidden runner, background service, benchmark-only
execution mode, or external execution fallback.
