<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Observability And Support

Schema marker: `shardloom.v1_observability_support.v1`.

This document defines the v1 local observability, supportability, and troubleshooting boundary for
supported source-checkout and local package workflows. It does not authorize production telemetry,
remote support uploads, OpenTelemetry/OpenLineage exporters, live profiling, package publication,
or performance claims.

## Supported V1 Boundary

The v1 boundary is local, deterministic, and side-effect-free by default:

- `doctor --format json` exposes local static health fields without filesystem, environment, or
  network probes.
- `support-bundle --format json` emits a redacted in-envelope local bundle by default and does not
  write files.
- `agent-contract-pack --format json` exposes stable agent inspection order and confirms all listed
  surfaces are side-effect-free by default.
- `capabilities certification --format json` exposes support and claim gates without executing
  runtime work.
- `runtime-report --format json` exposes benchmark/runtime observability schema availability and
  unsupported live profiling/exporter boundaries.
- `observability-schema-coverage --format json` proves local schema coverage, redaction
  requirements, and certificate-link requirements.
- `explain local-file-query --format json` and `estimate local-file-query --format json` fail
  deterministically as plan-only unsupported surfaces until real planning/estimation support is
  implemented.
- Benchmark artifacts expose route lane, timing surface, evidence tier, stage inclusion, certificate
  status, sink timing inclusion, and no-fallback fields for ShardLoom rows.

Validate with:

```text
python scripts/check_v1_observability_support.py
```

The report writes:

```text
target/v1-observability-support-report.json
```

## Troubleshooting Contract

Troubleshooting starts with the machine-readable JSON envelope:

1. Run `doctor --format json`.
2. Run `support-bundle --format json` with a redacted note if context is needed.
3. Inspect the command `diagnostics[]` array and diagnostic `code`.
4. Confirm `fallback_attempted=false` and `external_engine_invoked=false`.
5. Use `docs/release/troubleshooting-diagnostics.md` and
   `docs/release/diagnostic-code-stability.md` for stable diagnostic-code meaning.

## Issue Intake Contract

Issue templates must request the command, JSON envelope, diagnostic code, route id, fallback status,
external-engine status, CLI version, Python version, Rust version, and OS. The templates must not
ask users to upload secrets or unredacted data.

## Technique Review

This closeout uses ShardLoom-native evidence techniques where they are relevant:

- PulseWeave and capillary runtime decisions are reported when present in benchmark fields, not
  inferred by support tooling.
- Dynamic admission and resource decisions are consumed from route/resource reports rather than
  recomputed by doctor or support-bundle.
- Metadata-first inspection is required: support commands read static contracts and do not execute
  effectful probes.
- Timing-surface and evidence-tier separation is required in benchmark rows so support reports do
  not mix hot runtime with publication-proof work.

## Non-Claims

- no OpenTelemetry exporter claim.
- no OpenLineage exporter claim.
- no remote support upload claim.
- no live profiler collection claim.
- no production observability claim.
- no package publication claim.
- no Spark, DataFusion, DuckDB, Polars, Velox, or other fallback execution claim.

