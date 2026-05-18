<!-- SPDX-License-Identifier: Apache-2.0 -->

# Enterprise Evidence Export Pack

Status: report-only design for `GAR-COMMERCIAL-1D`. This document does not add an exporter, emit
lineage events, emit OpenTelemetry traces or metrics, configure a backend, make network calls,
invoke Foundry, publish packages, or expand ShardLoom runtime support.

The machine-readable source of truth is
[`docs/release/enterprise-evidence-export-pack.json`](enterprise-evidence-export-pack.json) with
schema `shardloom.enterprise_evidence_export_pack.v1`. Validate it with:

```powershell
python scripts\check_enterprise_evidence_export_pack.py
```

## Purpose

The enterprise evidence export pack is a local-first packaging contract for ShardLoom evidence. It
is intended to make the existing evidence envelope easier to inspect and later integrate with
enterprise lineage and observability stacks without creating default network side effects.

The pack is useful only as evidence portability. It is not production readiness, managed-platform
certification, package-publication proof, or a performance claim.

## Pack Contents

The planned local artifact layout is:

```text
target/enterprise-evidence-export-pack/<run-id>/
  manifest.json
  shardloom-evidence.json
  openlineage-facets.json
  opentelemetry-trace.json
  summary.md
  redaction-report.json
```

| File | Component | Current posture |
| --- | --- | --- |
| `manifest.json` | Pack metadata, schema version, run id, component refs, claim boundary. | Report-only contract. |
| `shardloom-evidence.json` | Native ShardLoom execution/evidence envelope. | Planned local artifact. |
| `openlineage-facets.json` | ShardLoom-owned OpenLineage custom facet payloads. | Report-only mapping; no event emitted. |
| `opentelemetry-trace.json` | ShardLoom span/metric payload preview. | Report-only mapping; no trace, metric, log, SDK, OTLP exporter, collector, or backend. |
| `summary.md` | Optional Markdown summary for human review. | Optional planned local artifact. |
| `redaction-report.json` | Redaction/retention decision report. | Required before any export implementation. |

## Source Mappings

The pack composes already-defined report-only evidence surfaces:

- `docs/architecture/evidence-native-generated-execution-observability-confidence.md`
- `shardloom.openlineage_facet_mapping.v1`
- `shardloom.opentelemetry_trace_export_contract.v1`
- ShardLoom JSON execution evidence envelope
- generated-source evidence where applicable
- no-fallback/no-external-engine evidence

OpenLineage and OpenTelemetry payloads are local payload previews only in this slice. They do not
emit events, configure SDKs, publish schemas, connect to a collector, or call any backend.

## Redaction And Retention

The default redaction policy is `strict_local_enterprise_redaction`.

Fields that must be redacted or replaced with refs/digests before export:

- secrets and credentials
- access tokens and environment variables
- full local paths
- query text
- schema names
- sample values
- endpoint URLs
- object-store credentials
- platform dataset identifiers

Allowed field classes by default:

- schema and report version
- run id and artifact ids
- execution mode and engine mode
- claim gate status
- `fallback_attempted=false`
- `external_engine_invoked=false`
- artifact digests
- row/byte counts
- timing fields
- certificate status
- redacted refs

The default retention posture is local target-directory storage only. Uploads, backend retention,
object-store export, and managed-platform retention are not configured.

## Opt-In Boundary

Future local export should require an explicit command shaped like:

```powershell
shardloom evidence export-pack --output <dir> --local-only
```

That command does not exist in this slice. It documents the desired safety boundary:

- explicit output directory
- local-only default
- no network export flag in this slice
- redaction report required
- no credential resolution
- no object-store I/O
- no external engine invocation
- no fallback execution

## Claim Boundary

The export pack cannot upgrade claim status. In this slice:

```text
export_pack_runtime_supported=false
export_pack_enabled_by_default=false
opt_in_required=true
network_calls_by_default=false
backend_integration_configured=false
lineage_event_emitted=false
telemetry_trace_emitted=false
telemetry_metric_emitted=false
telemetry_log_emitted=false
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
```

No production observability, managed-platform certification, Foundry, object-store/lakehouse,
SQL/DataFrame, package-publication, performance, Spark-displacement, or external-backend claim is
allowed by this pack.

## Validation

Run:

```powershell
python scripts\check_enterprise_evidence_export_pack.py
cargo test -p shardloom-contract-tests --test release_readiness_metadata
```

The validator accepts the current report-only posture. It fails if the manifest overclaims support,
allows default network/backend behavior, omits redaction/retention policy, or drops the no-fallback
fields.
