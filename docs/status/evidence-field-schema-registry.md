# Evidence Field Schema Registry Status

Schema: `shardloom.evidence_field_schema_registry.v1`

Source: `shardloom-cli/src/evidence_schema_registry.rs`

Report id: `review-p1-2.evidence_field_schema_registry`

Surface count: 8

Field count: 265

Surface order: execution_mode_selection_report, compute_flow_evidence,
execution_certificate_report, native_io_report, benchmark_plan_report,
benchmark_constitution_report, benchmark_claim_evidence_report,
compute_capability_matrix_report

Schema command: `shardloom evidence-schema [surface] --format json`

Capability surface: `shardloom capabilities api-surfaces --format json`

Claim boundary: schema consistency and drift prevention only. This registry does not authorize
runtime support, public API stability, benchmark superiority, package readiness, or production
claims.

No-fallback status: fallback_attempted=false and external_engine_invoked=false.

Deprecation policy: additive v1; field removal or rename requires a compatibility note and tests.

This page is a status snippet, not a separate hand-maintained evidence table. The full per-surface
and per-field rows are generated from the registry through the CLI schema and capability surfaces.
