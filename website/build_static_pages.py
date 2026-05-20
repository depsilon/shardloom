#!/usr/bin/env python
"""Build the ShardLoom public website.

The current generator keeps Cloudflare deployment static while the public site
evolves into a light-mode evidence-console surface. Repo docs remain in the repository as the deep
source of truth; generated pages translate the current route, benchmark, and claim-boundary model
for human readers.
"""

from __future__ import annotations

import argparse
import html
import importlib.util
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WEBSITE = ROOT / "website"
ASSETS = WEBSITE / "assets"
DATA = ASSETS / "data"
BENCHMARK_LATEST = ASSETS / "benchmarks" / "latest"
FLOW_SOURCE = ROOT / "docs" / "architecture" / "compute-engine-flow-reference.md"
BENCHMARK_MANIFEST = BENCHMARK_LATEST / "manifest.json"
BENCHMARK_RESULTS = BENCHMARK_LATEST / "benchmark-results.json"
USE_CASE_INDEX = ROOT / "docs" / "use-cases" / "use-case-index.yml"
USE_CASE_INDEX_CHECKER = ROOT / "scripts" / "check_use_case_index.py"
SITE_LASTMOD = "2026-05-20"


STATUS_LABELS = {
    "ready_local": "runtime_supported",
    "smoke_supported": "smoke_supported",
    "report_only": "report_only",
    "planned": "not_planned",
    "blocked": "blocked",
    "unsupported": "unsupported",
    "runtime_supported": "runtime_supported",
    "fixture_smoke_only": "fixture_smoke_only",
    "not_planned": "not_planned",
}


FIELD_GUIDE_TERMS: list[dict[str, Any]] = [
    {
        "slug": "what-is-shardloom",
        "title": "What is ShardLoom?",
        "category": "Start Here",
        "status": "runtime_supported",
        "summary": "A pre-release compute engine focused on Vortex-prepared routes, explicit evidence, and no hidden fallback.",
        "route": "overview",
        "evidence_fields": ["claim_gate_status", "fallback_attempted", "external_engine_invoked"],
        "related_use_cases": ["first-10-minutes-local-smoke", "evidence-audit-claim-gates"],
        "references": ["README.md", "docs/getting-started/first-10-minutes.md"],
    },
    {
        "slug": "evidence-gated-compute",
        "title": "Evidence-gated compute",
        "category": "Start Here",
        "status": "smoke_supported",
        "summary": "Every supported route must emit evidence before any claim can be upgraded.",
        "route": "all routes",
        "evidence_fields": ["evidence_level", "claim_gate_status", "claim_boundary"],
        "related_use_cases": ["evidence-audit-claim-gates", "benchmark-interpretation-evidence-not-leaderboard"],
        "references": ["docs/architecture/compute-engine-flow-reference.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "no-fallback",
        "title": "No fallback",
        "category": "Start Here",
        "status": "runtime_supported",
        "summary": "Unsupported work blocks or reports a deterministic diagnostic instead of running through another engine.",
        "route": "all routes",
        "evidence_fields": ["fallback_attempted=false", "external_engine_invoked=false", "blocker_id"],
        "related_use_cases": ["sql-dataframe-capability-posture", "object-store-boundary-report"],
        "references": ["README.md", "docs/architecture/compute-engine-flow-reference.md"],
    },
    {
        "slug": "compatibility-import-certified",
        "title": "compatibility_import_certified",
        "category": "Execution Routes",
        "status": "smoke_supported",
        "summary": "The certified cold ingest/stage route for local compatibility inputs and evidence-heavy runs.",
        "route": "Certified import/stage route",
        "evidence_fields": ["timing_scope=cold_certified_end_to_end", "source_read_millis", "vortex_write_millis", "claim_gate_status"],
        "related_use_cases": ["compatibility-import-certified-local", "local-file-etl-cleanup-smoke"],
        "references": ["docs/getting-started/certified-local-workload.md", "docs/benchmarks/local-taxonomy-benchmark.md"],
    },
    {
        "slug": "direct-compatibility-transient",
        "title": "direct_compatibility_transient",
        "category": "Execution Routes",
        "status": "smoke_supported",
        "summary": "A scoped one-shot local route without persistent Vortex preparation.",
        "route": "Direct one-shot route",
        "evidence_fields": ["execution_mode", "source_format", "materialization_boundary"],
        "related_use_cases": ["sql-local-source-csv-smoke", "python-local-csv-query-builder-smoke"],
        "references": ["python/README.md", "docs/architecture/compute-engine-flow-reference.md"],
    },
    {
        "slug": "generated-source-route",
        "title": "Generated source route",
        "category": "Execution Routes",
        "status": "smoke_supported",
        "summary": "No input dataset is read; ShardLoom creates deterministic rows and writes local outputs with evidence.",
        "route": "Source-free generated route",
        "evidence_fields": ["input_dataset_count=0", "generated_source_created", "generated_source_certificate_status"],
        "related_use_cases": ["source-free-generated-output-boundary", "foundry-local-proof-boundary"],
        "references": ["docs/foundry/proof-of-use-certification.md", "python/README.md"],
    },
    {
        "slug": "universal-ingress",
        "title": "UniversalIngress",
        "category": "UniversalIngress",
        "status": "report_only",
        "summary": "The source admission layer that recognizes, admits, or blocks every potential input family.",
        "route": "UniversalIngress",
        "evidence_fields": ["source_adapter_status", "source_adapter_blocker_id", "ingress_status"],
        "related_use_cases": ["vortex-ingest-prepare-once-local", "object-store-boundary-report"],
        "references": ["docs/architecture/universal-ingress-route-taxonomy.md", "docs/architecture/compute-engine-flow-reference.md"],
    },
    {
        "slug": "source-state",
        "title": "SourceState",
        "category": "UniversalIngress",
        "status": "smoke_supported",
        "summary": "Reusable source identity, schema, fingerprint, adapter posture, and source evidence.",
        "route": "UniversalIngress -> SourceState",
        "evidence_fields": ["source_state_id", "source_state_digest", "schema_digest", "source_state_reuse_hit"],
        "related_use_cases": ["prepared-native-vortex-runtime-direction", "query-scenario-cookbook-smoke"],
        "references": ["docs/architecture/universal-input-contract.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "source-adapter-status",
        "title": "Source adapter status",
        "category": "UniversalIngress",
        "status": "smoke_supported",
        "summary": "The visible admit/block posture for an input adapter and source format.",
        "route": "UniversalIngress",
        "evidence_fields": ["source_adapter_id", "source_adapter_status", "source_adapter_blocker_id"],
        "related_use_cases": ["compatibility-import-certified-local", "python-local-csv-query-builder-smoke"],
        "references": ["docs/architecture/compute-engine-flow-reference.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "vortex-ingest",
        "title": "vortex_ingest",
        "category": "Vortex Ingest",
        "status": "smoke_supported",
        "summary": "The prepare-once stage that turns admitted non-Vortex sources into VortexPreparedState.",
        "route": "Vortex ingest / prepare once route",
        "evidence_fields": ["vortex_ingest_status", "prepared_state_id", "vortex_artifact_digest"],
        "related_use_cases": ["vortex-ingest-prepare-once-local", "prepared-native-vortex-runtime-direction"],
        "references": ["docs/architecture/universal-ingress-route-taxonomy.md", "python/README.md"],
    },
    {
        "slug": "vortex-prepared-state",
        "title": "VortexPreparedState",
        "category": "Vortex Ingest",
        "status": "smoke_supported",
        "summary": "The prepared artifact/state that prepared_vortex executes from.",
        "route": "VortexPreparedState -> prepared_vortex",
        "evidence_fields": ["prepared_state_id", "prepared_state_digest", "prepared_state_reuse_hit"],
        "related_use_cases": ["vortex-ingest-prepare-once-local", "prepared-native-vortex-runtime-direction"],
        "references": ["docs/architecture/compute-engine-flow-reference.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "prepared-state-reuse",
        "title": "Prepared state reuse",
        "category": "Vortex Ingest",
        "status": "smoke_supported",
        "summary": "Reuse of a prepared Vortex artifact across scenarios, queries, or outputs.",
        "route": "Prepared warm route",
        "evidence_fields": ["prepared_state_reuse_hit", "source_state_reuse_hit", "invalidation_reason"],
        "related_use_cases": ["prepared-native-vortex-runtime-direction", "output-result-sink-and-fanout-boundary"],
        "references": ["docs/benchmarks/local-taxonomy-benchmark.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "prepared-vortex",
        "title": "prepared_vortex",
        "category": "Prepared/Native Vortex",
        "status": "smoke_supported",
        "summary": "Prepared warm execution over an existing VortexPreparedState, not direct non-Vortex input.",
        "route": "Prepared Vortex route",
        "evidence_fields": ["execution_mode=prepared_vortex", "timing_scope=warm_prepared_query", "prepared_state_id"],
        "related_use_cases": ["prepared-native-vortex-runtime-direction", "vortex-ingest-prepare-once-local"],
        "references": ["docs/architecture/compute-engine-flow-reference.md", "docs/benchmarks/local-taxonomy-benchmark.md"],
    },
    {
        "slug": "native-vortex",
        "title": "native_vortex",
        "category": "Prepared/Native Vortex",
        "status": "smoke_supported",
        "summary": "Execution over input that already exists as Vortex before the query starts.",
        "route": "Native Vortex route",
        "evidence_fields": ["execution_mode=native_vortex", "vortex_artifact_ref", "claim_gate_status"],
        "related_use_cases": ["prepared-native-vortex-runtime-direction", "benchmark-interpretation-evidence-not-leaderboard"],
        "references": ["docs/architecture/compute-engine-flow-reference.md", "benchmarks/traditional_analytics/README.md"],
    },
    {
        "slug": "source-backed-scan",
        "title": "Source-backed scan",
        "category": "Prepared/Native Vortex",
        "status": "smoke_supported",
        "summary": "Prepared/native scan evidence showing columns, pushdown, decode, and materialization posture.",
        "route": "Prepared/native scan",
        "evidence_fields": ["scan_filter_pushed_down", "scan_projection_pushed_down", "data_decoded", "data_materialized"],
        "related_use_cases": ["prepared-native-vortex-runtime-direction", "query-scenario-cookbook-smoke"],
        "references": ["docs/architecture/compute-engine-flow-reference.md", "docs/benchmarks/local-taxonomy-benchmark.md"],
    },
    {
        "slug": "claim-gate-status",
        "title": "claim_gate_status",
        "category": "Evidence + Certificates",
        "status": "runtime_supported",
        "summary": "The field that says whether evidence is claim-grade, fixture-smoke-only, report-only, or blocked.",
        "route": "Evidence -> ClaimGate",
        "evidence_fields": ["claim_gate_status", "claim_boundary", "evidence_preserved"],
        "related_use_cases": ["evidence-audit-claim-gates", "benchmark-interpretation-evidence-not-leaderboard"],
        "references": ["docs/architecture/compute-engine-flow-reference.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "native-io-certificate",
        "title": "Native I/O certificate",
        "category": "Evidence + Certificates",
        "status": "smoke_supported",
        "summary": "Evidence for admitted local source or sink I/O without external engine fallback.",
        "route": "Evidence",
        "evidence_fields": ["native_io_certificate_status", "output_native_io_certificate_status", "certificate_ref"],
        "related_use_cases": ["local-file-etl-cleanup-smoke", "output-result-sink-and-fanout-boundary"],
        "references": ["docs/getting-started/certified-local-workload.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "materialization-boundary",
        "title": "Materialization boundary",
        "category": "Evidence + Certificates",
        "status": "smoke_supported",
        "summary": "Evidence describing when data was decoded, materialized, or kept source-backed.",
        "route": "Evidence",
        "evidence_fields": ["materialization_boundary", "data_decoded", "data_materialized"],
        "related_use_cases": ["prepared-native-vortex-runtime-direction", "evidence-audit-claim-gates"],
        "references": ["docs/architecture/compute-engine-flow-reference.md", "docs/benchmarks/local-taxonomy-benchmark.md"],
    },
    {
        "slug": "result-sink-replay",
        "title": "Result-sink replay",
        "category": "Evidence + Certificates",
        "status": "smoke_supported",
        "summary": "Output proof that a local result sink can be replayed or checked for evidence consistency.",
        "route": "OutputPlan -> SinkArtifact -> Evidence",
        "evidence_fields": ["result_sink_write_millis", "result_replay_verified", "output_certificate_ref"],
        "related_use_cases": ["output-result-sink-and-fanout-boundary", "evidence-audit-claim-gates"],
        "references": ["docs/getting-started/certified-local-workload.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "benchmark-evidence",
        "title": "Benchmark evidence",
        "category": "Benchmarks",
        "status": "smoke_supported",
        "summary": "Committed local timing and coverage artifacts interpreted as evidence, not a leaderboard.",
        "route": "Benchmark artifact",
        "evidence_fields": ["benchmark_profile", "expected_lanes", "available_lanes", "performance_claim_allowed=false"],
        "related_use_cases": ["benchmark-interpretation-evidence-not-leaderboard", "query-scenario-cookbook-smoke"],
        "references": ["docs/benchmarks/local-taxonomy-benchmark.md", "docs/benchmarks/baseline-comparison-boundary.md"],
    },
    {
        "slug": "certified-cold-route",
        "title": "Certified cold route",
        "category": "Benchmarks",
        "status": "smoke_supported",
        "summary": "Timing scope that includes source read, parse, ingest, Vortex write/reopen, compute, output, and evidence.",
        "route": "compatibility_import_certified",
        "evidence_fields": ["timing_scope=cold_certified_end_to_end", "preparation_included=true", "total_runtime_millis"],
        "related_use_cases": ["compatibility-import-certified-local", "benchmark-interpretation-evidence-not-leaderboard"],
        "references": ["docs/benchmarks/local-taxonomy-benchmark.md", "docs/architecture/compute-engine-flow-reference.md"],
    },
    {
        "slug": "prepared-warm-route",
        "title": "Prepared warm route",
        "category": "Benchmarks",
        "status": "smoke_supported",
        "summary": "Timing scope for query/runtime after VortexPreparedState already exists.",
        "route": "prepared_vortex",
        "evidence_fields": ["timing_scope=warm_prepared_query", "preparation_included=false", "prepared_state_id"],
        "related_use_cases": ["prepared-native-vortex-runtime-direction", "benchmark-interpretation-evidence-not-leaderboard"],
        "references": ["docs/benchmarks/local-taxonomy-benchmark.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "external-baseline-only",
        "title": "external_baseline_only",
        "category": "Benchmarks",
        "status": "runtime_supported",
        "summary": "Competitor engines are comparison context only and never satisfy ShardLoom evidence gates.",
        "route": "Benchmark artifact",
        "evidence_fields": ["external_baseline_only", "external_engine_invoked=false", "fallback_attempted=false"],
        "related_use_cases": ["benchmark-interpretation-evidence-not-leaderboard", "evidence-audit-claim-gates"],
        "references": ["docs/benchmarks/baseline-comparison-boundary.md", "benchmarks/traditional_analytics/README.md"],
    },
    {
        "slug": "output-plan",
        "title": "OutputPlan",
        "category": "I/O + Outputs",
        "status": "smoke_supported",
        "summary": "The output planning layer that stays separate from input format and execution route.",
        "route": "OutputPlan",
        "evidence_fields": ["output_plan_id", "output_plan_status", "output_format"],
        "related_use_cases": ["output-result-sink-and-fanout-boundary", "source-free-generated-output-boundary"],
        "references": ["docs/use-cases/use-case-index.yml", "docs/architecture/compute-engine-flow-reference.md"],
    },
    {
        "slug": "sink-artifact",
        "title": "SinkArtifact",
        "category": "I/O + Outputs",
        "status": "smoke_supported",
        "summary": "The written output artifact plus digest, format, replay, and certificate posture.",
        "route": "SinkArtifact",
        "evidence_fields": ["output_path", "output_format", "output_native_io_certificate_status"],
        "related_use_cases": ["output-result-sink-and-fanout-boundary", "source-free-generated-output-boundary"],
        "references": ["docs/use-cases/use-case-index.yml", "python/README.md"],
    },
    {
        "slug": "output-fanout",
        "title": "Output fanout",
        "category": "I/O + Outputs",
        "status": "planned",
        "summary": "One prepared source or result writing multiple output formats without coupling input and output types.",
        "route": "Output fanout route",
        "evidence_fields": ["fanout_output_count", "output_plan_reuse_hit", "output_replay_millis"],
        "related_use_cases": ["output-result-sink-and-fanout-boundary", "source-free-generated-output-boundary"],
        "references": ["docs/architecture/phased-execution-plan.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "scale-classes",
        "title": "Scale classes",
        "category": "Scale + Resource Envelope",
        "status": "planned",
        "summary": "Bounded scale claims such as local smoke, local claim-grade, split-local, object-store runtime, and distributed runtime.",
        "route": "Scale contract",
        "evidence_fields": ["scale_profile", "scale_claim_status", "memory_budget_bytes", "claim_gate_status"],
        "related_use_cases": ["benchmark-interpretation-evidence-not-leaderboard", "object-store-boundary-report"],
        "references": ["docs/architecture/phased-execution-plan.md", "docs/architecture/compute-engine-flow-reference.md"],
    },
    {
        "slug": "object-store-boundary",
        "title": "Object-store boundary",
        "category": "Platform Boundaries",
        "status": "blocked",
        "summary": "S3/GCS/ADLS remain separate blocked/report-only routes until runtime evidence is admitted.",
        "route": "Future object-store route",
        "evidence_fields": ["credential_policy_status", "object_store_involved", "fallback_attempted=false"],
        "related_use_cases": ["object-store-boundary-report", "table-lakehouse-boundary-report"],
        "references": ["docs/use-cases/use-case-index.yml", "docs/architecture/phased-execution-plan.md"],
    },
    {
        "slug": "table-lakehouse-boundary",
        "title": "Table/lakehouse boundary",
        "category": "Platform Boundaries",
        "status": "blocked",
        "summary": "Iceberg, Delta, and Hudi table metadata/runtime/commit support are distinct gated claims.",
        "route": "Future table route",
        "evidence_fields": ["table_format_involved", "table_snapshot_id", "output_commit_status"],
        "related_use_cases": ["table-lakehouse-boundary-report", "object-store-boundary-report"],
        "references": ["docs/use-cases/use-case-index.yml", "docs/architecture/phased-execution-plan.md"],
    },
    {
        "slug": "foundry-boundary",
        "title": "Foundry boundary",
        "category": "Platform Boundaries",
        "status": "smoke_supported",
        "summary": "Local/dev-stack Foundry proof posture without production Foundry runtime or Spark substitution claims.",
        "route": "Foundry proof boundary",
        "evidence_fields": ["foundry_runtime_invoked", "foundry_spark_invoked=false", "public_foundry_claim_allowed=false"],
        "related_use_cases": ["foundry-local-proof-boundary", "source-free-generated-output-boundary"],
        "references": ["docs/foundry/proof-of-use-certification.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "deterministic-blockers",
        "title": "Deterministic blockers",
        "category": "Unsupported Diagnostics",
        "status": "runtime_supported",
        "summary": "Unsupported work must return an explicit blocker instead of silently disappearing or falling back.",
        "route": "all routes",
        "evidence_fields": ["blocker_id", "support_status", "claim_gate_status=not_claim_grade"],
        "related_use_cases": ["sql-dataframe-capability-posture", "object-store-boundary-report"],
        "references": ["docs/architecture/compute-engine-flow-reference.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "slug": "report-only",
        "title": "report_only",
        "category": "Unsupported Diagnostics",
        "status": "report_only",
        "summary": "A visible design or capability posture that is not runtime execution.",
        "route": "capability posture",
        "evidence_fields": ["support_status=report_only", "runtime_execution=false", "claim_gate_status=not_claim_grade"],
        "related_use_cases": ["sql-dataframe-capability-posture", "package-channel-readiness-boundary"],
        "references": ["docs/use-cases/use-case-index.yml", "docs/architecture/phased-execution-plan.md"],
    },
]


STATUS_ROWS: list[dict[str, Any]] = [
    {
        "capability": "Local CSV",
        "status": "smoke_supported",
        "inputs": ["local_csv"],
        "outputs": ["jsonl", "csv", "evidence"],
        "route": "direct_compatibility_transient compatibility_import_certified vortex_ingest",
        "platform": "local",
        "evidence": ["source_adapter_status", "fallback_attempted=false", "claim_gate_status"],
        "works": "Scoped local CSV smokes cover Python query-builder paths, SQL local-source smokes, certified import/stage runs, and feature-gated Vortex ingest.",
        "blocked": "Not production CSV ETL, not broad SQL/DataFrame parity, and not a performance claim.",
        "references": ["python/README.md", "docs/getting-started/examples.md"],
    },
    {
        "capability": "Local JSONL / NDJSON",
        "status": "smoke_supported",
        "inputs": ["local_jsonl", "local_ndjson"],
        "outputs": ["jsonl", "evidence"],
        "route": "direct_compatibility_transient vortex_ingest",
        "platform": "local",
        "evidence": ["source_format", "source_state_id", "fallback_attempted=false"],
        "works": "Flat local JSONL/NDJSON fixture smokes are admitted through scoped local-source and ingest paths.",
        "blocked": "Nested JSON, JSONPath, broad schema evolution, and production JSON processing remain outside the claim.",
        "references": ["docs/use-cases/use-case-index.yml", "python/README.md"],
    },
    {
        "capability": "Local JSON",
        "status": "smoke_supported",
        "inputs": ["local_json"],
        "outputs": ["jsonl", "evidence"],
        "route": "direct_compatibility_transient vortex_ingest",
        "platform": "local",
        "evidence": ["source_format", "source_adapter_status", "claim_gate_status"],
        "works": "Flat local JSON source smokes are visible through Python/local-source evidence paths.",
        "blocked": "Nested objects, arrays, JSONPath, and broad document runtime support remain blocked or report-only.",
        "references": ["python/README.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "capability": "Local Parquet",
        "status": "smoke_supported",
        "inputs": ["local_parquet"],
        "outputs": ["jsonl", "parquet", "evidence"],
        "route": "compatibility_import_certified direct_compatibility_transient",
        "platform": "local",
        "evidence": ["source_format", "feature_gate_status", "output_native_io_certificate_status"],
        "works": "Scoped Parquet fixture paths are documented where feature gates and flat scalar constraints are satisfied.",
        "blocked": "Default builds may block Parquet writes; nested/complex types and production Parquet parity require later runtime slices.",
        "references": ["docs/getting-started/certified-local-workload.md", "python/README.md"],
    },
    {
        "capability": "Arrow IPC",
        "status": "smoke_supported",
        "inputs": ["arrow_ipc"],
        "outputs": ["evidence"],
        "route": "compatibility_import_certified prepared_vortex",
        "platform": "local",
        "evidence": ["source_format", "scenario_family", "claim_gate_status"],
        "works": "Traditional benchmark fixture rows can expose Arrow IPC posture where the benchmark admits the format.",
        "blocked": "User-facing broad Arrow IPC runtime is not production-claimable.",
        "references": ["benchmarks/traditional_analytics/README.md", "docs/benchmarks/local-taxonomy-benchmark.md"],
    },
    {
        "capability": "Avro",
        "status": "smoke_supported",
        "inputs": ["avro"],
        "outputs": ["evidence"],
        "route": "compatibility_import_certified prepared_vortex",
        "platform": "local",
        "evidence": ["source_format", "scenario_family", "claim_gate_status"],
        "works": "Traditional benchmark fixture rows can expose Avro posture where the benchmark admits the format.",
        "blocked": "Broad Avro runtime, schema evolution, and production writer support are not claimed.",
        "references": ["benchmarks/traditional_analytics/README.md", "docs/benchmarks/local-taxonomy-benchmark.md"],
    },
    {
        "capability": "ORC",
        "status": "smoke_supported",
        "inputs": ["orc"],
        "outputs": ["evidence"],
        "route": "compatibility_import_certified prepared_vortex",
        "platform": "local",
        "evidence": ["source_format", "scenario_family", "claim_gate_status"],
        "works": "Traditional benchmark fixture rows can expose ORC posture where the benchmark admits the format.",
        "blocked": "Broad ORC runtime and production writer support are not claimed.",
        "references": ["benchmarks/traditional_analytics/README.md", "docs/benchmarks/local-taxonomy-benchmark.md"],
    },
    {
        "capability": "Vortex input",
        "status": "smoke_supported",
        "inputs": ["vortex"],
        "outputs": ["jsonl", "evidence"],
        "route": "prepared_vortex native_vortex",
        "platform": "local",
        "evidence": ["prepared_state_id", "execution_mode=native_vortex", "fallback_attempted=false"],
        "works": "Prepared/native Vortex benchmark and source-backed scan lanes are visible with no-fallback fields.",
        "blocked": "Broad encoded-native operator coverage and production Vortex writer support remain gated.",
        "references": ["docs/architecture/compute-engine-flow-reference.md", "docs/benchmarks/local-taxonomy-benchmark.md"],
    },
    {
        "capability": "Generated/source-free output",
        "status": "smoke_supported",
        "inputs": ["none", "generated"],
        "outputs": ["jsonl", "csv", "evidence"],
        "route": "generated_source",
        "platform": "local",
        "evidence": ["input_dataset_count=0", "generated_source_created", "output_native_io_certificate_status"],
        "works": "Python helpers can generate local reference rows, ranges, calendars, VALUES, and literal SELECT outputs.",
        "blocked": "This is not a broad SQL generator, Foundry production output, or object-store sink claim.",
        "references": ["python/README.md", "docs/foundry/proof-of-use-certification.md"],
    },
    {
        "capability": "Python",
        "status": "runtime_supported",
        "inputs": ["python"],
        "outputs": ["typed_report", "evidence"],
        "route": "python_client direct_compatibility_transient generated_source",
        "platform": "local",
        "evidence": ["protocol_version", "resolved_cli_path", "claim_gate_status"],
        "works": "The Python wrapper exposes status/capability checks and scoped local workflows through the local CLI.",
        "blocked": "It is not yet a native in-process SparkSession-equivalent runtime for all SQL/DataFrame operations.",
        "references": ["python/README.md", "docs/getting-started/first-10-minutes.md"],
    },
    {
        "capability": "SQL / DataFrame",
        "status": "report_only",
        "inputs": ["sql", "dataframe"],
        "outputs": ["diagnostic", "evidence"],
        "route": "direct_compatibility_transient report_only",
        "platform": "local",
        "evidence": ["sql_parser_executed", "runtime_execution", "claim_gate_status"],
        "works": "Scoped local CSV SQL/query-builder smokes exist for projection/filter/limit, aggregates, top-N, and a narrow join bridge.",
        "blocked": "Broad SQL/DataFrame runtime parity, catalogs, subqueries, generalized joins, and production semantics remain blocked.",
        "references": ["python/README.md", "docs/architecture/compute-engine-flow-reference.md"],
    },
    {
        "capability": "S3 / GCS / ADLS",
        "status": "blocked",
        "inputs": ["s3", "gcs", "adls"],
        "outputs": ["blocked_diagnostic"],
        "route": "object_store_report_only",
        "platform": "object_store",
        "evidence": ["credential_policy_status", "object_store_io=false", "fallback_attempted=false"],
        "works": "Object-store posture is documented as a boundary.",
        "blocked": "No object-store runtime, byte-range read, write, commit, or credentialed network path is claimed.",
        "references": ["docs/use-cases/use-case-index.yml", "docs/architecture/phased-execution-plan.md"],
    },
    {
        "capability": "Iceberg / Delta / Hudi",
        "status": "blocked",
        "inputs": ["iceberg", "delta", "hudi"],
        "outputs": ["blocked_diagnostic"],
        "route": "table_report_only",
        "platform": "table",
        "evidence": ["table_scan_status", "commit_protocol_status", "claim_gate_status=not_claim_grade"],
        "works": "Table/lakehouse metadata and runtime ladders are visible as planned boundaries.",
        "blocked": "No table runtime, merge/update/delete, commit, rollback, or lakehouse production support is claimed.",
        "references": ["docs/use-cases/use-case-index.yml", "docs/architecture/phased-execution-plan.md"],
    },
    {
        "capability": "Foundry",
        "status": "smoke_supported",
        "inputs": ["foundry"],
        "outputs": ["local_proof", "evidence"],
        "route": "foundry_dev_stack_proof",
        "platform": "foundry",
        "evidence": ["foundry_runtime_invoked", "foundry_spark_invoked=false", "public_foundry_claim_allowed=false"],
        "works": "Local/dev-stack proof docs and no-dataset/generated-output boundaries are documented.",
        "blocked": "No Foundry production runtime, package publication, or Spark-backed ShardLoom execution claim.",
        "references": ["docs/foundry/proof-of-use-certification.md", "docs/use-cases/use-case-index.yml"],
    },
    {
        "capability": "Benchmarks",
        "status": "smoke_supported",
        "inputs": ["benchmark_artifact"],
        "outputs": ["benchmark_dashboard", "evidence"],
        "route": "benchmark_publishing",
        "platform": "local",
        "evidence": ["benchmark_profile", "expected_lanes", "performance_claim_allowed=false"],
        "works": "Committed static benchmark artifacts expose lanes, missing reasons, coverage, and claim boundaries.",
        "blocked": "Benchmark evidence is not a leaderboard, public speed claim, or Spark-replacement claim.",
        "references": ["docs/benchmarks/local-taxonomy-benchmark.md", "docs/benchmarks/baseline-comparison-boundary.md"],
    },
    {
        "capability": "Package / release",
        "status": "report_only",
        "inputs": ["source_checkout"],
        "outputs": ["release_readiness_report"],
        "route": "release_readiness",
        "platform": "local",
        "evidence": ["package_install_mode", "sbom_status", "provenance_status"],
        "works": "Release dry-run and provenance docs exist for local validation.",
        "blocked": "No package publication or public install-channel claim is made.",
        "references": ["README.md", "docs/use-cases/use-case-index.yml"],
    },
]


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def text(value: Any) -> str:
    return "" if value is None else str(value)


def strip_md(value: Any) -> str:
    raw = text(value)
    raw = re.sub(r"<br\s*/?>", " - ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", raw)
    raw = re.sub(r"`([^`]+)`", r"\1", raw)
    raw = re.sub(r"\*\*([^*]+)\*\*", r"\1", raw)
    raw = re.sub(r"<[^>]+>", "", raw)
    return re.sub(r"\s+", " ", raw).strip()


def compact(value: Any, limit: int = 180) -> str:
    clean = strip_md(value)
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "..."


def code(value: Any) -> str:
    return f"<code>{esc(value)}</code>"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value.rstrip() + "\n", encoding="utf-8")


def load_use_case_index() -> dict[str, Any]:
    spec = importlib.util.spec_from_file_location("shardloom_use_case_index_checker", USE_CASE_INDEX_CHECKER)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import use-case index parser from {USE_CASE_INDEX_CHECKER}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.load_index(USE_CASE_INDEX)


def slug(value: str) -> str:
    clean = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return clean or "item"


def site_status(value: Any) -> str:
    return STATUS_LABELS.get(str(value), str(value))


def token_values(values: Any) -> str:
    if isinstance(values, list):
        parts = values
    else:
        parts = str(values).split()
    tokens = [slug(str(part)) for part in parts if str(part).strip()]
    return " ".join(tokens)


def chip(value: Any, extra_class: str = "") -> str:
    status = site_status(value)
    class_name = f"status-chip status-{slug(status)} {extra_class}".strip()
    return f'<span class="{class_name}">{esc(status)}</span>'


def link_to_reference(reference: str) -> str:
    href = f"https://github.com/depsilon/shardloom/blob/main/{reference}"
    return f'<a href="{esc(href)}">{esc(reference)}</a>'


def list_items(values: list[Any], class_name: str = "pill-list") -> str:
    items = "".join(f"<li>{esc(value)}</li>" for value in values)
    return f'<ul class="{class_name}">{items}</ul>'


def reference_block(references: list[str]) -> str:
    items = "".join(f"<li>{link_to_reference(reference)}</li>" for reference in references)
    return f'<ul class="reference-list">{items}</ul>'


def related_use_case_links(ids: list[str], use_cases: dict[str, dict[str, Any]]) -> str:
    links = []
    for use_case_id in ids:
        use_case = use_cases.get(use_case_id)
        label = use_case.get("title", use_case_id) if use_case else use_case_id
        links.append(f'<a href="/use-cases/{esc(use_case_id)}">{esc(label)}</a>')
    return '<div class="related-links">' + "".join(links) + "</div>"


def filter_controls(filters: list[tuple[str, str, list[str]]]) -> str:
    rendered = ['<div class="filter-bar" aria-label="Filter content">']
    rendered.append(
        '<label>Search<span><input data-filter="search" type="search" placeholder="Search routes, evidence, blockers..." autocomplete="off"></span></label>'
    )
    for key, label, values in filters:
        options = ['<option value="">All</option>']
        for value in values:
            options.append(f'<option value="{esc(slug(value))}">{esc(value)}</option>')
        rendered.append(f'<label>{esc(label)}<span><select data-filter="{esc(key)}">{"".join(options)}</select></span></label>')
    rendered.append('<p class="filter-count" data-filter-count></p>')
    rendered.append("</div>")
    return "".join(rendered)


def nav(active: str) -> str:
    links = [
        ("Home", "/", "home"),
        ("Start", "/start", "start"),
        ("Field Guide", "/field-guide", "field-guide"),
        ("Use Cases", "/use-cases", "use-cases"),
        ("Benchmarks", "/benchmarks", "benchmarks"),
        ("Architecture", "/architecture", "architecture"),
        ("Status", "/status", "status"),
        ("GitHub", "https://github.com/depsilon/shardloom", "github"),
    ]
    rendered = []
    for label, href, key in links:
        class_name = ' class="active" aria-current="page"' if key == active else ""
        rendered.append(f'<a{class_name} href="{href}">{label}</a>')
    return "\n          ".join(rendered)


def page(title: str, description: str, body: str, active: str, canonical_path: str = "") -> str:
    canonical_url = f"https://shardloom.io/{canonical_path}".rstrip("/")
    if canonical_path == "":
        canonical_url = "https://shardloom.io/"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <meta name="description" content="{esc(description)}">
  <meta name="robots" content="index,follow">
  <link rel="canonical" href="{esc(canonical_url)}">
  <link rel="icon" type="image/png" href="/assets/logo/shardloom-favicon.png">
  <link rel="apple-touch-icon" href="/assets/logo/shardloom-favicon.png">
  <link rel="stylesheet" href="/assets/site.css">
  <script src="/assets/site.js" defer></script>
  <meta property="og:title" content="{esc(title)}">
  <meta property="og:description" content="{esc(description)}">
  <meta property="og:image" content="https://shardloom.io/assets/logo/shardloom-logo.png">
  <meta property="og:type" content="website">
  <meta property="og:url" content="{esc(canonical_url)}">
  <meta name="twitter:card" content="summary_large_image">
</head>
<body>
  <header class="site-header">
    <a class="brand" href="/" aria-label="ShardLoom home">
      <img src="/assets/logo/shardloom-favicon.png" alt="" width="40" height="40" aria-hidden="true">
      <span>ShardLoom</span>
    </a>
    <nav aria-label="Primary">
      {nav(active)}
    </nav>
  </header>
  <main>
{body}
  </main>
  <footer class="site-footer">
    <img src="/assets/logo/shardloom-logo-trim.png" alt="ShardLoom">
    <p>Pre-release technical preview. Vortex-first. No fallback. Benchmark evidence is not a public speed or production claim.</p>
  </footer>
</body>
</html>"""


def card(title: str, body: str, badge: str | None = None) -> str:
    badge_html = f'<span class="badge">{esc(badge)}</span>' if badge else ""
    return f"""<article class="card">
      {badge_html}
      <h3>{esc(title)}</h3>
      <p>{body}</p>
    </article>"""


def metric(label: str, value: Any, detail: str = "") -> str:
    detail_html = f"<span>{esc(detail)}</span>" if detail else ""
    return f"""<div class="metric">
      <strong>{esc(value)}</strong>
      <span>{esc(label)}</span>
      {detail_html}
    </div>"""


def table(headers: list[str], rows: list[list[Any]], class_name: str = "") -> str:
    head = "".join(f"<th>{esc(header)}</th>" for header in headers)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{esc(cell)}</td>" for cell in row) + "</tr>")
    return (
        f'<div class="table-wrap {class_name}"><table>'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table></div>"
    )


def details(summary: str, inner: str) -> str:
    return f"""<details class="drawer">
      <summary>{esc(summary)}</summary>
      {inner}
    </details>"""


def split_table_row(line: str) -> list[str]:
    return [strip_md(cell) for cell in line.strip().strip("|").split("|")]


def table_after(markdown: str, header_start: str) -> list[list[str]]:
    start = markdown.find(header_start)
    if start < 0:
        return []
    rows: list[list[str]] = []
    for line in markdown[start:].splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            if rows:
                break
            continue
        if re.match(r"^\|\s*-", stripped):
            continue
        rows.append(split_table_row(stripped))
    return rows[1:]


def code_block_after(markdown: str, marker: str) -> str:
    start = markdown.find(marker)
    if start < 0:
        return ""
    fence = markdown.find("```", start)
    if fence < 0:
        return ""
    body_start = markdown.find("\n", fence)
    body_end = markdown.find("```", body_start + 1)
    if body_start < 0 or body_end < 0:
        return ""
    return markdown[body_start + 1 : body_end].strip()


def mermaid_blocks(markdown: str) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    current_heading = "Architecture diagram"
    lines = markdown.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.startswith("## "):
            current_heading = strip_md(line.lstrip("# "))
        if line.strip() == "```mermaid":
            block_lines: list[str] = []
            index += 1
            while index < len(lines) and lines[index].strip() != "```":
                block_lines.append(lines[index])
                index += 1
            blocks.append((current_heading, "\n".join(block_lines).strip()))
        index += 1
    return blocks


def route_steps() -> str:
    steps = [
        ("Front door", "Python, SQL, CLI, benchmarks, or an adapter express the work."),
        ("UniversalIngress", "The source is admitted, classified, or blocked with a reason."),
        ("SourceState", "Schema, fingerprint, adapter status, and source evidence become reusable state."),
        ("vortex_ingest", "Admitted non-Vortex data is prepared into VortexPreparedState."),
        ("Execution", "prepared_vortex, native_vortex, certified cold route, direct one-shot, or generated source."),
        ("OutputPlan", "Result, local sink, Vortex artifact, or future platform sink is planned separately."),
        ("Evidence", "Certificates, no-fallback fields, materialization boundaries, timing, and claim gate."),
    ]
    return "".join(
        f"""<article class="route-step">
          <span>{number:02d}</span>
          <h3>{esc(title)}</h3>
          <p>{esc(detail)}</p>
        </article>"""
        for number, (title, detail) in enumerate(steps, start=1)
    )


def benchmark_summary() -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_json(BENCHMARK_MANIFEST)
    results = load_json(BENCHMARK_RESULTS)
    return manifest, results


def lane_rows(manifest: dict[str, Any]) -> list[list[str]]:
    expected = manifest.get("expected_lanes", [])
    available = set(manifest.get("available_lanes", []))
    missing = set(manifest.get("missing_lanes", []))
    versions = manifest.get("lane_versions", {})
    reasons = manifest.get("lane_availability_reasons", {})
    rows = []
    for lane in expected:
        status = "available" if lane in available else "missing" if lane in missing else "not listed"
        version_or_reason = versions.get(lane) or reasons.get(lane) or "not reported"
        rows.append([lane, status, version_or_reason])
    return rows


def comparative_rows(results: dict[str, Any]) -> list[list[Any]]:
    overview = results.get("comparative_dashboard", {}).get("engine_timing_overview", {})
    rows = overview.get("rows", []) if isinstance(overview, dict) else []
    return [[strip_md(cell) for cell in row] for row in rows]


def claim_gate_rows(results: dict[str, Any]) -> list[list[Any]]:
    distribution = results.get("comparative_dashboard", {}).get("claim_gate_distribution", {})
    rows = distribution.get("rows", []) if isinstance(distribution, dict) else []
    return [[strip_md(cell) for cell in row] for row in rows]


def timing_rows(results: dict[str, Any]) -> list[list[Any]]:
    rows = []
    for row in results.get("rows", []):
        rows.append(
            [
                row.get("scenario", ""),
                row.get("selected_execution_mode", ""),
                row.get("storage_format", ""),
                row.get("total_runtime_millis", ""),
                row.get("vortex_scan_millis", ""),
                row.get("operator_compute_millis", ""),
                row.get("claim_gate_status", ""),
            ]
        )
    return rows


def source_state_coverage_rows(results: dict[str, Any]) -> list[list[Any]]:
    rows = []
    for batch in results.get("batch_rows", []):
        rows.append(
            [
                batch.get("scenario", "prepared/native batch"),
                batch.get("source_state_coverage_all_requested_scenarios_classified", ""),
                batch.get("source_state_coverage_reused_scenario_count", ""),
                batch.get("source_state_coverage_not_needed_scenario_count", ""),
                batch.get("source_state_digest_status", ""),
                batch.get("source_state_coverage_matrix_ref", ""),
            ]
        )
    return rows


def home_page(manifest: dict[str, Any], results: dict[str, Any]) -> str:
    available_count = len(manifest.get("available_lanes", []))
    expected_count = len(manifest.get("expected_lanes", []))
    generated = manifest.get("generated_at_utc", "unknown")
    start_href = "/start"
    field_guide_href = "/field-guide"
    python_example = """import shardloom as sl

ctx = sl.context()
report = (
    ctx.read_csv("orders.csv")
       .select(["order_id", "amount", "status"])
       .where(sl.col("status") == "paid")
       .limit(100)
       .write_jsonl("out/paid-orders.jsonl")
)

print(report.claim_gate_status)
print(report.fallback_attempted)"""
    body = f"""
    <section class="hero console-hero">
      <div class="hero-copy">
        <p class="eyebrow">Pre-release technical preview</p>
        <h1>Evidence-gated compute over Vortex-prepared data.</h1>
        <p class="lede">ShardLoom prepares admitted inputs, runs prepared/native workflows, writes outputs, and emits evidence proving what happened without hiding unsupported work behind fallback engines.</p>
        <div class="status-chips" aria-label="ShardLoom public status">
          <span>technical preview</span>
          <span>no fallback</span>
          <span>Vortex-first</span>
          <span>claim-gated</span>
          <span>local-first</span>
          <span>not production claim</span>
        </div>
        <div class="actions">
          <a class="button primary" href="{start_href}">Start local proof</a>
          <a class="button" href="{field_guide_href}">Read Field Guide</a>
          <a class="button" href="/benchmarks">View benchmark evidence</a>
          <a class="button ghost" href="https://github.com/depsilon/shardloom">Open GitHub</a>
        </div>
      </div>
      <div class="route-console" aria-label="ShardLoom route and evidence console">
        <div class="console-brand">
          <img src="/assets/logo/shardloom-logo-trim.png" alt="ShardLoom">
          <span>Evidence Console</span>
        </div>
        <div class="pipeline" aria-label="Compute route">
          <span>Source</span>
          <span>UniversalIngress</span>
          <span>vortex_ingest</span>
          <span>VortexPreparedState</span>
          <span>Execution</span>
          <span>OutputPlan</span>
          <span>Evidence</span>
          <span>ClaimGate</span>
        </div>
        <div class="evidence-grid">
          <div><span>selected_execution_mode</span><strong>prepared_vortex</strong></div>
          <div><span>vortex_ingest_status</span><strong>prepared_state_created</strong></div>
          <div><span>prepared_state_reuse_hit</span><strong>true</strong></div>
          <div><span>fallback_attempted</span><strong>false</strong></div>
          <div><span>external_engine_invoked</span><strong>false</strong></div>
          <div><span>claim_gate_status</span><strong>fixture_smoke_only</strong></div>
        </div>
        <p class="console-note">Prepared rows begin after <code>VortexPreparedState</code> exists. Certified import/stage rows include source read, parse, ingest, write/reopen, compute, output, and evidence.</p>
      </div>
    </section>

    <section class="strip">
      {metric("Benchmark lanes", f"{available_count} of {expected_count}", "full_local artifact")}
      {metric("Performance claim", "none", "evidence only")}
      {metric("Fallback policy", "no fallback", "external engines are baselines")}
      {metric("Artifact refreshed", generated[:10], "UTC")}
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">Why it exists</p>
        <h2>Make compute routes inspectable.</h2>
        <p>ShardLoom is being built for workflows where it matters whether a row was parsed, prepared, scanned, decoded, materialized, written, replayed, or blocked. The public surface should show those boundaries instead of hiding them behind a broad engine label.</p>
      </div>
      <div class="card-grid">
        {card("Normal engines hide what happened", "ShardLoom emits evidence fields for execution mode, source state, materialization, output, fallback status, and claim gate.", "Evidence")}
        {card("Benchmarks blur setup and query time", "ShardLoom separates certified cold ingest/stage timing from prepared warm query timing and native Vortex timing.", "Timing")}
        {card("Compatibility paths can become fallback traps", "ShardLoom keeps unsupported paths visible and blocks deterministically instead of delegating execution elsewhere.", "No fallback")}
      </div>
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">Route model</p>
        <h2>The front door is not the execution route.</h2>
        <p>Users can enter through Python, SQL, CLI, benchmarks, or future adapters. ShardLoom still records the source route, ingress route, preparation route, execution route, output route, and evidence route separately.</p>
      </div>
      <div class="route-card-grid">
        {card("Certified import/stage", "<code>compatibility_import_certified</code><br>Cold audited ingest/stage route. Not pure query speed.", "cold route")}
        {card("Prepare Vortex once", "<code>vortex_ingest</code><br>Admitted non-Vortex source to <code>VortexPreparedState</code>.", "prepare once")}
        {card("Prepared Vortex", "<code>prepared_vortex</code><br>Warm route that executes from prepared state.", "warm route")}
        {card("Native Vortex", "<code>native_vortex</code><br>Already-Vortex input with no compatibility parse/import.", "native")}
        {card("Generated source", "<code>GeneratedSourceCertificate</code><br>No input dataset; generated rows still need output evidence.", "source-free")}
        {card("Direct one-shot", "<code>direct_compatibility_transient</code><br>Quick local route, not Vortex-native.", "direct")}
      </div>
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">Try it</p>
        <h2>Code first, evidence next.</h2>
        <p>The supported local surface is intentionally scoped. The important part is that successful and blocked paths both report what happened, what was not attempted, and what claim gate applies.</p>
      </div>
      <div class="code-panel">
        <div class="panel-label">Python local smoke shape</div>
        <pre><code>{esc(python_example)}</code></pre>
        <div class="code-evidence">
          <span><strong>fallback_attempted</strong>false</span>
          <span><strong>external_engine_invoked</strong>false</span>
          <span><strong>claim_gate_status</strong>fixture_smoke_only</span>
        </div>
      </div>
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">Can I use it?</p>
        <h2>Status stays visible.</h2>
        <p>The rebuilt site will keep supported, smoke-supported, report-only, blocked, unsupported, and not-planned paths in the same place so users do not have to infer maturity from scattered docs.</p>
      </div>
      <div class="status-preview">
        <div><strong>Local CSV</strong><span>smoke/runtime supported</span></div>
        <div><strong>Local JSONL/NDJSON</strong><span>smoke supported for flat scalar rows</span></div>
        <div><strong>Local Parquet</strong><span>feature-gated scoped support</span></div>
        <div><strong>Vortex input</strong><span>prepared/native runtime direction</span></div>
        <div><strong>S3/GCS/ADLS</strong><span>blocked/report-only</span></div>
        <div><strong>Foundry</strong><span>local proof boundary only</span></div>
      </div>
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">Benchmark evidence</p>
        <h2>Evidence, not a leaderboard.</h2>
        <p>The benchmark page should answer which route was measured, which lanes were expected, which lanes were available, and whether any row is claim-grade. Raw timing tables belong behind interpretation.</p>
        <div class="actions">
          <a class="button primary" href="/benchmarks">Open benchmark evidence</a>
          <a class="button" href="/compute-engine-flow">Open architecture map</a>
        </div>
      </div>
      <div class="benchmark-preview">
        <div><span>benchmark_profile</span><strong>{esc(manifest.get("benchmark_profile", "unknown"))}</strong></div>
        <div><span>available_lanes</span><strong>{available_count} / {expected_count}</strong></div>
        <div><span>performance_claim_allowed</span><strong>false</strong></div>
        <div><span>claim_boundary</span><strong>local evidence only</strong></div>
      </div>
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">Claim boundary</p>
        <h2>What this site will not imply.</h2>
        <p>ShardLoom is pre-release. The website can make the system easier to understand, but it cannot upgrade runtime, benchmark, package, or platform claims without evidence.</p>
      </div>
      <div class="boundary-list">
        <span>No performance or superiority claim</span>
        <span>No Apache Spark substitute claim</span>
        <span>No production SQL/DataFrame claim</span>
        <span>No production object-store/lakehouse/Foundry claim</span>
        <span>No hidden fallback engine</span>
      </div>
    </section>
"""
    return page(
        "ShardLoom",
        "Evidence-gated compute over Vortex-prepared data with no fallback, route-level evidence, and claim-safe benchmark interpretation.",
        body,
        "home",
    )


def use_case_maps(index: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    use_cases = {case["id"]: case for case in index.get("use_cases", [])}
    families = {family["id"]: family["title"] for family in index.get("capability_families", [])}
    return use_cases, families


def start_page(index: dict[str, Any]) -> str:
    use_cases, _ = use_case_maps(index)
    start = use_cases["first-10-minutes-local-smoke"]
    body = f"""
    <section class="page-hero">
      <p class="eyebrow">Start</p>
      <h1>Run the local proof first.</h1>
      <p class="lede">The first ShardLoom experience should be a small source-checkout smoke that reports status and evidence without pretending to be production runtime, package publication, or a speed claim.</p>
      <div class="actions">
        <a class="button primary" href="/use-cases/{esc(start["id"])}">Open the full recipe</a>
        <a class="button" href="/field-guide/no-fallback">Understand no fallback</a>
        <a class="button" href="/status">Check support status</a>
      </div>
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">Quick command</p>
        <h2>{esc(start["title"])}</h2>
        <p>{esc(start["claim_boundary"])}</p>
      </div>
      <div class="code-panel">
        <div class="panel-label">PowerShell</div>
        <pre><code>{esc(start["runnable_example"])}</code></pre>
        <div class="code-evidence">
          <span><strong>fallback_attempted</strong>false</span>
          <span><strong>external_engine_invoked</strong>false</span>
          <span><strong>surface</strong>local checkout</span>
        </div>
      </div>
    </section>

    <section class="section-grid">
      <div>
        <h2>What you get</h2>
        <p>{esc(start["expected_output_evidence"])}</p>
      </div>
      <div>
        <h3>Evidence fields</h3>
        {list_items(start["evidence_fields"])}
      </div>
    </section>
"""
    return page(
        "ShardLoom Start",
        "First ShardLoom local proof command and evidence boundary.",
        body,
        "start",
        "start",
    )


def field_guide_page(index: dict[str, Any]) -> str:
    use_cases, _ = use_case_maps(index)
    categories = list(dict.fromkeys(term["category"] for term in FIELD_GUIDE_TERMS))
    filters = [
        ("category", "Category", categories),
        ("status", "Status", sorted({site_status(term["status"]) for term in FIELD_GUIDE_TERMS})),
    ]
    toc = "".join(
        f'<a href="#{esc(slug(category))}">{esc(category)}</a>'
        for category in categories
    )
    reading_paths = [
        ("New to ShardLoom", ["what-is-shardloom", "evidence-gated-compute", "no-fallback"]),
        ("Run a local workflow", ["source-state", "vortex-ingest", "prepared-vortex", "output-plan"]),
        ("Understand benchmarks", ["benchmark-evidence", "certified-cold-route", "prepared-warm-route", "external-baseline-only"]),
        ("Know what is blocked", ["deterministic-blockers", "report-only", "object-store-boundary", "table-lakehouse-boundary"]),
    ]
    reading_html = "".join(
        f"""<article class="mini-card">
          <h3>{esc(title)}</h3>
          <div class="related-links">{"".join(f'<a href="/field-guide/{esc(term)}">{esc(next(t["title"] for t in FIELD_GUIDE_TERMS if t["slug"] == term))}</a>' for term in terms)}</div>
        </article>"""
        for title, terms in reading_paths
    )
    sections = []
    for category in categories:
        rows = []
        for term in [term for term in FIELD_GUIDE_TERMS if term["category"] == category]:
            rows.append(
                f"""<a class="term-row" href="/field-guide/{esc(term["slug"])}" data-filter-card
                    data-category="{esc(slug(term["category"]))}" data-status="{esc(slug(site_status(term["status"])))}">
                  <span>{esc(term["title"])}</span>
                  <p>{esc(term["summary"])}</p>
                  <small>{chip(term["status"])}<em>{esc(term["route"])}</em><em>{esc(", ".join(term["evidence_fields"][:3]))}</em></small>
                </a>"""
            )
        sections.append(
            f"""<section id="{esc(slug(category))}" class="atlas-section">
              <div class="section-heading">
                <h2>{esc(category)}</h2>
                <p>{len(rows)} terms</p>
              </div>
              <div class="term-list">{"".join(rows)}</div>
            </section>"""
        )
    body = f"""
    <section class="page-hero">
      <p class="eyebrow">Field Guide</p>
      <h1>A compact atlas for ShardLoom vocabulary.</h1>
      <p class="lede">Start with the words users see in evidence, benchmarks, and status rows: routes, source state, Vortex ingest, claim gates, outputs, and blocked platform boundaries.</p>
    </section>

    <section class="atlas-shell" data-filter-scope>
      <aside class="atlas-rail">
        <h2>Contents</h2>
        <nav class="toc-list" aria-label="Field Guide categories">{toc}</nav>
        <h3>Reading paths</h3>
        <div class="mini-card-grid">{reading_html}</div>
      </aside>
      <div class="atlas-main">
        {filter_controls(filters)}
        {"".join(sections)}
      </div>
    </section>
"""
    return page(
        "ShardLoom Field Guide",
        "Dense ShardLoom field guide for routes, evidence fields, Vortex ingest, and claim gates.",
        body,
        "field-guide",
        "field-guide",
    )


def field_guide_term_page(term: dict[str, Any], index: dict[str, Any]) -> str:
    use_cases, _ = use_case_maps(index)
    fields = term["evidence_fields"]
    references = term["references"]
    related = term["related_use_cases"]
    body = f"""
    <section class="page-hero dossier-hero">
      <p class="eyebrow">{esc(term["category"])}</p>
      <h1>{esc(term["title"])}</h1>
      <p class="lede">{esc(term["summary"])}</p>
      <div class="status-chips">{chip(term["status"])}<span>{esc(term["route"])}</span></div>
    </section>

    <section class="dossier-grid">
      <article class="dossier-card">
        <h2>Plain-English meaning</h2>
        <p>{esc(term["summary"])}</p>
      </article>
      <article class="dossier-card">
        <h2>Why it matters</h2>
        <p>ShardLoom uses this concept to keep user-facing workflows separate from internal route, evidence, and claim-gate mechanics.</p>
      </article>
      <article class="dossier-card">
        <h2>How ShardLoom uses it</h2>
        <p>The term appears in route evidence, benchmark interpretation, or status diagnostics so unsupported work remains visible instead of becoming hidden fallback execution.</p>
      </article>
      <article class="dossier-card">
        <h2>Current support</h2>
        <p>{chip(term["status"])} <span class="inline-note">Status is documentation-level unless the linked use case has runnable evidence.</span></p>
      </article>
    </section>

    <section class="section-grid">
      <div>
        <h2>Evidence fields</h2>
        {list_items(fields)}
      </div>
      <div>
        <h2>What it does not claim</h2>
        <ul class="boundary-list as-list">
          <li>No production support expansion.</li>
          <li>No performance, superiority, or Spark-replacement claim.</li>
          <li>No external engine fallback.</li>
        </ul>
      </div>
    </section>

    <section class="section-grid">
      <div>
        <h2>Try it / related use cases</h2>
        {related_use_case_links(related, use_cases)}
      </div>
      <div>
        <h2>Reference files</h2>
        {reference_block(references)}
      </div>
    </section>

    <section>
      <h2>Related concepts</h2>
      <div class="related-links">
        <a href="/field-guide">Field Guide index</a>
        <a href="/use-cases">Use Case browser</a>
        <a href="/status">Status matrix</a>
      </div>
    </section>
"""
    return page(
        f"ShardLoom Field Guide - {term['title']}",
        compact(term["summary"], 150),
        body,
        "field-guide",
        f"field-guide/{term['slug']}",
    )


def use_cases_page(index: dict[str, Any]) -> str:
    use_cases, families = use_case_maps(index)
    all_cases = list(use_cases.values())
    filters = [
        ("status", "Status", sorted({site_status(case["status"]) for case in all_cases})),
        ("input", "Input", sorted({str(value) for case in all_cases for value in case.get("inputs", [])})[:60]),
        ("output", "Output", sorted({str(value) for case in all_cases for value in case.get("outputs", [])})[:60]),
        ("route", "Route", sorted({str(case.get("execution_mode", "")) for case in all_cases})),
    ]
    cards = []
    for case in all_cases:
        site = site_status(case["status"])
        blocker = case.get("blocked_explanation") or case.get("runnable_example") or ""
        cards.append(
            f"""<article class="use-case-card" data-filter-card
                data-status="{esc(slug(site))}"
                data-input="{esc(token_values(case.get("inputs", [])))}"
                data-output="{esc(token_values(case.get("outputs", [])))}"
                data-route="{esc(slug(str(case.get("execution_mode", ""))))}">
              <div class="card-top">{chip(case["status"])}<span>{esc(families.get(case["capability_family"], case["capability_family"]))}</span></div>
              <h3><a href="/use-cases/{esc(case["id"])}">{esc(case["title"])}</a></h3>
              <p>{esc(compact(case["claim_boundary"], 210))}</p>
              <dl class="mini-meta">
                <div><dt>Route</dt><dd>{esc(case["execution_mode"])}</dd></div>
                <div><dt>Evidence</dt><dd>{esc(", ".join(str(v) for v in case.get("evidence_fields", [])[:3]))}</dd></div>
              </dl>
              <p class="blocked-note">{esc(compact(blocker, 180))}</p>
            </article>"""
        )
    body = f"""
    <section class="page-hero">
      <p class="eyebrow">Use Cases</p>
      <h1>Can ShardLoom do my thing?</h1>
      <p class="lede">Browse supported, smoke-supported, report-only, blocked, and unsupported workflows without reading the full phase plan.</p>
    </section>

    <section data-filter-scope>
      {filter_controls(filters)}
      <div class="use-case-grid">{"".join(cards)}</div>
    </section>
"""
    return page(
        "ShardLoom Use Cases",
        "Filterable ShardLoom use-case browser with evidence fields, routes, blockers, and claim boundaries.",
        body,
        "use-cases",
        "use-cases",
    )


def use_case_page(case: dict[str, Any], index: dict[str, Any]) -> str:
    use_cases, families = use_case_maps(index)
    quick = case.get("runnable_example") or case.get("blocked_explanation") or "No runnable command is currently admitted."
    quick_label = "Runnable example" if case.get("runnable_example") else "Blocked explanation"
    related = related_use_case_links(case.get("related_use_cases", []), use_cases)
    body = f"""
    <section class="page-hero dossier-hero">
      <p class="eyebrow">{esc(families.get(case["capability_family"], case["capability_family"]))}</p>
      <h1>{esc(case["title"])}</h1>
      <p class="lede">{esc(case["claim_boundary"])}</p>
      <div class="status-chips">{chip(case["status"])}<span>{esc(case["execution_mode"])}</span><span>{esc(case["engine_mode"])}</span></div>
    </section>

    <section class="section-grid">
      <div>
        <h2>{esc(quick_label)}</h2>
        <p>{esc(case.get("expected_output_evidence", ""))}</p>
      </div>
      <div class="code-panel">
        <div class="panel-label">{esc(quick_label)}</div>
        <pre><code>{esc(quick)}</code></pre>
      </div>
    </section>

    <section class="dossier-grid">
      <article class="dossier-card">
        <h2>Inputs</h2>
        {list_items(case.get("inputs", []))}
      </article>
      <article class="dossier-card">
        <h2>Outputs</h2>
        {list_items(case.get("outputs", []))}
      </article>
      <article class="dossier-card">
        <h2>Evidence fields</h2>
        {list_items(case.get("evidence_fields", []))}
      </article>
      <article class="dossier-card">
        <h2>Common mistakes</h2>
        {list_items(case.get("common_mistakes", []))}
      </article>
    </section>

    <section class="section-grid">
      <div>
        <h2>Claim boundary</h2>
        <p>{esc(case["claim_boundary"])}</p>
      </div>
      <div>
        <h2>Reference files</h2>
        {reference_block(case.get("references", []))}
      </div>
    </section>

    <section>
      <h2>Related use cases</h2>
      {related}
    </section>
"""
    return page(
        f"ShardLoom Use Case - {case['title']}",
        compact(case["claim_boundary"], 150),
        body,
        "use-cases",
        f"use-cases/{case['id']}",
    )


def status_page() -> str:
    filters = [
        ("status", "Status", sorted({site_status(row["status"]) for row in STATUS_ROWS})),
        ("input", "Input", sorted({str(value) for row in STATUS_ROWS for value in row.get("inputs", [])})),
        ("output", "Output", sorted({str(value) for row in STATUS_ROWS for value in row.get("outputs", [])})),
        ("route", "Route", sorted({str(value) for row in STATUS_ROWS for value in str(row.get("route", "")).split()})),
        ("platform", "Platform", sorted({str(row.get("platform", "")) for row in STATUS_ROWS})),
    ]
    cards = []
    for row in STATUS_ROWS:
        cards.append(
            f"""<article class="status-row-card" data-filter-card
                data-status="{esc(slug(site_status(row["status"])))}"
                data-input="{esc(token_values(row.get("inputs", [])))}"
                data-output="{esc(token_values(row.get("outputs", [])))}"
                data-route="{esc(token_values(str(row.get("route", "")).split()))}"
                data-platform="{esc(slug(str(row.get("platform", ""))))}">
              <div class="card-top">{chip(row["status"])}<span>{esc(row["platform"])}</span></div>
              <h3>{esc(row["capability"])}</h3>
              <p><strong>What works:</strong> {esc(row["works"])}</p>
              <p><strong>Blocked:</strong> {esc(row["blocked"])}</p>
              <dl class="mini-meta">
                <div><dt>Route</dt><dd>{esc(row["route"])}</dd></div>
                <div><dt>Evidence</dt><dd>{esc(", ".join(row["evidence"]))}</dd></div>
              </dl>
              <div class="reference-inline">{reference_block(row["references"])}</div>
            </article>"""
        )
    body = f"""
    <section class="page-hero">
      <p class="eyebrow">Status</p>
      <h1>Support status stays visible.</h1>
      <p class="lede">Blocked and report-only rows are part of the product surface. They keep ShardLoom from implying support before evidence exists.</p>
    </section>

    <section data-filter-scope>
      {filter_controls(filters)}
      <div class="status-matrix">{"".join(cards)}</div>
    </section>
"""
    return page(
        "ShardLoom Status",
        "Filterable ShardLoom support matrix for inputs, outputs, routes, platforms, and evidence.",
        body,
        "status",
        "status",
    )


def benchmarks_page(manifest: dict[str, Any], results: dict[str, Any]) -> str:
    raw_timing = comparative_rows(results)
    raw_timing_table = table(
        ["Engine", "Available", "Success / total", "Geomean", "CSV/Parquet", "local fastest count", "local timing context"],
        raw_timing,
    )
    claim_distribution = table(["Claim gate", "Rows", "Share"], claim_gate_rows(results))
    lane_table = table(["Expected lane", "Status", "Version / reason"], lane_rows(manifest))
    source_state_table = table(
        [
            "Scenario",
            "source_state_coverage_all_requested_scenarios_classified",
            "source_state_coverage_reused_scenario_count",
            "source-state-not-needed",
            "source_state_digest_status",
            "Reference",
        ],
        source_state_coverage_rows(results),
    )
    shardloom_rows = table(
        ["Scenario", "Mode", "Format", "Total ms", "Scan ms", "Compute ms", "Claim gate"],
        timing_rows(results),
    )
    source_rows = table(
        ["Scenario", "Provider", "Rows scanned", "Projected columns", "Materialized", "Native I/O", "Claim"],
        [
            [
                row.get("scenario", ""),
                row.get("provider", ""),
                row.get("rows_scanned", ""),
                row.get("projected_columns", ""),
                row.get("data_materialized", ""),
                row.get("native_io", ""),
                row.get("claim_gate", ""),
            ]
            for row in results.get("source_backed_scan_rows", [])
        ],
    )
    encoded_rows = table(
        ["Scenario", "Status", "Encoding summary", "Selected rows", "Claim allowed"],
        [
            [
                row.get("scenario", ""),
                row.get("status", ""),
                row.get("encoding_summary", ""),
                row.get("selected_rows", ""),
                row.get("claim_allowed", ""),
            ]
            for row in results.get("encoded_predicate_provider_rows", [])
        ],
    )
    body = f"""
    <section class="page-hero">
      <p class="eyebrow">Benchmark evidence</p>
      <h1>Evidence, not a leaderboard.</h1>
      <p class="lede">These artifacts explain what ShardLoom measured locally: workflow coverage, prepared/native direction, no-fallback evidence, and claim boundaries. They do not claim public speed, superiority, production readiness, or Apache Spark substitution.</p>
    </section>

    <section class="strip">
      {metric("Profile", manifest.get("benchmark_profile", "unknown"), manifest.get("artifact_status", ""))}
      {metric("Available lanes", len(manifest.get("available_lanes", [])), f"of {len(manifest.get('expected_lanes', []))} expected")}
      {metric("Missing lanes", len(manifest.get("missing_lanes", [])), "shown, never hidden")}
      {metric("Performance claim", "not allowed", "claim gate closed")}
    </section>

    <section class="section-grid">
      <div>
        <h2>How to read this page</h2>
        <p>Compare ShardLoom routes with each other first: certified cold route, prepared warm route, native Vortex route, and source-backed scan evidence. External engines provide local baseline context only.</p>
      </div>
      <div class="card-grid">
        {card("Certified cold route", "Compatibility import rows include ingress, parse, Vortex ingest, write/reopen, scan, result sink, and evidence work.", "Do compare")}
        {card("Prepared warm route", "Prepared/native rows are the runtime-development direction after Vortex preparation exists.", "Do compare")}
        {card("External engines", "Pandas, Polars, DuckDB, DataFusion, Dask, and Spark rows are baseline context, not ShardLoom evidence gates.", "Do not rank")}
      </div>
    </section>

    <section>
      <h2>Artifact lane availability</h2>
      <p class="narrow">The website renders a committed artifact. It does not discover installed Python libraries during page render.</p>
      {lane_table}
    </section>

    <section>
      <h2>Claim-gate distribution</h2>
      {claim_distribution}
    </section>

    <section>
      <h2>Prepared/native source-state coverage</h2>
      <p class="narrow">The committed artifact keeps source-state reuse evidence visible for the prepared/native batch path. This remains evidence context, not a performance claim.</p>
      {source_state_table}
    </section>

    <section>
      <h2>Local timing context</h2>
      <p class="narrow">This table is timing context for engineering interpretation. It is not a public ranking.</p>
      {raw_timing_table}
    </section>

    <section>
      <h2>ShardLoom timing rows</h2>
      {details("Open scoped ShardLoom timing rows", shardloom_rows)}
      {details("Open source-backed scan evidence", source_rows)}
      {details("Open encoded predicate evidence", encoded_rows)}
    </section>
"""
    return page(
        "ShardLoom Benchmark Evidence",
        "Claim-safe local benchmark evidence for ShardLoom, framed as evidence rather than a leaderboard.",
        body,
        "benchmarks",
        "benchmarks",
    )


def compute_flow_page(markdown: str, canonical_path: str = "compute-engine-flow") -> str:
    mode_rows = table_after(markdown, "| Mode | User-facing label | What it means | Primary use |")
    mode_table = table(
        ["Mode", "Label", "Meaning", "Primary use", "Vortex-native claim?", "Claim posture"],
        mode_rows,
    )
    timing_fields = [
        line.strip()
        for line in code_block_after(markdown, "Mode timing fields must stay visible:").splitlines()
        if line.strip()
    ]
    timing_list = "".join(f"<li>{code(field)}</li>" for field in timing_fields)
    never_block = code_block_after(markdown, "## What Should Never Happen")
    never_items = [strip_md(line) for line in never_block.splitlines() if line.strip()][:8]
    never_list = "".join(f"<li>{esc(item)}</li>" for item in never_items)
    diagram_drawers = "".join(
        details(f"Raw Mermaid source: {heading}", f"<pre><code>{esc(block)}</code></pre>")
        for heading, block in mermaid_blocks(markdown)[:8]
    )
    body = f"""
    <section class="page-hero">
      <p class="eyebrow">Compute-flow translation</p>
      <h1>SQL and Python are front doors. The route is the contract.</h1>
      <p class="lede">ShardLoom separates user surface from execution route: source admission, Vortex preparation, execution mode, output route, and evidence policy are all explicit.</p>
    </section>

    <section class="route">{route_steps()}</section>

    <section class="section-grid">
      <div>
        <h2>Prepared Vortex means prepared state.</h2>
        <p><code>prepared_vortex</code> executes from <code>VortexPreparedState</code>. Non-Vortex data reaches it only after <code>UniversalIngress</code> and <code>vortex_ingest</code>. Compatibility import is the certified cold route, not pure query speed.</p>
      </div>
      <div class="card-grid">
        {card("Source route", "What kind of input is this, and is it admitted or blocked?", "Ingress")}
        {card("Preparation route", "Does this create or reuse SourceState and VortexPreparedState?", "vortex_ingest")}
        {card("Execution route", "Which explicit mode ran, and what timing scope applies?", "Mode")}
        {card("Evidence route", "Which certificates, no-fallback fields, and claim gate came out?", "Claim")}
      </div>
    </section>

    <section>
      <h2>Execution modes</h2>
      {mode_table}
    </section>

    <section class="section-grid">
      <div>
        <h2>Timing fields stay visible.</h2>
        <p>Compatibility rows must not be read as pure query speed. They include source, parse, ingest, Vortex write/reopen, scan, operator, sink, and evidence timing.</p>
      </div>
      <ul class="check-list">{timing_list}</ul>
    </section>

    <section class="section-grid">
      <div>
        <h2>What must never happen</h2>
        <p>The compute-flow contract exists so unsupported work is blocked or diagnosed instead of becoming hidden fallback execution.</p>
      </div>
      <ul class="boundary-list as-list">{never_list}</ul>
    </section>

    <section>
      <h2>Raw diagram source</h2>
      <p class="narrow">Mermaid remains available as source text, but the public page leads with human-readable route structure.</p>
      {diagram_drawers}
      <p class="source-link"><a href="https://github.com/depsilon/shardloom/blob/main/docs/architecture/compute-engine-flow-reference.md">Open canonical Markdown on GitHub</a></p>
    </section>
"""
    return page(
        "ShardLoom Compute Flow",
        "Human-readable ShardLoom compute-flow route map and execution-mode translation.",
        body,
        "architecture",
        canonical_path,
    )


def not_found_page() -> str:
    body = """
    <section class="page-hero">
      <p class="eyebrow">404</p>
      <h1>This page is not part of the public surface.</h1>
      <p class="lede">The website is organized around local start, Field Guide terms, use cases, benchmark evidence, architecture, status, and the repository.</p>
      <div class="actions">
        <a class="button primary" href="/">Return home</a>
        <a class="button" href="/field-guide">Read Field Guide</a>
        <a class="button" href="https://github.com/depsilon/shardloom">Open GitHub</a>
      </div>
    </section>
"""
    return page("ShardLoom 404", "ShardLoom page not found.", body, "home", "404")


def sitemap(paths: list[str]) -> str:
    urls = ['<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for path in paths:
        loc = f"https://shardloom.io/{path}".rstrip("/") + ("/" if path == "" else "")
        priority = "1.0" if path == "" else "0.9"
        urls.append(
            f"  <url><loc>{esc(loc)}</loc><lastmod>{SITE_LASTMOD}</lastmod><priority>{priority}</priority></url>"
        )
    urls.append("</urlset>")
    return "\n".join(urls)


def write_support_files() -> None:
    write(
        WEBSITE / "_redirects",
        """
/home /
/index /
/index.html /
/telemetry /benchmarks
/benchmark /benchmarks
/benchmarks.html /benchmarks
/start.html /start
/flow /compute-engine-flow
/compute-flow /compute-engine-flow
/compute-engine-flow.html /compute-engine-flow
/architecture.html /architecture
/field-guide.html /field-guide
/use-cases.html /use-cases
/can-i-use-this /status
/status.html /status
/docs https://github.com/depsilon/shardloom
/readme https://github.com/depsilon/shardloom#readme
/readme.html https://github.com/depsilon/shardloom#readme
""",
    )
    write(
        WEBSITE / "_headers",
        """
/*
  X-Content-Type-Options: nosniff
  X-Frame-Options: DENY
  Referrer-Policy: strict-origin-when-cross-origin
  Permissions-Policy: camera=(), microphone=(), geolocation=()
  Content-Security-Policy: default-src 'self'; script-src 'self'; worker-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'

/assets/*
  Cache-Control: public, max-age=3600

/assets/data/*
  Cache-Control: public, max-age=300

/*.html
  Cache-Control: public, max-age=300

/robots.txt
  Cache-Control: public, max-age=3600

/sitemap.xml
  Cache-Control: public, max-age=3600
""",
    )
    write(WEBSITE / "robots.txt", "User-agent: *\nAllow: /\nSitemap: https://shardloom.io/sitemap.xml")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--benchmark-manifest",
        type=Path,
        default=BENCHMARK_MANIFEST,
        help="Committed benchmark manifest used for website rendering.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = args.benchmark_manifest
    if manifest_path != BENCHMARK_MANIFEST:
        raise SystemExit("custom benchmark manifests are not supported by the minimal website reset")
    manifest, results = benchmark_summary()
    use_case_index = load_use_case_index()
    use_cases, _ = use_case_maps(use_case_index)
    flow_markdown = FLOW_SOURCE.read_text(encoding="utf-8")

    DATA.mkdir(parents=True, exist_ok=True)
    write(DATA / "compute-engine-flow-reference.md", flow_markdown)
    write(DATA / "benchmark-evidence.json", json.dumps(results, indent=2, sort_keys=True))
    write(DATA / "use-case-index.json", json.dumps(use_case_index, indent=2, sort_keys=True))
    home_html = home_page(manifest, results)
    benchmark_html = benchmarks_page(manifest, results)
    flow_html = compute_flow_page(flow_markdown)
    architecture_html = compute_flow_page(flow_markdown, "architecture")
    start_html = start_page(use_case_index)
    field_guide_html = field_guide_page(use_case_index)
    use_cases_html = use_cases_page(use_case_index)
    status_html = status_page()
    write(WEBSITE / "index.html", home_html)
    write(WEBSITE / "start.html", start_html)
    write(WEBSITE / "start" / "index.html", start_html)
    write(WEBSITE / "benchmarks.html", benchmark_html)
    write(WEBSITE / "benchmarks" / "index.html", benchmark_html)
    write(WEBSITE / "compute-engine-flow.html", flow_html)
    write(WEBSITE / "compute-engine-flow" / "index.html", flow_html)
    write(WEBSITE / "architecture.html", architecture_html)
    write(WEBSITE / "architecture" / "index.html", architecture_html)
    write(WEBSITE / "field-guide.html", field_guide_html)
    write(WEBSITE / "field-guide" / "index.html", field_guide_html)
    for term in FIELD_GUIDE_TERMS:
        write(WEBSITE / "field-guide" / term["slug"] / "index.html", field_guide_term_page(term, use_case_index))
    write(WEBSITE / "use-cases.html", use_cases_html)
    write(WEBSITE / "use-cases" / "index.html", use_cases_html)
    for case in use_cases.values():
        write(WEBSITE / "use-cases" / case["id"] / "index.html", use_case_page(case, use_case_index))
    write(WEBSITE / "status.html", status_html)
    write(WEBSITE / "status" / "index.html", status_html)
    write(WEBSITE / "404.html", not_found_page())
    sitemap_paths = [
        "",
        "start",
        "field-guide",
        *[f"field-guide/{term['slug']}" for term in FIELD_GUIDE_TERMS],
        "use-cases",
        *[f"use-cases/{case_id}" for case_id in use_cases],
        "benchmarks",
        "architecture",
        "compute-engine-flow",
        "status",
    ]
    write(WEBSITE / "sitemap.xml", sitemap(sitemap_paths))
    write_support_files()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
