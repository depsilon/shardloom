#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the GAR-RUNTIME-IMPL-6D runtime gap family burn-down map."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.runtime_gap_family_burn_down.v1"
GATE_ID = "gar-runtime-impl-6d.runtime_gap_family_burn_down"
PHASE_PLAN = Path("docs/architecture/phased-execution-plan.md")
PHASE_COMPLETED_LEDGER = Path("docs/architecture/phased-execution-completed-ledger.md")
ACTIVE_GLOBAL_GAP_PHASE_OWNER = "GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1"


@dataclass(frozen=True)
class GapFamily:
    family_id: str
    display_name: str
    phase_items: tuple[str, ...]
    public_surfaces: tuple[str, ...]
    owning_modules: tuple[str, ...]
    required_evidence: tuple[str, ...]
    validators: tuple[str, ...]
    no_fallback_invariant: str
    claim_boundary: str
    next_action: str


@dataclass(frozen=True)
class GapMapping:
    global_review_title: str
    family_id: str


GAP_FAMILIES: tuple[GapFamily, ...] = (
    GapFamily(
        family_id="language_front_door_runtime",
        display_name="SQL/Python/DataFrame front-door runtime breadth",
        phase_items=(
            "GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar",
            "GAR-RUNTIME-IMPL-6D:last_order.python_dataframe_api_breadth",
        ),
        public_surfaces=("SQL", "Python context/session", "DataFrame-style helpers", "CLI"),
        owning_modules=(
            "shardloom-cli/src/sql_local_source_runtime.rs",
            "python/src/shardloom/query.py",
            "python/src/shardloom/context.py",
            "python/src/shardloom/session.py",
        ),
        required_evidence=(
            "positive runtime fixtures",
            "decoded-reference expectations",
            "front-door parity rows",
            "deterministic unsupported diagnostics",
        ),
        validators=(
            "python3 scripts/check_sql_python_dataframe_parity.py",
            "python3 scripts/check_python_user_surface_completion.py",
            "python3 -m unittest python/tests/test_query_builder.py",
        ),
        no_fallback_invariant="SQL and DataFrame helpers must lower to ShardLoom-owned runtime or fail before fallback or external execution.",
        claim_boundary="Scoped language/runtime admission only; no broad SQL/DataFrame, production, or performance claim.",
        next_action="Promote the next admitted grammar/API family only with runtime fixtures and parity evidence.",
    ),
    GapFamily(
        family_id="native_vortex_operator_runtime",
        display_name="Native Vortex source/sink/operator and encoded execution coverage",
        phase_items=("GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down",),
        public_surfaces=("Vortex-native CLI", "prepared/native benchmark rows", "capability reports"),
        owning_modules=(
            "shardloom-vortex/src",
            "shardloom-cli/src",
            "benchmarks/traditional_analytics/run.py",
        ),
        required_evidence=(
            "Native I/O certificates",
            "operator blocker matrix",
            "encoded/residual/materialized mode evidence",
            "Vortex provider admission evidence",
        ),
        validators=(
            "cargo test -p shardloom-vortex",
            "cargo test -p shardloom-cli vortex_",
            "cargo test -p shardloom-contract-tests --test traditional_benchmark_harness",
        ),
        no_fallback_invariant="Vortex query-engine integrations and external engines remain prohibited as runtime fallbacks.",
        claim_boundary="Scoped Vortex-native/provider evidence only; no universal operator, source, sink, or production claim.",
        next_action="Convert the next Vortex operator/source/sink blocker into encoded/runtime evidence or deterministic admission denial.",
    ),
    GapFamily(
        family_id="object_store_table_lakehouse_runtime",
        display_name="Object-store, table, catalog, lakehouse, commit, and recovery runtime",
        phase_items=("GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime",),
        public_surfaces=("object-store CLI/Python helpers", "table helpers", "lakehouse capability rows"),
        owning_modules=(
            "shardloom-cli/src/object_store_runtime.rs",
            "shardloom-cli/src/table_intelligence_plan.rs",
            "python/src/shardloom/context.py",
        ),
        required_evidence=(
            "credential/effect policy",
            "local or isolated provider fixture",
            "commit/rollback/recovery proof",
            "cleanup evidence",
        ),
        validators=(
            "cargo test -p shardloom-cli object_store",
            "cargo test -p shardloom-cli local_table",
            "python3 scripts/check_user_route_capability_report.py",
        ),
        no_fallback_invariant="Provider probes, credentials, commits, and table writes require explicit policy and cannot delegate fallback execution.",
        claim_boundary="Scoped local/fixture object-store or table proof only; no production lakehouse/cloud claim.",
        next_action="Promote one credential-safe local/table workflow or leave it as deterministic blocked evidence.",
    ),
    GapFamily(
        family_id="output_sink_runtime",
        display_name="Production output, fanout, sink, and user-facing write runtime",
        phase_items=(
            "GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime",
            "GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime",
        ),
        public_surfaces=("output helpers", "fanout routes", "native Vortex output", "compatibility exports"),
        owning_modules=(
            "shardloom-cli/src/sql_local_source_runtime.rs",
            "shardloom-vortex/src",
            "python/src/shardloom/session.py",
        ),
        required_evidence=(
            "OutputPlan",
            "sink artifact proof",
            "metadata preservation/loss report",
            "replay/fidelity evidence",
        ),
        validators=(
            "cargo test -p shardloom-cli output",
            "python3 scripts/check_user_route_capability_report.py",
            "python3 scripts/check_release_readiness.py",
        ),
        no_fallback_invariant="Compatibility export is translation, not fallback execution; partial writes must fail closed.",
        claim_boundary="Scoped output/fanout proof only; no production sink or platform write claim.",
        next_action="Promote the next sink only after artifact, replay, and metadata-loss evidence exist.",
    ),
    GapFamily(
        family_id="performance_claim_evidence",
        display_name="Claim-grade benchmark, performance, Spark-displacement, and replacement evidence",
        phase_items=("GAR-RUNTIME-IMPL-6D:last_order.front_door_performance_benchmark_publication",),
        public_surfaces=("benchmark artifacts", "website benchmark page", "README claims", "release gates"),
        owning_modules=(
            "benchmarks/traditional_analytics/run.py",
            "website-src/src",
            "scripts/check_benchmark_publication_claim_gate.py",
            "scripts/check_front_door_benchmark_publication.py",
        ),
        required_evidence=(
            "reproducible artifact",
            "correctness evidence",
            "hardware/runtime context",
            "claim-grade route/certificate linkage",
            "front-door equivalence admission blockers",
        ),
        validators=(
            "python3 scripts/check_benchmark_artifact_completeness.py",
            "python3 scripts/check_benchmark_publication_claim_gate.py",
            "python3 scripts/check_front_door_benchmark_publication.py",
            "python3 scripts/check_website_readiness.py",
        ),
        no_fallback_invariant="External engines are comparison baselines or test oracles only, never ShardLoom execution or fallback.",
        claim_boundary="No performance, superiority, Spark-displacement, or replacement claim until CG-5/CG-6 evidence passes.",
        next_action="Keep front-door performance equivalence fail-closed until measured equivalent front-door rows, rerun approval, and claim gates are satisfied.",
    ),
    GapFamily(
        family_id="effects_extensions_runtime",
        display_name="Extensions, UDFs, LLM/API calls, embeddings, vector search, and external effects",
        phase_items=("GAR-RUNTIME-IMPL-6D:last_order.effectful_operations",),
        public_surfaces=("extension reports", "UDF helpers", "effect policy", "security/release gates"),
        owning_modules=(
            "shardloom-cli/src/extension_manifest_effect_matrix.rs",
            "shardloom-cli/src/effect_budget_plan.rs",
            "python/src/shardloom/context.py",
        ),
        required_evidence=(
            "capability declaration",
            "permission policy",
            "sandbox posture",
            "security/effect diagnostics",
        ),
        validators=(
            "cargo test -p shardloom-cli extension",
            "cargo test -p shardloom-cli effect",
            "python3 scripts/check_release_security_gate.py",
        ),
        no_fallback_invariant="Effectful work requires explicit admission and must not hide network, credentials, plugin execution, or fallback.",
        claim_boundary="Scoped deterministic effect admission only; no arbitrary plugin/UDF/LLM/vector/platform claim.",
        next_action="Promote one effect family only after policy, sandbox, and deterministic denial evidence exist.",
    ),
    GapFamily(
        family_id="streaming_live_hybrid_runtime",
        display_name="Streaming, live/hybrid, CDC, broker/state-store, and remote API runtime",
        phase_items=("GAR-RUNTIME-IMPL-6D:last_order.live_hybrid_runtime",),
        public_surfaces=("live/hybrid CLI", "engine selection", "REST planning", "remote API surfaces"),
        owning_modules=(
            "shardloom-cli/src/cg22_engine_fabric.rs",
            "shardloom-cli/src/rest_api_planning.rs",
            "python/src/shardloom/context.py",
        ),
        required_evidence=(
            "freshness/snapshot proof",
            "bounded state fixture",
            "retry/cancellation cleanup proof",
            "remote/control-plane safety fields",
        ),
        validators=(
            "cargo test -p shardloom-cli cg22",
            "cargo test -p shardloom-cli rest_api",
            "python3 scripts/check_user_route_capability_report.py",
        ),
        no_fallback_invariant="Live, hybrid, broker, and remote execution must be ShardLoom-owned and explicit or blocked before fallback.",
        claim_boundary="Fixture/contract scope only; no production streaming, broker, REST, or exactly-once claim.",
        next_action="Promote the next bounded live/hybrid transition or keep remote API paths contract-only.",
    ),
    GapFamily(
        family_id="spill_fault_tolerance_runtime",
        display_name="Spill, OOM, adaptive execution, retry, cancellation, commit, and fault tolerance",
        phase_items=("GAR-RUNTIME-IMPL-6D:last_order.distributed_spill_oom_runtime",),
        public_surfaces=("memory/spill diagnostics", "runtime reports", "benchmark safety rows"),
        owning_modules=(
            "shardloom-cli/src/cg14_memory_runtime_hardening.rs",
            "shardloom-cli/src/fault_tolerance_promotion_gate.rs",
            "shardloom-exec/src",
        ),
        required_evidence=(
            "memory reservation proof",
            "pre-OOM deterministic blocker",
            "spill cleanup proof",
            "retry/cancellation state evidence",
        ),
        validators=(
            "cargo test -p shardloom-cli memory",
            "cargo test -p shardloom-cli fault_tolerance",
            "cargo test -p shardloom-exec",
        ),
        no_fallback_invariant="Memory pressure, retry, and commit failures must fail deterministically before fallback, external delegation, or process OOM.",
        claim_boundary="Scoped local safety evidence only; no distributed/shuffle/spill production claim.",
        next_action="Promote one bounded memory/fault-tolerance guard with cleanup evidence.",
    ),
    GapFamily(
        family_id="observability_runtime",
        display_name="Live profiling, traces, metrics exporters, debug bundles, and runtime introspection",
        phase_items=("GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down",),
        public_surfaces=("observability reports", "profile plans", "debug/trace artifacts"),
        owning_modules=(
            "shardloom-cli/src/observability_schema_coverage.rs",
            "shardloom-cli/src/query_trace.rs",
        ),
        required_evidence=(
            "safe profiler admission",
            "artifact redaction policy",
            "exporter capability matrix",
            "side-effect-free diagnostics",
        ),
        validators=(
            "cargo test -p shardloom-cli observability",
            "cargo test -p shardloom-cli query_trace",
            "python3 scripts/check_release_security_gate.py",
        ),
        no_fallback_invariant="Profiling/exporting must not execute hidden probes, external collectors, or fallback engines.",
        claim_boundary="Diagnostics and scoped trace evidence only; no production observability/exporter claim.",
        next_action="Split live profiling/exporters into explicit safe-admission slices before runtime promotion.",
    ),
    GapFamily(
        family_id="plan_interop_harness_runtime",
        display_name="Plan interoperability, Substrait direction, and universal harness execution",
        phase_items=("GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down",),
        public_surfaces=("plan import/export", "universal harness", "capability reports"),
        owning_modules=(
            "shardloom-cli/src/plan_portability.rs",
            "shardloom-cli/src/universal_harness_plan.rs",
        ),
        required_evidence=(
            "import/export fixtures",
            "capability checks",
            "round-trip or deterministic blocked diagnostics",
            "license/dependency review",
        ),
        validators=(
            "cargo test -p shardloom-cli plan_",
            "cargo test -p shardloom-cli universal_harness",
            "cargo test -p shardloom-contract-tests --test release_readiness_metadata",
        ),
        no_fallback_invariant="Imported plans must pass ShardLoom capability checks and cannot execute residual work through external engines or fallback.",
        claim_boundary="Interop direction/report scope only; no broad imported-plan execution claim.",
        next_action="Add one import/export or harness execution fixture only after capability and provenance gates are explicit.",
    ),
    GapFamily(
        family_id="release_package_platform_readiness",
        display_name="Public package, release, signing, attestations, platform packs, and publication readiness",
        phase_items=("GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down",),
        public_surfaces=("release gates", "package docs", "Foundry/package channels", "publication APIs"),
        owning_modules=(
            "scripts/check_release_readiness.py",
            "scripts/final_release_rehearsal.py",
            "docs/release",
        ),
        required_evidence=(
            "publication approval gate",
            "package-channel matrix",
            "security/provenance attestations",
            "final release rehearsal",
        ),
        validators=(
            "python3 scripts/check_release_readiness.py",
            "python3 scripts/final_release_rehearsal.py --allow-blocked",
            "python3 scripts/check_package_channel_readiness.py --require-local-evidence",
        ),
        no_fallback_invariant="Release/package readiness cannot imply runtime fallback, hidden publication, or unapproved secrets.",
        claim_boundary="Release readiness remains blocked until explicit publication approval and all runtime claim gates pass.",
        next_action="Keep publication blocked until runtime families and release/security/package gates close.",
    ),
    GapFamily(
        family_id="result_envelope_migration",
        display_name="Typed result-envelope migration and legacy flat-field compatibility",
        phase_items=("GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down",),
        public_surfaces=("CLI JSON", "Python client reports", "agent-facing typed envelopes"),
        owning_modules=(
            "shardloom-cli/src/typed_envelope.rs",
            "python/src/shardloom/client.py",
            "shardloom-cli/tests/typed_envelope_contract_snapshots.rs",
        ),
        required_evidence=(
            "typed envelope snapshots",
            "Python accessor coverage",
            "legacy mirror migration plan",
            "agent contract compatibility",
        ),
        validators=(
            "cargo test -p shardloom-cli --test typed_envelope_contract_snapshots",
            "cargo test -p shardloom-cli --test typed_envelope_compatibility_lock",
            "PYTHONPATH=python/src python3 -m unittest python.tests.test_cli_client",
        ),
        no_fallback_invariant="Result-envelope migration must not hide support, claim, fallback, or diagnostic fields.",
        claim_boundary="API-shape migration only; no runtime or production claim.",
        next_action="Migrate one remaining command family to typed payloads with compatibility lock coverage.",
    ),
    GapFamily(
        family_id="io_reuse_fanout_followthrough",
        display_name="I/O reuse and cross-format fanout follow-through",
        phase_items=("GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime",),
        public_surfaces=("fanout outputs", "benchmark rows", "website summaries", "agent handoff packets"),
        owning_modules=(
            "benchmarks/traditional_analytics/run.py",
            "shardloom-cli/src/sql_local_source_runtime.rs",
            "website-src/src",
        ),
        required_evidence=(
            "shared conversion DAG evidence",
            "prepared-state reuse evidence",
            "fanout replay proof",
            "route-stage attribution",
        ),
        validators=(
            "cargo test -p shardloom-cli fanout",
            "cargo test -p shardloom-contract-tests --test traditional_benchmark_harness",
            "python3 scripts/check_benchmark_publish_doctor.py",
        ),
        no_fallback_invariant="Fanout and reuse must reuse ShardLoom-prepared state and local output stages, not external engines or fallback.",
        claim_boundary="Scoped fanout/reuse evidence only; no broad performance or production claim.",
        next_action="Attach remaining fanout/reuse follow-through to concrete output or benchmark slices.",
    ),
)


GLOBAL_REVIEW_MAPPINGS: tuple[GapMapping, ...] = (
    GapMapping("Executable SQL/DataFrame runtime, distributed runtime, broad lakehouse-compatible output, and", "language_front_door_runtime"),
    GapMapping("Native Vortex support is not universal across every source, sink, operator, and workload.", "native_vortex_operator_runtime"),
    GapMapping("Full production Vortex segment extraction and broad operator coverage remain incomplete.", "native_vortex_operator_runtime"),
    GapMapping("Table/catalog metadata reads, object-store commits, generalized manifest serialization, CDC", "object_store_table_lakehouse_runtime"),
    GapMapping("Broad Vortex reader/writer execution, object-store Vortex I/O execution, general", "native_vortex_operator_runtime"),
    GapMapping("Claim-grade broad predicate, DType, nested, null, and production metadata-only runtime", "native_vortex_operator_runtime"),
    GapMapping("Production output sink APIs, object-store output, broad user-facing write methods,", "output_sink_runtime"),
    GapMapping("Object-store I/O providers, probes, coordinator/worker runtime, checkpoint writes, retry", "object_store_table_lakehouse_runtime"),
    GapMapping("Broad claim-grade Spark-displacement evidence and public performance claims remain gated.", "performance_claim_evidence"),
    GapMapping("Extension execution, UDF execution, LLM/API calls, embeddings, and external effects remain", "effects_extensions_runtime"),
    GapMapping("Full streaming runtime and object-store streaming reads remain gated/report-only.", "streaming_live_hybrid_runtime"),
    GapMapping("Actual runtime spill/OOM production enforcement remains limited to synthetic or local", "spill_fault_tolerance_runtime"),
    GapMapping("Broad property/fuzz execution and claim-grade benchmark superiority coverage remain blocked", "performance_claim_evidence"),
    GapMapping("Runtime adaptive execution, runtime filters, skew handling, and compaction writes remain", "spill_fault_tolerance_runtime"),
    GapMapping("Broad retry, cancellation, and commit execution remain incomplete.", "spill_fault_tolerance_runtime"),
    GapMapping("Live profiling collectors, profile artifacts, debug bundles, metrics exporters, trace", "observability_runtime"),
    GapMapping("Broad catalog/table metadata integration, real table data I/O, delete/tombstone execution,", "object_store_table_lakehouse_runtime"),
    GapMapping("Real Substrait parser/exporter support, dependency adoption, round-trip fixtures, and", "plan_interop_harness_runtime"),
    GapMapping("Full competitive replacement remains incomplete until every sufficiency row has", "performance_claim_evidence"),
    GapMapping("Generalized direct encoded count/filter/project execution and production compressed-execution", "native_vortex_operator_runtime"),
    GapMapping("Real SIMD/vectorized dispatch, production vectorized kernel path, adaptive parallelism", "native_vortex_operator_runtime"),
    GapMapping("Broad CG-5/CG-6 coverage, production stateful reuse runtime, and performance/superiority", "performance_claim_evidence"),
    GapMapping("Imported-plan execution and actual universal harness execution remain unimplemented without", "plan_interop_harness_runtime"),
    GapMapping("CG-19 is not universal across object-store/range-read, streaming sinks, table/catalog,", "native_vortex_operator_runtime"),
    GapMapping("Executable SQL parser/binder/runtime, DataFrame execution, UDF runtime, notebook runtime,", "language_front_door_runtime"),
    GapMapping("Mature DataFrame execution, SQL execution, joins, aggregations, windows, data-quality", "language_front_door_runtime"),
    GapMapping("Production live/hybrid engines, broker/state-store runtime, object-store execution,", "streaming_live_hybrid_runtime"),
    GapMapping("HTTP listener, remote execution, Flight/ADBC runtime bridge, broker integration, production", "streaming_live_hybrid_runtime"),
    GapMapping("Production `shardloom-foundry`, package publication, Foundry service invocation, Artifact", "release_package_platform_readiness"),
    GapMapping("SQL/DataFrame runtime, object-store runtime, writes, and any executable legacy facade shim", "language_front_door_runtime"),
    GapMapping("Legacy flat `fields` mirror, remaining command-family result migration beyond the", "result_envelope_migration"),
    GapMapping("Full comparative reruns, source-backed claim-grade promotion, managed-platform lanes,", "performance_claim_evidence"),
    GapMapping("Passing public release/package/performance/production/platform claims remain incomplete until", "release_package_platform_readiness"),
    GapMapping("Generalized Source/Split runtime paths, field-mask/predicate-ordering proof, layout/write", "native_vortex_operator_runtime"),
    GapMapping("Prepared/native Vortex rows now carry a typed operator blocker matrix, explicit", "native_vortex_operator_runtime"),
    GapMapping("`GAR-IOREUSE-1` adds I/O reuse and cross-format fanout follow-through. `GAR-IOREUSE-1A`", "io_reuse_fanout_followthrough"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--global-review",
        type=Path,
        default=Path("docs/architecture/global-architecture-review.md"),
    )
    parser.add_argument(
        "--phase-plan",
        type=Path,
        default=Path("docs/architecture/phased-execution-plan.md"),
    )
    parser.add_argument(
        "--burn-down-doc",
        type=Path,
        default=Path("docs/architecture/runtime-gap-family-burn-down.md"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/runtime-gap-family-burn-down.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def unchecked_items(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        match = re.match(r"^-\s+\[\s\]\s+(?P<title>.+?)\s*$", line)
        if match:
            rows.append({"line": line_number, "title": match.group("title").strip()})
    return rows


def family_by_id() -> dict[str, GapFamily]:
    return {family.family_id: family for family in GAP_FAMILIES}


def phase_items_for_family(family: GapFamily) -> tuple[str, ...]:
    return tuple(dict.fromkeys((ACTIVE_GLOBAL_GAP_PHASE_OWNER, *family.phase_items)))


def command_script_exists(repo_root: Path, command: str) -> bool:
    match = re.match(r"^(?:PYTHONPATH=[^ ]+\s+)?python3?\s+(scripts/[^ ]+\.py)\b", command)
    if not match:
        return True
    return (repo_root / match.group(1)).exists()


def row_payload(review_row: dict[str, Any], mapping: GapMapping, family: GapFamily) -> dict[str, Any]:
    return {
        "global_review_line": review_row["line"],
        "global_review_title": review_row["title"],
        "runtime_gap_family": family.family_id,
        "family_display_name": family.display_name,
        "phase_items": list(phase_items_for_family(family)),
        "public_surfaces": list(family.public_surfaces),
        "owning_modules": list(family.owning_modules),
        "required_evidence": list(family.required_evidence),
        "validators": list(family.validators),
        "no_fallback_invariant": family.no_fallback_invariant,
        "claim_boundary": family.claim_boundary,
        "next_action": family.next_action,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade",
        "runtime_support_claim_allowed": False,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
    }


def build_report(
    repo_root: Path,
    mappings: tuple[GapMapping, ...] = GLOBAL_REVIEW_MAPPINGS,
) -> dict[str, Any]:
    global_review_text = read_text(repo_root / "docs/architecture/global-architecture-review.md")
    phase_plan_text = read_text(repo_root / PHASE_PLAN)
    phase_completed_ledger_text = read_text(repo_root / PHASE_COMPLETED_LEDGER)
    phase_registry_text = "\n".join((phase_plan_text, phase_completed_ledger_text))
    burn_down_doc_text = read_text(repo_root / "docs/architecture/runtime-gap-family-burn-down.md")
    unchecked = unchecked_items(global_review_text)
    unchecked_by_title = {str(row["title"]): row for row in unchecked}
    family_index = family_by_id()
    blockers: list[str] = []
    rows: list[dict[str, Any]] = []

    if not burn_down_doc_text:
        blockers.append("missing runtime gap family burn-down doc")
    elif SCHEMA_VERSION not in burn_down_doc_text:
        blockers.append("runtime gap family burn-down doc missing schema version")

    mapped_titles = [mapping.global_review_title for mapping in mappings]
    duplicate_titles = sorted(title for title in set(mapped_titles) if mapped_titles.count(title) > 1)
    if duplicate_titles:
        blockers.append("duplicate global-review mapping titles: " + ",".join(duplicate_titles))

    for mapping in mappings:
        review_row = unchecked_by_title.get(mapping.global_review_title)
        if review_row is None:
            blockers.append(
                "mapped global-review row is no longer unchecked or title drifted: "
                + mapping.global_review_title
            )
            continue
        family = family_index.get(mapping.family_id)
        if family is None:
            blockers.append(f"{mapping.global_review_title}: unknown family {mapping.family_id}")
            continue
        rows.append(row_payload(review_row, mapping, family))

    missing = sorted(set(unchecked_by_title) - set(mapped_titles))
    extra = sorted(set(mapped_titles) - set(unchecked_by_title))
    if missing:
        blockers.append("unchecked global-review rows lack burn-down family: " + " | ".join(missing))
    if extra:
        blockers.append("burn-down mappings are not unchecked global-review rows: " + " | ".join(extra))

    for family in GAP_FAMILIES:
        if family.family_id not in burn_down_doc_text:
            blockers.append(f"burn-down doc missing family id {family.family_id}")
        for field_name in [
            "phase_items",
            "public_surfaces",
            "owning_modules",
            "required_evidence",
            "validators",
        ]:
            if not getattr(family, field_name):
                blockers.append(f"{family.family_id}: {field_name} is required")
        phase_items = phase_items_for_family(family)
        if not any(phase_item in phase_plan_text for phase_item in phase_items):
            blockers.append(f"{family.family_id}: unchecked gap family lacks active phase owner")
        for phase_item in phase_items:
            if phase_item not in phase_registry_text:
                blockers.append(
                    f"{family.family_id}: phase item not present in phase registry: {phase_item}"
                )
        for command in family.validators:
            if not command_script_exists(repo_root, command):
                blockers.append(f"{family.family_id}: validator command references missing script: {command}")
        if "fallback" not in family.no_fallback_invariant.lower():
            blockers.append(f"{family.family_id}: no_fallback_invariant must mention fallback")
        if not family.claim_boundary.strip():
            blockers.append(f"{family.family_id}: claim_boundary is required")

    family_counts: dict[str, int] = {family.family_id: 0 for family in GAP_FAMILIES}
    for row in rows:
        family_counts[str(row["runtime_gap_family"])] += 1

    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "global_review_unchecked_count": len(unchecked),
        "mapped_gap_count": len(rows),
        "runtime_gap_family_count": len(GAP_FAMILIES),
        "runtime_gap_family_counts": family_counts,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade",
        "runtime_support_claim_allowed": False,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "acceptance_summary": {
            "all_unchecked_global_review_rows_mapped": not missing and not extra,
            "all_families_have_phase_items": not any(
                "phase item not present" in blocker for blocker in blockers
            ),
            "all_families_have_active_phase_owner": not any(
                "active phase owner" in blocker for blocker in blockers
            ),
            "all_families_have_evidence_and_validators": not any(
                "required_evidence is required" in blocker
                or "validators is required" in blocker
                or "missing script" in blocker
                for blocker in blockers
            ),
            "all_no_fallback_invariants_named": not any(
                "no_fallback_invariant" in blocker for blocker in blockers
            ),
            "all_claim_boundaries_named": not any(
                "claim_boundary is required" in blocker for blocker in blockers
            ),
        },
        "rows": rows,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = build_report(repo_root)
    output = resolve(repo_root, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for blocker in report["blockers"]:
        print(f"runtime gap family burn-down blocker: {blocker}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
