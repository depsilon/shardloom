#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the agent-facing ShardLoom user-surface index."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.user_surface_index.v1"
MD_PATH = Path("docs/reference/shardloom-user-surface-index.md")
JSON_PATH = Path("docs/reference/shardloom-user-surface-index.json")
COMMAND_REGISTRY_PATH = Path("shardloom-cli/src/command_registry.rs")

REQUIRED_MD_MARKERS = (
    "Schema marker: `shardloom.user_surface_index.v1`",
    "shardloom --version",
    "shardloom command-metadata --format json",
    "shardloom agent-contract-pack --format json",
    "ctx.front_door_semantic_surface_matrix()",
    "ctx.read(path)",
    "ctx.sql(\"SELECT ...\")",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "Hidden fallback execution in DuckDB, DataFusion, Spark, Polars, pandas",
)

REQUIRED_BACKLINKS = {
    "README.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
    "python/README.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
    "docs/architecture/agent-contract-pack.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
    "docs/architecture/v1-front-door-runtime-scope.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
    "docs/skills/developer-agent-experience.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
}

REQUIRED_JSON_POINTERS = (
    "cli.version_command",
    "cli.exhaustive_inventory_command",
    "cli.agent_contract_pack_command",
    "python.context_readers",
    "python.query_builder_methods",
    "semantic_claim_surface.agent_source",
    "semantic_claim_surface.disallowed_broad_claims",
    "sql.entrypoints",
    "guardrails.no_fallback_policy",
    "native_relational_order_spill.reference",
    "native_relational_order_spill.public_resource_arguments",
    "native_relational_order_spill.spill_fields",
    "native_nested_composition.reference",
    "native_nested_composition.payload_types",
    "native_nested_composition.nested_writers",
    "native_nested_composition.denied_nested_writers",
    "native_typed_payloads.reference",
    "native_typed_payloads.types",
    "native_typed_keys.reference",
    "native_typed_keys.types",
    "native_typed_unary.reference",
    "native_typed_unary.types",
    "native_typed_unary.operators",
    "native_typed_reductions.reference",
    "native_typed_reductions.computed_aggregates",
    "native_typed_reductions.decimal_rolling",
    "native_nested_keys_state.reference",
    "native_nested_keys_state.types",
    "native_nested_keys_state.retained_unary",
    "native_nested_keys_state.nested_pivot_reference",
    "native_dynamic_pivot_composition.nested_reference",
    "native_nested_pivot_state.reference",
    "native_nested_pivot_state.roles",
    "native_nested_pivot_state.selected_cell_aggregates",
)

REQUIRED_COMMANDS = (
    "command-metadata",
    "help",
    "agent-contract-pack",
    "capabilities",
    "route",
    "run",
    "prepare",
    "status",
    "vortex-prepare",
)

REQUIRED_PYTHON_METHODS = (
    "ctx.read",
    "ctx.read_csv",
    "ctx.read_json",
    "ctx.read_parquet",
    "ctx.read_arrow_ipc",
    "ctx.read_avro",
    "ctx.read_orc",
    "ctx.read_vortex",
    "filter",
    "select",
    "group_by",
    "join",
    "collect",
    "write_json",
    "write_jsonl",
    "write_vortex",
)

REQUIRED_SQL_ENTRYPOINTS = (
    "ctx.sql",
    "sl.sql",
    'shardloom run sql --sql "SELECT 1 AS value" --request collect --bounded true --format json',
    'shardloom run sql --input events.csv --input-format csv --sql "SELECT * FROM events" --request collect --bounded true --format json',
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    return parser.parse_args()


def read_text(repo_root: Path, path: Path) -> str:
    return (repo_root / path).read_text(encoding="utf-8")


def load_json(repo_root: Path, path: Path) -> dict[str, Any]:
    return json.loads(read_text(repo_root, path))


def registry_commands(repo_root: Path) -> list[str]:
    source = read_text(repo_root, COMMAND_REGISTRY_PATH)
    match = re.search(
        r"REGISTERED_COMMANDS:\s*&\[[^\]]+\]\s*=\s*&\[(?P<body>.*?)\];",
        source,
        flags=re.S,
    )
    if match is None:
        raise ValueError("could not locate REGISTERED_COMMANDS in command registry")
    return re.findall(r'"([^"]+)"', match.group("body"))


def nested_get(payload: dict[str, Any], pointer: str) -> Any:
    current: Any = payload
    for part in pointer.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(pointer)
        current = current[part]
    return current


def validate_acceptance(repo_root: Path, name: str, contract: dict[str, Any]) -> list[str]:
    blockers = []
    local = contract.get("local_acceptance_complete")
    hosted = contract.get("hosted_acceptance_complete")
    evidence = contract.get("local_acceptance_report")
    reference = contract.get("hosted_acceptance_reference")
    if type(local) is not bool or type(hosted) is not bool:
        blockers.append(f"{JSON_PATH}: {name} acceptance states must be explicit")
    if local is True:
        if (not isinstance(evidence, str) or not evidence.startswith("docs/benchmarks/")
                or ".." in Path(evidence).parts or not (repo_root / evidence).is_file()):
            blockers.append(f"{JSON_PATH}: accepted {name} requires its local report")
    elif evidence is not None:
        blockers.append(f"{JSON_PATH}: pending {name} cannot claim an acceptance report")
    if hosted is True:
        if (local is not True or not isinstance(reference, str)
                or re.fullmatch(r"https://github\.com/depsilon/shardloom/pull/[1-9][0-9]*", reference) is None):
            blockers.append(f"{JSON_PATH}: hosted {name} requires local acceptance and its PR reference")
    elif reference is not None:
        blockers.append(f"{JSON_PATH}: pending {name} cannot claim hosted acceptance")
    return blockers


def validate(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    md = read_text(repo_root, MD_PATH)
    payload = load_json(repo_root, JSON_PATH)
    commands = registry_commands(repo_root)

    if payload.get("schema_version") != SCHEMA_VERSION:
        blockers.append(f"{JSON_PATH}: schema_version must be {SCHEMA_VERSION}")
    if payload.get("canonical_human_reference") != MD_PATH.as_posix():
        blockers.append(f"{JSON_PATH}: canonical_human_reference must point at {MD_PATH}")
    if payload.get("fallback_attempted") is not False:
        blockers.append(f"{JSON_PATH}: fallback_attempted must be false")
    if payload.get("external_engine_invoked") is not False:
        blockers.append(f"{JSON_PATH}: external_engine_invoked must be false")

    for marker in REQUIRED_MD_MARKERS:
        if marker not in md:
            blockers.append(f"{MD_PATH}: missing marker {marker!r}")

    for path_raw, markers in REQUIRED_BACKLINKS.items():
        path = Path(path_raw)
        text = read_text(repo_root, path)
        for marker in markers:
            if marker not in text:
                blockers.append(f"{path}: missing user-surface index backlink {marker}")

    for pointer in REQUIRED_JSON_POINTERS:
        try:
            value = nested_get(payload, pointer)
        except KeyError:
            blockers.append(f"{JSON_PATH}: missing {pointer}")
            continue
        if value in ("", [], {}, None):
            blockers.append(f"{JSON_PATH}: {pointer} must not be empty")

    reported_count = nested_get(payload, "cli.registered_command_count")
    if reported_count != len(commands):
        blockers.append(
            f"{JSON_PATH}: cli.registered_command_count={reported_count} "
            f"does not match registry count {len(commands)}"
        )

    command_set = set(commands)
    for command in REQUIRED_COMMANDS:
        if command not in command_set:
            blockers.append(f"{COMMAND_REGISTRY_PATH}: missing registered command {command}")
    declared_commands = [value for value in payload.get("cli", {}).values()
                         if isinstance(value, str)]
    for values in payload.get("cli", {}).values():
        if isinstance(values, list):
            declared_commands.extend(value for value in values if isinstance(value, str))
    declared_commands.extend(payload.get("sql", {}).get("entrypoints", []))
    command_text = "\n".join([md, *declared_commands])
    for command in set(re.findall(r"^shardloom ([a-z][a-z-]*)\b", command_text, re.MULTILINE)):
        if command not in command_set:
            blockers.append(f"user-surface reference names an unregistered command: {command}")

    if payload["dynamic_sources"]["cli_command_registry_source"] != COMMAND_REGISTRY_PATH.as_posix():
        blockers.append(f"{JSON_PATH}: CLI registry source path drifted")

    python_surface = payload["python"]
    all_python_values = {
        value
        for key in (
            "context_readers",
            "query_builder_methods",
            "bounded_inspection_and_materialization",
            "expression_helpers",
            "source_free_helpers",
            "capability_and_diagnostic_methods",
        )
        for value in python_surface.get(key, [])
    }
    for method in REQUIRED_PYTHON_METHODS:
        if method not in all_python_values:
            blockers.append(f"{JSON_PATH}: missing Python surface {method}")

    sql_entrypoints = set(payload["sql"].get("entrypoints", []))
    for entrypoint in REQUIRED_SQL_ENTRYPOINTS:
        if entrypoint not in sql_entrypoints:
            blockers.append(f"{JSON_PATH}: missing SQL entrypoint {entrypoint}")

    relational_spill = payload.get("native_relational_order_spill", {})
    for field in ("cleanup_required_before_publication", "stable_ties", "multiple_keys"):
        if relational_spill.get(field) is not True:
            blockers.append(f"{JSON_PATH}: native_relational_order_spill.{field} must be true")
    for field in ("other_relational_state_spill", "relational_fanout", "total_rss_bound",
                  "fallback_attempted", "external_engine_invoked"):
        if relational_spill.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_relational_order_spill.{field} must be false")

    guardrails = payload["guardrails"]
    nested = payload.get("native_nested_composition", {})
    if nested.get("ordered_repeated_explode") is not True:
        blockers.append(f"{JSON_PATH}: native_nested_composition.ordered_repeated_explode must be true")
    for field in ("nested_text_schema_hints", "nested_keys", "general_nested_unary_state",
                  "dynamic_pivot_composition", "variant_extension_composition", "total_rss_bound",
                  "fallback_attempted", "external_engine_invoked"):
        if nested.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_nested_composition.{field} must be false")
    if set(nested.get("nested_writers", [])) != {"vortex", "json", "jsonl", "arrow_ipc", "parquet", "avro", "csv"}:
        blockers.append(f"{JSON_PATH}: nested writers must match the representable static nested contract")
    if set(nested.get("denied_nested_writers", [])) != {"orc"}:
        blockers.append(f"{JSON_PATH}: the pinned nested ORC writer must remain explicitly denied")
    if "CSV_uses_quoted_JSON_text_cells_without_native_dtype_persistence" not in nested.get("writer_fidelity", ""):
        blockers.append(f"{JSON_PATH}: nested CSV must declare its JSON text and dtype-loss boundary")

    typed = payload.get("native_typed_payloads", {})
    types = {"binary", "decimal128", "date32", "timestamp_micros"}
    if set(typed.get("types", [])) != types or not types.issubset(nested.get("leaf_types", [])):
        blockers.append(f"{JSON_PATH}: typed payloads and nested leaves must share exact type admission")
    for field in ("typed_keys_arithmetic_and_unary_state", "total_rss_bound",
                  "fallback_attempted", "external_engine_invoked"):
        if typed.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_typed_payloads.{field} must be false")
    for field, value in (("nested_leaves", True), ("decimal_precision", [1, 38]),
                         ("decimal_scale", "0_to_precision"), ("binary_local_writer_count", 8),
                         ("decimal_temporal_local_writer_count", 7),
                         ("denied_decimal_temporal_writers", ["orc"])):
        if typed.get(field) != value:
            blockers.append(f"{JSON_PATH}: native_typed_payloads.{field} differs from its contract")
    if "timestamp_timezone" not in typed or typed["timestamp_timezone"] is not None:
        blockers.append(f"{JSON_PATH}: typed temporal payloads require timezone-free microseconds")

    keys = payload.get("native_typed_keys", {})
    if set(keys.get("types", [])) != types:
        blockers.append(f"{JSON_PATH}: typed keys must share the payload type admission")
    for field, value in (
        ("scope", "current_source_build_flat_native_relational_keys"),
        ("binary_order", "unsigned_lexicographic_bytes"),
        ("decimal_comparison", "exact_i128_with_identical_precision_and_scale"),
        ("temporal_comparison", "exact_signed_storage_with_distinct_logical_types"),
        ("operators", ["join", "set", "group", "sort", "window", "subquery"]),
        ("aggregates", ["count", "count_distinct", "min", "max"]),
        ("scalar_selection", ["comparison", "is_null", "is_not_null", "case", "coalesce", "nullif"]),
        ("native_sort_spill", "explicit_relational_sort_policy"),
        ("ordinary_sql_source_handoff", True),
        ("inspection_side_effect_free", True),
    ):
        if keys.get(field) != value:
            blockers.append(f"{JSON_PATH}: native_typed_keys.{field} differs from its contract")
    for field in ("arithmetic", "casts", "retained_unary_state", "nested_keys",
                  "group_join_window_spill", "total_rss_bound", "fallback_attempted",
                  "external_engine_invoked"):
        if keys.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_typed_keys.{field} must be false")

    unary = payload.get("native_typed_unary", {})
    if set(unary.get("types", [])) != types:
        blockers.append(f"{JSON_PATH}: typed unary state must share the payload type admission")
    for field, value in (
        ("scope", "current_source_build_flat_retained_unary_state"),
        ("operators", ["distinct", "drop_duplicates", "duplicate_mask", "tail", "sample",
                       "scalar_rewrite", "melt", "rolling_count", "pivot"]),
        ("shared_direct_and_relational_state", True),
        ("compact_selected_variable_payload", True),
        ("declared_logical_types_preserved", True),
        ("decimal_arithmetic", "same_scale_checked_native_kernel"),
        ("melt_conversion", "lossless_common_scalar_type"),
        ("typed_pivot_aggregates", ["first", "first_unique", "count"]),
        ("pivot_fill", "missing_cells_only"),
        ("legacy_predicate_domain", "primitive_boolean_utf8"),
    ):
        if unary.get(field) != value:
            blockers.append(f"{JSON_PATH}: native_typed_unary.{field} differs from its contract")
    for field in ("nested_state", "state_spill", "total_rss_bound", "fallback_attempted",
                  "external_engine_invoked"):
        if unary.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_typed_unary.{field} must be false")

    reductions = payload.get("native_typed_reductions", {})
    for field, value in (
        ("scope", "current_source_build_computed_arguments_and_decimal_reductions"),
        ("computed_aggregates", ["count", "count_distinct", "sum", "avg", "min", "max"]),
        ("argument_lowering", "shared_native_projection_then_aggregate"),
        ("distinct_aggregates", ["count"]),
        ("decimal_precision", [1, 38]),
        ("decimal_scale", "0_to_precision"),
        ("decimal_sum_result", "decimal128(38,input_scale)"),
        ("decimal_mean_result", "decimal128(38,max(input_scale,6))"),
        ("decimal_extrema_result", "preserve_input_precision_and_scale"),
        ("decimal_average_policy", "exact_or_error"),
        ("decimal_overflow_policy", "checked_error"),
        ("decimal_rolling", ["sum", "mean", "min", "max"]),
        ("decimal_numeric_pivot", ["sum", "mean", "min", "max"]),
        ("numeric_pivot_nulls", "error"),
        ("pivot_margins", "merge_totals_and_counts_before_finalization"),
        ("untyped_null_projection", "nullable_boolean_carrier"),
        ("shared_direct_and_relational_state", True),
    ):
        if reductions.get(field) != value:
            blockers.append(f"{JSON_PATH}: native_typed_reductions.{field} differs from its contract")
    for field in ("state_spill", "wider_analytic_frames",
                  "total_rss_bound", "fallback_attempted", "external_engine_invoked"):
        if reductions.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_typed_reductions.{field} must be false")
    blockers.extend(validate_acceptance(repo_root, "native_typed_reductions", reductions))

    nested_state = payload.get("native_nested_keys_state", {})
    for field, value in (
        ("scope", "current_source_build_static_nested_keys_and_retained_state"),
        ("types", ["list", "fixed_size_list", "struct"]),
        ("leaf_contract", "native_nested_composition"),
        ("key_schema", "same_recursive_logical_types_ignoring_nullability"),
        ("list_order", "lexicographic_children_then_length"),
        ("struct_order", "declared_field_order"),
        ("child_null_order", "first"),
        ("null_parent_hides_children", True),
        ("relational_float_zero", "normalized"),
        ("unary_float_zero", "exact_bits"),
        ("operators", ["join", "set", "group", "sort", "window", "subquery"]),
        ("aggregates", ["count", "count_distinct", "min", "max"]),
        ("scalar_selection", ["comparison", "is_null", "is_not_null", "case", "coalesce", "nullif"]),
        ("retained_unary", ["distinct", "drop_duplicates", "duplicate_mask", "tail",
                            "sample", "forward_fill", "melt", "rolling_count"]),
        ("forward_fill", "null_parent_only"),
        ("melt_conversion", "same_shape_and_declared_child_types"),
        ("shared_direct_and_relational_state", True),
        ("compact_selected_payload", True),
        ("native_sort_spill", "explicit_relational_sort_policy"),
        ("nested_pivot_state", True),
        ("nested_pivot_reference", "docs/architecture/native-nested-pivot-state-2026-10-06.md"),
    ):
        if nested_state.get(field) != value:
            blockers.append(f"{JSON_PATH}: native_nested_keys_state.{field} differs from its contract")
    for field in ("structured_scalar_literals", "nested_arithmetic",
                  "group_join_window_spill", "general_state_spill", "total_rss_bound",
                  "fallback_attempted", "external_engine_invoked"):
        if nested_state.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_nested_keys_state.{field} must be false")

    pivot = payload.get("native_dynamic_pivot_composition", {})
    for field, value in (
        ("scope", "current_source_build_scalar_and_static_nested_pivot_admission"),
        ("nested_reference", "docs/architecture/native-nested-pivot-state-2026-10-06.md"),
    ):
        if pivot.get(field) != value:
            blockers.append(f"{JSON_PATH}: native_dynamic_pivot_composition.{field} differs from its contract")
    for field in ("preparation_metadata_only", "inspection_side_effect_free",
                  "single_use_execution_references", "correlated_parameter_scopes",
                  "declaration_reuse", "absent_domain_column_is_error", "pivot_state_spill"):
        if pivot.get(field) is not True:
            blockers.append(f"{JSON_PATH}: native_dynamic_pivot_composition.{field} must be true")
    for field in ("query_answer_reuse", "total_rss_bound",
                  "fallback_attempted", "external_engine_invoked"):
        if pivot.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_dynamic_pivot_composition.{field} must be false")
    for field, value in (("schema_binding", "during_execution"),
                         ("maximum_top_level_fields", 128),
                         ("small_collection_maximum_rows", 65_536),
                         ("small_collection_maximum_jsonl_bytes", 8 * 1024 * 1024)):
        if pivot.get(field) != value:
            blockers.append(f"{JSON_PATH}: native_dynamic_pivot_composition.{field} differs from its contract")
    if set(pivot.get("local_writers", [])) != {"vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"}:
        blockers.append(f"{JSON_PATH}: scalar dynamic pivot must retain all eight representable local writers")

    nested_pivot = payload.get("native_nested_pivot_state", {})
    for field, value in (
        ("reference", "docs/architecture/native-nested-pivot-state-2026-10-06.md"),
        ("scope", "post_v0_4_0_source_build_static_nested_pivot_state"),
        ("types", ["list", "fixed_size_list", "struct"]),
        ("roles", ["index", "domain", "value"]),
        ("leaf_contract", "native_nested_composition"),
        ("identity", "unary_exact_keys_finite_float_bits"),
        ("domain_names", "sanitized_type_tagged_exact_key_with_collision_suffix"),
        ("selected_cell_aggregates", ["first", "first_unique", "count", "min", "max"]),
        ("python_first_alias", "first_unique"),
        ("first_nulls", "retain_first_complete_value_including_null_parent"),
        ("extrema_nulls", "skip_null_parents_shared_recursive_child_order"),
        ("count_nulls", "count_rows_including_null_values"),
        ("cell_schema", "preserve_recursive_types_widen_root_nullability_only"),
        ("nested_cell_fill", "absent_or_null"),
        ("nested_extrema_margins", "utf8_index_only"),
        ("shared_direct_and_relational_state", True),
        ("compact_selected_payload", True),
        ("maximum_top_level_fields", 128),
        ("nested_writers", ["vortex", "parquet", "arrow_ipc", "avro", "json", "jsonl"]),
        ("csv_nested_translation", "quoted_json_text_without_logical_dtype"),
        ("denied_nested_writers", ["orc"]),
    ):
        if nested_pivot.get(field) != value:
            blockers.append(f"{JSON_PATH}: native_nested_pivot_state.{field} differs from its contract")
    for field in ("nested_index_margins", "nested_sum_mean", "total_rss_bound",
                  "performance_claim", "fallback_attempted", "external_engine_invoked"):
        if nested_pivot.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_nested_pivot_state.{field} must be false")
    blockers.extend(validate_acceptance(repo_root, "native_nested_pivot_state", nested_pivot))

    for name, section in (("native_dynamic_pivot_composition", pivot),
                          ("native_nested_pivot_state", nested_pivot)):
        if section.get("pivot_state_spill") is not True or section.get("pivot_state_spill_contract") != "native_relational_pivot_spill":
            blockers.append(f"{JSON_PATH}: {name} must reference its explicit relational pivot spill contract")
    pivot_spill = payload.get("native_relational_pivot_spill", {})
    for field, value in (
        ("reference", "docs/architecture/native-pivot-pressure-2026-10-08.md"),
        ("required_features", ["vortex-local-primitives", "vortex-write"]),
        ("required_platform", "unix"),
        ("public_resource_arguments", ["memory_gb", "max_parallelism", "spill"]),
        ("spill_fields", ["workspace", "quota_bytes", "buffer_bytes"]),
        ("minimum_buffer_bytes", 1048576),
        ("minimum_quota_bytes", 32768),
        ("default_strategy", "resident"),
        ("explicit_strategy", "native_sparse_latest_state_runs_with_bounded_exact_lookup"),
        ("source_scope", ["file", "resident_memory"]),
        ("type_scope", "existing_scalar_typed_and_static_nested_pivot_contracts"),
        ("maximum_top_level_fields", 128),
        ("file_and_resident_scalar_writer_count", 8),
        ("typed_nested_writers", ["vortex", "json", "jsonl", "arrow_ipc", "parquet", "avro", "csv"]),
        ("dead_owner_recovery", "identity_checked_owned_cleanup_and_restart"),
    ):
        if pivot_spill.get(field) != value:
            blockers.append(f"{JSON_PATH}: native_relational_pivot_spill.{field} differs from its contract")
    for field in ("existing_observation_and_error_order_preserved", "shared_query_grant_and_disk_quota",
                  "incremental_result_delivery", "cleanup_required_before_publication"):
        if pivot_spill.get(field) is not True:
            blockers.append(f"{JSON_PATH}: native_relational_pivot_spill.{field} must be true")
    for field in ("direct_prepared_unary_spill", "one_shot_batch_input", "execution_resume",
                  "total_rss_bound", "performance_claim", "fallback_attempted", "external_engine_invoked"):
        if pivot_spill.get(field) is not False:
            blockers.append(f"{JSON_PATH}: native_relational_pivot_spill.{field} must be false")
    if "CSV_uses_quoted_JSON_text_cells_without_native_dtype_persistence" not in pivot_spill.get("writer_fidelity", ""):
        blockers.append(f"{JSON_PATH}: pivot spill must retain the nested CSV type-loss boundary")
    blockers.extend(validate_acceptance(repo_root, "native_relational_pivot_spill", pivot_spill))

    for field in (
        "no_fallback_policy",
        "metadata_first_discovery",
        "unsupported_paths_fail_closed",
        "public_claims_require_dynamic_evidence",
    ):
        if guardrails.get(field) is not True:
            blockers.append(f"{JSON_PATH}: guardrails.{field} must be true")
    for field in (
        "production_claim_allowed",
        "performance_claim_allowed",
        "broad_sql_dataframe_parity_claim_allowed",
    ):
        if guardrails.get(field) is not False:
            blockers.append(f"{JSON_PATH}: guardrails.{field} must be false")

    report = {
        "schema_version": "shardloom.user_surface_index_gate.v1",
        "checked_files": [
            MD_PATH.as_posix(),
            JSON_PATH.as_posix(),
            COMMAND_REGISTRY_PATH.as_posix(),
            *REQUIRED_BACKLINKS.keys(),
        ],
        "registered_command_count": len(commands),
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
        "status": "passed" if not blockers else "failed",
    }
    return report, blockers


def main() -> int:
    args = parse_args()
    report, blockers = validate(args.repo_root)
    print(json.dumps(report, indent=2, sort_keys=True))
    if blockers:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
