//! Extracted unit test suite for `public_workflow_route` (`E2E-ARCH-01`).

fn native_sql_pipeline_field<'a>(unified: &'a str, legacy: &'a str) -> &'a str {
    if cfg!(all(feature = "vortex-local-primitives", unix)) {
        unified
    } else {
        legacy
    }
}

fn native_sql_pipeline_route(legacy: &str) -> &str {
    native_sql_pipeline_field("native_vortex_relational_collect", legacy)
}

#[test]
fn sql_predicate_summary_requires_lossless_literal_representation() {
    for (sql, expected) in [
        ("text = ''", "eq:text:"),
        ("text = 'é'", "eq:text:é"),
        ("text = 'O''Reilly'", "eq:text:O'Reilly"),
        ("number = 123", "eq:number:123"),
        ("text LIKE '%123%'", "contains:text:123"),
        ("number IN (1,2)", "in:number:1,2"),
    ] {
        assert_eq!(
            super::summary_tiny_predicate_from_sql(sql).as_deref(),
            Some(expected),
            "{sql}"
        );
    }
    for sql in [
        "text = ' '",
        "text = '  text  '",
        "text = '123'",
        "text = 'a,b'",
        "text = 'a;b'",
        "text = 'a|b'",
        "text LIKE '% a %'",
        "text LIKE '%a;b%'",
        "text LIKE '%a|b%'",
        "text IN ('', 'x')",
        "text IN ('123', 'x')",
        "text IN ('a,b', 'x')",
        "text = 'x' AND text != ' a '",
    ] {
        assert!(
            super::summary_tiny_predicate_from_sql(sql).is_none(),
            "{sql}"
        );
    }
}

#[test]
fn native_vortex_row_export_fidelity_status_classifies_supported_formats() {
    let cases = [
        (
            "vortex",
            "native_vortex_logical_types_preserved_physical_fidelity_reported",
        ),
        ("parquet", "typed_compatibility_binary_export"),
        ("arrow-ipc", "typed_compatibility_binary_export"),
        ("avro", "typed_compatibility_binary_export"),
        ("orc", "typed_compatibility_binary_export"),
        (
            "json",
            "compatibility_text_row_export_type_metadata_not_preserved",
        ),
        (
            "jsonl",
            "compatibility_text_row_export_type_metadata_not_preserved",
        ),
        (
            "csv",
            "compatibility_text_row_export_type_metadata_not_preserved",
        ),
        ("unknown", "unknown_output_format"),
    ];

    for (output_format, expected) in cases {
        assert_eq!(
            super::native_vortex_row_export_fidelity_status(output_format),
            expected,
            "unexpected fidelity status for {output_format}"
        );
    }
}

#[test]
fn small_count_selection_survives_public_summary_without_invented_workers() {
    let payload = serde_json::json!({
        "aggregate_worker_selection": "small_numeric_count_direct",
        "aggregate_worker_selection_source_rows": 257,
        "aggregate_worker_selection_max_rows": 32768,
        "aggregate_provider_cpu_scope": "same_prepared_source;small_numeric_count_direct_selected_before_scan"
    });
    let summary = format!("simple aggregate values={payload}");
    let mut fields = Vec::new();
    super::append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&summary));
    for (key, value) in payload.as_object().unwrap() {
        assert!(
            fields.contains(&(
                format!("local_primitive_{key}"),
                value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_owned)
            ))
        );
    }
    assert!(
        !fields
            .iter()
            .any(|(key, _)| key.starts_with("local_primitive_aggregate_workers_"))
    );
}

#[test]
fn partitioned_native_summary_preserves_typed_result_boundary_evidence() {
    let payload = serde_json::json!({
        "aggregate_result_boundary": "owned_native_columns_from_completed_state",
        "aggregate_result_serialized_json_round_trip": false,
        "rows": 4097,
        "values": null,
    });
    for summary in [payload.to_string(), format!("aggregate values={payload}")] {
        let partitioned = format!(
            "partitioned_vortex_sources=3;partitioned_execution_strategy=sequential_capillary_parts;{summary}"
        );
        assert_eq!(
            local_primitive_result_summary_payload(&partitioned),
            Some(payload.clone())
        );
        let mut fields = Vec::new();
        append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&partitioned));
        assert!(fields.contains(&(
            "local_primitive_aggregate_result_serialized_json_round_trip".into(),
            "false".into(),
        )));
    }
}

#[test]
fn exact_distinct_summary_preserves_counts_and_reservation_scope() {
    let payload = serde_json::json!({
        "aggregate_workers_distinct_group_reduction_jobs": 1,
        "aggregate_workers_exact_distinct_complete_pairs": 19,
        "aggregate_workers_exact_distinct_committed_rows": 101,
        "aggregate_workers_exact_distinct_partition_comparisons": 73,
        "aggregate_workers_exact_distinct_partition_lock_wait_nanos": 21,
        "aggregate_workers_exact_distinct_partition_reconcile_nanos": 90,
        "aggregate_workers_exact_distinct_group_reduce_nanos": 17,
        "aggregate_workers_exact_distinct_entry_credit_claims": 3,
        "aggregate_workers_exact_distinct_entry_credit_returns": 3,
        "aggregate_workers_exact_distinct_scope": "complete pair reduction; not local top-k",
        "exact_distinct_final_reserved_bytes": 4096,
        "exact_distinct_final_reservation_scope": "retained complete groups"
    });
    let mut fields = Vec::new();
    super::append_local_primitive_result_summary_evidence_fields(
        &mut fields,
        Some(&payload.to_string()),
    );
    for (key, value) in payload.as_object().unwrap() {
        assert!(
            fields.contains(&(
                format!("local_primitive_{key}"),
                value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_owned)
            ))
        );
    }
    assert!(
        !fields
            .iter()
            .any(|(key, _)| key == "local_primitive_aggregate_workers_rows")
    );
}

#[test]
fn aggregate_worker_observations_survive_public_summary_without_invented_counters() {
    let summary = serde_json::json!({
        "aggregate_workers_rows": 101,
        "aggregate_workers_peak_active_workers": 3,
        "aggregate_workers_count_work_nanos": 900,
        "aggregate_workers_scope": "parallel work sums; caller merge separate"
    })
    .to_string();
    let mut fields = Vec::new();
    super::append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&summary));
    for (key, value) in [
        ("rows", "101"),
        ("peak_active_workers", "3"),
        ("count_work_nanos", "900"),
        ("scope", "parallel work sums; caller merge separate"),
    ] {
        assert!(fields.contains(&(
            format!("local_primitive_aggregate_workers_{key}"),
            value.into()
        )));
    }
    assert!(
        !fields
            .iter()
            .any(|(key, _)| key == "local_primitive_aggregate_workers_compute_threads")
    );
}

#[test]
fn complete_partition_observations_preserve_each_provided_counter() {
    let mut payload = serde_json::Map::new();
    let counters = [
        "count",
        "complete_groups",
        "committed_rows",
        "lock_wait_nanos",
        "reconcile_work_nanos",
        "arrange_work_nanos",
        "selection_work_nanos",
        "equality_comparisons",
        "native_handoffs",
        "selection_jobs",
        "retry_jobs",
        "source_replays",
        "discarded_input_rows",
        "discarded_attempt_nanos",
        "source_replay_nanos",
    ];
    for (index, name) in counters.iter().enumerate() {
        payload.insert(
            format!("aggregate_workers_partition_{name}"),
            (index + 1).into(),
        );
    }
    let summary = serde_json::Value::Object(payload).to_string();
    let mut fields = Vec::new();
    super::append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&summary));
    for (index, name) in counters.iter().enumerate() {
        assert!(fields.contains(&(
            format!("local_primitive_aggregate_workers_partition_{name}"),
            (index + 1).to_string()
        )));
    }
    assert!(
        !fields
            .iter()
            .any(|(key, _)| key == "local_primitive_aggregate_workers_rows")
    );
}

use super::*;

fn field(fields: &[(String, String)], key: &str) -> String {
    fields
        .iter()
        .find_map(|(field_key, value)| (field_key == key).then(|| value.clone()))
        .unwrap_or_else(|| panic!("missing route field: {key}"))
}

fn assert_field_values(fields: &[(String, String)], expected_values: &[(&str, &str)]) {
    for (key, expected_value) in expected_values {
        assert_eq!(
            field(fields, key),
            *expected_value,
            "unexpected route field value for {key}"
        );
    }
}

#[test]
fn local_primitive_result_summary_lifts_native_numeric_decode_work_exactly() {
    let scope = "retained_aggregate_attempt;one_source_array_native_primitive_execution_then_typed_values_copy;Array_nbytes_estimates_not_unique_allocations;referenced_child_data_can_exceed_selected_rows;null_mask_work_additional;no_Arrow_or_external_engine;not_zero_decode_or_RSS_bound";
    let payload = serde_json::json!({
        "aggregate_native_numeric_accessor": {
            "native_decode_calls": 3,
            "rows": 23,
            "source_array_nbytes_estimate": 101,
            "canonical_array_nbytes_estimate": 191,
            "typed_value_bytes_copied": 184,
            "decode_and_typed_copy_nanos": 9_007_199_254_740_993_u64,
            "max_source_array_rows": 11,
            "columns": ["renamed_measure", "東京"],
            "scope": scope,
            "future_unadmitted_counter": 99
        }
    });
    for summary in [payload.to_string(), format!("aggregate values={payload}")] {
        let mut fields = Vec::new();
        append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&summary));
        for (key, expected) in [
            ("native_decode_calls", "3"),
            ("rows", "23"),
            ("source_array_nbytes_estimate", "101"),
            ("canonical_array_nbytes_estimate", "191"),
            ("typed_value_bytes_copied", "184"),
            ("decode_and_typed_copy_nanos", "9007199254740993"),
            ("max_source_array_rows", "11"),
            ("columns", "[\"renamed_measure\",\"東京\"]"),
            ("scope", scope),
        ] {
            assert_eq!(
                field(
                    &fields,
                    &format!("local_primitive_aggregate_native_numeric_accessor_{key}")
                ),
                expected
            );
        }
        assert_eq!(fields.len(), 9);
    }
    for summary in ["{}", "{\"aggregate_native_numeric_accessor\":null}"] {
        let mut fields = Vec::new();
        append_local_primitive_result_summary_evidence_fields(&mut fields, Some(summary));
        assert_eq!(fields, [] as [(String, String); 0]);
    }
}

#[test]
fn local_primitive_result_summary_lifts_encoded_numeric_work_without_precision_loss() {
    let payload = serde_json::json!({
        "aggregate_encoded_numeric_reduction": {
            "work": {"calls": 2, "constant_arrays": 0, "run_end_arrays": 2,
                "logical_rows": 100_000_000, "child_primitive_executions": 4,
                "child_rows": 8, "max_child_rows": 2, "weighted_value_visits": 4,
                "elapsed_nanos": 9_007_199_254_740_993_u64, "unadmitted": 42},
            "scope": "native run children; ordered sums; no expanded logical array"
        }
    });
    for summary in [payload.to_string(), format!("aggregate values={payload}")] {
        let mut fields = Vec::new();
        append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&summary));
        assert_eq!(fields.len(), 10);
        assert_eq!(
            field(
                &fields,
                "local_primitive_aggregate_encoded_numeric_reduction_elapsed_nanos"
            ),
            "9007199254740993"
        );
        assert_eq!(
            field(
                &fields,
                "local_primitive_aggregate_encoded_numeric_reduction_constant_arrays"
            ),
            "0"
        );
        assert_eq!(
            field(
                &fields,
                "local_primitive_aggregate_encoded_numeric_reduction_child_rows"
            ),
            "8"
        );
    }
}

#[test]
fn local_primitive_group_candidate_peak_is_conditional() {
    for observed in [None, Some(0), Some(62)] {
        let payload = observed.map_or_else(
            || serde_json::json!({}),
            |peak| serde_json::json!({"peak_buffered_order_candidates": peak}),
        );
        let mut fields = Vec::new();
        append_local_primitive_result_summary_evidence_fields(
            &mut fields,
            Some(&payload.to_string()),
        );
        let reported = fields
            .iter()
            .find(|(k, _)| k == "local_primitive_peak_buffered_order_candidates");
        assert_eq!(
            reported.map(|(_, value)| value.clone()),
            observed.map(|n| n.to_string())
        );
    }
}

#[test]
fn local_primitive_result_summary_lifts_sort_driver_count_and_scope() {
    let scope = "admitted provider drivers, not measured utilization";
    for drivers in [0, 1] {
        let payload = serde_json::json!({
            "sort_provider_background_workers": drivers,
            "sort_provider_cpu_scope": scope,
        });
        for summary in [payload.to_string(), format!("sort_rows values={payload}")] {
            let mut fields = Vec::new();
            append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&summary));
            assert_eq!(fields.len(), 2);
            assert_eq!(
                field(&fields, "local_primitive_sort_provider_background_workers"),
                drivers.to_string()
            );
            assert_eq!(
                field(&fields, "local_primitive_sort_provider_cpu_scope"),
                scope
            );
        }
    }
}

#[test]
fn local_primitive_result_summary_lifts_sort_scan_split_policy() {
    for policy in [
        "not_scanned",
        "vortex_layout",
        "vortex_layout_with_scratch_headroom",
        "fixed_2048_for_scratch_headroom",
    ] {
        let payload = serde_json::json!({"sort_scan_split_policy": policy});
        for summary in [payload.to_string(), format!("sort_rows values={payload}")] {
            let mut fields = Vec::new();
            append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&summary));
            assert_eq!(fields.len(), 1);
            assert_eq!(
                field(&fields, "local_primitive_sort_scan_split_policy"),
                policy
            );
        }
    }
}

#[test]
fn local_primitive_result_summary_lifts_segment_reuse_scopes_and_live_owners() {
    let payload = serde_json::json!({"scan_segment_reuse": {
        "hits": 2, "closed": true, "retained_entries": 0,
        "retention_live_owned_bytes": 97, "completed_segment_bytes": 9_007_199_254_740_993_u64,
        "completed_segment_bytes_scope": "logical segments, not filesystem reads",
        "memory_scope": "evicted result slices retain full allocation credit",
        "unknown_future_counter": 123
    }});
    let mut fields = Vec::new();
    append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&payload.to_string()));
    assert_eq!(fields.len(), 7);
    assert_eq!(
        field(&fields, "local_primitive_scan_segment_reuse_closed"),
        "true"
    );
    assert_eq!(
        field(
            &fields,
            "local_primitive_scan_segment_reuse_retained_entries"
        ),
        "0"
    );
    assert_eq!(
        field(
            &fields,
            "local_primitive_scan_segment_reuse_retention_live_owned_bytes"
        ),
        "97"
    );
    assert_eq!(
        field(
            &fields,
            "local_primitive_scan_segment_reuse_completed_segment_bytes"
        ),
        "9007199254740993"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn local_primitive_result_summary_lifts_runtime_strategy_fields() {
    let mut payload = serde_json::json!({
        "group_output_strategy": "source_order_limited_group_admission_no_sort",
        "group_admission_strategy": "first_k_source_order_groups_then_existing_key_updates",
        "candidate_groups": 10,
        "retained_candidate_groups": 10,
        "aggregate_key_encoding_mode": "typed_hash_exact",
        "aggregate_update_strategy": "transformed_dictionary_general_measure_group_update",
        "expression_fusion_strategy": "dictionary_weighted_transform_fusion",
        "expression_plan_fingerprint_status": "shared_dictionary_value_transform_update",
        "embedded_derived_column_rewrite_status": "applied",
        "embedded_derived_column_rewrites": ["__shardloom_derived_utf8_len_SearchPhrase"],
        "aggregate_accessor_summary": "URL:vortex_utf8_dictionary",
        "aggregate_accessor_materialization_status": "vortex_dictionary_or_primitive_only",
        "aggregate_vortex_dictionary_accessor_columns": "URL",
        "aggregate_materialized_accessor_columns": "none",
        "aggregate_accessor_blockers": "none",
        "aggregate_accessor_layout_correlation_status": "not_required_no_materialized_accessors",
        "aggregate_accessor_layout_correlation_columns": "none",
        "aggregate_accessor_layout_correlation_blockers": "none",
        "aggregate_accessor_layout_correlation_dictionary_status": "vortex_dictionary_layout_present",
        "aggregate_accessor_layout_correlation_encoding_inventory": "chunked,dict",
        "distinct_state_strategy": "proofbound_candidate_exact_distinct_sets",
        "uniqueness_proof_status": "proofbound_string_count_distinct_exact_topk",
        "candidate_rows_seen": 15_911,
        "retained_candidate_rows": 10,
        "retention_selection_strategy": "capillary_select_nth_retention_window",
        "late_materialization_chunks_scanned": 1_626,
        "late_materialization_early_stop_applied": true,
        "late_materialization_max_selected_source_ordinal": 98_655_788,
        "sort_predicate_strategy": "residual_predicate_source_ordinals",
        "string_count_topk_candidate_signature_prefilter": true,
        "numeric_utf8_topk_heavy_hitter_second_pass": true,
        "numeric_utf8_topk_heavy_hitter_candidate_groups": 59_813,
        "numeric_utf8_topk_heavy_hitter_capacity": 65_536,
        "numeric_utf8_topk_heavy_hitter_threshold": 1_525,
        "numeric_utf8_topk_heavy_hitter_exact_proof": true,
        "string_count_distinct_topk_heavy_hitter_second_pass": true,
        "string_count_distinct_topk_heavy_hitter_candidate_groups": 25_503,
        "string_count_distinct_topk_heavy_hitter_capacity": 65_536,
        "string_count_distinct_topk_heavy_hitter_threshold": 200,
        "string_count_distinct_topk_heavy_hitter_exact_proof": true,
        "string_count_distinct_topk_candidate_signature_prefilter": true,
    });
    for (key, value) in [
        (
            "string_count_topk_candidate_code_prefilter",
            serde_json::Value::Bool(true),
        ),
        (
            "string_count_topk_heavy_hitter_exact_counts_source",
            serde_json::Value::String("first_pass_dictionary_exact_histogram".to_string()),
        ),
        (
            "string_count_topk_first_pass_exact_histogram_enabled",
            serde_json::Value::Bool(true),
        ),
        (
            "string_count_topk_first_pass_exact_histogram_promoted",
            serde_json::Value::Bool(true),
        ),
        (
            "string_count_topk_first_pass_exact_histogram_disabled",
            serde_json::Value::Bool(false),
        ),
        (
            "string_count_topk_first_pass_exact_histogram_entries",
            serde_json::Value::Number(serde_json::Number::from(18_342_019)),
        ),
        (
            "string_count_topk_first_pass_exact_histogram_entry_budget",
            serde_json::Value::Number(serde_json::Number::from(67_108_864)),
        ),
        (
            "string_count_topk_first_pass_exact_histogram_input_rows",
            serde_json::Value::Number(serde_json::Number::from(99_997_497)),
        ),
        (
            "string_count_topk_first_pass_exact_histogram_input_values",
            serde_json::Value::Number(serde_json::Number::from(99_997_497)),
        ),
        (
            "string_count_topk_candidate_free_chunks_skipped",
            serde_json::Value::Number(serde_json::Number::from(3)),
        ),
        (
            "string_count_topk_candidate_free_rows_skipped",
            serde_json::Value::Number(serde_json::Number::from(1_024)),
        ),
        (
            "numeric_utf8_topk_candidate_code_prefilter",
            serde_json::Value::Bool(true),
        ),
        (
            "numeric_utf8_topk_candidate_free_chunks_skipped",
            serde_json::Value::Number(serde_json::Number::from(2)),
        ),
        (
            "numeric_utf8_topk_candidate_free_rows_skipped",
            serde_json::Value::Number(serde_json::Number::from(768)),
        ),
        (
            "string_count_distinct_topk_candidate_code_prefilter",
            serde_json::Value::Bool(true),
        ),
        (
            "string_count_distinct_topk_candidate_free_chunks_skipped",
            serde_json::Value::Number(serde_json::Number::from(4)),
        ),
        (
            "string_count_distinct_topk_candidate_free_rows_skipped",
            serde_json::Value::Number(serde_json::Number::from(2_048)),
        ),
        (
            "string_count_distinct_topk_exact_sets_source",
            serde_json::Value::String("first_pass_bounded_exact_sets".to_string()),
        ),
        (
            "string_count_distinct_topk_first_pass_exact_sets",
            serde_json::Value::Bool(true),
        ),
        (
            "string_count_distinct_topk_first_pass_exact_sets_disabled",
            serde_json::Value::Bool(false),
        ),
        (
            "string_count_distinct_topk_first_pass_exact_set_entries",
            serde_json::Value::Number(serde_json::Number::from(2_930_104)),
        ),
        (
            "string_count_distinct_topk_first_pass_exact_set_entry_budget",
            serde_json::Value::Number(serde_json::Number::from(50_331_648)),
        ),
    ] {
        payload
            .as_object_mut()
            .expect("payload object")
            .insert(key.to_string(), value);
    }
    payload.as_object_mut().expect("payload object").insert(
        "late_materialization_selected_row_refs_used".to_string(),
        serde_json::Value::Bool(true),
    );
    payload.as_object_mut().expect("payload object").insert(
        "transformed_dictionary_lazy_utf8_minmax_updates".to_string(),
        serde_json::Value::Bool(true),
    );
    let payload = payload.to_string();
    for summary in [
        payload.clone(),
        format!("simple_aggregate input_rows=100 output_rows=10 values={payload}"),
    ] {
        let mut fields = Vec::new();

        append_local_primitive_result_summary_evidence_fields(&mut fields, Some(&summary));

        assert_field_values(
            &fields,
            &[
                (
                    "local_primitive_group_output_strategy",
                    "source_order_limited_group_admission_no_sort",
                ),
                (
                    "local_primitive_group_admission_strategy",
                    "first_k_source_order_groups_then_existing_key_updates",
                ),
                ("local_primitive_candidate_groups", "10"),
                (
                    "local_primitive_aggregate_key_encoding_mode",
                    "typed_hash_exact",
                ),
                (
                    "local_primitive_aggregate_update_strategy",
                    "transformed_dictionary_general_measure_group_update",
                ),
                (
                    "local_primitive_expression_fusion_strategy",
                    "dictionary_weighted_transform_fusion",
                ),
                (
                    "local_primitive_expression_plan_fingerprint_status",
                    "shared_dictionary_value_transform_update",
                ),
                (
                    "local_primitive_transformed_dictionary_lazy_utf8_minmax_updates",
                    "true",
                ),
                (
                    "local_primitive_embedded_derived_column_rewrite_status",
                    "applied",
                ),
                (
                    "local_primitive_embedded_derived_column_rewrites",
                    r#"["__shardloom_derived_utf8_len_SearchPhrase"]"#,
                ),
                (
                    "local_primitive_aggregate_accessor_summary",
                    "URL:vortex_utf8_dictionary",
                ),
                (
                    "local_primitive_aggregate_accessor_materialization_status",
                    "vortex_dictionary_or_primitive_only",
                ),
                (
                    "local_primitive_aggregate_vortex_dictionary_accessor_columns",
                    "URL",
                ),
                (
                    "local_primitive_aggregate_materialized_accessor_columns",
                    "none",
                ),
                ("local_primitive_aggregate_accessor_blockers", "none"),
                (
                    "local_primitive_aggregate_accessor_layout_correlation_status",
                    "not_required_no_materialized_accessors",
                ),
                (
                    "local_primitive_aggregate_accessor_layout_correlation_columns",
                    "none",
                ),
                (
                    "local_primitive_aggregate_accessor_layout_correlation_blockers",
                    "none",
                ),
                (
                    "local_primitive_aggregate_accessor_layout_correlation_dictionary_status",
                    "vortex_dictionary_layout_present",
                ),
                (
                    "local_primitive_aggregate_accessor_layout_correlation_encoding_inventory",
                    "chunked,dict",
                ),
                (
                    "local_primitive_distinct_state_strategy",
                    "proofbound_candidate_exact_distinct_sets",
                ),
                (
                    "local_primitive_uniqueness_proof_status",
                    "proofbound_string_count_distinct_exact_topk",
                ),
                ("local_primitive_candidate_rows_seen", "15911"),
                (
                    "local_primitive_retention_selection_strategy",
                    "capillary_select_nth_retention_window",
                ),
                (
                    "local_primitive_late_materialization_chunks_scanned",
                    "1626",
                ),
                (
                    "local_primitive_late_materialization_early_stop_applied",
                    "true",
                ),
                (
                    "local_primitive_late_materialization_max_selected_source_ordinal",
                    "98655788",
                ),
                (
                    "local_primitive_late_materialization_selected_row_refs_used",
                    "true",
                ),
                (
                    "local_primitive_sort_predicate_strategy",
                    "residual_predicate_source_ordinals",
                ),
                (
                    "local_primitive_string_count_topk_candidate_signature_prefilter",
                    "true",
                ),
                (
                    "local_primitive_string_count_topk_heavy_hitter_exact_counts_source",
                    "first_pass_dictionary_exact_histogram",
                ),
                (
                    "local_primitive_string_count_topk_first_pass_exact_histogram_enabled",
                    "true",
                ),
                (
                    "local_primitive_string_count_topk_first_pass_exact_histogram_promoted",
                    "true",
                ),
                (
                    "local_primitive_string_count_topk_first_pass_exact_histogram_disabled",
                    "false",
                ),
                (
                    "local_primitive_string_count_topk_first_pass_exact_histogram_entries",
                    "18342019",
                ),
                (
                    "local_primitive_string_count_topk_first_pass_exact_histogram_entry_budget",
                    "67108864",
                ),
                (
                    "local_primitive_string_count_topk_first_pass_exact_histogram_input_rows",
                    "99997497",
                ),
                (
                    "local_primitive_string_count_topk_first_pass_exact_histogram_input_values",
                    "99997497",
                ),
                (
                    "local_primitive_string_count_topk_candidate_code_prefilter",
                    "true",
                ),
                (
                    "local_primitive_string_count_topk_candidate_free_chunks_skipped",
                    "3",
                ),
                (
                    "local_primitive_string_count_topk_candidate_free_rows_skipped",
                    "1024",
                ),
                (
                    "local_primitive_numeric_utf8_topk_heavy_hitter_second_pass",
                    "true",
                ),
                (
                    "local_primitive_numeric_utf8_topk_heavy_hitter_candidate_groups",
                    "59813",
                ),
                (
                    "local_primitive_numeric_utf8_topk_heavy_hitter_threshold",
                    "1525",
                ),
                (
                    "local_primitive_numeric_utf8_topk_heavy_hitter_exact_proof",
                    "true",
                ),
                (
                    "local_primitive_numeric_utf8_topk_candidate_code_prefilter",
                    "true",
                ),
                (
                    "local_primitive_numeric_utf8_topk_candidate_free_chunks_skipped",
                    "2",
                ),
                (
                    "local_primitive_numeric_utf8_topk_candidate_free_rows_skipped",
                    "768",
                ),
                (
                    "local_primitive_string_count_distinct_topk_heavy_hitter_second_pass",
                    "true",
                ),
                (
                    "local_primitive_string_count_distinct_topk_heavy_hitter_candidate_groups",
                    "25503",
                ),
                (
                    "local_primitive_string_count_distinct_topk_heavy_hitter_threshold",
                    "200",
                ),
                (
                    "local_primitive_string_count_distinct_topk_heavy_hitter_exact_proof",
                    "true",
                ),
                (
                    "local_primitive_string_count_distinct_topk_exact_sets_source",
                    "first_pass_bounded_exact_sets",
                ),
                (
                    "local_primitive_string_count_distinct_topk_first_pass_exact_sets",
                    "true",
                ),
                (
                    "local_primitive_string_count_distinct_topk_first_pass_exact_sets_disabled",
                    "false",
                ),
                (
                    "local_primitive_string_count_distinct_topk_first_pass_exact_set_entries",
                    "2930104",
                ),
                (
                    "local_primitive_string_count_distinct_topk_first_pass_exact_set_entry_budget",
                    "50331648",
                ),
                (
                    "local_primitive_string_count_distinct_topk_candidate_signature_prefilter",
                    "true",
                ),
                (
                    "local_primitive_string_count_distinct_topk_candidate_code_prefilter",
                    "true",
                ),
                (
                    "local_primitive_string_count_distinct_topk_candidate_free_chunks_skipped",
                    "4",
                ),
                (
                    "local_primitive_string_count_distinct_topk_candidate_free_rows_skipped",
                    "2048",
                ),
            ],
        );
    }
}

#[test]
fn local_primitive_embedded_layout_lifts_footer_pruning_fields() {
    let mut embedded_layout =
        shardloom_vortex::VortexLocalPrimitiveEmbeddedLayoutReport::not_available();
    embedded_layout.status = "metadata_pruned_entire_input".to_string();
    embedded_layout.footer_row_count = 5;
    embedded_layout.footer_segment_count = 2;
    embedded_layout.footer_statistics_available = true;
    embedded_layout.footer_statistics_status = "file_statistics_available".to_string();
    embedded_layout.footer_approx_bytes = Some(4096);
    embedded_layout.footer_dtype_summary = "struct(value=u32,metric=i64)".to_string();
    embedded_layout.footer_layout_summary = "vortex_footer_root_layout".to_string();
    embedded_layout.layout_inventory_scope = "root_only;full_tree_deferred".to_string();
    embedded_layout.layout_inventory_nodes_inspected = 1;
    embedded_layout.metadata_persisted_in_artifact = true;
    embedded_layout.metadata_first_pruning_available = true;
    embedded_layout.metadata_first_pruning_consulted = true;
    embedded_layout.metadata_pruned_entire_input = true;
    embedded_layout.selected_segment_count = 0;
    embedded_layout.skipped_segment_count = 2;
    embedded_layout.vortex_file_stats_reader_available = true;
    embedded_layout.planner_consumption_status =
        "metadata_first_pruning_consulted_pruned".to_string();
    embedded_layout.per_column_metadata_contract =
        "value:logical=primitive_u32:encoding=typed_encoded_scan:derived=source:stats=file_stats_available:roles=statistics_pruning|encoded_execution|late_materialization"
            .to_string();
    embedded_layout.operator_selection_metadata_status =
        "per_column_footer_metadata_available_for_diagnostic_and_pruning".to_string();
    embedded_layout.dictionary_encoding_policy =
        "preserve_vortex_dictionary_encoding_when_present".to_string();
    embedded_layout.late_materialization_status =
        "projection_bound_before_materialization".to_string();
    embedded_layout.no_query_answer_cache = true;
    let mut fields = Vec::new();

    append_local_primitive_embedded_layout_fields(&mut fields, &embedded_layout);

    assert_eq!(
        field(&fields, "local_primitive_embedded_layout_status"),
        "metadata_pruned_entire_input"
    );
    assert_eq!(field(&fields, "local_primitive_footer_row_count"), "5");
    assert_eq!(
        field(&fields, "local_primitive_layout_inventory_scope"),
        "root_only;full_tree_deferred"
    );
    assert_eq!(
        field(&fields, "local_primitive_layout_inventory_nodes_inspected"),
        "1"
    );
    assert_eq!(
        field(&fields, "local_primitive_footer_statistics_available"),
        "true"
    );
    assert_eq!(
        field(&fields, "local_primitive_metadata_first_pruning_consulted"),
        "true"
    );
    assert_eq!(
        field(&fields, "local_primitive_metadata_pruned_entire_input"),
        "true"
    );
    assert_eq!(
        field(&fields, "local_primitive_selected_segment_count"),
        "0"
    );
    assert_eq!(field(&fields, "local_primitive_skipped_segment_count"), "2");
    assert_eq!(
        field(&fields, "local_primitive_planner_consumption_status"),
        "metadata_first_pruning_consulted_pruned"
    );
    assert_eq!(
        field(&fields, "local_primitive_per_column_metadata_contract"),
        "value:logical=primitive_u32:encoding=typed_encoded_scan:derived=source:stats=file_stats_available:roles=statistics_pruning|encoded_execution|late_materialization"
    );
    assert_eq!(
        field(
            &fields,
            "local_primitive_operator_selection_metadata_status"
        ),
        "per_column_footer_metadata_available_for_diagnostic_and_pruning"
    );
    assert_eq!(
        field(&fields, "local_primitive_no_query_answer_cache"),
        "true"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn prepared_local_route_marks_admitted_olap_state_attached_to_embedded_layout_metadata() {
    let preparation_fields = vec![
        (
            "public_workflow_preparation_vortex_prepared_olap_state_status".to_string(),
            "prepared_olap_state_read_through_hit".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_query_time_contract"
                .to_string(),
            "single_vortex_artifact_native_runtime_no_query_answer_sidecar".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_exact_sidecar_family_count"
                .to_string(),
            "0".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_artifact_model"
                .to_string(),
            "single_prepared_vortex_artifact".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_metadata_pruning_contract"
                .to_string(),
            "metadata_first_when_vortex_layout_statistics_are_available_no_query_answer_cache"
                .to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_query_answer_sidecar_status"
                .to_string(),
            "disabled_rejected_for_public_default_runtime".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_evidence_persistence"
                .to_string(),
            "embedded_in_single_prepared_vortex_artifact".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_external_manifest_written"
                .to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_inventory_status"
                .to_string(),
            "opened_single_vortex_artifact_footer".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_inventory_digest"
                .to_string(),
            "fnv64:layout".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_footer_row_count"
                .to_string(),
            "100000000".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_footer_segment_count"
                .to_string(),
            "64".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_footer_statistics_status"
                .to_string(),
            "available".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_footer_encoding_layout_status"
                .to_string(),
            "segment_map_available".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_footer_approx_bytes"
                .to_string(),
            "4096".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_footer_dtype_summary"
                .to_string(),
            "Struct".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_per_column_metadata_contract"
                .to_string(),
            "URL:logical=utf8:encoding=dictionary_or_code_available:derived=source:stats=file_stats_available:roles=predicate_candidate|group_key_candidate|late_materialization".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_operator_selection_metadata_status"
                .to_string(),
            "per_column_dictionary_metadata_available_for_operator_selection".to_string(),
        ),
        (
            "public_workflow_preparation_vortex_prepared_olap_state_layout_metadata_persisted_in_artifact"
                .to_string(),
            "true".to_string(),
        ),
    ];

    let fields = local_prepared_olap_query_attachment_fields(&preparation_fields);

    assert_eq!(
        field(&fields, "public_workflow_prepared_olap_state_attached"),
        "true"
    );
    assert_eq!(
        field(&fields, "public_workflow_prepared_olap_state_consumed"),
        "false"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_embedded_layout_metadata_consumed"
        ),
        "true"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_consumption_status"
        ),
        "attached_to_prepared_native_vortex_route_embedded_layout_metadata_available"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_read_through_status"
        ),
        "attached_to_prepared_native_vortex_route_embedded_layout_metadata_available"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_exact_sidecar_family_count"
        ),
        "0"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_artifact_model"
        ),
        "single_prepared_vortex_artifact"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_metadata_pruning_contract"
        ),
        "metadata_first_when_vortex_layout_statistics_are_available_no_query_answer_cache"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_query_answer_sidecar_status"
        ),
        "disabled_rejected_for_public_default_runtime"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_evidence_persistence"
        ),
        "embedded_in_single_prepared_vortex_artifact"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_external_manifest_written"
        ),
        "false"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_inventory_status"
        ),
        "opened_single_vortex_artifact_footer"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_inventory_digest"
        ),
        "fnv64:layout"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_footer_row_count"
        ),
        "100000000"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_footer_segment_count"
        ),
        "64"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_footer_statistics_status"
        ),
        "available"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_footer_encoding_layout_status"
        ),
        "segment_map_available"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_footer_approx_bytes"
        ),
        "4096"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_footer_dtype_summary"
        ),
        "Struct"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_per_column_metadata_contract"
        ),
        "URL:logical=utf8:encoding=dictionary_or_code_available:derived=source:stats=file_stats_available:roles=predicate_candidate|group_key_candidate|late_materialization"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_operator_selection_metadata_status"
        ),
        "per_column_dictionary_metadata_available_for_operator_selection"
    );
    assert_eq!(
        field(
            &fields,
            "public_workflow_prepared_olap_state_layout_metadata_persisted_in_artifact"
        ),
        "true"
    );
}

fn assert_native_vortex_plan_contract_fields(
    fields: &[(String, String)],
    route_alias: &str,
    payload_kind: &str,
    operator_capillaries: &str,
    source_count: &str,
) {
    assert_eq!(
        field(fields, "native_vortex_plan_contract_schema_version"),
        NATIVE_VORTEX_PLAN_CONTRACT_SCHEMA_VERSION
    );
    assert_eq!(
        field(fields, "native_vortex_plan_contract_status"),
        "admitted"
    );
    assert_eq!(
        field(fields, "native_vortex_plan_route_family"),
        "native_vortex_unified_plan"
    );
    assert_eq!(field(fields, "native_vortex_plan_route_alias"), route_alias);
    assert_eq!(
        field(fields, "native_vortex_plan_payload_kind"),
        payload_kind
    );
    assert_eq!(
        field(fields, "native_vortex_plan_operator_capillaries"),
        operator_capillaries
    );
    assert_eq!(
        field(fields, "native_vortex_plan_source_count"),
        source_count
    );
}

fn assert_public_workflow_plan_contract_fields(fields: &[(String, String)], payload_kind: &str) {
    assert_eq!(
        field(fields, "public_workflow_native_vortex_plan_route_family"),
        "native_vortex_unified_plan"
    );
    assert_eq!(
        field(fields, "public_workflow_native_vortex_plan_payload_kind"),
        payload_kind
    );
}

fn assert_incomplete_native_plan_blocked(plan: &PublicWorkflowRoutePlan) {
    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(
        plan.blocker_id,
        NativeVortexOperationFamily::GeneralQuery.blocker_id()
    );
    assert_eq!(
        plan.blocker_reason,
        NativeVortexOperationFamily::GeneralQuery.blocker_reason()
    );
}

#[test]
fn flat_aggregate_and_sort_sql_preserve_explicit_null_order() {
    let lower = |statement: &str| {
        let request = PublicWorkflowRouteRequest::parse(
            ["sql", "--sql", statement, "--bounded", "true"]
                .into_iter()
                .map(str::to_owned),
        )
        .unwrap();
        effective_public_workflow_request(&request)
    };
    for descending in [false, true] {
        for nulls in ["FIRST", "LAST"] {
            let direction = if descending { "DESC" } else { "ASC" };
            let statement = format!(
                "SELECT cohort, AVG(reading) AS mean, COUNT(*) AS n FROM 'absent.vortex' GROUP BY cohort HAVING n > 1 ORDER BY mean {direction} NULLS {nulls}, cohort ASC NULLS LAST LIMIT 7 OFFSET 2"
            );
            let request = lower(&statement);
            let aggregate: serde_json::Value =
                serde_json::from_str(request.vortex_aggregate.as_deref().unwrap()).unwrap();
            assert_eq!(
                aggregate["order_by"],
                serde_json::json!([
                    {"column":"mean","descending":descending,"nulls":nulls.to_ascii_lowercase()},
                    {"column":"cohort","descending":false,"nulls":"last"},
                ])
            );
            assert_eq!(aggregate["offset"], 2);
            assert_eq!(request.vortex_source_order_limit.as_deref(), Some("7"));
            let scalar = lower(&format!(
                "SELECT AVG(reading) AS mean FROM 'absent.vortex' ORDER BY mean {direction} NULLS {nulls} LIMIT 1"
            ));
            assert_eq!(scalar.vortex_primitive.as_deref(), Some("aggregate"));
            let sorted = lower(&format!(
                "SELECT metric FROM 'absent.vortex' ORDER BY metric {direction} NULLS {nulls} LIMIT 10"
            ));
            let sort: serde_json::Value =
                serde_json::from_str(sorted.vortex_sort_rows.as_deref().unwrap()).unwrap();
            assert_eq!(sort["order_by"][0]["nulls"], nulls.to_ascii_lowercase());
        }
    }
    // Historical provider schema names must not hide unsupported clauses.
    for suffix in ["NULLS MIDDLE", "NULLS FIRST NULLS LAST", "DESC EXTRA"] {
        let request = lower(&format!(
            "SELECT group_key, count(*) AS rows, sum(metric) AS total_metric FROM 'absent.vortex' GROUP BY group_key ORDER BY total_metric {suffix} LIMIT 10"
        ));
        assert!(request.vortex_primitive.is_none());
        assert_ne!(
            plan_public_workflow_route(&request).status,
            CommandStatus::Success
        );
    }
    let request = lower(
        "SELECT group_key, count(*) AS rows, sum(metric) AS total_metric FROM 'absent.vortex' GROUP BY group_key ORDER BY total_metric DESC NULLS FIRST LIMIT 10",
    );
    assert_eq!(request.vortex_primitive.as_deref(), Some("aggregate"));
    let request = lower(
        "SELECT COUNT(*) AS n FROM 'absent.vortex' WHERE nested_payload LIKE '%target%' ORDER BY n NULLS MIDDLE LIMIT 10",
    );
    assert!(request.vortex_primitive.is_none());
    assert_ne!(
        plan_public_workflow_route(&request).status,
        CommandStatus::Success
    );
}

#[test]
fn flat_source_bindings_retain_schema_and_reject_conflicts_without_io() {
    let parse = |source: &str, bindings: &str, extra: &[&str]| {
        let statement = format!(
            "SELECT label, count(*) AS n FROM '{source}' GROUP BY label ORDER BY n DESC NULLS FIRST LIMIT 2"
        );
        let mut args = vec!["sql", "--sql", &statement, "--source-bindings", bindings];
        args.extend_from_slice(extra);
        PublicWorkflowRouteRequest::parse(args.into_iter().map(str::to_owned))
    };
    for source in ["absent.csv", "declared.data"] {
        let bindings =
            format!(r#"{{"{source}":{{"input_format":"csv","source_schema":"label:utf8"}}}}"#);
        let request = parse(source, &bindings, &[]).unwrap();
        assert_eq!(request.input_uri.as_deref(), Some(source));
        assert_eq!(request.input_format.as_deref(), Some("csv"));
        assert_eq!(request.source_schema.as_deref(), Some("label:utf8"));
        let prepared = prepared_local_workflow_native_request(&request).unwrap();
        assert_eq!(
            prepared.request.vortex_primitive.as_deref(),
            Some("aggregate")
        );
        assert!(!prepared.left_target.exists());
        let native_bindings = &prepared.request.source_bindings;
        assert_eq!(native_bindings.len(), 1);
        let binding = native_bindings
            .get(&prepared.left_target.display().to_string())
            .unwrap();
        assert_eq!(binding.input_format, "vortex");
        assert!(binding.source_schema.is_none());
        assert!(request.source_bindings.contains_key(source));
        for extra in [
            vec!["--input", "other.csv"],
            vec!["--input-format", "jsonl"],
            vec!["--source-schema", "label:int64"],
            vec!["--request", "prepare"],
        ] {
            assert!(
                parse(source, &bindings, &extra).is_err(),
                "{source} {extra:?}"
            );
        }
    }
    assert!(
        parse(
            "absent.csv",
            r#"{"unused.csv":{"input_format":"csv"}}"#,
            &[]
        )
        .is_err()
    );
    assert!(
        parse(
            "absent.csv",
            r#"{"absent.csv":{"input_format":"csv"},"unused.csv":{"input_format":"csv"}}"#,
            &[]
        )
        .is_err()
    );
}

#[test]
fn flat_source_bindings_accept_zero_limit_and_offset_without_io() {
    for source in ["absent.csv", "declared.data", "absent.vortex"] {
        let input_format = if source.ends_with(".vortex") {
            "vortex"
        } else {
            "csv"
        };
        let bindings = format!(r#"{{"{source}":{{"input_format":"{input_format}"}}}}"#);
        for suffix in ["LIMIT 0", "LIMIT 00 OFFSET 0", "LIMIT 2 OFFSET 0"] {
            let statement =
                format!("SELECT label, count(*) AS n FROM '{source}' GROUP BY label {suffix}");
            let request = PublicWorkflowRouteRequest::parse(
                ["sql", "--sql", &statement, "--source-bindings", &bindings]
                    .into_iter()
                    .map(str::to_owned),
            )
            .unwrap();
            assert_eq!(request.input_uri.as_deref(), Some(source));
            assert_eq!(request.input_format.as_deref(), Some(input_format));
            if input_format == "vortex" {
                let lowered = effective_public_workflow_request(&request);
                if suffix.starts_with("LIMIT 0") {
                    assert!(lowered.vortex_primitive.is_none());
                } else {
                    assert_eq!(lowered.vortex_primitive.as_deref(), Some("aggregate"));
                }
            }
        }
    }
    for invalid in ["-1", "+1", "1.0", "1 trailing", "18446744073709551616"] {
        for keyword in ["LIMIT", "OFFSET"] {
            let statement = format!("SELECT value FROM 'absent.vortex' {keyword} {invalid}");
            assert!(parse_native_vortex_sql_single_source_shape(&statement).is_none());
        }
    }
}

#[test]
fn source_schema_admission_and_automatic_targets_are_explicit() {
    let parse = |source: &str, schema: &str| {
        PublicWorkflowRouteRequest::parse(
            ["sql", "--input", source, "--source-schema", schema]
                .into_iter()
                .map(str::to_string),
        )
    };
    assert_eq!(
        parse("labels.csv", "label:utf8")
            .unwrap()
            .source_schema
            .as_deref(),
        Some("label:utf8")
    );
    assert!(parse("labels.csv", "invalid").is_err());
    assert!(parse("labels.vortex", "label:utf8").is_err());
    assert!(
        PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--generated-source-kind",
                "range",
                "--source-schema",
                "label:utf8"
            ]
            .into_iter()
            .map(str::to_string),
        )
        .is_err()
    );
    let original = auto_prepared_vortex_target_path("labels.csv", "csv");
    assert_eq!(
        original,
        auto_prepared_vortex_target_path_with_schema("labels.csv", "csv", None)
    );
    let text =
        auto_prepared_vortex_target_path_with_schema("labels.csv", "csv", Some("label:utf8"));
    let integer =
        auto_prepared_vortex_target_path_with_schema("labels.csv", "csv", Some("label:int64"));
    assert_ne!(text, integer);
    assert_ne!(text, original);
}

#[test]
fn prepared_dataframe_preserves_lowered_sql_alongside_its_operation_summary() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "labels.csv",
            "--input-format",
            "csv",
            "--source-schema",
            "label:utf8",
            "--sql",
            "SELECT MIN(label) AS label FROM 'labels.csv' LIMIT 1",
            "--plan",
            "read_csv(labels.csv) -> aggregate(MIN(label) AS label) -> limit(1)",
            "--request",
            "write_vortex",
            "--output",
            "minimum.vortex",
            "--bounded",
            "true",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .unwrap();
    let prepared = prepared_local_workflow_native_request(&request).unwrap();
    assert!(
        prepared
            .request
            .sql_statement
            .as_ref()
            .unwrap()
            .contains("SELECT MIN(label) AS label")
    );
    assert!(prepared.request.source_schema.is_none());
    assert_eq!(
        prepared.request.vortex_primitive.as_deref(),
        Some("aggregate")
    );
    assert!(prepared.request.vortex_aggregate.is_some());
    assert!(
        prepared
            .request
            .sql_statement
            .as_ref()
            .unwrap()
            .contains(&prepared.left_target.display().to_string())
    );
}

#[test]
fn source_rebinding_changes_only_relation_source_tokens() {
    let target = Path::new("target/prepared.vortex");
    for (source, statement, expected) in [
        (
            "target/input.csv",
            "SELECT 'target/input.csv' AS value FROM 'target/input.csv' WHERE label = 'target/input.csv'",
            "SELECT 'target/input.csv' AS value FROM 'target/prepared.vortex' WHERE label = 'target/input.csv'",
        ),
        (
            "target/input.csv",
            "SELECT label IS DISTINCT FROM 'target/input.csv' FROM /* 'target/input.csv' */ 'target/input.csv' WHERE label IS NOT DISTINCT FROM 'target/input.csv' -- FROM 'target/input.csv'\n",
            "SELECT label IS DISTINCT FROM 'target/input.csv' FROM /* 'target/input.csv' */ 'target/prepared.vortex' WHERE label IS NOT DISTINCT FROM 'target/input.csv' -- FROM 'target/input.csv'\n",
        ),
        (
            "target/input.csv",
            "SELECT EXTRACT(YEAR FROM EventTime) FROM input WHERE label IS /* keep */ DISTINCT FROM 'target/input.csv'",
            "SELECT EXTRACT(YEAR FROM EventTime) FROM 'target/prepared.vortex' WHERE label IS /* keep */ DISTINCT FROM 'target/input.csv'",
        ),
        (
            "target/input.csv",
            "SELECT q.label FROM (SELECT label FROM 'target/input.csv' WHERE label = 'target/input.csv') AS q",
            "SELECT q.label FROM (SELECT label FROM 'target/prepared.vortex' WHERE label = 'target/input.csv') AS q",
        ),
        (
            "target/é's.csv",
            "SELECT 'target/é''s.csv' AS value FROM 'target/é''s.csv' WHERE label = 'target/é''s.csv'",
            "SELECT 'target/é''s.csv' AS value FROM 'target/prepared.vortex' WHERE label = 'target/é''s.csv'",
        ),
    ] {
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.input_uri = Some(source.into());
        request.input_format = Some("csv".into());
        request.sql_statement = Some(statement.into());
        assert_eq!(
            prepared_local_workflow_sql_statement(&request, target, None).as_deref(),
            Some(expected),
            "{statement}",
        );
        let spans = sql_source_ref_ranges(statement);
        assert_eq!(spans.len(), 1, "{statement}");
        assert_eq!(
            extract_first_sql_source_ref(statement, false).as_deref(),
            Some(spans[0].value.as_str()),
        );
    }
    let mut request = PublicWorkflowRouteRequest::new("sql".into());
    request.input_uri = Some("left.csv".into());
    request.input_format = Some("csv".into());
    request.sql_statement = Some(
        "SELECT l.id FROM 'left.csv' AS l JOIN 'right.csv' AS r ON l.id = r.id WHERE l.label = 'right.csv'".into(),
    );
    let right = PreparedLocalWorkflowRightSource {
        source_uri: "right.csv".into(),
        source_format: "csv".into(),
        target: PathBuf::from("right.vortex"),
    };
    assert_eq!(
        prepared_local_workflow_sql_statement(&request, target, Some(&right)).as_deref(),
        Some(
            "SELECT l.id FROM 'target/prepared.vortex' AS l JOIN 'right.vortex' AS r ON l.id = r.id WHERE l.label = 'right.csv'"
        ),
    );
}

#[cfg(all(
    unix,
    feature = "vortex-local-primitives",
    feature = "vortex-write",
    feature = "universal-format-io"
))]
#[test]
fn sql_native_strategy_inference_is_independent_of_input_format() {
    let queries = [
        "SELECT COUNT(*) FROM input",
        "SELECT COUNT(*) FROM input WHERE metric > 3",
        "SELECT SUM(metric),COUNT(*),AVG(metric) FROM input",
        "SELECT label,COUNT(*) AS n FROM input GROUP BY label ORDER BY n DESC LIMIT 3",
        "SELECT metric FROM input WHERE metric > 3 LIMIT 2",
        "SELECT label,metric FROM input ORDER BY metric DESC LIMIT 2",
    ];
    let make_request = |format: &str, sql: &str, output: &str| {
        let input = format!("target/input.{format}");
        let mut arguments = vec![
            "sql",
            "--input",
            &input,
            "--input-format",
            format,
            "--sql",
            sql,
            "--request",
            output,
            "--bounded",
            "true",
        ];
        if output != "collect" {
            arguments.extend(["--output", "target/result"]);
        }
        effective_public_workflow_request(
            &PublicWorkflowRouteRequest::parse(arguments.into_iter().map(str::to_string)).unwrap(),
        )
    };
    for sql in queries {
        let collected = make_request("vortex", sql, "collect");
        for output in [
            "collect",
            "write_vortex",
            "write_parquet",
            "write_arrow_ipc",
            "write_avro",
            "write_orc",
            "write_csv",
            "write_json",
            "write_jsonl",
        ] {
            let native = make_request("vortex", sql, output);
            assert!(native.vortex_primitive.is_some(), "native control: {sql}");
            assert_eq!(
                native.vortex_primitive, collected.vortex_primitive,
                "{sql} {output}"
            );
            for format in [
                "csv",
                "tsv",
                "json",
                "jsonl",
                "ndjson",
                "parquet",
                "arrow-ipc",
                "avro",
                "orc",
            ] {
                let request = make_request(format, sql, output);
                assert_eq!(
                    (
                        &request.vortex_primitive,
                        &request.vortex_predicate,
                        &request.vortex_columns,
                        &request.vortex_aggregate,
                        &request.vortex_sort_rows,
                        &request.vortex_source_order_limit,
                    ),
                    (
                        &native.vortex_primitive,
                        &native.vortex_predicate,
                        &native.vortex_columns,
                        &native.vortex_aggregate,
                        &native.vortex_sort_rows,
                        &native.vortex_source_order_limit,
                    ),
                    "format={format} output={output} sql={sql}",
                );
                let plan = plan_public_workflow_route(&request);
                assert_eq!(plan.status, CommandStatus::Success, "{plan:?}");
                assert_eq!(
                    plan.route_id,
                    match output {
                        "collect" => "native_vortex_relational_collect",
                        _ => "native_vortex_relational_write",
                    },
                    "format={format} output={output} sql={sql}",
                );
                let prepared = prepared_local_workflow_native_request(&request).unwrap();
                let prepared_plan = plan_public_workflow_route(&prepared.request);
                assert_eq!(
                    prepared_plan.route_id,
                    plan_public_workflow_route(&native).route_id
                );
            }
        }
    }
}

#[test]
fn native_source_binding_inference_honors_declared_format_without_filename_hint() {
    for format in [
        "vortex",
        "csv",
        "json",
        "jsonl",
        "ndjson",
        "parquet",
        "arrow-ipc",
        "avro",
        "orc",
    ] {
        for (sql, expected) in [
            ("SELECT COUNT(*) FROM 'declared.data'", "count"),
            ("SELECT SUM(value) FROM 'declared.data'", "aggregate"),
        ] {
            // Embedded SQL declarations have no separately supplied primary
            // input; the binding is authoritative even with an opaque name.
            let mut request = PublicWorkflowRouteRequest::new("python".into());
            request.sql_statement = Some(sql.into());
            request.source_bindings.insert(
                "declared.data".into(),
                PublicSourceBinding {
                    input_format: format.into(),
                    source_schema: None,
                    memory_input: None,
                    table_snapshot: None,
                },
            );
            request.infer_defaults();
            let request = effective_public_workflow_request(&request);
            assert_eq!(request.input_uri.as_deref(), Some("declared.data"));
            assert_eq!(request.input_format.as_deref(), Some(format));
            assert_eq!(
                request.vortex_primitive.as_deref(),
                Some(expected),
                "{format} {sql}"
            );
        }
    }
}

#[test]
fn sql_source_alias_inference_does_not_reinterpret_quoted_paths() {
    for format in ["vortex", "parquet", "orc", "csv"] {
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.input_format = Some(format.into());
        request.input_uri = Some(format!("target/input.{format}"));
        request.requested_output = "collect".into();
        request.sql_statement = Some("SELECT COUNT(*) FROM 'input'".into());
        request.infer_defaults();
        assert!(
            infer_native_vortex_sql_payload(&request).is_none(),
            "{format}"
        );
        assert_eq!(
            request.sql_statement.as_deref(),
            Some("SELECT COUNT(*) FROM 'input'")
        );
        for alias in ["input", "hits"] {
            request.sql_statement = Some(format!(
                "SELECT COUNT(*) FROM {alias} WHERE label = '{alias}'"
            ));
            request.infer_defaults();
            assert_eq!(
                request.sql_statement,
                Some(format!(
                    "SELECT COUNT(*) FROM 'target/input.{format}' WHERE label = '{alias}'"
                ))
            );
            assert!(
                infer_native_vortex_sql_payload(&request).is_some(),
                "{format} {alias}"
            );
        }
    }
}

#[test]
fn route_planner_admits_equivalent_sql_and_dataframe_direct_local_file_routes() {
    let sql = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT id FROM 'target/input.csv' LIMIT 10",
            "--request",
            "collect",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("sql route request");
    let dataframe = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.csv",
            "--input-format",
            "csv",
            "--sql",
            "SELECT id FROM 'target/input.csv' LIMIT 10",
            "--request",
            "collect",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("dataframe route request");

    let sql_plan = plan_public_workflow_route(&sql);
    let dataframe_plan = plan_public_workflow_route(&dataframe);

    if !shardloom_vortex::vortex_ingest_write_feature_enabled() {
        assert_eq!(sql_plan.status, CommandStatus::Unsupported);
        assert_eq!(
            sql_plan.blocker_id,
            "cg21.route.local_file_vortex_ingest_feature_gated"
        );
        assert_eq!(dataframe_plan.status, CommandStatus::Unsupported);
        assert_eq!(dataframe_plan.blocker_id, sql_plan.blocker_id);
        return;
    }

    if cfg!(feature = "vortex-local-primitives") {
        for plan in [&sql_plan, &dataframe_plan] {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_relational_collect");
            assert_eq!(plan.resolved_internal_command, "native-vortex-relational");
            assert_eq!(
                plan.vortex_normalization_point,
                "source_schemas_bound_before_native_demand;compatibility_conversion_is_an_admitted_source_boundary"
            );
            assert_eq!(plan.execution_mode, "native_vortex");
            assert!(plan.preparation_included);
            assert!(plan.query_timing_starts_after_preparation);
        }
    } else {
        for plan in [&sql_plan, &dataframe_plan] {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "cg21.route.local_file_vortex_primitive_feature_gated"
            );
        }
    }
}

#[test]
fn route_request_source_fingerprint_policy_defaults_to_metadata_and_allows_explicit_digest() {
    let default_request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.parquet",
            "--input-format",
            "parquet",
            "--request",
            "prepare",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("default route request");
    let default_plan = plan_public_workflow_route(&default_request);
    let default_fields = execution_attachment_fields("prepare", &default_request, &default_plan);
    assert_eq!(
        field(&default_fields, "public_workflow_source_fingerprint_policy"),
        "metadata_only"
    );

    let explicit_request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.parquet",
            "--input-format",
            "parquet",
            "--request",
            "prepare",
            "--source-fingerprint-policy",
            "content-digest",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("explicit content digest route request");
    let explicit_plan = plan_public_workflow_route(&explicit_request);
    let explicit_fields = execution_attachment_fields("prepare", &explicit_request, &explicit_plan);
    assert_eq!(
        field(
            &explicit_fields,
            "public_workflow_source_fingerprint_policy"
        ),
        "content_digest"
    );

    let invalid = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.parquet",
            "--input-format",
            "parquet",
            "--request",
            "prepare",
            "--source-fingerprint-policy",
            "mandatory-sha",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect_err("invalid source fingerprint policy is rejected");
    assert!(
        invalid
            .to_string()
            .contains("unsupported route source fingerprint policy"),
        "{invalid}"
    );
}

#[test]
fn route_planner_blocks_explicit_direct_local_file_policy() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.csv",
            "--input-format",
            "csv",
            "--sql",
            "SELECT id FROM 'target/input.csv' LIMIT 10",
            "--plan",
            "read_csv(target/input.csv) -> select(id) -> limit(10)",
            "--request",
            "collect",
            "--execution-policy",
            "direct",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("dataframe run request");
    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(plan.route_id, "blocked");
    assert_eq!(plan.blocker_id, "cg21.route.direct_local_file_blocked");
    assert_eq!(
        field(&fields, "route_support_status"),
        "unsupported_boundary"
    );
    assert_eq!(
        field(&fields, "vortex_middle_status"),
        "blocked_or_unsupported"
    );
    assert_eq!(field(&fields, "underlying_runtime_command"), "not_resolved");
    assert_eq!(
        field(&fields, "local_workflow_runtime_profile"),
        "not_applicable"
    );
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_blocks_unbounded_collect_before_execution() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "python",
            "--input",
            "target/input.csv",
            "--input-format",
            "csv",
            "--plan",
            "read_csv(target/input.csv)",
            "--request",
            "collect",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(plan.route_id, "blocked");
    assert_eq!(
        field(&fields, "blocker_id"),
        "cg21.route.unbounded_collect_blocked"
    );
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
    assert_eq!(field(&fields, "runtime_execution"), "false");
}

#[test]
fn route_planner_detects_sql_limit_token_outside_quoted_literals() {
    let limited = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT id FROM 'target/input.csv'\nLIMIT\n10",
            "--request",
            "collect",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("limited sql route request");
    let blocked = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT 'limit' AS label FROM 'target/input.csv'",
            "--request",
            "collect",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("quoted sql route request");

    let limited_plan = plan_public_workflow_route(&limited);
    let blocked_plan = plan_public_workflow_route(&blocked);

    assert_ne!(
        limited_plan.blocker_id,
        "cg21.route.unbounded_collect_blocked"
    );
    assert_eq!(
        blocked_plan.blocker_id,
        "cg21.route.unbounded_collect_blocked"
    );
}

#[test]
fn route_planner_does_not_infer_scalar_path_literals_as_sources() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT 'target/input.csv' AS label LIMIT 1",
            "--request",
            "collect",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("scalar literal route request");

    let plan = plan_public_workflow_route(&request);

    assert_eq!(request.input_uri, None);
    assert!(request.source_bindings.is_empty());
    if cfg!(all(feature = "vortex-local-primitives", unix)) {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.resolved_internal_command, "native-vortex-relational");
        assert!(!plan.preparation_included);
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
        assert!(!plan.preparation_included);
    }
}

#[test]
fn route_planner_ignores_limit_inside_sql_comments() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT id FROM 'target/input.csv' -- LIMIT 1\nWHERE id > 0",
            "--request",
            "collect",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("commented limit route request");

    let plan = plan_public_workflow_route(&request);

    assert_eq!(request.input_uri.as_deref(), Some("target/input.csv"));
    assert!(!request.bounded);
    assert_eq!(plan.blocker_id, "cg21.route.unbounded_collect_blocked");
}

#[test]
fn route_planner_blocks_newline_from_source_without_declared_input() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT *\nFROM events",
            "--request",
            "write_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("newline from route request");

    let plan = plan_public_workflow_route(&request);

    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(
        plan.blocker_id,
        if cfg!(all(feature = "vortex-local-primitives", unix)) {
            "cg21.route.native_relational_not_admitted"
        } else {
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        }
    );
}

#[test]
fn route_planner_requires_vortex_input_for_native_vortex_policy() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "cli",
            "--input",
            "target/input.csv",
            "--input-format",
            "csv",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native policy route request");

    let plan = plan_public_workflow_route(&request);

    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(plan.blocker_id, "cg21.route.native_vortex_input_required");
}

#[test]
fn route_planner_blocks_collect_fanout_payloads() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.csv",
            "--input-format",
            "csv",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--fanout-output",
            "csv=target/out.csv",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("collect fanout route request");

    let plan = plan_public_workflow_route(&request);

    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(plan.blocker_id, "cg21.route.collect_fanout_blocked");
}

#[test]
fn route_planner_preserves_count_producer_for_output() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "cli",
            "--input",
            "target/input.vortex",
            "--input-format",
            "vortex",
            "--request",
            "write_jsonl",
            "--output",
            "target/out.jsonl",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--vortex-primitive",
            "count",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native output route request");

    let request = effective_public_workflow_request(&request);
    assert_eq!(request.vortex_primitive.as_deref(), Some("count"));
    assert!(request.vortex_aggregate.is_none());
    let plan = plan_public_workflow_route(&request);
    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        );
    }
    let fields = route_fields(&request, &plan);
    assert_eq!(field(&fields, "native_vortex_operation_family"), "sink");
    assert_eq!(
        field(&fields, "native_vortex_required_feature_gate"),
        "vortex-local-primitives"
    );
    if !cfg!(feature = "vortex-local-primitives") {
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "feature_gated"
        );
    }
}

#[test]
fn route_planner_attaches_native_vortex_contract_for_primitive() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "filter_project_limit",
            "--vortex-primitive",
            "filter_project",
            "--vortex-predicate",
            "metric >= 10",
            "--vortex-columns",
            "id,metric",
            "--vortex-source-order-limit",
            "100",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native primitive route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(plan.route_id, "native_vortex_filter_project");
    assert_eq!(
        field(&fields, "native_vortex_user_route_contract_schema_version"),
        NATIVE_VORTEX_USER_ROUTE_CONTRACT_SCHEMA_VERSION
    );
    assert_native_vortex_plan_contract_fields(
        &fields,
        "native_vortex_filter_project",
        "primitive_operator",
        "vortex_scan,filter_project_limit,bounded_materialization",
        "1",
    );
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        "filter_project_limit"
    );
    assert_eq!(
        field(&fields, "native_vortex_capability_status"),
        "supported_with_materialization_boundary"
    );
    assert_eq!(
        field(&fields, "native_vortex_required_feature_gate"),
        "default"
    );
    assert_eq!(
        field(&fields, "typed_result_contract"),
        "bounded_python_rows_with_explicit_materialization_boundary"
    );
    assert_eq!(
        field(&fields, "decode_materialization_boundary"),
        "native_vortex_zero_decode_runtime_with_bounded_python_materialization_boundary"
    );
    assert_eq!(
        field(
            &attachments,
            "public_workflow_native_vortex_operation_family"
        ),
        "filter_project_limit"
    );
    assert_public_workflow_plan_contract_fields(&attachments, "primitive_operator");
    assert_eq!(
        field(&attachments, "public_workflow_source_format"),
        "vortex"
    );
}

#[test]
fn route_planner_preserves_explicit_runtime_parallelism_ceiling_with_evidence() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--vortex-primitive",
            "filter_project",
            "--vortex-predicate",
            "metric >= 10",
            "--vortex-columns",
            "id,metric",
            "--vortex-source-order-limit",
            "100",
            "--max-parallelism",
            "1",
            "--memory-gb",
            "4",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native primitive route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(field(&fields, "requested_max_parallelism"), "1");
    assert_eq!(field(&fields, "max_parallelism"), "1");
    assert_eq!(field(&fields, "dynamic_parallelism_floor_applied"), "false");
    assert_eq!(
        field(&attachments, "public_workflow_requested_max_parallelism"),
        "1"
    );
    assert_eq!(field(&attachments, "public_workflow_max_parallelism"), "1");
    assert_eq!(
        field(
            &attachments,
            "public_workflow_dynamic_parallelism_floor_applied"
        ),
        "false"
    );
    assert_eq!(
        public_workflow_effective_resource_envelope(&request)
            .unwrap()
            .max_parallelism(),
        1
    );
    let mut invalid = request.clone();
    invalid.resources.max_parallelism = Some("0".into());
    assert!(public_workflow_effective_resource_envelope(&invalid).is_err());
    assert_eq!(
        public_workflow_effective_max_parallelism_label(&invalid),
        "invalid"
    );
    let mut incomplete = request;
    incomplete.resources.max_parallelism = None;
    let error = public_workflow_effective_resource_envelope(&incomplete).unwrap_err();
    assert!(error.to_string().contains("max_parallelism"));
}

#[test]
fn route_planner_infers_payloadless_native_vortex_primitive_facade_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(orders.vortex) -> filter(gte:value:3) -> select(metric,value) -> limit(5)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("payloadless native primitive route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(plan.route_id, "native_vortex_filter_project");
    assert_eq!(
        field(&fields, "route_runtime_status"),
        "global_runtime_supported"
    );
    assert_eq!(field(&fields, "vortex_primitive"), "filter_project");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        "filter_project_limit"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "filter_project"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_predicate"),
        "gte:value:3"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_columns"),
        "metric,value"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_source_order_limit"),
        "5"
    );
}

#[test]
fn route_planner_scalar_projections_require_complete_sql_admission() {
    for projection in [
        "NULL",
        "true",
        "False",
        "NULL AS missing",
        "1 AS one",
        "metric + 1 AS next_metric",
        "COALESCE(NULL,NULL) AS missing",
        "unsupported_fn(metric) AS invalid",
    ] {
        for suffix in ["", " WHERE metric >= 0", " WHERE metric >= 0 LIMIT 2"] {
            let statement = format!("SELECT {projection} FROM 'orders.vortex'{suffix}");
            let request = PublicWorkflowRouteRequest::parse(
                [
                    "sql",
                    "--input",
                    "orders.vortex",
                    "--input-format",
                    "vortex",
                    "--sql",
                    &statement,
                    "--request",
                    "collect",
                    "--bounded",
                    "true",
                ]
                .into_iter()
                .map(str::to_string),
            )
            .unwrap();
            assert!(
                infer_native_vortex_sql_payload(&request).is_none(),
                "{statement}"
            );
        }
    }
}

#[test]
fn route_planner_grouped_projections_preserve_complete_sql_declarations() {
    for projection in [
        "SUM(metric) AS total,value",
        "SUM(metric) AS total",
        "value AS id,SUM(metric) AS total",
        "value AS id,SUM(metric+1) AS total",
    ] {
        let statement =
            format!("SELECT {projection} FROM 'orders.vortex' GROUP BY value ORDER BY value");
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--sql",
                &statement,
                "--request",
                "collect",
                "--bounded",
                "true",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        assert!(
            infer_native_vortex_sql_payload(&request).is_none(),
            "{statement}"
        );
    }
    assert!(
        aggregate_payload_from_sql_projection(
            "value,SUM(metric) AS total",
            Some("value"),
            None,
            Some("value"),
            None,
        )
        .is_some()
    );
}

#[test]
fn route_planner_infers_native_vortex_sql_not_equal_count_where_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--input",
            "hits.vortex",
            "--input-format",
            "vortex",
            "--sql",
            "SELECT COUNT(*) FROM 'hits.vortex' WHERE AdvEngineID <> 0",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL not-equal count route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(
        plan.route_id,
        native_sql_pipeline_route("native_vortex_count_where")
    );
    assert_eq!(field(&fields, "vortex_primitive"), "count_where");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        native_sql_pipeline_field("general_query", "count")
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_predicate"),
        "neq:AdvEngineID:0"
    );
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_payloadless_native_vortex_tail_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(orders.vortex) -> select(id,metric) -> tail(10)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("payloadless native tail route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_tail");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "bounded_python_rows_with_explicit_materialization_boundary"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_required_feature_gate"),
            "vortex-local-primitives"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "tail");
    assert_eq!(field(&fields, "native_vortex_operation_family"), "top_n");
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "tail"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_columns"),
        "id,metric"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_source_order_limit"),
        "10"
    );
}

#[test]
fn route_planner_infers_payloadless_native_vortex_duplicate_mask_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(orders.vortex) -> select(id,label) -> duplicate_mask(id) -> limit(2)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("payloadless native duplicate-mask route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_duplicate_mask");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "bounded_python_rows_with_explicit_materialization_boundary"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_required_feature_gate"),
            "vortex-local-primitives"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "duplicate_mask");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        "duplicate_mask"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "duplicate_mask"
    );
    assert_eq!(field(&attachments, "public_workflow_vortex_columns"), "id");
    assert_eq!(
        field(&attachments, "public_workflow_vortex_source_order_limit"),
        "2"
    );
}

#[test]
fn route_planner_separates_payloadless_duplicate_keep_policy_from_columns() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(orders.vortex) -> select(id,label) -> duplicate_mask(id,keep=last) -> limit(2)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("payloadless native duplicate-mask keep route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_duplicate_mask");
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
    }
    assert_eq!(field(&fields, "vortex_primitive"), "duplicate_mask");
    assert_eq!(field(&fields, "vortex_columns"), "id");
    assert_eq!(field(&fields, "vortex_duplicate_keep"), "last");
    assert_eq!(field(&attachments, "public_workflow_vortex_columns"), "id");
    assert_eq!(
        field(&attachments, "public_workflow_vortex_duplicate_keep"),
        "last"
    );
}

#[test]
fn route_planner_infers_payloadless_native_vortex_drop_duplicates_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(orders.vortex) -> select(id,label) -> drop_duplicates(id,keep=last) -> limit(2)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("payloadless native drop-duplicates route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_drop_duplicates");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "bounded_python_rows_with_explicit_materialization_boundary"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "drop_duplicates");
    assert_eq!(field(&fields, "vortex_columns"), "id,label");
    assert_eq!(field(&fields, "vortex_deduplicate_key_columns"), "id");
    assert_eq!(field(&fields, "vortex_duplicate_keep"), "last");
    assert_eq!(field(&fields, "native_vortex_operation_family"), "distinct");
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "drop_duplicates"
    );
    assert_eq!(
        field(
            &attachments,
            "public_workflow_vortex_deduplicate_key_columns"
        ),
        "id"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_duplicate_keep"),
        "last"
    );
}

#[test]
fn route_planner_blocks_native_vortex_drop_duplicates_without_key_columns() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "distinct",
            "--vortex-primitive",
            "drop_duplicates",
            "--vortex-columns",
            "id,label",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("drop-duplicates without key columns route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(plan.route_id, "blocked");
    assert_eq!(plan.blocker_id, "cg21.route.native_vortex_payload_invalid");
    assert_eq!(
        plan.diagnostics
            .first()
            .and_then(|diagnostic| diagnostic.feature.as_deref()),
        Some("public_workflow_route.vortex_deduplicate_key_columns")
    );
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_attaches_native_vortex_expression_project_route() {
    let expression_projection = r#"{"columns":["id","amount"],"rewrites":[{"kind":"mask_scalar","target_column":"amount","predicate":"lt:amount:0","replacement":{"type":"int64","value":0}}]}"#;
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "expression_project",
            "--vortex-primitive",
            "expression_project",
            "--vortex-columns",
            "id,amount",
            "--vortex-source-order-limit",
            "5",
            "--vortex-expression-projection",
            expression_projection,
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native expression-project route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_expression_project");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "bounded_python_rows_with_explicit_materialization_boundary"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_required_feature_gate"),
            "vortex-local-primitives"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "expression_project");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        "expression_project"
    );
    assert_eq!(
        field(&fields, "vortex_expression_projection_present"),
        "true"
    );
    assert_eq!(
        field(&fields, "vortex_expression_projection_changed_columns"),
        "amount"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "expression_project"
    );
    assert_eq!(
        field(
            &attachments,
            "public_workflow_vortex_expression_projection_present"
        ),
        "true"
    );
    assert_eq!(
        field(
            &attachments,
            "public_workflow_vortex_expression_projection_changed_columns"
        ),
        "amount"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_columns"),
        "id,amount"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_source_order_limit"),
        "5"
    );
}

#[test]
fn route_planner_attaches_native_vortex_melt_route() {
    let melt_projection = r#"{"id_columns":["id"],"value_columns":["amount_a","amount_b"],"variable_column":"measure","value_column":"amount"}"#;
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "melt",
            "--vortex-primitive",
            "melt",
            "--vortex-columns",
            "id,amount_a,amount_b",
            "--vortex-source-order-limit",
            "4",
            "--vortex-melt-projection",
            melt_projection,
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native melt route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_melt");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "bounded_python_rows_with_explicit_materialization_boundary"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_required_feature_gate"),
            "vortex-local-primitives"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "melt");
    assert_eq!(field(&fields, "native_vortex_operation_family"), "melt");
    assert_eq!(field(&fields, "vortex_melt_projection_present"), "true");
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "melt"
    );
    assert_eq!(
        field(
            &attachments,
            "public_workflow_vortex_melt_projection_present"
        ),
        "true"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_columns"),
        "id,amount_a,amount_b"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_source_order_limit"),
        "4"
    );
}

#[test]
fn route_planner_attaches_native_vortex_explode_route() {
    let explode_projection = r#"{"column":"items"}"#;
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "explode",
            "--vortex-primitive",
            "explode",
            "--vortex-columns",
            "id,items",
            "--vortex-source-order-limit",
            "4",
            "--vortex-explode-projection",
            explode_projection,
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native explode route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_explode");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "bounded_python_rows_with_explicit_materialization_boundary"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_required_feature_gate"),
            "vortex-local-primitives"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "explode");
    assert_eq!(field(&fields, "native_vortex_operation_family"), "explode");
    assert_eq!(field(&fields, "vortex_explode_projection_present"), "true");
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "explode"
    );
    assert_eq!(
        field(
            &attachments,
            "public_workflow_vortex_explode_projection_present"
        ),
        "true"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_columns"),
        "id,items"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_source_order_limit"),
        "4"
    );
}

#[test]
fn route_planner_passes_explode_projection_columns_to_primitive_arg() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "explode",
            "--vortex-primitive",
            "explode",
            "--vortex-columns",
            "id,items",
            "--vortex-source-order-limit",
            "4",
            "--vortex-explode-projection",
            r#"{"column":"items"}"#,
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native explode route request");

    let primitive_arg =
        native_vortex_primitive_arg_for_request(&request, PublicVortexPrimitive::Explode)
            .expect("primitive arg");

    assert!(primitive_arg.starts_with("explode:"));
    assert!(primitive_arg.contains(r#""column":"items""#));
    assert!(primitive_arg.contains(r#""columns":"id,items""#));
}

#[test]
fn native_source_expression_predicate_survives_export_and_collect_normalization() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.vortex",
            "--input-format",
            "vortex",
            "--request",
            "write_vortex",
            "--output",
            "target/output.vortex",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--vortex-primitive",
            "expression_project",
            "--vortex-expression-projection",
            r#"{"structured_columns":[{"name":"renamed","source":"source_value"}]}"#,
            "--vortex-predicate",
            "gte:filter_only:3",
            "--vortex-source-order-limit",
            "2",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
        ]
        .into_iter()
        .map(str::to_owned),
    )
    .unwrap();
    let binding = NativeVortexInputBinding::single(
        "target/input.vortex",
        &shardloom_exec::live_memory::LiveMemoryPool::new(1 << 20).unwrap(),
    )
    .unwrap();
    let exported = native_vortex_primitive_row_export_execution(
        &request,
        "target/input.vortex".to_owned(),
        PublicVortexPrimitive::ExpressionProject,
    )
    .unwrap()
    .primitive_request;
    let (collected, _, _) = native_vortex_bound_request_and_arg(
        &request,
        PublicVortexPrimitive::ExpressionProject,
        &binding,
    )
    .unwrap();
    for parsed in [exported, collected] {
        assert_eq!(
            parsed.predicate,
            Some(vortex_primitive_execution::parse_tiny_predicate("gte:filter_only:3").unwrap())
        );
        assert_eq!(parsed.source_order_limit, Some(2));
        assert_eq!(
            parsed.structured_projection.unwrap().output_columns(),
            ["renamed"]
        );
    }
    let mut constructed = request.clone();
    constructed.vortex_expression_projection =
        Some(r#"{"structured_columns":[{"name":"renamed","source":"source_value"},{"name":"constructed","array":[1]}]}"#.to_owned());
    let exported = native_vortex_primitive_row_export_execution(
        &constructed,
        "target/input.vortex".to_owned(),
        PublicVortexPrimitive::ExpressionProject,
    )
    .unwrap()
    .primitive_request;
    let (collected, _, _) = native_vortex_bound_request_and_arg(
        &constructed,
        PublicVortexPrimitive::ExpressionProject,
        &binding,
    )
    .unwrap();
    for parsed in [exported, collected] {
        assert_eq!(
            parsed.predicate,
            Some(vortex_primitive_execution::parse_tiny_predicate("gte:filter_only:3").unwrap())
        );
        assert_eq!(parsed.source_order_limit, Some(2));
        assert_eq!(
            parsed.structured_projection.unwrap().output_columns(),
            ["renamed", "constructed"]
        );
    }
}

#[test]
fn route_planner_attaches_native_vortex_pivot_route() {
    let pivot_projection = r#"{"aggregate":"first_unique","index_column":"id","pivot_column":"label","value_column":"amount"}"#;
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "pivot",
            "--vortex-primitive",
            "pivot",
            "--vortex-columns",
            "id,label,amount",
            "--vortex-source-order-limit",
            "4",
            "--vortex-pivot-projection",
            pivot_projection,
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native pivot route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_pivot");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "bounded_python_rows_with_explicit_materialization_boundary"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "pivot");
    assert_eq!(field(&fields, "native_vortex_operation_family"), "pivot");
    assert_eq!(field(&fields, "vortex_pivot_projection_present"), "true");
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "pivot"
    );
    assert_eq!(
        field(
            &attachments,
            "public_workflow_vortex_pivot_projection_present"
        ),
        "true"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_columns"),
        "id,label,amount"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_source_order_limit"),
        "4"
    );
}

#[test]
fn route_planner_attaches_native_vortex_pivot_row_export_route() {
    let pivot_projection =
        r#"{"aggregate":"sum","index_column":"id","pivot_column":"label","value_column":"amount"}"#;
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--request",
            "write_jsonl",
            "--output",
            "target/pivot.jsonl",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "sink",
            "--vortex-primitive",
            "pivot",
            "--vortex-columns",
            "id,label,amount",
            "--vortex-pivot-projection",
            pivot_projection,
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native pivot row export route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_primitive_row_export");
        assert_eq!(
            plan.resolved_internal_command,
            "vortex-local-primitive-row-export"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        );
    }
    assert_eq!(field(&fields, "native_vortex_operation_family"), "sink");
    assert_eq!(field(&fields, "vortex_pivot_projection_present"), "true");
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

fn structured_export_request(
    request_output: &'static str,
    output_path: &'static str,
) -> PublicWorkflowRouteRequest {
    PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(orders.vortex) -> select(id) -> with_column(values,ARRAY[1,2,NULL]) -> with_column(payload,STRUCT(label, amount)) -> limit(2)",
            "--request",
            request_output,
            "--output",
            output_path,
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--allow-overwrite",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("structured export route request")
}

fn assert_structured_export_route(
    request_output: &'static str,
    output_path: &'static str,
    expected_contract: &str,
    expected_feature_gate: &str,
) {
    let request = structured_export_request(request_output, output_path);

    let effective = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(
        effective.vortex_primitive.as_deref(),
        Some("expression_project")
    );
    assert_eq!(
        effective.native_vortex_operation_family.as_deref(),
        Some("expression_project")
    );
    assert!(
        effective
            .vortex_expression_projection
            .as_deref()
            .is_some_and(native_vortex_expression_projection_is_structured)
    );
    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_primitive_row_export");
        assert_eq!(field(&fields, "typed_sink_contract"), expected_contract);
        assert_eq!(
            field(&fields, "native_vortex_required_feature_gate"),
            expected_feature_gate
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "expression_project");
    assert_eq!(
        field(&fields, "vortex_expression_projection_present"),
        "true"
    );
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_structured_binary_export_from_plan_summary() {
    assert_structured_export_route(
        "write_parquet",
        "target/nested.parquet",
        "native_vortex_structured_row_stream_to_parquet_arrow_avro_compatibility_sink",
        "vortex-local-primitives,universal-format-io",
    );
}

#[test]
fn route_planner_infers_structured_vortex_export_from_plan_summary() {
    assert_structured_export_route(
        "write_vortex",
        "target/nested.vortex",
        "native_vortex_structured_row_stream_to_vortex_sink",
        "vortex-local-primitives,vortex-write,universal-format-io",
    );
}

#[test]
fn route_planner_lowers_sql_star_projection_for_collection_and_export() {
    for operation in ["collect", "write_vortex", "write_parquet", "write_json"] {
        let mut args = vec![
            "sql",
            "--sql",
            "SELECT * FROM 'hits.vortex'",
            "--input-format",
            "vortex",
            "--request",
            operation,
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ];
        if operation != "collect" {
            args.extend(["--output", "target/star-output"]);
        }
        let request = PublicWorkflowRouteRequest::parse(args.into_iter().map(str::to_string))
            .expect("star projection route");
        let effective = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective);
        let fields = route_fields(&effective, &plan);
        assert_eq!(field(&fields, "vortex_primitive"), "project", "{operation}");
        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success, "{operation}");
        }
    }
}

#[test]
fn route_planner_admits_plain_projection_binary_export() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--request",
            "write_parquet",
            "--output",
            "target/out.parquet",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "sink",
            "--vortex-primitive",
            "project",
            "--vortex-columns",
            "id,label",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("plain binary export route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        );
    }
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_attaches_native_vortex_rolling_window_route() {
    let rolling_window = r#"{"aggregate":"sum","min_periods":3,"output_column":"rolling_amount","source_column":"amount","window_size":3}"#;
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "rolling_window",
            "--vortex-primitive",
            "rolling_window",
            "--vortex-columns",
            "amount",
            "--vortex-source-order-limit",
            "4",
            "--vortex-rolling-window",
            rolling_window,
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native rolling-window route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_rolling_window");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "bounded_python_rows_with_explicit_materialization_boundary"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_required_feature_gate"),
            "vortex-local-primitives"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "rolling_window");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        "rolling_window"
    );
    assert_eq!(field(&fields, "vortex_rolling_window_present"), "true");
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "rolling_window"
    );
    assert_eq!(
        field(
            &attachments,
            "public_workflow_vortex_rolling_window_present"
        ),
        "true"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_columns"),
        "amount"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_source_order_limit"),
        "4"
    );
}

#[test]
fn route_planner_does_not_infer_primitive_from_partial_plan_summary() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "fact.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(fact.vortex) -> join(dim.vortex,dim_key,dim_key,inner,f,d,) -> filter(gte:value:3)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("partial native primitive route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_incomplete_native_plan_blocked(&plan);
    assert_eq!(field(&fields, "vortex_primitive"), "none");
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_rejects_benchmark_shortcuts_before_public_sql_or_sinks() {
    for scenario in [
        "group-by-aggregation",
        "null-heavy-aggregate",
        "hash-join",
        "sort-and-top-k",
        "clean-cast-filter-write",
        "malformed-timestamp-dirty-csv",
        "nested-json-field-scan",
    ] {
        for surface in ["sql", "dataframe"] {
            for output in ["collect", "write_vortex"] {
                let mut args = vec![
                    surface,
                    "--input",
                    "fact.vortex",
                    "--input-format",
                    "vortex",
                    "--request",
                    output,
                    "--bounded",
                    "true",
                    "--native-vortex-provider-scenario",
                    scenario,
                ];
                if surface == "sql" {
                    args.extend(["--sql", "SELECT * FROM 'fact.vortex' LIMIT 1"]);
                }
                if output == "write_vortex" {
                    args.extend(["--output", "target/never-created.vortex"]);
                }
                let error = PublicWorkflowRouteRequest::parse(args.into_iter().map(str::to_string))
                    .expect_err("removed benchmark selector must fail during parsing");
                assert!(
                    error
                        .message()
                        .contains("--native-vortex-provider-scenario")
                );
            }
        }
    }
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[test]
fn route_planner_does_not_infer_filtered_group_by_provider_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "fact.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(fact.vortex) -> filter(metric >= 0) -> group_by(group_key) -> aggregate(count(*) AS rows,sum(metric) AS total_metric) -> limit(100)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("filtered group-by provider route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_incomplete_native_plan_blocked(&plan);
    assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[test]
fn route_planner_does_not_infer_top_n_provider_with_noncanonical_limit() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "fact.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(fact.vortex) -> select(id,group_key,metric) -> sort(desc,metric) -> limit(1)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("top-N provider route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(plan.route_id, "native_vortex_sort_rows");
    assert_eq!(field(&fields, "resolved_internal_command"), "vortex-run");
    assert_eq!(field(&fields, "native_vortex_operation_family"), "top_n");
    assert_eq!(field(&fields, "vortex_primitive"), "sort_rows");
    assert_eq!(field(&fields, "vortex_source_order_limit"), "1");
    assert_eq!(field(&fields, "vortex_sort_rows_present"), "true");
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[test]
fn route_planner_does_not_infer_provider_from_sql_literals() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--input",
            "orders.vortex",
            "--input-format",
            "vortex",
            "--sql",
            "SELECT 'count(*) AS rows', 'sum(metric) AS total_metric', 'WHERE metric >= 0', 'GROUP BY group_key' FROM 'orders.vortex' LIMIT 1",
            "--plan",
            "read_vortex(orders.vortex) -> filter(metric >= 0) -> group_by(group_key) -> aggregate(count(*) AS rows,sum(metric) AS total_metric) -> limit(100)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("literal bait SQL route request");

    // Literal projections are admitted by the shared scalar binder. SQL
    // keywords inside those values must not infer aggregate/filter routes.
    assert!(infer_native_vortex_sql_payload(&request).is_none());
    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(plan.route_id, "native_vortex_relational_collect");
    assert_eq!(plan.diagnostics, [] as [shardloom_core::Diagnostic; 0]);
    assert_eq!(
        field(&fields, "resolved_internal_command"),
        "native-vortex-relational"
    );
    assert_eq!(field(&fields, "vortex_primitive"), "none");
    assert_eq!(field(&fields, "runtime_execution"), "false");
    assert_eq!(field(&fields, "source_io_performed"), "false");
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[test]
fn route_planner_does_not_infer_provider_for_report_requests() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "fact.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(fact.vortex) -> filter(metric >= 0) -> group_by(group_key) -> aggregate(count(*) AS rows,sum(metric) AS total_metric) -> limit(100)",
            "--request",
            "explain",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("payloadless report route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_general_route_missing"
    );
    assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[test]
fn route_planner_does_not_infer_provider_from_quoted_plan_arguments() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "fact.vortex",
            "--input-format",
            "vortex",
            "--plan",
            "read_vortex(fact.vortex) -> filter('metric >= 0') -> group_by(group_key) -> aggregate('count(*) AS rows','sum(metric) AS total_metric') -> limit(1)",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("quoted payloadless provider route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_incomplete_native_plan_blocked(&plan);
    assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_attaches_native_vortex_scalar_aggregate_route() {
    let aggregate = r#"{"measures":[{"function":"sum","column":"metric","alias":"sum_metric"},{"function":"count","alias":"rows"}]}"#;
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--input",
            "target/input.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "aggregate",
            "--vortex-primitive",
            "aggregate",
            "--vortex-aggregate",
            aggregate,
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native aggregate primitive route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);
    let attachments = execution_attachment_fields("run", &request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_aggregate");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        "aggregate"
    );
    assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
    assert_eq!(
        field(&attachments, "public_workflow_vortex_primitive"),
        "aggregate"
    );
    assert_eq!(
        field(&attachments, "public_workflow_vortex_aggregate_present"),
        "true"
    );
}

#[test]
fn route_planner_attaches_native_vortex_scalar_aggregate_row_export_route() {
    let aggregate = r#"{"measures":[{"function":"sum","column":"metric","alias":"sum_metric"},{"function":"count","alias":"rows"}]}"#;
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.vortex",
            "--input-format",
            "vortex",
            "--request",
            "write_jsonl",
            "--output",
            "target/native-vortex-aggregate-output.jsonl",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "aggregate",
            "--vortex-primitive",
            "aggregate",
            "--vortex-aggregate",
            aggregate,
            "--allow-overwrite",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native aggregate row export route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_primitive_row_export");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "production_admitted_local_workflow"
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(field(&fields, "native_vortex_operation_family"), "sink");
    assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
}

#[test]
fn route_planner_infers_native_vortex_sql_scalar_aggregate_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT SUM(metric), COUNT(*), AVG(metric) FROM 'target/input.vortex'",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL aggregate route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        native_sql_pipeline_field("general_query", "aggregate")
    );
    assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
}

#[test]
fn route_planner_infers_native_vortex_sql_wide_sum_offset_aggregate_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth - 2) FROM 'hits.vortex'",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL wide aggregate route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        native_sql_pipeline_field("general_query", "aggregate")
    );
    assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
    assert!(aggregate.contains(r#""column":"ResolutionWidth""#));
    assert!(aggregate.contains(r#""argument_offset":1"#));
    assert!(aggregate.contains(r#""argument_offset":-2"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_grouped_aggregate_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT UserID, SearchPhrase, COUNT(*) FROM 'hits.vortex' GROUP BY UserID, SearchPhrase LIMIT 10",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL grouped aggregate route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        native_sql_pipeline_field("general_query", "aggregate")
    );
    assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
    assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
}

#[test]
fn route_planner_infers_native_vortex_sql_grouped_count_distinct_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM 'hits.vortex' GROUP BY RegionID ORDER BY u DESC LIMIT 10",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL grouped count-distinct route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        native_sql_pipeline_field("general_query", "aggregate")
    );
    assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
    assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
    assert!(aggregate.contains(r#""function":"count_distinct""#));
    assert!(aggregate.contains(r#""column":"UserID""#));
    assert!(aggregate.contains(r#""alias":"u""#));
    assert!(aggregate.contains(r#""column":"u""#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_filtered_grouped_topk_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT SearchPhrase, COUNT(*) AS c FROM 'hits.vortex' WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL filtered grouped top-K route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        native_sql_pipeline_field("general_query", "aggregate")
    );
    assert_eq!(field(&fields, "vortex_predicate"), "neq:SearchPhrase:");
    assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
    assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
    assert!(aggregate.contains(r#""group_by":["SearchPhrase"]"#));
    assert!(aggregate.contains(r#""column":"c""#));
    assert!(aggregate.contains(r#""descending":true"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_like_count_as_count_where_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT COUNT(*) FROM 'hits.vortex' WHERE URL LIKE '%google%'",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL LIKE count route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);

    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(
        plan.route_id,
        native_sql_pipeline_route("native_vortex_count_where")
    );
    assert_eq!(field(&fields, "vortex_primitive"), "count_where");
    assert_eq!(field(&fields, "vortex_predicate"), "contains:URL:google");
    assert_eq!(field(&fields, "vortex_aggregate_present"), "false");
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[test]
fn route_planner_infers_native_vortex_sql_nested_payload_shared_filter_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--input",
            "events.vortex",
            "--input-format",
            "vortex",
            "--sql",
            "SELECT id, nested_payload FROM 'events.vortex' WHERE nested_payload LIKE '%target%' LIMIT 100",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL nested-payload contains provider route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(
        plan.route_id,
        native_sql_pipeline_route("native_vortex_filter_project")
    );
    assert_eq!(field(&fields, "vortex_primitive"), "filter_project");
    let lowered = effective_public_workflow_request(&request);
    assert_eq!(
        lowered.vortex_predicate.as_deref(),
        Some("contains:nested_payload:target")
    );
    assert_eq!(lowered.vortex_columns.as_deref(), Some("id,nested_payload"));
    assert_eq!(lowered.vortex_source_order_limit.as_deref(), Some("100"));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_like_grouped_topk_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM 'hits.vortex' WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL LIKE grouped route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "vortex_predicate"),
        "and(contains:URL:google;neq:SearchPhrase:)"
    );
    assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
    assert!(aggregate.contains(r#""function":"min""#));
    assert!(aggregate.contains(r#""column":"URL""#));
    assert!(aggregate.contains(r#""group_by":["SearchPhrase"]"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_binds_clickbench_bare_table_to_declared_vortex_input_for_like_count() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--input",
            "hits.vortex",
            "--input-format",
            "vortex",
            "--sql",
            "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL LIKE count route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);

    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(
        plan.route_id,
        native_sql_pipeline_route("native_vortex_count_where")
    );
    assert_eq!(field(&fields, "vortex_primitive"), "count_where");
    assert_eq!(field(&fields, "vortex_predicate"), "contains:URL:google");
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn local_file_prepare_once_rewrites_bare_declared_sql_source_to_prepared_vortex() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--input",
            "target/hits.parquet",
            "--input-format",
            "parquet",
            "--sql",
            "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "prepare_once",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("local SQL ClickBench route request");
    let plan = plan_public_workflow_route(&request);

    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "local_file_prepare_once_first_query");
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            if cfg!(feature = "vortex-write") {
                "cg21.route.local_file_vortex_primitive_feature_gated"
            } else {
                "cg21.route.local_file_vortex_ingest_feature_gated"
            }
        );
    }

    let prepared =
        prepared_local_workflow_native_request(&request).expect("prepared native request");
    let rewritten = prepared
        .request
        .sql_statement
        .as_deref()
        .expect("rewritten SQL statement");
    assert!(rewritten.contains("FROM 'target/.shardloom/prepared/hits-"));
    assert!(!rewritten.contains("FROM hits"));
    assert_eq!(prepared.request.input_format.as_deref(), Some("vortex"));
    assert_eq!(
        prepared.request.vortex_primitive.as_deref(),
        Some("count_where")
    );
    assert_eq!(
        prepared.request.vortex_predicate.as_deref(),
        Some("contains:URL:google")
    );
}

#[test]
fn prepare_profile_runtime_request_reuses_prepared_target_for_sql_count_profile() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--memory-bytes",
            "1500000001",
            "--max-parallelism",
            "2",
            "--memory-origin",
            "platform",
            "--parallelism-origin",
            "environment",
            "--input",
            "target/hits.parquet",
            "--input-format",
            "parquet",
            "--sql",
            "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
            "--request",
            "prepare",
            "--output",
            "target/hits.vortex",
            "--bounded",
            "true",
            "--execution-policy",
            "prepare_once",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("prepare SQL count profile route request");

    let native = prepared_profile_native_runtime_request(&request, Path::new("target/hits.vortex"))
        .expect("prepared profile native request");
    assert_eq!(
        native.resources.resolve().unwrap(),
        request.resources.resolve().unwrap()
    );
    let plan = plan_public_workflow_route(&native);
    let fields = route_fields(&native, &plan);

    assert_eq!(native.input_format.as_deref(), Some("vortex"));
    assert_eq!(native.input_uri.as_deref(), Some("target/hits.vortex"));
    assert_eq!(native.requested_output, "collect");
    assert_eq!(native.execution_policy, "native_vortex");
    assert_eq!(native.resources.max_parallelism.as_deref(), Some("2"));
    assert_eq!(
        plan.status,
        CommandStatus::Success,
        "route={} blocker={} reason={}",
        plan.route_id,
        plan.blocker_id,
        plan.blocker_reason
    );
    assert_eq!(
        plan.route_id,
        native_sql_pipeline_route("native_vortex_count_where")
    );
    assert_eq!(field(&fields, "vortex_primitive"), "count_where");
    assert_eq!(field(&fields, "vortex_predicate"), "contains:URL:google");
}

#[test]
fn prepare_profile_runtime_request_reuses_prepared_target_for_sql_aggregate_profile() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--memory-gb", "4", "--max-parallelism", "2",
            "--input",
            "target/hits.parquet",
            "--input-format",
            "parquet",
            "--sql",
            "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
            "--request",
            "prepare",
            "--output",
            "target/hits.vortex",
            "--bounded",
            "true",
            "--execution-policy",
            "prepare_once",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("prepare SQL profile route request");

    let native = prepared_profile_native_runtime_request(&request, Path::new("target/hits.vortex"))
        .expect("prepared profile native request");
    let plan = plan_public_workflow_route(&native);
    let fields = route_fields(&native, &plan);

    assert_eq!(native.input_format.as_deref(), Some("vortex"));
    assert_eq!(native.input_uri.as_deref(), Some("target/hits.vortex"));
    assert_eq!(native.requested_output, "collect");
    assert_eq!(native.execution_policy, "native_vortex");
    assert_eq!(native.resources.max_parallelism.as_deref(), Some("2"));
    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(
            plan.status,
            CommandStatus::Success,
            "route={} blocker={} reason={}",
            plan.route_id,
            plan.blocker_id,
            plan.blocker_reason
        );
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "vortex_predicate"),
        "and(contains:URL:google;neq:SearchPhrase:)"
    );
}

#[test]
fn prepare_profile_runtime_request_reuses_prepared_target_for_dataframe_topn_profile() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--memory-gb", "4", "--max-parallelism", "2",
            "--input",
            "target/hits.parquet",
            "--input-format",
            "parquet",
            "--plan",
            "read_parquet(target/hits.parquet) -> select(WatchID,EventTime,URL) -> sort(desc,EventTime) -> limit(10)",
            "--request",
            "prepare",
            "--output",
            "target/hits.vortex",
            "--bounded",
            "true",
            "--execution-policy",
            "prepare_once",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("prepare DataFrame profile route request");

    let native = prepared_profile_native_runtime_request(&request, Path::new("target/hits.vortex"))
        .expect("prepared profile native request");
    let plan = plan_public_workflow_route(&native);
    let fields = route_fields(&native, &plan);

    assert_eq!(native.input_format.as_deref(), Some("vortex"));
    assert_eq!(native.input_uri.as_deref(), Some("target/hits.vortex"));
    assert_eq!(native.surface, "dataframe");
    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(
            plan.status,
            CommandStatus::Success,
            "route={} blocker={} reason={}",
            plan.route_id,
            plan.blocker_id,
            plan.blocker_reason
        );
        assert_eq!(plan.route_id, "native_vortex_sort_rows");
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
    }
    assert_eq!(field(&fields, "vortex_primitive"), "sort_rows");
}

#[test]
fn local_file_prepare_once_does_not_rewrite_unmatched_quoted_sql_source() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--input",
            "target/hits.parquet",
            "--input-format",
            "parquet",
            "--sql",
            "SELECT COUNT(*) FROM 'visits' WHERE URL LIKE '%google%'",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "vortex_middle",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("local SQL unmatched table route request");

    let blocked =
        prepared_local_workflow_native_request(&request).expect_err("unmatched source blocks");
    assert_eq!(
        blocked.blocker_id,
        "cg21.route.local_file_vortex_middle_required"
    );
}

#[test]
fn route_planner_binds_clickbench_bare_table_to_declared_vortex_input_for_like_grouped_topk() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--input",
            "hits.vortex",
            "--input-format",
            "vortex",
            "--sql",
            "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL LIKE grouped route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "vortex_predicate"),
        "and(contains:URL:google;neq:SearchPhrase:)"
    );
    assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
    assert!(aggregate.contains(r#""function":"min""#));
    assert!(aggregate.contains(r#""group_by":["SearchPhrase"]"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_in_list_grouped_topk_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM 'hits.vortex' WHERE CounterID = 62 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL IN-list grouped route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "vortex_predicate"),
        "and(eq:CounterID:62;in:TraficSourceID:-1,6;eq:RefererHash:3594120000172545465)"
    );
    assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
    assert!(aggregate.contains(r#""offset":100"#));
    assert!(aggregate.contains(r#""group_by":["URLHash","EventDate"]"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_raw_order_topk_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT * FROM 'hits.vortex' WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL raw top-K route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let sort_rows = effective_request
        .vortex_sort_rows
        .as_ref()
        .expect("sort rows payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_sort_rows")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "sort_rows");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        native_sql_pipeline_field("general_query", "top_n")
    );
    assert_eq!(field(&fields, "vortex_predicate"), "contains:URL:google");
    assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
    assert_eq!(field(&fields, "vortex_sort_rows_present"), "true");
    assert!(sort_rows.contains(r#""column":"EventTime""#));
    assert!(sort_rows.contains(r#""limit":10"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_length_having_grouped_topk_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM 'hits.vortex' WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL length/HAVING route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(field(&fields, "vortex_source_order_limit"), "25");
    assert!(aggregate.contains(r#""value_transform":"length""#));
    assert!(aggregate.contains(r#""column":"c""#));
    assert!(aggregate.contains(r#""op":"gt""#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_regex_domain_grouped_topk_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM 'hits.vortex' WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL regex-domain route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert!(aggregate.contains(r#""function":"regex_domain""#));
    assert!(aggregate.contains(r#""alias":"k""#));
    assert!(aggregate.contains(r#""value_transform":"length""#));
    assert!(aggregate.contains(r#""op":"gt""#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn regex_domain_specialization_requires_the_complete_pattern_and_replacement() {
    let expression = r"REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')";
    assert_eq!(
        parse_sql_regex_domain_expression(expression),
        Some("Referer")
    );
    for changed in [
        expression.replace("^https?", "https?"),
        expression.replace("/.*$", "/.*"),
        expression.replace("/.*$", ".*$"),
        expression.replace("(?:www\\.)?", "(?:WWW\\.)?"),
        expression.replace("^https?", "(?i)^https?"),
        expression.replace("'\\1'", "'prefix\\1'"),
    ] {
        assert_eq!(
            parse_sql_regex_domain_expression(&changed),
            None,
            "{changed}"
        );
    }
}

#[test]
fn shared_pipeline_admits_extract_minute_sql_syntax() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM 'hits.vortex' GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL extract-minute route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(all(feature = "vortex-local-primitives", unix)) {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert!(aggregate.contains(r#""function":"extract_minute""#));
    assert!(aggregate.contains(r#""alias":"m""#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_arithmetic_group_key_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM 'hits.vortex' GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL arithmetic group route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert!(aggregate.contains(r#""function":"add_offset""#));
    assert!(aggregate.contains(r#""argument_offset":-1"#));
    assert!(aggregate.contains(r#""argument_offset":-2"#));
    assert!(aggregate.contains(r#""argument_offset":-3"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn shared_pipeline_admits_date_trunc_minute_sql_syntax() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM 'hits.vortex' WHERE CounterID = 62 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL date-trunc route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(all(feature = "vortex-local-primitives", unix)) {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
    assert!(aggregate.contains(r#""function":"date_trunc_minute""#));
    assert!(aggregate.contains(r#""offset":1000"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_case_grouped_topk_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM 'hits.vortex' WHERE CounterID = 62 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL CASE grouped route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert!(aggregate.contains(r#""function":"case_search_adv_zero_referer_else_empty""#));
    assert!(aggregate.contains(r#""alias":"Src""#));
    assert!(aggregate.contains(r#""alias":"Dst""#));
    assert!(aggregate.contains(r#""offset":1000"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_ordinal_constant_group_key_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT 1, URL, COUNT(*) AS c FROM 'hits.vortex' GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL ordinal constant group route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert!(aggregate.contains(r#""function":"constant_int""#));
    assert!(aggregate.contains(r#""argument_offset":1"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_infers_native_vortex_sql_conjunctive_filtered_grouped_topk_route() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT URL, COUNT(*) AS PageViews FROM 'hits.vortex' WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native SQL conjunctive filtered grouped top-K route request");
    let effective_request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&effective_request);
    let fields = route_fields(&effective_request, &plan);
    let aggregate = effective_request
        .vortex_aggregate
        .as_ref()
        .expect("aggregate payload");

    if cfg!(feature = "vortex-local-primitives") {
        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(
            plan.route_id,
            native_sql_pipeline_route("native_vortex_aggregate")
        );
    } else {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        );
    }
    assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        native_sql_pipeline_field("general_query", "aggregate")
    );
    assert_eq!(
        field(&fields, "vortex_predicate"),
        "and(eq:CounterID:62;gte:EventDate:2013-07-01;lte:EventDate:2013-07-31;eq:DontCountHits:0;eq:IsRefresh:0;neq:URL:)"
    );
    assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
    assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
    assert!(aggregate.contains(r#""group_by":["URL"]"#));
    assert!(aggregate.contains(r#""column":"PageViews""#));
    assert!(aggregate.contains(r#""descending":true"#));
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_blocks_native_vortex_aggregate_family_with_contract() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--input",
            "target/input.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--native-vortex-operation-family",
            "aggregate",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native aggregate route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_aggregate_route_missing"
    );
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        "aggregate"
    );
    assert_eq!(
        field(&fields, "native_vortex_capability_status"),
        "blocked_until_native_route_admitted"
    );
    assert_eq!(
        field(&fields, "typed_result_contract"),
        "none_blocked_before_execution"
    );
    assert_eq!(field(&fields, "fallback_attempted"), "false");
    assert_eq!(field(&fields, "external_engine_invoked"), "false");
}

#[test]
fn route_planner_blocks_unshaped_native_vortex_query_before_execution() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "dataframe",
            "--input",
            "target/input.vortex",
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("native direct query route request");

    let plan = plan_public_workflow_route(&request);
    let fields = route_fields(&request, &plan);

    assert_eq!(plan.status, CommandStatus::Unsupported);
    assert_eq!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_general_route_missing"
    );
    assert_eq!(
        field(&fields, "native_vortex_operation_family"),
        "general_query"
    );
    assert_eq!(
        field(&fields, "native_vortex_next_action"),
        "declare an operation family and route only admitted native Vortex primitive or promoted operator families"
    );
}

#[test]
fn runtime_args_forward_declared_local_input_format() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT id FROM 'target/input' LIMIT 1",
            "--input-format",
            "csv",
            "--request",
            "collect",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("declared input format route request");
    let mut args = vec![
        request
            .sql_statement
            .clone()
            .expect("request carries SQL statement"),
    ];
    append_declared_local_input_format_args(&mut args, &request);

    assert_eq!(
        args,
        vec![
            "SELECT id FROM 'target/input' LIMIT 1".to_string(),
            "--input-format".to_string(),
            "csv".to_string(),
        ]
    );
}

#[test]
fn route_infers_vortex_manifest_as_native_vortex_input() {
    assert_eq!(
        infer_input_format_from_ref("target/hits_parts.vortex-manifest"),
        Some("vortex")
    );
    let request = PublicWorkflowRouteRequest::parse(
        [
            "sql",
            "--sql",
            "SELECT COUNT(*) FROM 'target/hits_parts.vortex-manifest'",
            "--request",
            "collect",
            "--bounded",
            "true",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("manifest route request");
    let plan = plan_public_workflow_route(&request);

    assert_eq!(
        request.input_uri.as_deref(),
        Some("target/hits_parts.vortex-manifest")
    );
    assert_eq!(request.input_format.as_deref(), Some("vortex"));
    assert_eq!(plan.status, CommandStatus::Success);
    assert_eq!(
        plan.route_id,
        native_sql_pipeline_route("native_vortex_count_all")
    );
    assert_eq!(
        plan.resolved_internal_command,
        native_sql_pipeline_field("native-vortex-relational", "vortex-run")
    );
}

#[test]
fn route_planner_admits_prepare_once_policy_without_execution() {
    let request = PublicWorkflowRouteRequest::parse(
        [
            "cli",
            "--input",
            "target/input.parquet",
            "--input-format",
            "parquet",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "prepare-once",
        ]
        .into_iter()
        .map(str::to_string),
    )
    .expect("prepare route request");
    let plan = plan_public_workflow_route(&request);

    assert_eq!(plan.route_id, "blocked");
    assert!(
        matches!(
            plan.blocker_id,
            "cg21.route.local_file_vortex_ingest_feature_gated"
                | "cg21.route.local_file_vortex_middle_required"
        ),
        "unexpected blocker: {}",
        plan.blocker_id
    );
    assert!(!plan.preparation_included);
    assert!(!plan.query_timing_starts_after_preparation);
}
