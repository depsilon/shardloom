//! Empty provider output must preserve source work, typed delivery and certification.

use super::*;

fn request(value: i64) -> VortexQueryPrimitiveRequest {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/local_primitive_struct_five.vortex");
    VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).unwrap(),
        ProjectionRequest::columns(vec![ColumnRef::new("metric").unwrap()]),
        Some(PredicateExpr::Compare {
            column: ColumnRef::new("metric").unwrap(),
            op: ComparisonOp::Eq,
            value: StatValue::Int64(value),
        }),
        crate::VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new(
            "value", false,
        )]),
        11,
    )
}

#[test]
fn filtered_sort_certifies_empty_provider_output_and_complete_typed_delivery() {
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
    // 17 is inside the fixture's [10, 50] range but matches no value. 99 is
    // metadata-prunable; 30 is a nonempty control with the same hidden sort key.
    for (value, expected, pruned) in [
        (17, "", false),
        (99, "", true),
        (30, "{\"metric\":30}\n", false),
    ] {
        let request = request(value);
        let source = prepared_dispatch::prepare_source(&request, policy).unwrap();
        let memory = source.retained_session().memory().clone();
        let report = prepared_dispatch::execute_sort(&request, policy, &source).unwrap();
        assert!(!report.has_errors());
        assert_eq!(report.embedded_layout.metadata_pruned_entire_input, pruned);
        if value == 17 {
            assert_eq!(report.rows_scanned, 5);
            assert_eq!(report.rows_selected, Some(0));
            assert_eq!(report.arrays_read_count, 0);
            assert!(report.upstream_scan_called && report.streaming_scan_used);
            assert!(report.data_read && report.data_decoded && report.data_materialized);
            assert!(!report.row_read && !report.full_stream_collected);
            assert!(report.materialization_boundary_reported);
            assert!(
                report
                    .result_summary
                    .as_deref()
                    .unwrap()
                    .contains("provider_filter_may_read_decode_materialize_not_observed_bytes")
            );
        }
        let certificate = local_primitive_native_io_certificate(&request, &report).unwrap();
        assert!(certificate.is_certified());
        assert!(!certificate.fallback_attempted);
        let collected = prepared_dispatch::collect_sort(&request, policy, &source).unwrap();
        assert_eq!(collected.result_jsonl.value(), expected);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(collected.result_schema_json.value())
                .unwrap(),
            serde_json::json!({"Struct":[{"names":["metric"],"dtypes":[{"Primitive":["i64",false]}]},false]})
        );
        assert!(collected.native_io_certificate.is_certified());
        assert!(!collected.native_io_certificate.fallback_attempted);
        drop(source);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            collected.result_jsonl.reserved_bytes() + collected.result_schema_json.reserved_bytes()
        );
        drop(collected);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn empty_sort_certification_still_rejects_missing_work_and_unsafe_effects() {
    let request = request(17);
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
    let source = prepared_dispatch::prepare_source(&request, policy).unwrap();
    let report = prepared_dispatch::execute_sort(&request, policy, &source).unwrap();
    assert!(
        local_primitive_native_io_certificate(&request, &report)
            .unwrap()
            .is_certified()
    );
    for mutation in 0..6 {
        let mut changed = report.clone();
        match mutation {
            0 => changed.upstream_scan_called = false,
            1 => changed.streaming_scan_used = false,
            2 => changed.materialization_boundary_reported = false,
            3 => changed.arrow_converted = true,
            4 => changed.external_effects_executed = true,
            5 => changed.data_read = false,
            _ => unreachable!(),
        }
        assert!(
            !local_primitive_native_io_certificate(&request, &changed)
                .unwrap()
                .is_certified(),
            "mutation {mutation}"
        );
    }
}
