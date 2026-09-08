//! A successful provider filter may produce no arrays after doing real work.

use super::*;

#[test]
fn unprunable_empty_aggregate_preserves_zero_values_and_certifies_scan_scope() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/local_primitive_struct_five.vortex");
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "present".to_owned(),
        )]),
    );
    // The checked fixture's metric values are exactly 10,20,30,40,50.
    // Seventeen cannot match but its comparison cannot be pruned by min/max.
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("metric").unwrap(),
        op: ComparisonOp::Eq,
        value: StatValue::Int64(17),
    });
    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    assert!(!report.has_errors());
    assert!(!report.embedded_layout.metadata_pruned_entire_input);
    assert_eq!(report.rows_selected, Some(0));
    assert_eq!(report.rows_projected, Some(1));
    assert_eq!(report.arrays_read_count, 0);
    assert!(report.upstream_scan_called && report.streaming_scan_used);
    assert!(report.data_read && report.data_decoded && report.data_materialized);
    assert!(!report.row_read && !report.arrow_converted);
    assert!(report.materialization_boundary_reported);
    let summary = report.result_summary.as_deref().unwrap();
    assert!(summary.contains("provider_filter_may_read_decode_materialize_not_observed_bytes"));
    let values: serde_json::Value =
        serde_json::from_str(summary.split_once(" values=").unwrap().1).unwrap();
    assert_eq!(values["values"]["present"], 0);
    assert!(
        local_primitive_native_io_certificate(&request, &report)
            .unwrap()
            .is_certified()
    );
    assert!(!report.fallback_execution_allowed);
}
