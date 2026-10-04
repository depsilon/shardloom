use super::*;
use runtime::{VortexLocalPrimitiveRowExportFormat as Format, VortexPivotProjectionRequest};
use vortex::array::{arrays::VarBinArray, dtype::PType};

fn array(keys: &[Option<u64>], domains: &[&str], values: &[Option<u64>]) -> ArrayRef {
    StructArray::new(
        FieldNames::from([KEY, "category", VALUE]),
        vec![
            PrimitiveArray::new(
                keys.iter().map(|v| v.unwrap_or(0)).collect::<Vec<_>>(),
                Validity::from_iter(keys.iter().map(Option::is_some)),
            )
            .into_array(),
            VarBinArray::from(domains.to_vec()).into_array(),
            PrimitiveArray::new(
                values.iter().map(|v| v.unwrap_or(0)).collect::<Vec<_>>(),
                Validity::from_iter(values.iter().map(Option::is_some)),
            )
            .into_array(),
        ],
        keys.len(),
        Validity::NonNullable,
    )
    .into_array()
}
pub(super) fn fixture() -> Fixture {
    Fixture::from_array(
        array(
            &[Some(2), Some(10), Some(2), Some(10), Some(2)],
            &["z", "a", "a", "a", "z"],
            &[Some(10), Some(7), Some(20), Some(8), Some(5)],
        ),
        2,
    )
}
pub(super) fn request(fixture: &Fixture, aggregate: &str) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::pivot_rows(
        fixture.uri(),
        VortexPivotProjectionRequest::new(
            ColumnRef::new(KEY).unwrap(),
            ColumnRef::new("category").unwrap(),
            ColumnRef::new(VALUE).unwrap(),
            aggregate,
        ),
    )
}

#[test]
fn unary_pivot_aggregates_duplicate_cells_and_preserves_provider_key_order() {
    let fixture = fixture();
    for (aggregate, a10, a2, z2) in [
        ("sum", 15.0, 20.0, 15.0),
        ("mean", 7.5, 20.0, 7.5),
        ("min", 7.0, 20.0, 5.0),
        ("max", 8.0, 20.0, 10.0),
    ] {
        let prepared = prepare(&request(&fixture, aggregate));
        let expected = serde_json::json!([{KEY:10,"pivot_a":a10,"pivot_z":null},{KEY:2,"pivot_a":a2,"pivot_z":z2}]).as_array().unwrap().clone();
        for _ in 0..2 {
            let result = prepared.execute_owned().unwrap();
            assert_eq!(json_rows(&result.result), expected);
            assert_eq!(
                result.execution.report.projected_columns,
                vec![KEY, "pivot_a", "pivot_z"]
            );
            assert!(result.execution.native_io_certificate.is_certified());
        }
        assert_eq!(prepared.snapshot().prepared_source_opens, 1);
        assert_eq!(prepared.snapshot().completed_executions, 2);
    }
    assert_eq!(
        json_rows(
            &prepare(&request(&fixture, "count"))
                .execute_owned()
                .unwrap()
                .result
        ),
        serde_json::json!([{KEY:10,"pivot_a":2,"pivot_z":null},{KEY:2,"pivot_a":1,"pivot_z":2}])
            .as_array()
            .unwrap()
            .clone()
    );
    assert_eq!(
        json_rows(
            &prepare(&request(&fixture, "first"))
                .execute_owned()
                .unwrap()
                .result
        ),
        serde_json::json!([{KEY:10,"pivot_a":7,"pivot_z":null},{KEY:2,"pivot_a":20,"pivot_z":10}])
            .as_array()
            .unwrap()
            .clone()
    );
    assert!(
        prepare(&request(&fixture, "first_unique"))
            .execute_owned()
            .err()
            .unwrap()
            .to_string()
            .contains("multiple values")
    );
    let equal = Fixture::from_array(
        array(
            &[Some(1), Some(1)],
            &["a", "a"],
            &[Some(u64::MAX), Some(u64::MAX)],
        ),
        1,
    );
    assert_eq!(
        json_rows(
            &prepare(&request(&equal, "first_unique"))
                .execute_owned()
                .unwrap()
                .result
        ),
        serde_json::json!([{KEY:1,"pivot_a":u64::MAX}])
            .as_array()
            .unwrap()
            .clone()
    );
}

#[test]
fn unary_pivot_fill_margins_and_limit_preserve_types_and_selected_margin_scope() {
    let fixture = fixture();
    let mut request = request(&fixture, "sum");
    let projection = request.pivot_projection.as_mut().unwrap();
    projection.fill_value = Some(shardloom_core::ScalarValue::Float64(0.0));
    projection.margins = true;
    projection.margins_name = "total".into();
    let prepared = prepare(&request);
    let result = prepared.execute_owned().unwrap();
    assert_eq!(
        result
            .result
            .dtype()
            .as_struct_fields_opt()
            .unwrap()
            .field(KEY),
        Some(DType::Variant(Nullability::Nullable))
    );
    assert_eq!(
        json_rows(&result.result),
        serde_json::json!([
            {KEY:10,"pivot_a":15.0,"pivot_z":0.0,"pivot_total":15.0},
            {KEY:2,"pivot_a":20.0,"pivot_z":15.0,"pivot_total":35.0},
            {KEY:"total","pivot_a":35.0,"pivot_z":15.0,"pivot_total":50.0}
        ])
        .as_array()
        .unwrap()
        .clone()
    );
    let before = prepared.snapshot().completed_executions;
    let target = fixture.0.join("margins.vortex");
    prepared.write(&target, Format::Vortex, false).unwrap();
    assert_eq!(prepared.snapshot().completed_executions, before + 1);
    let (dtype, rows) = native_rows(&target);
    assert_eq!(&dtype, result.result.dtype());
    assert_eq!(rows.len(), 3);
    assert_eq!(scalar_json(&rows[2][0]), serde_json::json!("total"));
    assert_eq!(scalar_json(&rows[2][3]), serde_json::json!(50.0));
    request.source_order_limit = Some(2);
    let limited = prepare(&request).execute_owned().unwrap();
    assert_eq!(
        json_rows(&limited.result),
        serde_json::json!([
            {KEY:10,"pivot_a":15.0,"pivot_z":0.0,"pivot_total":15.0},
            {KEY:"total","pivot_a":15.0,"pivot_z":0.0,"pivot_total":15.0}
        ])
        .as_array()
        .unwrap()
        .clone()
    );
    assert_eq!(
        limited.execution.report.source_order_limit_input_rows,
        Some(3)
    );
}

#[test]
fn unary_pivot_null_values_colliding_names_and_typed_fill_are_lossless() {
    let fixture = Fixture::from_array(
        array(&[None, Some(2)], &["A", "a"], &[None, Some(u64::MAX)]),
        1,
    );
    let mut request = request(&fixture, "first");
    request.pivot_projection.as_mut().unwrap().fill_value =
        Some(shardloom_core::ScalarValue::UInt64(0));
    let expected = serde_json::json!([{KEY:null,"pivot_a":null,"pivot_a_2":0},{KEY:2,"pivot_a":0,"pivot_a_2":u64::MAX}]).as_array().unwrap().clone();
    for dropna in [false, true] {
        request.pivot_projection.as_mut().unwrap().dropna = dropna;
        let result = prepare(&request).execute_owned().unwrap();
        assert_eq!(json_rows(&result.result), expected);
        assert_eq!(
            result
                .result
                .dtype()
                .as_struct_fields_opt()
                .unwrap()
                .field("pivot_a"),
            Some(DType::Primitive(PType::U64, Nullability::Nullable))
        );
    }
    request.pivot_projection.as_mut().unwrap().fill_value =
        Some(shardloom_core::ScalarValue::Utf8("missing".into()));
    let result = prepare(&request).execute_owned().unwrap();
    assert_eq!(
        result
            .result
            .dtype()
            .as_struct_fields_opt()
            .unwrap()
            .field("pivot_a"),
        Some(DType::Variant(Nullability::Nullable))
    );
    assert_eq!(
        json_rows(&result.result)[1]["pivot_a"],
        serde_json::json!("missing")
    );
    assert_eq!(
        json_rows(&result.result)[1]["pivot_a_2"],
        serde_json::json!(u64::MAX)
    );
    assert!(
        prepare(&self::request(&fixture, "sum"))
            .execute_owned()
            .is_err()
    );
    assert_eq!(
        json_rows(
            &prepare(&self::request(&fixture, "count"))
                .execute_owned()
                .unwrap()
                .result
        )[0]["pivot_a"],
        serde_json::json!(1)
    );
}

#[test]
fn unary_pivot_filters_before_discovery_and_preserves_empty_schema() {
    let fixture = fixture();
    let mut request = request(&fixture, "sum");
    request.predicate = Some(runtime::PredicateExpr::Compare {
        column: ColumnRef::new(KEY).unwrap(),
        op: shardloom_core::ComparisonOp::Eq,
        value: StatValue::UInt64(10),
    });
    let result = prepare(&request).execute_owned().unwrap();
    assert_eq!(
        json_rows(&result.result),
        serde_json::json!([{KEY:10,"pivot_a":15.0}])
            .as_array()
            .unwrap()
            .clone()
    );
    assert_eq!(
        result.execution.report.projected_columns,
        vec![KEY, "pivot_a"]
    );
    let empty = Fixture::from_array(array(&[], &[], &[]), 1);
    for margins in [false, true] {
        let mut request = self::request(&empty, "sum");
        request.pivot_projection.as_mut().unwrap().margins = margins;
        let prepared = prepare(&request);
        let result = prepared.execute_owned().unwrap();
        assert_eq!(json_rows(&result.result), Vec::<serde_json::Value>::new());
        let target = empty.0.join(format!("empty-{margins}.vortex"));
        prepared.write(&target, Format::Vortex, false).unwrap();
        let (dtype, rows) = native_rows(&target);
        assert_eq!(&dtype, result.result.dtype());
        assert_eq!(rows, Vec::<Vec<vortex::scalar::Scalar>>::new());
    }
}

#[test]
fn unary_pivot_reserves_state_and_cleans_failure_before_publication() {
    let keys = (0..500).map(Some).collect::<Vec<_>>();
    let fixture = Fixture::from_array(array(&keys, &vec!["a"; 500], &vec![Some(1); 500]), 31);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let prepared = prepare_unary_in_session(
        &request(&fixture, "sum"),
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &session,
    )
    .unwrap();
    assert!(prepared.execute_owned().is_err());
    let baseline = session.snapshot().memory.reserved_bytes;
    let target = fixture.0.join("denied.vortex");
    let failure = prepared.write(&target, Format::Vortex, false).unwrap_err();
    assert!(failure.to_string().contains("reservation"), "{failure}");
    assert!(!target.exists());
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
    let memory = session.memory().clone();
    drop(prepared);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn unary_pivot_cancellation_and_generation_failure_do_not_reopen_the_source() {
    let fixture = fixture();
    let prepared = prepare(&request(&fixture, "sum"));
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    let target = fixture.0.join("cancelled.vortex");
    assert!(
        prepared
            .write_controlled(&target, Format::Vortex, false, &cancellation)
            .is_err()
    );
    assert!(!target.exists());
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                fixture.replace();
                Ok(())
            })
            .is_err()
    );
    assert!(prepared.execute_owned().is_err());
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
}
