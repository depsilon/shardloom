use super::*;
use runtime::{VortexExplodeProjectionRequest, VortexLocalPrimitiveRowExportFormat as Format};
use vortex::array::{
    arrays::{FixedSizeListArray, ListViewArray, VarBinArray},
    dtype::PType,
};

fn list(values: ArrayRef, offsets: &[u64], sizes: &[u64], valid: &[bool]) -> ArrayRef {
    ListViewArray::try_new(
        values,
        PrimitiveArray::new(offsets.to_vec(), Validity::NonNullable).into_array(),
        PrimitiveArray::new(sizes.to_vec(), Validity::NonNullable).into_array(),
        Validity::from_iter(valid.iter().copied()),
    )
    .unwrap()
    .into_array()
}

fn fixture() -> Fixture {
    let items = list(
        PrimitiveArray::new(vec![7u64, 0, 9], Validity::from_iter([true, false, true]))
            .into_array(),
        &[0, 2, 2, 2],
        &[2, 0, 0, 1],
        &[true, true, false, true],
    );
    let labels = list(
        VarBinArray::from(vec!["red", "blue", "green"]).into_array(),
        &[0, 2, 2, 2],
        &[2, 0, 0, 1],
        &[true, true, false, true],
    );
    Fixture::from_array(
        StructArray::new(
            FieldNames::from([KEY, "items", "labels"]),
            vec![
                PrimitiveArray::new(vec![1u64, 2, 3, u64::MAX], Validity::NonNullable).into_array(),
                items,
                labels,
            ],
            4,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

fn request(fixture: &Fixture) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::explode_rows(
        fixture.uri(),
        projection(&[KEY, "items", "labels"]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").unwrap()).with_columns(vec![
            ColumnRef::new("items").unwrap(),
            ColumnRef::new("labels").unwrap(),
        ]),
    )
}

#[test]
fn unary_explode_zips_lists_preserves_null_empty_inner_null_and_filters_before_expansion() {
    let fixture = fixture();
    let request = request(&fixture);
    let prepared = prepare(&request);
    let expected = serde_json::json!([
        {KEY:1,"items":7,"labels":"red"}, {KEY:1,"items":null,"labels":"blue"},
        {KEY:3,"items":null,"labels":null}, {KEY:u64::MAX,"items":9,"labels":"green"}
    ])
    .as_array()
    .unwrap()
    .clone();
    for _ in 0..2 {
        assert_eq!(
            json_rows(&prepared.execute_owned().unwrap().result),
            expected
        );
    }
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    let output = fixture.0.join("complete.vortex");
    prepared.write(&output, Format::Vortex, false).unwrap();
    let (_, rows) = native_rows(&output);
    assert_eq!(
        rows.iter()
            .map(|row| row.iter().map(scalar_json).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![
            vec![1.into(), 7.into(), "red".into()],
            vec![1.into(), serde_json::Value::Null, "blue".into()],
            vec![3.into(), serde_json::Value::Null, serde_json::Value::Null],
            vec![u64::MAX.into(), 9.into(), "green".into()]
        ]
    );
    let mut filtered = request.clone();
    filtered.predicate = Some(runtime::PredicateExpr::Compare {
        column: ColumnRef::new(KEY).unwrap(),
        op: shardloom_core::ComparisonOp::Gt,
        value: StatValue::UInt64(2),
    });
    assert_eq!(
        json_rows(&prepare(&filtered).execute_owned().unwrap().result),
        expected[2..]
    );
    let mut limited = request;
    limited.source_order_limit = Some(1);
    assert_eq!(
        json_rows(&prepare(&limited).execute_owned().unwrap().result),
        expected[..1]
    );
}

#[test]
fn unary_explode_projects_nullable_struct_elements_and_fixed_size_lists() {
    let elements = StructArray::new(
        FieldNames::from(["code"]),
        vec![PrimitiveArray::new(vec![7u64, 8, 9], Validity::NonNullable).into_array()],
        3,
        Validity::from_iter([true, false, true]),
    )
    .into_array();
    let items = list(elements, &[0, 2, 2], &[2, 0, 1], &[true, false, true]);
    let fixture = Fixture::from_array(
        StructArray::new(
            FieldNames::from(["items"]),
            vec![items],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let request = VortexQueryPrimitiveRequest::explode_rows(
        fixture.uri(),
        projection(&["items"]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").unwrap())
            .with_element_field("code".into(), "result_code".into()),
    );
    let result = prepare(&request).execute_owned().unwrap();
    assert_eq!(json_rows(&result.result),serde_json::json!([{"result_code":7},{"result_code":null},{"result_code":null},{"result_code":9}]).as_array().unwrap().clone());
    assert_eq!(
        result
            .result
            .dtype()
            .as_struct_fields_opt()
            .unwrap()
            .field("result_code")
            .unwrap(),
        DType::Primitive(PType::U64, Nullability::Nullable)
    );
    let fixed = FixedSizeListArray::try_new(
        PrimitiveArray::new(vec![1u64, 2, 3, 4], Validity::NonNullable).into_array(),
        2,
        Validity::from_iter([true, false]),
        2,
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::from_array(
        StructArray::new(
            FieldNames::from(["items"]),
            vec![fixed],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let request = VortexQueryPrimitiveRequest::explode_rows(
        fixture.uri(),
        projection(&["items"]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").unwrap()),
    );
    assert_eq!(
        json_rows(&prepare(&request).execute_owned().unwrap().result),
        serde_json::json!([{"items":1},{"items":2},{"items":null}])
            .as_array()
            .unwrap()
            .clone()
    );
}

#[test]
fn unary_explode_denies_length_mismatch_and_preserves_typed_empty_result() {
    let a = list(
        PrimitiveArray::new(vec![1u64, 2], Validity::NonNullable).into_array(),
        &[0],
        &[2],
        &[true],
    );
    let b = list(
        PrimitiveArray::new(vec![3u64], Validity::NonNullable).into_array(),
        &[0],
        &[1],
        &[true],
    );
    let array = StructArray::new(
        FieldNames::from([KEY, "items", "labels"]),
        vec![
            PrimitiveArray::new(vec![1u64], Validity::NonNullable).into_array(),
            a,
            b,
        ],
        1,
        Validity::NonNullable,
    )
    .into_array();
    let fixture = Fixture::from_array(array.clone(), 1);
    let prepared = prepare(&request(&fixture));
    assert!(
        prepared
            .execute_owned()
            .err()
            .unwrap()
            .to_string()
            .contains("equal list lengths")
    );
    let target = fixture.0.join("denied.vortex");
    assert!(prepared.write(&target, Format::Vortex, false).is_err());
    assert!(!target.exists());
    let empty = Fixture::from_array(array.slice(0..0).unwrap(), 1);
    let prepared = prepare(&request(&empty));
    let result = prepared.execute_owned().unwrap();
    assert_eq!(json_rows(&result.result), Vec::<serde_json::Value>::new());
    let target = empty.0.join("empty.vortex");
    prepared.write(&target, Format::Vortex, false).unwrap();
    let (dtype, rows) = native_rows(&target);
    assert_eq!(&dtype, result.result.dtype());
    assert_eq!(rows, Vec::<Vec<vortex::scalar::Scalar>>::new());
}

#[test]
fn unary_explode_writes_complete_long_list_beyond_collection_limit() {
    let count = 65_541usize;
    let values = PrimitiveArray::new((0..count as u64).collect::<Vec<_>>(), Validity::NonNullable)
        .into_array();
    let array = StructArray::new(
        FieldNames::from(["items"]),
        vec![list(values, &[0], &[count as u64], &[true])],
        1,
        Validity::NonNullable,
    )
    .into_array();
    let fixture = Fixture::from_array(array, 1);
    let request = VortexQueryPrimitiveRequest::explode_rows(
        fixture.uri(),
        projection(&["items"]),
        VortexExplodeProjectionRequest::new(ColumnRef::new("items").unwrap()),
    );
    let prepared = prepare(&request);
    assert!(
        prepared
            .execute_owned()
            .err()
            .unwrap()
            .to_string()
            .contains("small collection")
    );
    let target = fixture.0.join("long.vortex");
    let report = prepared.write(&target, Format::Vortex, false).unwrap();
    assert_eq!(report.rows_written, count as u64);
    let (_, rows) = native_rows(&target);
    assert_eq!(rows.len(), count);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(scalar_json(&row[0]), serde_json::json!(index));
    }
}
