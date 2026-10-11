use super::*;
use crate::VortexMeltProjectionRequest;
use runtime::VortexLocalPrimitiveRowExportFormat as Format;
use vortex::array::{arrays::VarBinViewArray, scalar::Scalar};

fn fixture() -> Fixture {
    Fixture::from_array(
        StructArray::new(
            [KEY, VALUE, "label"].into(),
            vec![
                PrimitiveArray::new(vec![1u64, 0, 3], Validity::from_iter([true, false, true]))
                    .into_array(),
                PrimitiveArray::new(vec![u64::MAX, 0, 42], Validity::NonNullable).into_array(),
                VarBinViewArray::from_iter_nullable_str([Some("π"), None, Some("end")])
                    .into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

fn request(fixture: &Fixture, columns: &[&str]) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::melt_rows(
        fixture.uri(),
        VortexMeltProjectionRequest::new(
            vec![ColumnRef::new(KEY).unwrap()],
            columns
                .iter()
                .map(|name| ColumnRef::new(*name).unwrap())
                .collect(),
            "kind".into(),
            "cell".into(),
        ),
    )
}

#[test]
fn unary_melt_preserves_mixed_value_types_nulls_order_and_native_file_payload() {
    let fixture = fixture();
    let request = request(&fixture, &[VALUE, "label"]);
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let prepared = prepare_unary_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
            .expect("explicit fixture allocation"),
        &session,
    )
    .unwrap();
    let result = prepared.execute_owned().unwrap();
    let expected = vec![
        serde_json::json!({"account_key":1,"kind":"amount","cell":u64::MAX}),
        serde_json::json!({"account_key":1,"kind":"label","cell":"π"}),
        serde_json::json!({"account_key":null,"kind":"amount","cell":0}),
        serde_json::json!({"account_key":null,"kind":"label","cell":null}),
        serde_json::json!({"account_key":3,"kind":"amount","cell":42}),
        serde_json::json!({"account_key":3,"kind":"label","cell":"end"}),
    ];
    assert_eq!(json_rows(&result.result), expected);
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let rendered = collected
        .result_jsonl
        .value()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rendered, expected);
    drop(collected);
    let json = result
        .result
        .to_bounded_json(&[KEY.into(), "kind".into(), "cell".into()], 4096)
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(json.value()).unwrap(),
        serde_json::json!(expected)
    );
    drop(json);
    let dtype = result.result.dtype().clone();
    assert_eq!(
        dtype.as_struct_fields_opt().unwrap().field("cell"),
        Some(DType::Variant(Nullability::Nullable))
    );
    let output = fixture.0.join("mixed-output.vortex");
    let report = prepared.write(&output, Format::Vortex, false).unwrap();
    assert_eq!(report.rows_written, 6);
    assert_eq!(report.pre_limit_result_row_count, 6);
    assert!(!report.evidence.side_effects.fallback_attempted);
    let (reopened_dtype, reopened) = native_rows(&output);
    assert_eq!(reopened_dtype, dtype);
    let variant = DType::Variant(Nullability::Nullable);
    let expected_cells = [
        Scalar::from(u64::MAX),
        Scalar::from("π"),
        Scalar::from(0u64),
        Scalar::null(DType::Null),
        Scalar::from(42u64),
        Scalar::from("end"),
    ];
    for (index, (row, expected)) in reopened.iter().zip(expected_cells).enumerate() {
        let expected = if index == 3 {
            Scalar::null(variant.clone())
        } else {
            Scalar::variant(expected).cast(&variant).unwrap()
        };
        assert_eq!(row[2], expected);
    }
    assert_eq!(
        values(&prepared, "cell"),
        vec![
            serde_json::json!(u64::MAX),
            serde_json::json!("π"),
            serde_json::json!(0),
            serde_json::Value::Null,
            serde_json::json!(42),
            serde_json::json!("end")
        ]
    );
    let retained = result.result.arrays()[0].clone();
    drop(result);
    drop(prepared);
    assert!(session.snapshot().memory.reserved_bytes >= 6 * 1024);
    drop(retained);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn unary_melt_text_writers_consume_native_variants_and_preserve_complete_order() {
    let fixture = fixture();
    let prepared = prepare(&request(&fixture, &[VALUE, "label"]));
    for (format, suffix) in [
        (Format::Json, "json"),
        (Format::Jsonl, "jsonl"),
        (Format::Csv, "csv"),
    ] {
        let path = fixture.0.join(format!("melt.{suffix}"));
        let report = prepared.write(&path, format, false).unwrap();
        assert_eq!(report.rows_written, 6);
        let text = fs::read_to_string(path).unwrap();
        if format == Format::Json {
            assert_eq!(
                serde_json::from_str::<Vec<serde_json::Value>>(&text)
                    .unwrap()
                    .len(),
                6
            );
        } else if format == Format::Jsonl {
            let rows = text
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(rows, json_rows(&prepared.execute_owned().unwrap().result));
        } else {
            assert_eq!(
                text,
                format!(
                    "account_key,kind,cell\n1,amount,{}\n1,label,π\n,amount,0\n,label,\n3,amount,42\n3,label,end\n",
                    u64::MAX
                )
            );
        }
    }
}

#[test]
fn unary_melt_preserves_uniform_schema_limits_and_typed_empty_variant_output() {
    let fixture = fixture();
    let mut request = request(&fixture, &[VALUE, KEY]);
    request.source_order_limit = Some(3);
    let result = prepare(&request).execute_owned().unwrap();
    assert_eq!(
        result
            .result
            .dtype()
            .as_struct_fields_opt()
            .unwrap()
            .field("cell"),
        Some(DType::Primitive(
            vortex::array::dtype::PType::U64,
            Nullability::Nullable
        ))
    );
    assert_eq!(
        json_rows(&result.result),
        vec![
            serde_json::json!({"account_key":1,"kind":"amount","cell":u64::MAX}),
            serde_json::json!({"account_key":1,"kind":"account_key","cell":1}),
            serde_json::json!({"account_key":null,"kind":"amount","cell":0}),
        ]
    );
    let empty = Fixture::from_array(
        StructArray::new(
            [KEY, VALUE, "label"].into(),
            vec![
                PrimitiveArray::new(Vec::<u64>::new(), Validity::AllValid).into_array(),
                PrimitiveArray::new(Vec::<u64>::new(), Validity::NonNullable).into_array(),
                VarBinViewArray::from_iter_nullable_str(std::iter::empty::<Option<&str>>())
                    .into_array(),
            ],
            0,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let empty_request = super::melt_tests::request(&empty, &[VALUE, "label"]);
    let prepared = prepare(&empty_request);
    let empty_result = prepared.execute_owned().unwrap();
    assert_eq!(empty_result.result.row_count(), 0);
    let output = empty.0.join("empty-result.vortex");
    prepared.write(&output, Format::Vortex, false).unwrap();
    let (dtype, rows) = native_rows(&output);
    assert_eq!(&dtype, empty_result.result.dtype());
    assert_eq!(rows, Vec::<Vec<vortex::scalar::Scalar>>::new());
}
