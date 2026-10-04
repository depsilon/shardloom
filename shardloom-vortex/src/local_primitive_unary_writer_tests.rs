use super::*;
use runtime::VortexLocalPrimitiveRowExportFormat as Format;

fn read_binary(path: &std::path::Path, format: Format, columns: &[&str]) -> Vec<serde_json::Value> {
    if format == Format::Vortex {
        let (dtype, rows) = native_rows(path);
        assert_eq!(
            dtype
                .as_struct_fields_opt()
                .unwrap()
                .names()
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            columns
        );
        return rows
            .iter()
            .map(|row| {
                serde_json::Value::Object(
                    columns
                        .iter()
                        .zip(row)
                        .map(|(name, value)| ((*name).into(), scalar_json(value)))
                        .collect(),
                )
            })
            .collect();
    }
    let table = match format {
        Format::Parquet => crate::read_flat_parquet_source(path, 100_000),
        Format::ArrowIpc => crate::read_flat_arrow_ipc_source(path, 100_000),
        Format::Avro => crate::read_flat_avro_source(path, 100_000),
        Format::Orc => crate::read_flat_orc_source(path, 100_000),
        _ => panic!("binary output expected"),
    }
    .unwrap();
    assert_eq!(table.header, columns);
    table
        .rows
        .into_iter()
        .map(|row| {
            serde_json::Value::Object(
                row.into_iter()
                    .map(|(name, value)| {
                        let value = match value {
                            shardloom_core::ScalarValue::Null => serde_json::Value::Null,
                            shardloom_core::ScalarValue::Boolean(value) => value.into(),
                            shardloom_core::ScalarValue::Int64(value) => value.into(),
                            shardloom_core::ScalarValue::UInt64(value) => value.into(),
                            shardloom_core::ScalarValue::Float64(value) => serde_json::json!(value),
                            shardloom_core::ScalarValue::Utf8(value) => value.into(),
                            other => panic!("unexpected test value: {other:?}"),
                        };
                        (name, value)
                    })
                    .collect(),
            )
        })
        .collect()
}

fn verify(
    fixture: &Fixture,
    request: &VortexQueryPrimitiveRequest,
    label: &str,
    columns: &[&str],
    expected: serde_json::Value,
    csv: &str,
) {
    let serde_json::Value::Array(expected) = expected else {
        panic!("complete expected rows must be a JSON array");
    };
    let expected = &expected;
    let prepared = prepare(request);
    let cancellation = shardloom_exec::compute_pool::CancellationToken::default();
    let first_collect = prepared.collect_jsonl(&cancellation).unwrap();
    let first_rows = first_collect
        .result_jsonl
        .value()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&first_rows, expected);
    assert_eq!(
        first_collect.execution.report.rows_projected,
        Some(expected.len() as u64)
    );
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    drop(first_collect);

    let second_collect = prepared.collect_jsonl(&cancellation).unwrap();
    let second_rows = second_collect
        .result_jsonl
        .value()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&second_rows, expected);
    assert_eq!(
        second_collect.execution.report.rows_projected,
        Some(expected.len() as u64)
    );
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    assert_eq!(prepared.snapshot().completed_executions, 2);
    drop(second_collect);

    for (index, format) in [
        Format::Vortex,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Avro,
        Format::Orc,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ]
    .into_iter()
    .enumerate()
    {
        let path = fixture.0.join(format!("{label}.{}", format.as_str()));
        let report = prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{label} {format:?}: {error}"));
        assert_eq!(report.rows_written, expected.len() as u64);
        assert_eq!(report.projected_columns, columns);
        assert_eq!(prepared.snapshot().prepared_source_opens, 1);
        assert_eq!(prepared.snapshot().completed_executions, (index + 3) as u64);
        let rows = match format {
            Format::Json => serde_json::from_slice::<serde_json::Value>(&fs::read(&path).unwrap())
                .unwrap()
                .as_array()
                .unwrap()
                .clone(),
            Format::Jsonl => fs::read_to_string(&path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect(),
            Format::Csv => {
                assert_eq!(fs::read_to_string(&path).unwrap(), csv, "{label}");
                continue;
            }
            _ => read_binary(&path, format, columns),
        };
        assert_eq!(&rows, expected, "{label} {format:?}");
    }
}

#[test]
fn unary_writers_preserve_all_selector_results_through_all_eight_formats() {
    let fixture = Fixture::new(
        &[Some(1), Some(1), Some(2), Some(3), Some(3)],
        &[10, 20, 30, 40, 50],
        2,
    );
    verify(
        &fixture,
        &VortexQueryPrimitiveRequest::distinct_rows(fixture.uri(), projection(&[KEY]), None),
        "distinct",
        &[KEY],
        serde_json::json!([{KEY:1},{KEY:2},{KEY:3}]),
        "account_key\n1\n2\n3\n",
    );
    let mut dedup = VortexQueryPrimitiveRequest::drop_duplicate_rows(
        fixture.uri(),
        projection(&[KEY, VALUE]),
        projection(&[KEY]),
    );
    dedup.duplicate_keep = VortexDuplicateKeepPolicy::Last;
    verify(
        &fixture,
        &dedup,
        "dedup",
        &[KEY, VALUE],
        serde_json::json!([{KEY:1,VALUE:20},{KEY:2,VALUE:30},{KEY:3,VALUE:50}]),
        "account_key,amount\n1,20\n2,30\n3,50\n",
    );
    let mut mask =
        VortexQueryPrimitiveRequest::duplicate_mask_rows(fixture.uri(), projection(&[KEY]));
    mask.duplicate_keep = VortexDuplicateKeepPolicy::AllDuplicates;
    verify(
        &fixture,
        &mask,
        "mask",
        &["duplicated"],
        serde_json::json!([{"duplicated":true},{"duplicated":true},{"duplicated":false},{"duplicated":true},{"duplicated":true}]),
        "duplicated\ntrue\ntrue\nfalse\ntrue\ntrue\n",
    );
    verify(
        &fixture,
        &VortexQueryPrimitiveRequest::tail_rows(fixture.uri(), projection(&[VALUE]), 2),
        "tail",
        &[VALUE],
        serde_json::json!([{VALUE:40},{VALUE:50}]),
        "amount\n40\n50\n",
    );
    verify(
        &fixture,
        &VortexQueryPrimitiveRequest::sample_rows(fixture.uri(), projection(&[VALUE]), None, 2, 7),
        "sample",
        &[VALUE],
        serde_json::json!([{VALUE:20},{VALUE:50}]),
        "amount\n20\n50\n",
    );
}

#[test]
fn unary_writers_preserve_scalar_rewrite_rolling_melt_explode_and_pivot_through_all_eight_formats()
{
    let fixture = Fixture::new(&[Some(1), Some(2), Some(3)], &[10, 20, 30], 2);
    let rewrite = VortexQueryPrimitiveRequest::expression_project_rows(
        fixture.uri(),
        projection(&[VALUE]),
        crate::VortexExpressionProjectionRequest::new(vec![
            crate::VortexExpressionRewrite::NumericScalarArithmetic {
                target_column: ColumnRef::new(VALUE).unwrap(),
                operator: "+".into(),
                operand: shardloom_core::ScalarValue::UInt64(1),
            },
        ]),
    );
    verify(
        &fixture,
        &rewrite,
        "rewrite",
        &[VALUE],
        serde_json::json!([{VALUE:11},{VALUE:21},{VALUE:31}]),
        "amount\n11\n21\n31\n",
    );
    let rolling = VortexQueryPrimitiveRequest::rolling_window_rows(
        fixture.uri(),
        crate::VortexRollingWindowRequest::new(
            ColumnRef::new(VALUE).unwrap(),
            "total".into(),
            2,
            2,
            "sum".into(),
        ),
    );
    verify(
        &fixture,
        &rolling,
        "rolling",
        &["total"],
        serde_json::json!([{"total":30.0},{"total":50.0}]),
        "total\n30\n50\n",
    );
    let melt = VortexQueryPrimitiveRequest::melt_rows(
        fixture.uri(),
        crate::VortexMeltProjectionRequest::new(
            vec![ColumnRef::new(KEY).unwrap()],
            vec![ColumnRef::new(VALUE).unwrap()],
            "kind".into(),
            "cell".into(),
        ),
    );
    verify(
        &fixture,
        &melt,
        "melt",
        &[KEY, "kind", "cell"],
        serde_json::json!([{KEY:1,"kind":"amount","cell":10},{KEY:2,"kind":"amount","cell":20},{KEY:3,"kind":"amount","cell":30}]),
        "account_key,kind,cell\n1,amount,10\n2,amount,20\n3,amount,30\n",
    );
    let list = vortex::array::arrays::ListViewArray::try_new(
        PrimitiveArray::new(vec![7u64, 8, 9], Validity::NonNullable).into_array(),
        PrimitiveArray::new(vec![0u64, 2, 2], Validity::NonNullable).into_array(),
        PrimitiveArray::new(vec![2u64, 0, 1], Validity::NonNullable).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::from_array(
        StructArray::new(
            FieldNames::from([KEY, "items"]),
            vec![
                PrimitiveArray::new(vec![1u64, 2, 3], Validity::NonNullable).into_array(),
                list,
            ],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let explode = VortexQueryPrimitiveRequest::explode_rows(
        fixture.uri(),
        projection(&[KEY, "items"]),
        crate::VortexExplodeProjectionRequest::new(ColumnRef::new("items").unwrap()),
    );
    verify(
        &fixture,
        &explode,
        "explode",
        &[KEY, "items"],
        serde_json::json!([{KEY:1,"items":7},{KEY:1,"items":8},{KEY:3,"items":9}]),
        "account_key,items\n1,7\n1,8\n3,9\n",
    );
    let fixture = pivot_tests::fixture();
    verify(
        &fixture,
        &pivot_tests::request(&fixture, "sum"),
        "pivot",
        &[KEY, "pivot_a", "pivot_z"],
        serde_json::json!([{KEY:10,"pivot_a":15.0,"pivot_z":null},{KEY:2,"pivot_a":20.0,"pivot_z":15.0}]),
        "account_key,pivot_a,pivot_z\n10,15,\n2,20,15\n",
    );
}
