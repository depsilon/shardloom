use super::*;
use std::collections::{BTreeMap, BTreeSet};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::FieldNames,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

#[test]
fn true_empty_prepared_aggregates_certify_complete_values_and_reexecute_without_reopening() {
    let fixture = Fixture::new();
    write_empty_source(&fixture);
    for grouped in [false, true] {
        let mut aggregate = scalar();
        aggregate
            .measures
            .push(measure("count", Some("metric"), "present_alias"));
        if grouped {
            aggregate.group_by = vec![ColumnRef::new("value").unwrap()];
        }
        let expected = if grouped {
            serde_json::json!([])
        } else {
            serde_json::json!({"rows_alias":0,"unique_alias":0,"total_alias":null,"present_alias":0})
        };
        let request = fixture.request(aggregate);
        for parallelism in [1, 2] {
            let prepared = prepare_aggregate(
                &request,
                VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
            )
            .unwrap();
            let owner = prepared.session.clone();
            assert_eq!(prepared.snapshot().prepared_source_opens, 1);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            for execution in 1..=3 {
                let result = prepared.execute().unwrap();
                certified(&result, execution);
                assert_eq!(payload(&result.report)["values"], expected);
                assert_empty_source_scan(&result.report);
                assert!(
                    result
                        .native_io_certificate
                        .source_pushdown_report
                        .proof_basis
                        .contains("Vortex footer declares zero source rows")
                );
            }
            fixture.replace();
            assert!(
                prepared
                    .execute()
                    .err()
                    .expect("replaced source must fail")
                    .to_string()
                    .contains("prepared source changed")
            );
            drop(prepared);
            assert_eq!(owner.snapshot().memory.reserved_bytes, 0);
            write_empty_source(&fixture);
        }
    }
}

fn write_empty_source(fixture: &Fixture) {
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let array = StructArray::try_new(
        FieldNames::from(["value", "metric"]),
        vec![
            PrimitiveArray::new(Vec::<i64>::new(), Validity::NonNullable).into_array(),
            PrimitiveArray::new(Vec::<i64>::new(), Validity::NonNullable).into_array(),
        ],
        0,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut output = std::fs::File::create(fixture.path()).unwrap();
    let writer = session
        .write_options()
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, array.dtype().clone());
    assert_eq!(writer.finish().unwrap().row_count(), 0);
}

fn assert_empty_source_scan(report: &VortexLocalPrimitiveExecutionReport) {
    assert_eq!(report.rows_scanned, 0);
    assert_eq!(report.rows_selected, Some(0));
    assert_eq!(report.arrays_read_count, 0);
    assert!(report.upstream_scan_called && report.streaming_scan_used);
    assert!(!report.data_read && !report.data_decoded && !report.data_materialized);
    assert!(!report.row_read && !report.arrow_converted && !report.full_stream_collected);
    assert!(report.reader_splits.is_empty());
}

#[test]
fn true_empty_ordinary_aggregate_preserves_extrema_and_rejects_nonempty_missing_read_evidence() {
    let fixture = Fixture::new();
    write_empty_source(&fixture);
    let mut aggregate = scalar();
    for function in ["min", "max", "avg"] {
        aggregate
            .measures
            .push(measure(function, Some("metric"), function));
    }
    let request = fixture.request(aggregate);
    let report = crate::local_primitives::execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    assert_eq!(
        payload(&report)["values"],
        serde_json::json!({
            "rows_alias":0,"unique_alias":0,"total_alias":null,"min":null,"max":null,"avg":null,
        })
    );
    assert_empty_source_scan(&report);
    assert!(
        local_primitive_native_io_certificate(&request, &report)
            .unwrap()
            .is_certified()
    );
    for variation in 0..6 {
        let mut invalid = report.clone();
        match variation {
            0 => invalid.rows_scanned = 1,
            1 => invalid.embedded_layout.metadata_persisted_in_artifact = false,
            2 => invalid.upstream_scan_called = false,
            3 => {
                invalid.rows_projected = Some(2);
                invalid.source_order_limit_rows_output = Some(2);
            }
            4 => invalid.data_decoded = true,
            _ => invalid.embedded_layout.footer_row_count = 1,
        }
        assert!(
            !local_primitive_native_io_certificate(&request, &invalid)
                .unwrap()
                .is_certified()
        );
    }
}

#[test]
fn optional_preparation_declines_schema_without_execution_or_duplicate_source_open() {
    use vortex::array::arrays::VarBinViewArray;
    let fixture = Fixture::new();
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let array = StructArray::try_new(
        FieldNames::from(["text_key", "amount"]),
        vec![
            VarBinViewArray::from_iter_str(["a", "b", "a"]).into_array(),
            PrimitiveArray::new(vec![1.5_f64, 2.0, 3.5], Validity::NonNullable).into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut output = std::fs::File::create(fixture.path()).unwrap();
    let mut writer = session
        .write_options()
        .blocking(&runtime)
        .writer(&mut output, array.dtype().clone());
    writer.push(array).unwrap();
    writer.finish().unwrap();
    drop(output);
    let request = fixture.request(
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("text_key").unwrap()],
            vec![
                measure("count", None, "rows_alias"),
                measure("sum", Some("amount"), "total_alias"),
            ],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("text_key", false)]),
    );
    let disposition = prepare_aggregate_for_optional_reuse(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap()
    .unwrap();
    let PreparedAggregateDisposition::Unretained(operation) = disposition else {
        panic!("noninteger schema must not expand the retained API");
    };
    let owner = operation.0.session.clone();
    assert_eq!(owner.snapshot().prepared_source_opens, 1);
    assert_eq!(owner.snapshot().completed_executions, 0);
    let executed = operation.execute().unwrap();
    assert_eq!(
        payload(&executed.report)["values"],
        serde_json::json!([
            {"text_key":"a", "rows_alias":2, "total_alias":5.0},
            {"text_key":"b", "rows_alias":1, "total_alias":2.0},
        ])
    );
    assert!(executed.native_io_certificate.is_certified());
    assert_eq!(owner.snapshot().prepared_source_opens, 1);
    assert_eq!(owner.snapshot().completed_executions, 1);
    assert_eq!(owner.snapshot().memory.reserved_bytes, 0);
    assert!(
        prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap()
        )
        .is_err()
    );
}

#[derive(Clone, Copy)]
struct Row {
    key: Option<i64>,
    member: Option<u64>,
    amount: Option<i16>,
}

fn write_source(fixture: &Fixture, nullable: bool) -> Vec<Row> {
    let keys = [i64::MIN, i64::MAX, (1_i64 << 60) + 1, (1_i64 << 60) + 2];
    let rows = (0..24_usize)
        .map(|index| Row {
            key: (!(nullable && index.is_multiple_of(11))).then_some(keys[index % 4]),
            member: (!(nullable && index.is_multiple_of(5)))
                .then_some(u64::MAX - u64::try_from((index / 4) % 3).unwrap()),
            amount: (!(nullable && index.is_multiple_of(7)))
                .then_some(i16::try_from(index).unwrap() - 9),
        })
        .collect::<Vec<_>>();
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let arrays = rows
        .chunks(5)
        .map(|rows| {
            let columns = if nullable {
                vec![
                    PrimitiveArray::from_option_iter(rows.iter().map(|row| row.member))
                        .into_array(),
                    PrimitiveArray::from_option_iter(rows.iter().map(|row| row.amount))
                        .into_array(),
                    PrimitiveArray::from_option_iter(rows.iter().map(|row| row.key)).into_array(),
                ]
            } else {
                vec![
                    PrimitiveArray::new(
                        rows.iter()
                            .map(|row| row.member.unwrap())
                            .collect::<Vec<_>>(),
                        Validity::NonNullable,
                    )
                    .into_array(),
                    PrimitiveArray::new(
                        rows.iter()
                            .map(|row| row.amount.unwrap())
                            .collect::<Vec<_>>(),
                        Validity::NonNullable,
                    )
                    .into_array(),
                    PrimitiveArray::new(
                        rows.iter().map(|row| row.key.unwrap()).collect::<Vec<_>>(),
                        Validity::NonNullable,
                    )
                    .into_array(),
                ]
            };
            StructArray::try_new(
                FieldNames::from(["member_alias", "amount_alias", "identity_key"]),
                columns,
                rows.len(),
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        })
        .collect::<Vec<_>>();
    let mut output = std::fs::File::create(fixture.path()).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(
                arrays.len(),
            ),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, arrays[0].dtype().clone());
    for array in arrays {
        writer.push(array).unwrap();
    }
    assert_eq!(writer.finish().unwrap().row_count(), 24);
    rows
}

fn grouped_values(
    report: &VortexLocalPrimitiveExecutionReport,
) -> BTreeMap<Option<i64>, serde_json::Value> {
    payload(report)["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| {
            let key = &value["identity_key"];
            assert!(
                key.is_null() || key.as_i64().is_some(),
                "exact signed key: {key}"
            );
            (key.as_i64(), value.clone())
        })
        .collect()
}

#[test]
fn retained_grouped_count_distinct_sum_preserve_nulls_and_adjacent_large_identities() {
    for nullable in [false, true] {
        let fixture = Fixture::new();
        let rows = write_source(&fixture, nullable);
        let request = fixture.request(VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("identity_key").unwrap()],
            vec![
                measure("count", None, "rows_alias"),
                measure("count", Some("member_alias"), "present_alias"),
                measure("count_distinct", Some("member_alias"), "unique_alias"),
                measure("sum", Some("amount_alias"), "total_alias"),
            ],
        ));
        let mut expected = BTreeMap::<Option<i64>, Vec<Row>>::new();
        for row in &rows {
            expected.entry(row.key).or_default().push(*row);
        }
        let expected = expected
            .into_iter()
            .map(|(key, rows)| {
                let distinct = rows
                    .iter()
                    .filter_map(|row| row.member)
                    .collect::<BTreeSet<_>>();
                let sum = rows
                    .iter()
                    .filter_map(|row| row.amount)
                    .map(f64::from)
                    .sum::<f64>();
                (
                    key,
                    serde_json::json!({"identity_key":key,"rows_alias":rows.len(),
                "present_alias":rows.iter().filter(|row| row.member.is_some()).count(),
                "unique_alias":distinct.len(),"total_alias":sum}),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for parallelism in [1, 2] {
            let session = ResidentVortexSession::new(32 << 20, parallelism).unwrap();
            let prepared = prepare_aggregate_in_session(
                &request,
                VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
                &session,
            )
            .unwrap();
            for execution in 1..=3 {
                let result = prepared.execute().unwrap();
                certified(&result, execution);
                assert_eq!(grouped_values(&result.report), expected);
                assert_eq!(result.report.rows_selected, Some(24));
            }
            drop(prepared);
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }
}

#[test]
fn retained_exact_distinct_worker_admission_reuses_generation_with_fresh_partitions() {
    let fixture = Fixture::new();
    let rows = write_source(&fixture, false);
    let request = fixture
        .request(
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("identity_key").unwrap()],
                vec![measure(
                    "count_distinct",
                    Some("member_alias"),
                    "unique_alias",
                )],
            )
            .with_order_by(vec![
                VortexAggregateOrderExpr::new("unique_alias", true),
                VortexAggregateOrderExpr::new("identity_key", false),
            ]),
        )
        .with_source_order_limit(4);
    let expected = rows
        .iter()
        .map(|row| row.key)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|key| {
            let unique = rows
                .iter()
                .filter(|row| row.key == key)
                .filter_map(|row| row.member)
                .collect::<BTreeSet<_>>()
                .len();
            serde_json::json!({"identity_key":key,"unique_alias":unique})
        })
        .collect::<Vec<_>>();
    for parallelism in [1, 2, 4] {
        let prepared = prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
        )
        .unwrap();
        assert!(prepared.worker_pool);
        assert_eq!(prepared.snapshot().provider_background_workers, 0);
        for execution in 1..=3 {
            let result = prepared.execute().unwrap();
            certified(&result, execution);
            assert_eq!(
                payload(&result.report)["values"],
                serde_json::json!(expected)
            );
            assert_eq!(
                payload(&result.report)["aggregate_workers_provider_background_workers"],
                0
            );
        }
    }
}
