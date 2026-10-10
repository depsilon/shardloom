use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::VortexRelationalAggregate,
    resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds},
};

fn memory_scan(uri: DatasetUri) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri,
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

fn repeated_union(uri: &DatasetUri, levels: usize) -> VortexRelationalPlan {
    if levels == 0 {
        return memory_scan(uri.clone());
    }
    VortexRelationalPlan::Set(Box::new(crate::relational_query::VortexRelationalSet {
        left: repeated_union(uri, levels - 1),
        right: repeated_union(uri, levels - 1),
        kind: crate::relational_query::VortexRelationalSetKind::UnionAll,
    }))
}

#[test]
fn native_relational_plan_node_count_grows_under_shared_metadata_admission() {
    let uri = DatasetUri::new("memory://many-operators").unwrap();
    let plan = repeated_union(&uri, 7); // 128 scans and 127 set operators.
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_memory_source(uri.clone(), |session| {
            ResidentMemorySource::from_columns(
                session,
                &[MemoryColumn {
                    name: "n",
                    values: MemoryColumnValues::Int64(&[Some(1), None, Some(-1)]),
                }],
                MemorySourceBounds::default(),
            )
        })?;
        Ok(plan.clone())
    })
    .unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let expected = (0..128)
        .flat_map(|_| {
            [
                serde_json::json!({"n":1}),
                serde_json::json!({"n":null}),
                serde_json::json!({"n":-1}),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(json_rows(&result), expected);
    assert_eq!(result.execution.prepared_sources, 1);
    assert_eq!(result.execution.runtime.prepared_source_opens, 0);
    assert_eq!(result.execution.runtime.completed_executions, 1);
    drop(result);
    drop(prepared);

    let session = ResidentVortexSession::new(128 << 10, 1).unwrap();
    let retained = session.memory().reserve(2048).unwrap();
    let mut binder = super::super::bind::Binder::new(&session).unwrap();
    binder
        .register_memory_source(uri, |session| {
            ResidentMemorySource::from_int64_range(session, "n", 0, 1, 3)
        })
        .unwrap();
    let error = binder.bind(&plan, 0).err().unwrap().to_string();
    assert!(error.contains("memory reservation denied"), "{error}");
    assert!(session.snapshot().memory.denied_reservations > 0);
    drop(binder);
    assert_eq!(session.snapshot().memory.reserved_bytes, retained.bytes());
    drop(retained);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_relational_memory_range_uses_shared_reductions_without_file_preparation() {
    const ROWS: usize = 1_000_017;
    let mut bounded = policy();
    bounded.resource_envelope.memory_budget_bytes = 4 << 20;
    let uri = DatasetUri::new("memory://range").unwrap();
    let prepared = prepare_relational_with_schema(bounded, |schemas| {
        schemas.register_memory_source(uri.clone(), |session| {
            ResidentMemorySource::from_int64_range(
                session,
                "n",
                i64::try_from(ROWS - 1).unwrap(),
                -1,
                ROWS,
            )
        })?;
        assert_eq!(schemas.source_columns(&uri)?, ["n"]);
        Ok(VortexRelationalPlan::Aggregate(Box::new(
            VortexRelationalAggregate {
                input: memory_scan(uri),
                group_by: vec![],
                measures: ["count", "sum", "min", "max"]
                    .into_iter()
                    .map(|function| {
                        VortexSimpleAggregateMeasure::new(
                            function,
                            Some(ColumnRef::new("n").unwrap()),
                            function.into(),
                        )
                    })
                    .collect(),
            },
        )))
    })
    .unwrap();
    let rows_float = f64::from(u32::try_from(ROWS).unwrap());
    for execution in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            [
                serde_json::json!({"count":ROWS,"sum":rows_float*(rows_float-1.0)/2.0,"min":0,"max":ROWS-1})
            ]
        );
        assert_eq!(result.execution.prepared_sources, 1);
        assert_eq!(result.execution.runtime.prepared_source_opens, 0);
        assert_eq!(result.execution.runtime.completed_executions, execution);
        assert!(result.execution.native_io_certificate.is_certified());
        assert!(result.execution.runtime.memory.peak_reserved_bytes <= 4 << 20);
    }
}

#[test]
fn native_relational_memory_scan_preserves_nulls_empty_schema_and_native_predicates() {
    for values in [
        vec![Some(1_i64), None, Some(3), Some(3)],
        vec![],
        vec![None, None],
    ] {
        let uri = DatasetUri::new("memory://rows").unwrap();
        let prepared = prepare_relational_with_schema(policy(), |schemas| {
            schemas.register_memory_source(uri.clone(), |session| {
                ResidentMemorySource::from_columns(
                    session,
                    &[MemoryColumn {
                        name: "n",
                        values: MemoryColumnValues::Int64(&values),
                    }],
                    MemorySourceBounds::default(),
                )
            })?;
            let VortexRelationalPlan::Scan(mut scan) = memory_scan(uri) else {
                unreachable!()
            };
            scan.predicate = Some(shardloom_core::PredicateExpr::Compare {
                column: ColumnRef::new("n").unwrap(),
                op: shardloom_core::ComparisonOp::Gt,
                value: shardloom_core::StatValue::Int64(1),
            });
            Ok(VortexRelationalPlan::Scan(scan))
        })
        .unwrap();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let expected: Vec<_> = values
            .into_iter()
            .flatten()
            .filter(|value| *value > 1)
            .map(|value| serde_json::json!({"n":value}))
            .collect();
        assert_eq!(json_rows(&result), expected);
        assert_eq!(result.execution.output_columns, ["n"]);
        assert_eq!(result.execution.runtime.prepared_source_opens, 0);
    }
}

#[test]
fn native_relational_memory_admission_rejects_foreign_ownership_and_invalid_ranges() {
    let other = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let foreign = ResidentMemorySource::from_int64_range(&other, "n", 0, 1, 1).unwrap();
    let denied = prepare_relational_with_schema(policy(), |schemas| {
        let uri = DatasetUri::new("memory://foreign").unwrap();
        schemas.register_memory_source(uri.clone(), |_| Ok(foreign))?;
        Ok(memory_scan(uri))
    });
    assert!(denied.is_err());
    for (start, step, rows) in [
        (i64::MAX, 1, 2),
        (i64::MIN, -1, 2),
        (0, 0, 1),
        (0, 1, usize::MAX),
    ] {
        assert!(ResidentMemorySource::from_int64_range(&other, "n", start, step, rows).is_err());
    }
    let extreme =
        ResidentMemorySource::from_int64_range(&other, "n", i64::MIN, i64::MAX, 3).unwrap();
    let result = extreme
        .prepare_projection(&["n"], None, None)
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(result.values_json.value()).unwrap(),
        serde_json::json!([{ "n": i64::MIN }, { "n": -1 }, { "n": i64::MAX - 1 }])
    );
}

#[test]
fn native_relational_memory_and_file_inputs_share_join_state_and_generation_checks() {
    let fixture = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["n", "value"]),
            vec![
                PrimitiveArray::from_iter([1_i64, 1, 3]).into_array(),
                PrimitiveArray::from_iter([10_i64, 11, 30]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        1,
    );
    let uri = DatasetUri::new("memory://left").unwrap();
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_memory_source(uri.clone(), |session| {
            ResidentMemorySource::from_int64_range(session, "n", 0, 1, 3)
        })?;
        Ok(VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: memory_scan(uri),
            right: fixture.scan(),
            kind: JoinKind::Left,
            condition: None,
            keys: vec![VortexRelationalJoinKey {
                left: ColumnRef::new("n")?,
                right: ColumnRef::new("n")?,
            }],
            columns: vec![
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: ColumnRef::new("n")?,
                    output_column: "n".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Right,
                    column: ColumnRef::new("value")?,
                    output_column: "value".into(),
                },
            ],
        })))
    })
    .unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![
            serde_json::json!({"n":0,"value":null}),
            serde_json::json!({"n":1,"value":10}),
            serde_json::json!({"n":1,"value":11}),
            serde_json::json!({"n":2,"value":null}),
        ]
    );
    assert_eq!(result.execution.prepared_sources, 2);
    assert_eq!(result.execution.runtime.prepared_source_opens, 1);
    fixture.replace();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_relational_memory_uses_all_shared_output_adapters() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let uri = DatasetUri::new("memory://writer").unwrap();
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_memory_source(uri.clone(), |session| {
            ResidentMemorySource::from_columns(
                session,
                &[MemoryColumn {
                    name: "n",
                    values: MemoryColumnValues::Int64(&[Some(1), None, Some(i64::MAX)]),
                }],
                MemorySourceBounds::default(),
            )
        })?;
        Ok(memory_scan(uri))
    })
    .unwrap();
    let dtype = DType::struct_(
        [("n", DType::Primitive(PType::I64, Nullability::Nullable))],
        Nullability::NonNullable,
    );
    let targets = [
        Format::Vortex,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Avro,
        Format::Orc,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, format)| (fixture.0.join(format!("output-{index}")), format))
    .collect::<Vec<_>>();
    let written = prepared.write_many(&targets, false).unwrap();
    assert_eq!(written.len(), targets.len());
    for ((output, format), written) in targets.iter().zip(written) {
        let format = *format;
        assert_eq!(written.output.rows_written, 3);
        assert_eq!(written.output.output_path, output.display().to_string());
        assert_eq!(written.execution.runtime.prepared_source_opens, 0);
        if format == Format::Csv {
            assert_eq!(
                fs::read_to_string(output).unwrap(),
                "n\n1\n\"\"\n9223372036854775807\n"
            );
            continue;
        }
        assert_eq!(
            writer_tests::read_rows(output, format, &dtype),
            vec![
                serde_json::json!({"n":1}),
                serde_json::json!({"n":null}),
                serde_json::json!({"n":i64::MAX}),
            ]
        );
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_relational_wide_memory_projection_collects_and_reopens_all_destinations() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    const WIDTH: usize = 4097;
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let names = (0..WIDTH)
        .map(|index| format!("c{index}"))
        .collect::<Vec<_>>();
    let uri = DatasetUri::new("memory://wide-writer").unwrap();
    let mut admitted = policy();
    admitted.resource_envelope.memory_budget_bytes = 256 << 20;
    let prepared = prepare_relational_with_schema(admitted, |schemas| {
        schemas.register_memory_source(uri.clone(), |session| {
            let columns = names
                .iter()
                .map(|name| MemoryColumn {
                    name,
                    values: MemoryColumnValues::Int64(&[Some(i64::MIN), None, Some(i64::MAX)]),
                })
                .collect::<Vec<_>>();
            ResidentMemorySource::from_columns(session, &columns, MemorySourceBounds::default())
        })?;
        Ok(VortexRelationalPlan::Project(Box::new(
            crate::relational_query::VortexRelationalProject {
                input: memory_scan(uri),
                expressions: names
                    .iter()
                    .rev()
                    .map(|name| {
                        (
                            name.clone(),
                            shardloom_core::Expression::column(
                                shardloom_core::ExprId::new(name).unwrap(),
                                ColumnRef::new(name).unwrap(),
                            ),
                        )
                    })
                    .collect(),
            },
        )))
    })
    .unwrap();
    let expected = [Some(i64::MIN), None, Some(i64::MAX)]
        .into_iter()
        .map(|value| {
            serde_json::Value::Object(
                names
                    .iter()
                    .map(|name| (name.clone(), serde_json::json!(value)))
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    let dtype = DType::struct_(
        names.iter().rev().map(|name| {
            (
                name.as_str(),
                DType::Primitive(PType::I64, Nullability::Nullable),
            )
        }),
        Nullability::NonNullable,
    );
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&collected), expected);
    drop(collected);
    for (index, format) in writer_tests::FORMATS.into_iter().enumerate() {
        let output = fixture.0.join(format!("wide-{index}"));
        let written = prepared.write(&output, format, false).unwrap();
        assert_eq!(written.output.rows_written, 3);
        if format == Format::Csv {
            let csv = fs::read_to_string(&output).unwrap();
            let mut lines = csv.lines();
            assert_eq!(
                lines.next().unwrap(),
                names.iter().rev().cloned().collect::<Vec<_>>().join(",")
            );
            for value in [i64::MIN.to_string(), String::new(), i64::MAX.to_string()] {
                assert_eq!(lines.next().unwrap(), vec![value; WIDTH].join(","));
            }
            assert!(lines.next().is_none());
        } else {
            assert_eq!(
                writer_tests::read_rows(&output, format, &dtype),
                expected,
                "{format:?}"
            );
        }
    }
}
