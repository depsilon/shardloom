use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
use std::sync::Arc;

#[path = "resident_source_provenance_tests.rs"]
mod source_provenance_tests;

pub(super) const FORMATS: [Format; 8] = [
    Format::Vortex,
    Format::Parquet,
    Format::ArrowIpc,
    Format::Avro,
    Format::Orc,
    Format::Json,
    Format::Jsonl,
    Format::Csv,
];

fn native_rows(path: &std::path::Path, dtype: &DType) -> Vec<serde_json::Value> {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let source = session.prepare_file(path).unwrap();
    assert_eq!(source.dtype(), dtype);
    source
        .with_native_execution_controlled(&CancellationToken::default(), |file, context| {
            let mut rows = Vec::new();
            let names = dtype.as_struct_fields_opt().unwrap().names();
            let mut execution = context.native_session().create_execution_ctx();
            for array in file
                .scan()
                .map_err(vortex_error)?
                .with_ordered(true)
                .into_array_iter(context.runtime())
                .map_err(vortex_error)?
            {
                let array = array.map_err(vortex_error)?;
                let columns = names
                    .iter()
                    .map(|name| {
                        crate::local_primitives::logical_field_from_native_array(
                            &array,
                            name.as_ref(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..array.len() {
                    let object = names
                        .iter()
                        .zip(&columns)
                        .map(|(name, column)| {
                            Ok((
                                name.to_string(),
                                result_batch::scalar_value(column, row, &mut execution)?
                                    .into_json()?,
                            ))
                        })
                        .collect::<Result<serde_json::Map<_, _>>>()?;
                    rows.push(serde_json::Value::Object(object));
                }
            }
            Ok(rows)
        })
        .unwrap()
}

pub(super) fn read_rows(
    path: &std::path::Path,
    format: Format,
    dtype: &DType,
) -> Vec<serde_json::Value> {
    if format == Format::Vortex {
        return native_rows(path, dtype);
    }
    if format == Format::Json {
        return serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    }
    if format == Format::Jsonl {
        return fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|row| serde_json::from_str(row).unwrap())
            .collect();
    }
    let table = match format {
        Format::Parquet => crate::read_flat_parquet_source(path, 100_000),
        Format::ArrowIpc => crate::read_flat_arrow_ipc_source(path, 100_000),
        Format::Avro => crate::read_flat_avro_source(path, 100_000),
        Format::Orc => crate::read_flat_orc_source(path, 100_000),
        _ => panic!("CSV is checked as exact text"),
    }
    .unwrap();
    let names = dtype.as_struct_fields_opt().unwrap().names();
    assert_eq!(
        table.header,
        names.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
    table
        .rows
        .into_iter()
        .map(|row| {
            serde_json::Value::Object(
                row.into_iter()
                    .map(|(name, value)| {
                        let value =
                            crate::local_primitives::scalar_value_to_json_value(&value).unwrap();
                        (name, value)
                    })
                    .collect(),
            )
        })
        .collect()
}

pub(super) fn verify_writers(
    fixture: &Fixture,
    plan: &VortexRelationalPlan,
    label: &str,
    expected: &[serde_json::Value],
    csv: &str,
) {
    let prepared = prepare_relational(plan, policy()).unwrap();
    for (index, format) in FORMATS.into_iter().enumerate() {
        let path = fixture.0.join(format!("{label}.{}", format.as_str()));
        let report = prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{label} {format:?}: {error}"));
        assert_eq!(report.execution.output_rows, expected.len() as u64);
        assert_eq!(report.output.rows_written, expected.len() as u64);
        assert!(report.execution.max_output_batch_rows <= BATCH_ROWS);
        assert_eq!(prepared.snapshot().completed_executions, (index + 1) as u64);
        if format == Format::Csv {
            assert_eq!(fs::read_to_string(path).unwrap(), csv, "{label}");
        } else {
            assert_eq!(
                read_rows(&path, format, &prepared.output_dtype().unwrap()),
                expected,
                "{label} {format:?}"
            );
        }
    }
}

#[test]
fn native_relational_all_eight_writers_preserve_outer_join_and_set_values_and_empty_schema() {
    let left = Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 11, 12]), 1);
    let right = Fixture::new(keyed(&[Some(2), Some(2), Some(3)], &[20, 21, 22]), 1);
    verify_writers(
        &left,
        &join(&left, &right, JoinKind::Full),
        "join",
        &[
            serde_json::json!({"debit":10,"credit":20}),
            serde_json::json!({"debit":10,"credit":21}),
            serde_json::json!({"debit":11,"credit":null}),
            serde_json::json!({"debit":12,"credit":null}),
            serde_json::json!({"debit":null,"credit":22}),
        ],
        "debit,credit\n10,20\n10,21\n11,\n12,\n,22\n",
    );
    let other = Fixture::new(keyed(&[Some(2), None], &[10, 11]), 2);
    verify_writers(
        &left,
        &VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: left.scan(),
            right: other.scan(),
            kind: SetKind::UnionDistinct,
        })),
        "set",
        &[
            serde_json::json!({"entity":2,"amount":10}),
            serde_json::json!({"entity":null,"amount":11}),
            serde_json::json!({"entity":1,"amount":12}),
        ],
        "entity,amount\n2,10\n,11\n1,12\n",
    );
    let empty = Fixture::new(keyed(&[], &[]), 1);
    verify_writers(
        &empty,
        &join(&empty, &right, JoinKind::Inner),
        "empty",
        &[],
        "debit,credit\n",
    );
}

#[test]
fn native_relational_single_column_csv_keeps_null_records() {
    let input = Fixture::new(keyed(&[None, Some(7), None], &[1, 2, 3]), 1);
    let plan = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(input.path().display().to_string()).unwrap(),
        projection: shardloom_plan::ProjectionRequest::Columns(vec![
            ColumnRef::new("entity").unwrap(),
        ]),
        predicate: None,
    });
    verify_writers(
        &input,
        &plan,
        "nullable-column",
        &[
            serde_json::json!({"entity":null}),
            serde_json::json!({"entity":7}),
            serde_json::json!({"entity":null}),
        ],
        "entity\n\"\"\n7\n\"\"\n",
    );
}

#[test]
fn native_relational_complete_writes_exceed_small_collection_without_replay_or_truncation() {
    let values: Vec<u32> = (0..257).collect();
    let keys: Vec<_> = values.iter().map(|value| Some(u64::from(*value))).collect();
    let left = Fixture::new(keyed(&keys, &values), 127);
    let right = Fixture::new(keyed(&keys, &values), 31);
    let plan = join(&left, &right, JoinKind::Cross);
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert!(prepared.execute_owned().is_err());
    assert_eq!(prepared.snapshot().completed_executions, 0);
    let mut expected = Vec::new();
    let mut csv = String::from("debit,credit\n");
    for debit in &values {
        for credit in &values {
            use std::fmt::Write as _;
            expected.push(serde_json::json!({"debit":debit,"credit":credit}));
            writeln!(&mut csv, "{debit},{credit}").unwrap();
        }
    }
    assert_eq!(expected.len(), 66_049);
    verify_writers(&left, &plan, "large", &expected, &csv);
}

#[test]
fn native_relational_writers_reject_every_input_alias_and_pre_cancel_without_staging() {
    let left = Fixture::new(keyed(&[Some(1)], &[10]), 1);
    let right = Fixture::new(keyed(&[Some(1)], &[20]), 1);
    let hardlink = left.0.join("right-hardlink.vortex");
    let symlink = left.0.join("right-symlink.vortex");
    fs::hard_link(right.path(), &hardlink).unwrap();
    std::os::unix::fs::symlink(right.path(), &symlink).unwrap();
    let prepared = prepare_relational(&join(&left, &right, JoinKind::Inner), policy()).unwrap();
    let originals = [
        fs::read(left.path()).unwrap(),
        fs::read(right.path()).unwrap(),
    ];
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    for format in FORMATS {
        for path in [left.path(), right.path(), hardlink.clone(), symlink.clone()] {
            let error = prepared
                .write(&path, format, true)
                .err()
                .unwrap()
                .to_string();
            assert!(error.contains("source"), "{format:?}: {error}");
        }
        let path = left.0.join(format!("cancel.{}", format.as_str()));
        assert!(
            prepared
                .write_controlled(&path, format, false, &cancellation)
                .is_err()
        );
        assert!(!path.exists());
    }
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(fs::read(left.path()).unwrap(), originals[0]);
    assert_eq!(fs::read(right.path()).unwrap(), originals[1]);
    assert_eq!(fs::read_dir(&left.0).unwrap().count(), 3);
    assert_eq!(fs::read_dir(&right.0).unwrap().count(), 1);
}

#[test]
fn native_relational_windows_roundtrip_every_writer() {
    let fixture = window_tests::fixture();
    verify_writers(
        &fixture,
        &window_tests::plan(fixture.scan()),
        "window",
        &window_tests::expected(),
        concat!(
            "value,rn,rank,dense,lag,lead,tile,percent,cume,reverse\n",
            "30,2,2,2,40,,2,0.5,0.6666666666666666,2\n",
            "20,3,3,2,11,,2,0.6666666666666666,0.75,2\n",
            "10,1,1,1,,20,1,0,0.5,3\n",
            "11,2,1,1,10,90,1,0,0.5,4\n",
            "40,1,1,1,,50,1,0,0.3333333333333333,3\n",
            "50,3,3,3,30,,3,1,1,1\n",
            "60,1,1,1,,,1,0,1,1\n",
            "61,2,1,1,60,,2,0,1,2\n",
            "90,4,4,3,20,,3,1,1,1\n"
        ),
    );
}

#[test]
fn native_relational_windows_write_complete_output_above_the_collection_limit() {
    use crate::relational_query::VortexRelationalWindowFunction as Function;
    use std::fmt::Write as _;
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter((0u32..65_537).rev()).into_array(),
        ),
        4096,
    );
    let plan = window_tests::unpartitioned(&fixture, Function::RowNumber);
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    let mut expected = Vec::new();
    let mut csv = String::from("value,window\n");
    for value in (0u32..65_537).rev() {
        expected.push(serde_json::json!({"value":value,"window":value+1}));
        writeln!(&mut csv, "{value},{}", value + 1).unwrap();
    }
    verify_writers(&fixture, &plan, "large-window", &expected, &csv);
}

#[test]
fn native_relational_spill_writes_complete_ordered_output_through_all_eight_sinks() {
    use crate::relational_query::{
        VortexRelationalNullOrder, VortexRelationalOrderKey, VortexRelationalSort,
        VortexRelationalSpillPolicy,
    };
    use std::fmt::Write as _;
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter((0_u32..70_017).rev()).into_array(),
        ),
        4096,
    );
    let plan = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: fixture.scan(),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("value").unwrap(),
            descending: false,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }));
    let prepared = prepare_relational(&plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&fixture.0, 64 << 20, 1 << 20).unwrap())
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    let expected = (0..70_017)
        .map(|value| serde_json::json!({"value":value}))
        .collect::<Vec<_>>();
    let mut csv = String::from("value\n");
    for value in 0..70_017 {
        writeln!(&mut csv, "{value}").unwrap();
    }
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for (index, format) in FORMATS.into_iter().enumerate() {
        let path = fixture.0.join(format!("ordered-spill.{}", format.as_str()));
        let written = prepared.write(&path, format, false).unwrap();
        assert_eq!(written.execution.output_rows, 70_017);
        assert_eq!(written.output.rows_written, 70_017);
        assert_eq!(
            written.execution.runtime.completed_executions,
            (index + 1) as u64
        );
        let spill = written.execution.spill.as_ref().unwrap();
        assert!(spill.runs_written > 1);
        assert!(spill.owned_cleanup_completed);
        assert!(spill.peak_disk_bytes <= spill.quota_bytes);
        if format == Format::Csv {
            assert_eq!(fs::read_to_string(&path).unwrap(), csv);
        } else {
            assert_eq!(
                read_rows(&path, format, &prepared.output_dtype().unwrap()),
                expected,
                "{format:?}"
            );
        }
        drop(written);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert!(!fs::read_dir(&fixture.0).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("shardloom-query-")
        }));
    }
}

#[test]
fn native_relational_spill_cleanup_failure_prevents_publication_and_preserves_foreign_entries() {
    use crate::{
        local_primitives::native_relational_spill::BEFORE_RUN_OPEN,
        relational_query::{
            VortexRelationalNullOrder, VortexRelationalOrderKey, VortexRelationalSort,
            VortexRelationalSpillPolicy,
        },
    };
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter((0_u32..24_001).rev()).into_array(),
        ),
        1024,
    );
    let plan = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: fixture.scan(),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("value").unwrap(),
            descending: false,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }));
    let prepared = prepare_relational(&plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&fixture.0, 64 << 20, 1 << 20).unwrap())
        .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for format in FORMATS {
        let output = fixture.0.join(format!("unchanged.{}", format.as_str()));
        let foreign = std::rc::Rc::new(std::cell::RefCell::new(None));
        let captured = std::rc::Rc::clone(&foreign);
        BEFORE_RUN_OPEN.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |run| {
                let path = run.parent().unwrap().join("not-owned-by-query");
                fs::write(&path, b"foreign content").unwrap();
                *captured.borrow_mut() = Some(path);
            }));
        });
        let error = prepared
            .write(&output, format, false)
            .err()
            .expect("cleanup must fail");
        assert!(!output.exists(), "{format:?}: {error}");
        let foreign = foreign
            .borrow()
            .clone()
            .unwrap_or_else(|| panic!("{format:?} did not open a spill run: {error}"));
        assert_eq!(fs::read(&foreign).unwrap(), b"foreign content");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        // Test-owned interruption artifact; production cleanup must preserve it.
        fs::remove_file(&foreign).unwrap();
        fs::remove_dir(foreign.parent().unwrap()).unwrap();
        assert!(!fs::read_dir(&fixture.0).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("shardloom-query-")
        }));
    }
}

#[test]
fn native_relational_join_window_set_and_subquery_compose_through_every_writer() {
    use crate::relational_query::{
        VortexRelationalNullOrder, VortexRelationalOrderKey, VortexRelationalSubquery,
        VortexRelationalSubqueryKind, VortexRelationalWindow, VortexRelationalWindowExpression,
        VortexRelationalWindowFunction,
    };
    let left = Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 11, 12]), 1);
    let right = Fixture::new(keyed(&[Some(2), Some(2), Some(3)], &[20, 21, 22]), 1);
    let window = VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
        input: join(&left, &right, JoinKind::Full),
        columns: vec![
            ColumnRef::new("debit").unwrap(),
            ColumnRef::new("credit").unwrap(),
        ],
        expressions: vec![VortexRelationalWindowExpression {
            output_column: "rank".into(),
            function: VortexRelationalWindowFunction::RowNumber,
            partition_by: vec![],
            order_by: vec![VortexRelationalOrderKey {
                column: ColumnRef::new("credit").unwrap(),
                descending: false,
                nulls: Some(VortexRelationalNullOrder::Last),
            }],
            frame: None,
        }],
    }));
    let relation = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: left.scan(),
        right: right.scan(),
        kind: SetKind::UnionDistinct,
    }));
    let plan = VortexRelationalPlan::Subquery(Box::new(VortexRelationalSubquery {
        input: window,
        relation,
        kind: VortexRelationalSubqueryKind::In {
            columns: vec![VortexRelationalJoinKey {
                left: ColumnRef::new("debit").unwrap(),
                right: ColumnRef::new("amount").unwrap(),
            }],
        },
        correlation: vec![],
        output_column: "member".into(),
        evaluation_guard: None,
        negated: false,
    }));
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert_eq!(prepared.snapshot().prepared_source_opens, 2);
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(collected.execution.runtime.completed_executions, 1);
    assert_eq!(collected.execution.scan_rows_delivered, 12);
    let expected = vec![
        serde_json::json!({"debit":10,"credit":20,"rank":1,"member":true}),
        serde_json::json!({"debit":10,"credit":21,"rank":2,"member":true}),
        serde_json::json!({"debit":11,"credit":null,"rank":4,"member":true}),
        serde_json::json!({"debit":12,"credit":null,"rank":5,"member":true}),
        serde_json::json!({"debit":null,"credit":22,"rank":3,"member":null}),
    ];
    assert_eq!(json_rows(&collected), expected);
    verify_writers(
        &left,
        &plan,
        "composed",
        &expected,
        "debit,credit,rank,member\n10,20,1,true\n10,21,2,true\n11,,4,true\n12,,5,true\n,22,3,\n",
    );
}

#[test]
fn native_relational_every_writer_discards_staging_after_second_source_mutation_or_midstream_cancel()
 {
    let left = Fixture::new(keyed(&[Some(1), Some(1)], &[10, 11]), 1);
    let right = Fixture::new(keyed(&[Some(1), Some(1)], &[20, 21]), 1);
    for mutate in [true, false] {
        for format in FORMATS {
            let prepared =
                prepare_relational(&join(&left, &right, JoinKind::Inner), policy()).unwrap();
            let baseline = prepared.session.memory().snapshot().reserved_bytes;
            let plan = crate::local_primitives::native_sink::NativeSinkPlan::produced_sources(
                prepared.session.clone(),
                prepared.output_dtype().unwrap(),
                4,
                None,
                prepared.sources.clone(),
            )
            .unwrap();
            let request = VortexQueryPrimitiveRequest::project(
                DatasetUri::new(left.path().display().to_string()).unwrap(),
                shardloom_plan::ProjectionRequest::columns(vec![
                    ColumnRef::new("debit").unwrap(),
                    ColumnRef::new("credit").unwrap(),
                ]),
            );
            let path = left
                .0
                .join(format!("interrupted-{mutate}.{}", format.as_str()));
            let cancel = CancellationToken::default();
            let mut changed = false;
            let mut producer =
                |context: &NativeExecutionContext<'_>,
                 batch_rows: usize,
                 consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
                    prepared
                        .session
                        .with_admitted_sources_execution(&prepared.sources, context, |context| {
                            prepared.consume_in_context(
                                context,
                                batch_rows.min(BATCH_ROWS),
                                None,
                                &mut |array| {
                                    assert!(consume(array)?);
                                    if !changed {
                                        if mutate {
                                            right.replace();
                                        } else {
                                            cancel.cancel();
                                        }
                                        changed = true;
                                    }
                                    Ok(())
                                },
                            )
                        })
                        .map(|_| ())
                };
            assert!(
                crate::local_primitives::completed_result::write_stream(
                    plan,
                    &request,
                    &path,
                    format,
                    false,
                    policy(),
                    &mut producer,
                    &cancel,
                )
                .is_err(),
                "{mutate} {format:?}"
            );
            assert!(changed);
            assert!(!path.exists());
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(
                prepared.session.memory().snapshot().reserved_bytes,
                baseline
            );
            assert_eq!(fs::read_dir(&left.0).unwrap().count(), 1);
            assert_eq!(fs::read_dir(&right.0).unwrap().count(), 1);
        }
    }
}

#[test]
fn native_relational_every_writer_checks_original_preparation_source_through_commit() {
    use crate::prepared_source_binding::{
        KEY, local_preparation_binding, local_preparation_identity,
    };

    let origin = Fixture::new(keyed(&[], &[]), 1);
    let csv = origin.0.join("source.csv");
    for (during_consumption, source_owned) in
        [(true, false), (false, false), (true, true), (false, true)]
    {
        for format in FORMATS {
            fs::write(&csv, "entity,amount\n1,10\n1,11\n").unwrap();
            let binding = local_preparation_binding(&csv, "csv", "metadata_only").unwrap();
            let source = Fixture::with_metadata(
                keyed(&[Some(1), Some(1)], &[10, 11]),
                1,
                vec![(KEY, binding.as_bytes().to_vec())],
            );
            let identity = Arc::new(local_preparation_identity(&source.path(), &binding).unwrap());
            let prepared = source_provenance_tests::prepare(&source, identity, source_owned);
            let baseline = prepared.session.memory().snapshot().reserved_bytes;
            let plan = crate::local_primitives::native_sink::NativeSinkPlan::produced_sources(
                prepared.session.clone(),
                prepared.output_dtype().unwrap(),
                2,
                None,
                prepared.sources.clone(),
            )
            .unwrap()
            .with_preparation_sources(prepared.preparation_sources.clone());
            let request = VortexQueryPrimitiveRequest::project(
                DatasetUri::new(source.path().display().to_string()).unwrap(),
                shardloom_plan::ProjectionRequest::All,
            );
            let path = source.0.join(format!("output.{}", format.as_str()));
            let native_before = fs::read(source.path()).unwrap();
            let mut changed = false;
            let mut producer =
                |context: &NativeExecutionContext<'_>,
                 batch_rows: usize,
                 consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
                    prepared.consume_in_context(
                        context,
                        batch_rows.min(BATCH_ROWS),
                        None,
                        &mut |array| {
                            assert!(consume(array)?);
                            if during_consumption && !changed {
                                fs::write(&csv, "entity,amount\n1,10\n1,11\n1,99\n").unwrap();
                                changed = true;
                            }
                            Ok(())
                        },
                    )?;
                    if !during_consumption {
                        fs::write(&csv, "entity,amount\n1,10\n1,11\n1,99\n").unwrap();
                        changed = true;
                    }
                    Ok(())
                };
            let error = crate::local_primitives::completed_result::write_stream(
                plan,
                &request,
                &path,
                format,
                false,
                policy(),
                &mut producer,
                &CancellationToken::default(),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("prepared source binding"),
                "{format:?}: {error}"
            );
            assert!(changed);
            assert!(!path.exists());
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(
                prepared.session.memory().snapshot().reserved_bytes,
                baseline
            );
            assert_eq!(fs::read(source.path()).unwrap(), native_before);
            assert_eq!(fs::read_dir(&source.0).unwrap().count(), 1);
        }
    }
}
