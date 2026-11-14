//! Public alias shapes and finite compatibility-reader ownership.

use super::*;
use crate::relational_query::VortexRelationalProject;
use shardloom_core::{ExprId, Expression};

fn project(input: VortexRelationalPlan, columns: &[(&str, &str)]) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input,
        expressions: columns
            .iter()
            .map(|(source, output)| {
                (
                    (*output).to_owned(),
                    Expression::column(
                        ExprId::new(*output).unwrap(),
                        ColumnRef::new(*source).unwrap(),
                    ),
                )
            })
            .collect(),
    }))
}

fn aliased_plan(plan: VortexRelationalPlan) -> VortexRelationalPlan {
    let VortexRelationalPlan::Join(mut outer) = plan else {
        unreachable!()
    };
    let VortexRelationalPlan::Join(mut inner) = outer.right else {
        unreachable!()
    };
    outer.left = project(outer.left, &[("id", "id"), ("y", "y"), ("x", "x")]);
    inner.left = project(inner.left, &[("y", "y"), ("x", "x")]);
    inner.right = project(inner.right, &[("y", "y"), ("x", "x")]);
    outer.right = project(
        project(
            VortexRelationalPlan::Join(inner),
            &[("witness_a", "renamed_a"), ("witness_b", "renamed_b")],
        ),
        &[("renamed_b", "right_b"), ("renamed_a", "right_a")],
    );
    outer.keys = vec![key("y", "right_b"), key("x", "right_a")];
    VortexRelationalPlan::Join(outer)
}

fn observed_inner(
    plan: VortexRelationalPlan,
    columns: impl IntoIterator<Item = VortexRelationalJoinColumn>,
) -> VortexRelationalPlan {
    let VortexRelationalPlan::Join(mut outer) = plan else {
        unreachable!()
    };
    let VortexRelationalPlan::Join(mut inner) = outer.right else {
        unreachable!()
    };
    inner.columns.extend(columns);
    outer.right = project(
        VortexRelationalPlan::Join(inner),
        &[("witness_b", "witness_b"), ("witness_a", "witness_a")],
    );
    VortexRelationalPlan::Join(outer)
}

pub(super) fn hidden_ids(plan: VortexRelationalPlan) -> VortexRelationalPlan {
    observed_inner(
        plan,
        [
            column(Side::Left, "id", "hidden_middle"),
            column(Side::Right, "id", "hidden_last"),
        ],
    )
}

#[test]
fn cyclic_hidden_inner_payload_checks_unique_matching_rows_before_outer_witnesses() {
    let values = nullable_duplicates();
    let expected = existence_reference(&values, JoinKind::LeftSemi);
    let files = file_relations(&values);
    let matched = |left: &Relation, right: &Relation| {
        left.y
            .iter()
            .filter(|key| key.is_some() && right.y.contains(key))
            .count() as u64
    };
    let middle_rows = matched(&values[1], &values[2]);
    let last_rows = matched(&values[2], &values[1]);
    let plan = hidden_ids(file_plan(&files));
    for plan in [plan.clone(), plan.compact_streaming()] {
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let memory = prepared.session.memory().clone();
        for batch_rows in [1, 31] {
            assert_eq!(
                complete(&prepared, &expected, batch_rows, false).executions,
                1
            );
        }
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        let work = result.execution.native_io_certificate.join_work.cyclic;
        assert_eq!(work.middle_payload_rows_validated, middle_rows);
        assert_eq!(work.last_payload_rows_validated, last_rows);
        assert_eq!(work.payload_cells_validated, middle_rows + last_rows);
        drop(result);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn payload_file(values: &Relation, invalid: bool) -> Fixture {
    Fixture::new(
        StructArray::new(
            ["x", "y", "id", "payload"].into(),
            vec![
                PrimitiveArray::from_option_iter(values.x.iter().copied()).into_array(),
                PrimitiveArray::from_option_iter(values.y.iter().copied()).into_array(),
                PrimitiveArray::new(values.id.clone(), Validity::NonNullable).into_array(),
                PrimitiveArray::from_iter((0..values.id.len()).map(|row| {
                    if invalid && row + 1 == values.id.len() {
                        f64::NAN
                    } else {
                        1.0
                    }
                }))
                .into_array(),
            ],
            values.id.len(),
            Validity::NonNullable,
        )
        .into_array(),
        17,
    )
}

#[test]
fn cyclic_hidden_inner_payload_keeps_selected_errors_even_without_outer_witnesses() {
    for invalid_side in [1, 2] {
        for invalid_matches in [false, true] {
            let mut values = [
                Relation::new(&[(Some(9), Some(9)); 128]),
                Relation::new(&[(Some(0), Some(0)); 128]),
                Relation::new(&[(Some(0), Some(0)); 128]),
            ];
            if !invalid_matches {
                values[invalid_side].y[127] = Some(1);
            }
            let files =
                std::array::from_fn(|index| payload_file(&values[index], index == invalid_side));
            let plan = observed_inner(
                file_plan(&files),
                [column(
                    if invalid_side == 1 {
                        Side::Left
                    } else {
                        Side::Right
                    },
                    "payload",
                    "hidden_payload",
                )],
            );
            for ordinary in [false, true] {
                let mut plan = plan.clone();
                if ordinary {
                    let VortexRelationalPlan::Join(join) = &mut plan else {
                        unreachable!()
                    };
                    let VortexRelationalPlan::Scan(scan) = &mut join.left else {
                        unreachable!()
                    };
                    scan.predicate = Some(shardloom_core::PredicateExpr::Compare {
                        column: ColumnRef::new("id").unwrap(),
                        op: shardloom_core::ComparisonOp::GtEq,
                        value: shardloom_core::StatValue::Int64(0),
                    });
                }
                let prepared = prepare_relational(&plan, policy()).unwrap();
                let memory = prepared.session.memory().clone();
                let baseline = prepared.snapshot().reserved_without_source_segment_cache();
                let result = prepared.collect_jsonl(&CancellationToken::default());
                if invalid_matches {
                    let error = result
                        .err()
                        .expect("an inner-matched payload must still be checked");
                    assert!(
                        error.to_string().contains("finite declared dtype"),
                        "{error}"
                    );
                    assert_eq!(prepared.snapshot().completed_executions, 0);
                } else {
                    let result = result.unwrap();
                    assert_eq!(json_rows(&result), Vec::<Value>::new());
                    let work = result.execution.native_io_certificate.join_work.cyclic;
                    assert_eq!(work.executions, u64::from(!ordinary));
                    assert_eq!(work.payload_cells_validated, if ordinary { 0 } else { 127 });
                    drop(result);
                }
                assert_eq!(
                    prepared.snapshot().reserved_without_source_segment_cache(),
                    baseline
                );
                drop(prepared);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn cyclic_column_aliases_and_flat_stages_preserve_complete_native_values() {
    let values = nullable_duplicates();
    let expected = existence_reference(&values, JoinKind::LeftSemi);
    let files = file_relations(&values);
    for file in [false, true] {
        let plan = aliased_plan(if file {
            file_plan(&files)
        } else {
            existence_plan(JoinKind::LeftSemi, false)
        });
        for declaration in [plan.clone(), plan.compact_streaming()] {
            let prepared = if file {
                prepare_relational(&declaration, policy()).unwrap()
            } else {
                prepare(&values, declaration)
            };
            let memory = prepared.session.memory().clone();
            for rows in [1, 31] {
                let work = complete(&prepared, &expected, rows, false);
                assert_eq!(work.executions, 1);
                assert_eq!(work.witness_exits, 66);
            }
            drop(prepared);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn cyclic_inner_computed_alias_remains_an_observation_barrier() {
    use shardloom_core::{BinaryOp, ExpressionKind, ScalarValue};

    let values = nullable_duplicates();
    let expected = existence_reference(&values, JoinKind::LeftSemi);
    let VortexRelationalPlan::Join(mut outer) =
        aliased_plan(existence_plan(JoinKind::LeftSemi, false))
    else {
        unreachable!()
    };
    let VortexRelationalPlan::Project(project) = &mut outer.right else {
        unreachable!()
    };
    project.expressions[0].1 = Expression::new(
        ExprId::new("computed_alias").unwrap(),
        ExpressionKind::Binary {
            left: Box::new(project.expressions[0].1.clone()),
            op: BinaryOp::Add,
            right: Box::new(Expression::literal(
                ExprId::new("zero").unwrap(),
                ScalarValue::Int64(0),
            )),
        },
    );
    let plan = VortexRelationalPlan::Join(outer);
    for plan in [plan.clone(), plan.compact_streaming()] {
        let prepared = prepare(&values, plan);
        let memory = prepared.session.memory().clone();
        assert_eq!(complete(&prepared, &expected, 31, false).executions, 0);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
mod local {
    use super::*;
    use crate::universal_format_io::demanded::LocalColumnarSource;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::{io::Write as _, sync::Arc};

    struct Files {
        directory: PathBuf,
        paths: [PathBuf; 3],
        format: &'static str,
    }

    impl Files {
        fn new(values: &[Relation; 3], format: &'static str) -> Self {
            let directory = std::env::temp_dir().join(format!(
                "shardloom-cyclic-local-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&directory).unwrap();
            let paths = ["r", "s", "t"].map(|name| directory.join(format!("{name}.{format}")));
            for (path, value) in paths.iter().zip(values) {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("x", DataType::Int64, true),
                    Field::new("y", DataType::Int64, true),
                    Field::new("id", DataType::Int64, false),
                ]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(value.x.clone())),
                        Arc::new(Int64Array::from(value.y.clone())),
                        Arc::new(Int64Array::from(value.id.clone())),
                    ],
                )
                .unwrap();
                let file = fs::File::create(path).unwrap();
                if format == "parquet" {
                    let properties = parquet::file::properties::WriterProperties::builder()
                        .set_max_row_group_row_count(Some(17))
                        .build();
                    let mut writer =
                        parquet::arrow::ArrowWriter::try_new(file, schema, Some(properties))
                            .unwrap();
                    writer.write(&batch).unwrap();
                    writer.close().unwrap();
                } else {
                    let mut writer = arrow_ipc::writer::FileWriter::try_new(file, &schema).unwrap();
                    for start in (0..batch.num_rows()).step_by(17) {
                        writer
                            .write(&batch.slice(start, 17.min(batch.num_rows() - start)))
                            .unwrap();
                    }
                    writer.finish().unwrap();
                }
            }
            Self {
                directory,
                paths,
                format,
            }
        }

        fn plan(&self) -> VortexRelationalPlan {
            let VortexRelationalPlan::Join(mut outer) = existence_plan(JoinKind::LeftSemi, false)
            else {
                unreachable!()
            };
            let scans = self.paths.each_ref().map(|path| {
                VortexRelationalPlan::Scan(VortexRelationalScan {
                    source_uri: DatasetUri::new(path.display().to_string()).unwrap(),
                    projection: shardloom_plan::ProjectionRequest::All,
                    predicate: None,
                })
            });
            let [left, middle, last] = scans;
            outer.left = left;
            let VortexRelationalPlan::Join(inner) = &mut outer.right else {
                unreachable!()
            };
            inner.left = middle;
            inner.right = last;
            VortexRelationalPlan::Join(outer)
        }

        fn prepare(&self, plan: &VortexRelationalPlan) -> PreparedVortexRelational {
            prepare_relational_with_schema(policy(), |schemas| {
                for path in &self.paths {
                    let source = LocalColumnarSource::admit(
                        path,
                        self.format,
                        &schemas.shared_memory_pool(),
                        &schemas.cancellation_token(),
                    )?;
                    schemas.register_local_source(
                        DatasetUri::new(path.display().to_string())?,
                        source,
                    )?;
                }
                Ok(plan.clone())
            })
            .unwrap()
        }
    }

    impl Drop for Files {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.directory).unwrap();
        }
    }

    #[test]
    fn cyclic_local_readers_detach_builds_and_preserve_aliases_and_full_values() {
        let values = nullable_duplicates();
        let expected = existence_reference(&values, JoinKind::LeftSemi);
        for format in ["parquet", "arrow-ipc"] {
            let files = Files::new(&values, format);
            let plain = files.plan();
            let alias = aliased_plan(plain.clone());
            for plan in [plain, alias.clone(), alias.compact_streaming()] {
                let prepared = files.prepare(&plan);
                let memory = prepared.session.memory().clone();
                assert_eq!(complete(&prepared, &expected, 31, false).executions, 1);
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                assert_eq!(json_rows(&result), expected);
                let input = result.execution.input.as_ref().unwrap();
                assert_eq!(input.columnar_sources, 3);
                assert_eq!(input.columnar_rows_decoded, 396);
                assert_eq!(input.join_build_rows_detached, 264);
                assert!(input.join_build_batches_detached > 0);
                drop(result);
                drop(prepared);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }

    #[test]
    fn cyclic_local_readers_drain_failures_pressure_and_replaced_generations() {
        let values = nullable_duplicates();
        for format in ["parquet", "arrow-ipc"] {
            let files = Files::new(&values, format);
            let prepared = files.prepare(&aliased_plan(files.plan()));
            let memory = prepared.session.memory().clone();
            let baseline = memory.snapshot().reserved_bytes;
            let token = CancellationToken::default();
            token.cancel();
            assert!(prepared.collect_jsonl(&token).is_err());
            let held = memory
                .reserve(memory.snapshot().limit_bytes - baseline)
                .unwrap();
            assert!(
                prepared
                    .collect_jsonl(&CancellationToken::default())
                    .is_err()
            );
            drop(held);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
            let mut calls = 0;
            let failure =
                prepared.for_each_json_batch(&CancellationToken::default(), 1, 4096, |_| {
                    calls += 1;
                    Err(ShardLoomError::InvalidOperation(
                        "cyclic local consumer failed".into(),
                    ))
                });
            assert!(
                failure
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("cyclic local consumer failed")
            );
            assert_eq!(calls, 1);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
            assert_eq!(
                complete(
                    &prepared,
                    &existence_reference(&values, JoinKind::LeftSemi),
                    31,
                    false
                )
                .executions,
                1
            );
            let mut replaced = false;
            let failure =
                prepared.for_each_json_batch(&CancellationToken::default(), 31, 4096, |_| {
                    if !replaced {
                        let mut file = fs::OpenOptions::new()
                            .append(true)
                            .open(&files.paths[2])
                            .unwrap();
                        file.write_all(&[0x73]).unwrap();
                        file.sync_all().unwrap();
                        replaced = true;
                    }
                    Ok(())
                });
            assert!(replaced);
            assert!(failure.err().unwrap().to_string().contains("changed"));
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
            drop(prepared);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}
