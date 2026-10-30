//! Real Parquet pages exercise the common native planner, reader and lifecycle.
use super::*;
use crate::{
    relational_query::{VortexRelationalFilter, VortexRelationalProject, VortexRelationalSetKind},
    universal_format_io::demanded::LocalColumnarSource,
};
use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::{EnabledStatistics, WriterProperties},
};
use shardloom_core::{
    ComparisonOp, ExprId, Expression, ExpressionKind, PredicateExpr, ScalarValue, StatValue,
};
use shardloom_plan::ProjectionRequest;
use std::{
    io::{Seek, SeekFrom, Write},
    sync::Arc,
};

#[path = "local_primitive_relational_source_demand_lifecycle_tests.rs"]
mod lifecycle_tests;

#[path = "local_primitive_relational_source_demand_prefix_tests.rs"]
mod prefix_tests;

#[path = "local_primitive_relational_source_demand_collection_tests.rs"]
mod collection_tests;
#[path = "local_primitive_relational_source_demand_window_tests.rs"]
mod window_tests;

#[path = "local_primitive_relational_source_demand_join_tests.rs"]
mod join_tests;

struct ParquetFixture(PathBuf);

impl ParquetFixture {
    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("other", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..16)),
                Arc::new(Int64Array::from_iter_values(100..116)),
                Arc::new(StringArray::from_iter(
                    (0..16).map(|n| (n % 4 != 0).then(|| format!("row-{n}-λ"))),
                )),
            ],
        )
        .unwrap()
    }

    fn new() -> Self {
        Self::with_batch(Self::batch(), EnabledStatistics::Chunk)
    }

    fn with_batch(batch: RecordBatch, statistics: EnabledStatistics) -> Self {
        Self::with_batch_rows(batch, statistics, 4)
    }

    fn with_batch_rows(batch: RecordBatch, statistics: EnabledStatistics, rows: usize) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-source-demand-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let fixture = Self(directory);
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows))
            .set_statistics_enabled(statistics)
            .build();
        let mut writer = ArrowWriter::try_new(
            fs::File::create(fixture.path()).unwrap(),
            batch.schema(),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        drop(batch);
        writer.close().unwrap();
        fixture
    }

    fn path(&self) -> PathBuf {
        self.0.join("input.parquet")
    }

    fn scan(&self, names: &[&str], predicate: Option<PredicateExpr>) -> VortexRelationalPlan {
        VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(self.path().display().to_string()).unwrap(),
            projection: if names.is_empty() {
                ProjectionRequest::All
            } else {
                ProjectionRequest::Columns(
                    names
                        .iter()
                        .map(|name| ColumnRef::new(*name).unwrap())
                        .collect(),
                )
            },
            predicate,
        })
    }

    fn prepare(&self, plan: &VortexRelationalPlan) -> PreparedVortexRelational {
        prepare_relational_with_schema(policy(), |preparation| {
            let source = LocalColumnarSource::admit(
                &self.path(),
                "parquet",
                &preparation.shared_memory_pool(),
                &preparation.cancellation_token(),
            )?;
            preparation.register_local_source(
                DatasetUri::new(self.path().display().to_string())?,
                source,
            )?;
            Ok(plan.clone())
        })
        .unwrap()
    }

    fn corrupt_chunk(&self, group: usize, column: usize) {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(self.path()).unwrap()).unwrap();
        let (start, length) = builder
            .metadata()
            .row_group(group)
            .column(column)
            .byte_range();
        drop(builder);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(self.path())
            .unwrap();
        file.seek(SeekFrom::Start(start)).unwrap();
        file.write_all(&vec![0xff; usize::try_from(length).unwrap()])
            .unwrap();
        file.sync_all().unwrap();
    }
}

impl Drop for ParquetFixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn equal(name: &str, value: i64) -> PredicateExpr {
    PredicateExpr::Compare {
        column: ColumnRef::new(name).unwrap(),
        op: ComparisonOp::Eq,
        value: StatValue::Int64(value),
    }
}

fn column(name: &str) -> Expression {
    Expression::column(ExprId::new(name).unwrap(), ColumnRef::new(name).unwrap())
}

fn project(input: VortexRelationalPlan, names: &[(&str, &str)]) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input,
        expressions: names
            .iter()
            .map(|(source, target)| (target.to_string(), column(source)))
            .collect(),
    }))
}

#[test]
fn parquet_demand_projects_physical_columns_and_prunes_groups_with_residual() {
    let fixture = ParquetFixture::new();
    let metadata =
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(fixture.path()).unwrap()).unwrap();
    let selected_bytes: u64 = [0, 2]
        .into_iter()
        .map(|column| {
            metadata
                .metadata()
                .row_group(1)
                .column(column)
                .byte_range()
                .1
        })
        .sum();
    drop(metadata);
    let prepared = fixture.prepare(&fixture.scan(&["text"], Some(equal("n", 5))));
    let initial = prepared.snapshot().memory.reserved_bytes;
    for _ in 0..2 {
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&collected),
            vec![serde_json::json!({"text":"row-5-λ"})]
        );
        let report = collected.execution.input.as_ref().unwrap();
        assert_eq!(
            (
                report.columnar_sources,
                report.columnar_projected_columns,
                report.columnar_available_columns
            ),
            (1, 2, 3)
        );
        assert_eq!(
            (
                report.columnar_physical_leaf_columns,
                report.columnar_row_groups_total,
                report.columnar_row_groups_read
            ),
            (2, 4, 1)
        );
        assert_eq!(report.columnar_rows_decoded, 4);
        assert_eq!(report.columnar_data_read_bytes, selected_bytes);
        assert_eq!(report.columnar_data_read_requests, 2);
        assert!(report.columnar_data_read_bytes < fs::metadata(fixture.path()).unwrap().len());
        assert!(report.columnar_metadata_read_bytes > 0 && report.columnar_data_read_requests > 0);
        assert!(report.end_of_input_observed && report.output_ownership_detached);
        assert!(collected.execution.native_io_certificate.is_certified());
        assert!(
            !collected
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        drop(collected);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
}

#[test]
fn parquet_demand_common_filter_and_alias_projection_rebind_hidden_positions() {
    let fixture = ParquetFixture::new();
    let aliases = project(
        fixture.scan(&[], None),
        &[("text", "label"), ("n", "renamed")],
    );
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: aliases,
        predicate: Expression::new(
            ExprId::new("test").unwrap(),
            ExpressionKind::Compare {
                left: Box::new(column("renamed")),
                op: ComparisonOp::Eq,
                right: Box::new(Expression::literal(
                    ExprId::new("five").unwrap(),
                    ScalarValue::Int64(5),
                )),
            },
        ),
    }));
    let prepared = fixture.prepare(&project(filtered, &[("label", "result")]));
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&collected),
        vec![serde_json::json!({"result":"row-5-λ"})]
    );
    let report = collected.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            report.columnar_projected_columns,
            report.columnar_row_groups_read
        ),
        (2, 1)
    );
}

#[test]
fn parquet_demand_unions_repeated_source_columns_and_regions_before_replay() {
    let fixture = ParquetFixture::new();
    for unrestricted in [false, true] {
        let left = project(fixture.scan(&["n"], Some(equal("n", 5))), &[("n", "value")]);
        let right = project(
            fixture.scan(&["other"], (!unrestricted).then(|| equal("n", 13))),
            &[("other", "value")],
        );
        let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left,
            right,
            kind: VortexRelationalSetKind::UnionAll,
        }));
        let prepared = fixture.prepare(&plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let mut expected = vec![serde_json::json!({"value":5})];
        expected.extend(
            if unrestricted {
                (100..116).collect::<Vec<_>>()
            } else {
                vec![113]
            }
            .into_iter()
            .map(|n| serde_json::json!({"value":n})),
        );
        assert_eq!(json_rows(&collected), expected);
        let report = collected.execution.input.as_ref().unwrap();
        assert_eq!(report.columnar_projected_columns, 2);
        assert_eq!(
            report.columnar_row_groups_read,
            if unrestricted { 4 } else { 2 }
        );
        assert_eq!(
            report.columnar_rows_decoded,
            if unrestricted { 16 } else { 8 }
        );
        assert_eq!(report.replay_sources, 1);
        drop(collected);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_demand_empty_result_keeps_exact_schema_and_reads_no_data() {
    let fixture = ParquetFixture::new();
    let prepared = fixture.prepare(&fixture.scan(&["text"], Some(equal("n", 999))));
    let dtype = prepared.output_dtype().unwrap();
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&collected), [] as [serde_json::Value; 0]);
    assert_eq!(dtype, prepared.output_dtype().unwrap());
    assert_eq!(
        dtype.as_struct_fields_opt().unwrap().names(),
        &FieldNames::from(["text"])
    );
    let report = collected.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            report.columnar_row_groups_read,
            report.columnar_rows_decoded,
            report.columnar_data_read_bytes
        ),
        (0, 0, 0)
    );
    assert!(report.end_of_input_observed);
}

#[test]
fn parquet_demand_missing_statistics_retains_all_groups_and_residual_correctness() {
    let fixture = ParquetFixture::with_batch(ParquetFixture::batch(), EnabledStatistics::None);
    let prepared = fixture.prepare(&fixture.scan(&["text"], Some(equal("n", 5))));
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&collected),
        vec![serde_json::json!({"text":"row-5-λ"})]
    );
    assert_eq!(
        collected
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_row_groups_read,
        4
    );
}

#[test]
fn parquet_demand_unsigned_statistics_preserve_high_bit_and_nulls() {
    let values = [
        None,
        None,
        None,
        None,
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(1_u64 << 63),
        Some(u64::MAX - 2),
        Some(u64::MAX - 1),
        Some(u64::MAX),
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("u", DataType::UInt64, true)])),
        vec![Arc::new(UInt64Array::from_iter(values))],
    )
    .unwrap();
    let fixture = ParquetFixture::with_batch(batch, EnabledStatistics::Chunk);
    for (predicate, expected) in [
        (
            PredicateExpr::Compare {
                column: ColumnRef::new("u").unwrap(),
                op: ComparisonOp::Eq,
                value: StatValue::UInt64(u64::MAX),
            },
            vec![serde_json::json!({"u":u64::MAX})],
        ),
        (
            PredicateExpr::IsNull {
                column: ColumnRef::new("u").unwrap(),
            },
            vec![serde_json::json!({"u":null}); 4],
        ),
    ] {
        let prepared = fixture.prepare(&fixture.scan(&["u"], Some(predicate)));
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&collected), expected);
        assert_eq!(
            collected
                .execution
                .input
                .as_ref()
                .unwrap()
                .columnar_row_groups_read,
            1
        );
    }
}

#[test]
fn parquet_demand_skips_corrupt_undemanded_pages_but_requested_pages_fail() {
    for (group, column, good_value, bad_name, bad_value) in
        [(0, 0, 5, "n", 0), (1, 1, 5, "other", 5)]
    {
        let fixture = ParquetFixture::new();
        fixture.corrupt_chunk(group, column);
        let prepared = fixture.prepare(&fixture.scan(&["text"], Some(equal("n", good_value))));
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&collected),
            vec![serde_json::json!({"text":"row-5-λ"})]
        );
        let bad = fixture.prepare(&fixture.scan(&[bad_name], Some(equal("n", bad_value))));
        let initial = bad.snapshot().memory.reserved_bytes;
        assert!(bad.collect_jsonl(&CancellationToken::default()).is_err());
        assert_eq!(bad.snapshot().completed_executions, 0);
        assert_eq!(bad.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_demand_cancellation_consumer_error_and_pressure_release_readers() {
    let fixture = ParquetFixture::new();
    let prepared = fixture.prepare(&fixture.scan(&["n"], None));
    let initial = prepared.snapshot().memory.reserved_bytes;
    let cancellation = CancellationToken::default();
    let result = prepared.for_each_batch(&cancellation, |_, _| {
        cancellation.cancel();
        Ok(())
    });
    assert!(result.err().unwrap().to_string().contains("cancelled"));
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    let result = prepared.for_each_batch(&CancellationToken::default(), |_, _| {
        Err(failed("consumer stopped"))
    });
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("consumer stopped")
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    let pressure = prepared
        .session
        .memory()
        .reserve(prepared.snapshot().memory.limit_bytes - initial - 4096)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    drop(pressure);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        )
        .len(),
        16
    );
}

#[test]
fn parquet_demand_source_generation_checked_after_final_consumer() {
    let fixture = ParquetFixture::new();
    let prepared = fixture.prepare(&fixture.scan(&["n"], Some(equal("n", 5))));
    let initial = prepared.snapshot().memory.reserved_bytes;
    let mut replaced = false;
    let result = prepared.for_each_batch(&CancellationToken::default(), |_, _| {
        let replacement = fixture.0.join("replacement.parquet");
        fs::copy(fixture.path(), &replacement).unwrap();
        fs::rename(replacement, fixture.path()).unwrap();
        replaced = true;
        Ok(())
    });
    assert!(replaced && result.is_err());
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[test]
fn parquet_demand_rejects_source_output_alias_before_any_write() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let fixture = ParquetFixture::new();
    let prepared = fixture.prepare(&fixture.scan(&["n"], None));
    let before = fs::read(fixture.path()).unwrap();
    let alias = fixture.0.join("hard-link.parquet");
    fs::hard_link(fixture.path(), &alias).unwrap();
    for path in [fixture.path(), alias] {
        assert!(prepared.write(&path, Format::Parquet, true).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
    }
    assert_eq!(prepared.snapshot().completed_executions, 0);
}
