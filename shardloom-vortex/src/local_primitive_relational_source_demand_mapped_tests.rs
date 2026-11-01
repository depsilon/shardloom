//! Schema wrappers preserve held-source facts and actual payload exclusion.

use super::count_tests::{assert_metadata, count};
use super::*;
use crate::relational_query::VortexRelationalLimit;
use serde_json::json;
use std::path::Path;

fn write(path: &Path, start: i64, reordered: bool, rows: usize, stats: EnabledStatistics) {
    let fields = if reordered {
        vec![
            Field::new("only_right", DataType::Int64, false),
            Field::new("n", DataType::Int64, false),
        ]
    } else {
        vec![
            Field::new("n", DataType::Int64, false),
            Field::new("other", DataType::Int64, false),
        ]
    };
    let n: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from_iter_values(
        (0..rows).map(|i| start + i64::try_from(i).unwrap()),
    ));
    let payload: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from_iter_values(
        (0..rows).map(|i| start + i64::try_from(i).unwrap() + 1000),
    ));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        if reordered {
            vec![payload, n]
        } else {
            vec![n, payload]
        },
    )
    .unwrap();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(path).unwrap(),
        batch.schema(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(4))
                .set_statistics_enabled(stats)
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn fixture(mode: &str, rows: usize, stats: EnabledStatistics) -> (ParquetFixture, PathBuf) {
    let fixture = ParquetFixture::new();
    let hive = mode != "union";
    let names = if hive {
        vec![
            "country=US/a.parquet",
            "country=CA/b.parquet",
            "country=__HIVE_DEFAULT_PARTITION__/c.parquet",
        ]
    } else {
        vec!["a.parquet", "b.parquet"]
    };
    for (i, name) in names.iter().enumerate() {
        write(
            &fixture.0.join(name),
            i64::try_from(i).unwrap() * 100,
            mode != "hive" && i == 1,
            rows,
            stats,
        );
    }
    let path = fixture.0.join("dataset.manifest");
    fs::write(
        &path,
        json!({"version":1,"format":"parquet","files":names.into_iter().rev().collect::<Vec<_>>(),
        "schema":if mode == "hive" {"strict"} else {"union_by_name"},
        "partitioning":if hive {"hive"} else {"none"}})
        .to_string(),
    )
    .unwrap();
    (fixture, path)
}

fn scan(path: &Path, columns: &[&str], predicate: Option<PredicateExpr>) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(path.display().to_string()).unwrap(),
        projection: ProjectionRequest::Columns(
            columns
                .iter()
                .map(|name| ColumnRef::new(*name).unwrap())
                .collect(),
        ),
        predicate,
    })
}

fn prepare(path: &Path, plan: VortexRelationalPlan) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |preparation| {
        let source = LocalColumnarSource::admit(
            path,
            "parquet",
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        preparation.register_local_source(DatasetUri::new(path.display().to_string())?, source)?;
        Ok(plan)
    })
    .unwrap()
}

fn country(value: &str) -> PredicateExpr {
    PredicateExpr::Compare {
        column: ColumnRef::new("country").unwrap(),
        op: ComparisonOp::Eq,
        value: StatValue::Utf8(value.into()),
    }
}

fn null(column: &str, is_null: bool) -> PredicateExpr {
    let column = ColumnRef::new(column).unwrap();
    if is_null {
        PredicateExpr::IsNull { column }
    } else {
        PredicateExpr::IsNotNull { column }
    }
}

fn assert_work(result: &CollectedVortexRelational, groups: u64, decoded: u64) {
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (input.columnar_row_groups_read, input.columnar_rows_decoded),
        (groups, decoded)
    );
    assert_eq!(input.columnar_data_read_bytes > 0, groups > 0);
    assert_eq!(input.columnar_data_read_requests > 0, groups > 0);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
}

#[test]
fn parquet_mapped_count_preserves_exact_empty_and_nonempty_totals_without_statistics() {
    for mode in ["union", "hive", "both"] {
        for rows in [0, 16] {
            let (_fixture, path) = fixture(mode, rows, EnabledStatistics::None);
            let prepared = prepare(
                &path,
                count(project(scan(&path, &["n"], None), &[("n", "renamed")])),
            );
            let initial = prepared.snapshot().memory.reserved_bytes;
            let total = rows as u64 * if mode == "union" { 2 } else { 3 };
            for _ in 0..2 {
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                assert_eq!(json_rows(&result), vec![json!({"rows": total})]);
                assert_metadata(&result, total);
                drop(result);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
            }
        }
    }
}

#[test]
fn parquet_mapped_pruning_resolves_reordered_and_typed_null_fields_per_file() {
    let (_fixture, path) = fixture("union", 16, EnabledStatistics::Chunk);
    for (predicate, expected, groups, decoded) in [
        (
            equal("n", 105),
            vec![json!({"n":105,"other":null,"only_right":1105})],
            1,
            4,
        ),
        (
            equal("only_right", 1105),
            vec![json!({"n":105,"other":null,"only_right":1105})],
            1,
            4,
        ),
        (
            null("other", true),
            (100..116)
                .map(|n| json!({"n":n,"other":null,"only_right":n+1000}))
                .collect(),
            4,
            16,
        ),
        (
            null("only_right", true),
            (0..16)
                .map(|n| json!({"n":n,"other":n+1000,"only_right":null}))
                .collect(),
            4,
            16,
        ),
        (
            null("other", false),
            (0..16)
                .map(|n| json!({"n":n,"other":n+1000,"only_right":null}))
                .collect(),
            4,
            16,
        ),
    ] {
        let prepared = prepare(
            &path,
            scan(&path, &["n", "other", "only_right"], Some(predicate)),
        );
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        assert_work(&result, groups, decoded);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_mapped_hive_constants_exclude_payload_before_open_without_statistics() {
    for mode in ["hive", "both"] {
        for stats in [EnabledStatistics::None, EnabledStatistics::Chunk] {
            let (_fixture, path) = fixture(mode, 16, stats);
            for (predicate, range, value) in [
                (country("US"), 0..16, Some("US")),
                (country("CA"), 100..116, Some("CA")),
                (null("country", true), 200..216, None),
                (country("absent"), 0..0, None),
            ] {
                let prepared = prepare(&path, scan(&path, &["n", "country"], Some(predicate)));
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                let expected = range
                    .map(|n| json!({"n":n,"country":value}))
                    .collect::<Vec<_>>();
                assert_eq!(json_rows(&result), expected);
                assert_work(
                    &result,
                    if expected.is_empty() { 0 } else { 4 },
                    expected.len() as u64,
                );
            }
            let prepared = prepare(
                &path,
                scan(
                    &path,
                    &["n"],
                    Some(PredicateExpr::And(vec![country("CA"), equal("n", 105)])),
                ),
            );
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), vec![json!({"n":105})]);
            let selective = stats == EnabledStatistics::Chunk;
            assert_work(
                &result,
                if selective { 1 } else { 4 },
                if selective { 4 } else { 16 },
            );
        }
    }
}

#[test]
fn parquet_mapped_shared_demand_keeps_regions_needed_by_every_consumer() {
    let (_fixture, path) = fixture("both", 16, EnabledStatistics::Chunk);
    for unfiltered in [false, true] {
        let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: scan(&path, &["n"], Some(country("US"))),
            right: scan(
                &path,
                &["n"],
                if unfiltered {
                    None
                } else {
                    Some(country("CA"))
                },
            ),
            kind: VortexRelationalSetKind::UnionAll,
        }));
        let prepared = prepare(&path, plan);
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let mut expected = (0..16).map(|n| json!({"n":n})).collect::<Vec<_>>();
        expected.extend((100..116).map(|n| json!({"n":n})));
        if unfiltered {
            expected.extend((0..16).chain(200..216).map(|n| json!({"n":n})));
        }
        assert_eq!(json_rows(&result), expected);
        assert_work(
            &result,
            if unfiltered { 12 } else { 8 },
            if unfiltered { 48 } else { 32 },
        );
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .metadata_count_sources,
            0
        );
    }
}

#[test]
fn parquet_mapped_prefix_keeps_file_order_and_exact_declared_end() {
    let (_fixture, path) = fixture("both", 16, EnabledStatistics::Chunk);
    let prepared = prepare(
        &path,
        VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: scan(&path, &["n"], None),
            offset: 14,
            count: 5,
        })),
    );
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        [114, 115, 0, 1, 2].map(|n| json!({"n":n}))
    );
    assert_work(&result, 5, 19);
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_requested_prefix_rows,
        19
    );
}

fn corrupt(path: &Path, group: usize, column: usize) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap()).unwrap();
    let (start, length) = builder
        .metadata()
        .row_group(group)
        .column(column)
        .byte_range();
    drop(builder);
    let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(start)).unwrap();
    file.write_all(&vec![0xff; usize::try_from(length).unwrap()])
        .unwrap();
    file.sync_all().unwrap();
}

#[test]
fn parquet_mapped_count_and_exclusion_do_not_validate_undemanded_corrupt_pages() {
    let (fixture, path) = fixture("union", 16, EnabledStatistics::Chunk);
    corrupt(&fixture.0.join("a.parquet"), 0, 0);
    corrupt(&fixture.0.join("b.parquet"), 0, 1);
    let prepared = prepare(&path, count(scan(&path, &["n"], None)));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![json!({"rows":32})]);
    assert_metadata(&result, 32);
    let prepared = prepare(&path, scan(&path, &["n"], Some(equal("n", 105))));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![json!({"n":105})]);
    assert_work(&result, 1, 4);
    let prepared = prepare(&path, scan(&path, &["n"], None));
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[test]
fn parquet_mapped_metadata_and_payload_keep_generation_cancellation_and_cleanup() {
    for metadata_only in [false, true] {
        for cancel in [false, true] {
            let (_fixture, path) = fixture("both", 16, EnabledStatistics::Chunk);
            let plan = scan(&path, &["n"], Some(country("US")));
            let prepared = prepare(
                &path,
                if metadata_only {
                    count(scan(&path, &["n"], None))
                } else {
                    plan
                },
            );
            let initial = prepared.snapshot().memory.reserved_bytes;
            let mut calls = 0;
            let result = prepared.for_each_batch(&CancellationToken::default(), |_, context| {
                calls += 1;
                if cancel {
                    context.cancellation().cancel();
                } else if calls == 1 {
                    let bytes = fs::read(&path).unwrap();
                    fs::write(&path, [bytes.as_slice(), b" "].concat()).unwrap();
                }
                Ok(())
            });
            let error = result
                .err()
                .expect("invalidated execution cannot complete")
                .to_string();
            assert!(
                error.contains(if cancel {
                    "cancelled"
                } else {
                    "source changed"
                }),
                "{error}"
            );
            // Cancellation stops before another callback. Collection identity
            // also has a mandatory completion barrier; it need not be probed
            // after each batch from unchanged held payload files.
            assert_eq!(calls, if cancel || metadata_only { 1 } else { 4 });
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
            if !cancel {
                assert!(
                    prepared
                        .collect_jsonl(&CancellationToken::default())
                        .is_err()
                );
            }
        }
    }
}
