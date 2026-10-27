use super::*;
use crate::relational_query::VortexRelationalLimit;
use serde_json::json;
use std::path::Path;

fn scan(path: &Path, predicate: Option<PredicateExpr>) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(path.display().to_string()).unwrap(),
        projection: ProjectionRequest::Columns(vec![ColumnRef::new("n").unwrap()]),
        predicate,
    })
}

fn prepare(path: &Path, plan: VortexRelationalPlan, fingerprint: bool) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |preparation| {
        let source = LocalColumnarSource::admit_with_content_fingerprint(
            path,
            "parquet",
            fingerprint,
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        preparation.register_local_source(DatasetUri::new(path.display().to_string())?, source)?;
        Ok(plan)
    })
    .unwrap()
}

fn directory() -> ParquetFixture {
    let fixture = ParquetFixture::new();
    let batch = ParquetFixture::batch();
    let first = RecordBatch::try_new(
        batch.schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(100..116)),
            batch.column(1).clone(),
            batch.column(2).clone(),
        ],
    )
    .unwrap();
    let first = ParquetFixture::with_batch(first, EnabledStatistics::Chunk);
    fs::copy(first.path(), fixture.0.join("a.PARQUET")).unwrap();
    fs::write(fixture.0.join("ignored.txt"), b"not parquet").unwrap();
    fs::create_dir(fixture.0.join("nested")).unwrap();
    fs::write(fixture.0.join("nested/ignored.parquet"), b"not admitted").unwrap();
    fixture
}

#[test]
fn parquet_demand_directory_preserves_order_global_ranges_and_per_file_pruning() {
    let fixture = directory();
    let plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: scan(&fixture.0, None),
        offset: 14,
        count: 5,
    }));
    let prepared = prepare(&fixture.0, plan, false);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        [114, 115, 0, 1, 2]
            .into_iter()
            .map(|n| json!({"n":n}))
            .collect::<Vec<_>>()
    );
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            input.columnar_files,
            input.columnar_row_groups_total,
            input.columnar_row_groups_read,
            input.columnar_rows_decoded
        ),
        (2, 8, 5, 19)
    );
    assert_eq!(
        (
            input.columnar_prefix_sources,
            input.columnar_requested_prefix_rows
        ),
        (1, 19)
    );
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    let prepared = prepare(&fixture.0, scan(&fixture.0, Some(equal("n", 5))), false);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![json!({"n":5})]);
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            input.columnar_files,
            input.columnar_row_groups_read,
            input.columnar_rows_decoded
        ),
        (2, 1, 4)
    );
}

#[test]
fn parquet_demand_directory_membership_changes_poison_completion_and_reuse() {
    for change in ["add", "remove", "replace", "rename"] {
        let fixture = directory();
        let prepared = prepare(&fixture.0, scan(&fixture.0, None), false);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let mut changed = false;
        let result = prepared.for_each_batch(&CancellationToken::default(), |_, _| {
            if !changed {
                let first = fixture.0.join("a.PARQUET");
                match change {
                    "add" => {
                        fs::copy(fixture.path(), fixture.0.join("new.parquet")).unwrap();
                    }
                    "remove" => {
                        fs::remove_file(first).unwrap();
                    }
                    "replace" => {
                        fs::copy(fixture.path(), fixture.0.join("replacement.tmp")).unwrap();
                        fs::rename(fixture.0.join("replacement.tmp"), first).unwrap();
                    }
                    "rename" => {
                        fs::rename(first, fixture.0.join("renamed.parquet")).unwrap();
                    }
                    _ => unreachable!(),
                }
                changed = true;
            }
            Ok(())
        });
        assert!(changed && result.is_err(), "{change}");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        if change == "add" {
            fs::remove_file(fixture.0.join("new.parquet")).unwrap();
        }
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
    }
}

#[test]
fn parquet_demand_directory_ignores_nonmembers_and_rejects_output_members_before_writing() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let fixture = directory();
    let prepared = prepare(&fixture.0, scan(&fixture.0, None), false);
    let result = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| {
            fs::write(fixture.0.join("ignored.txt"), b"changed but not a source").unwrap();
            Ok(())
        })
        .unwrap();
    assert_eq!(result.output_rows, 32);
    let destination = fixture.0.join("new.parquet");
    assert!(
        prepared
            .write(&destination, Format::Parquet, false)
            .is_err()
    );
    assert!(!destination.exists());
    prepared
        .write(&fixture.0.join("native.vortex"), Format::Vortex, false)
        .unwrap();
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        )
        .len(),
        32
    );
}

#[test]
fn parquet_demand_directory_rejects_incompatible_and_empty_inventories_before_data() {
    let fixture = directory();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["different"]))],
    )
    .unwrap();
    let incompatible = ParquetFixture::with_batch(batch, EnabledStatistics::Chunk);
    fs::copy(incompatible.path(), fixture.0.join("incompatible.parquet")).unwrap();
    let memory = shardloom_exec::live_memory::LiveMemoryPool::new(32 << 20).unwrap();
    let error = LocalColumnarSource::admit(
        &fixture.0,
        "parquet",
        &memory,
        &CancellationToken::default(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("incompatible schemas"));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let empty = fixture.0.join("empty");
    fs::create_dir(&empty).unwrap();
    assert!(
        LocalColumnarSource::admit(&empty, "parquet", &memory, &CancellationToken::default())
            .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn parquet_demand_content_fingerprint_reads_all_bytes_without_decoding_undemanded_data() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(3, 1);
    let source = fs::read(fixture.path()).unwrap();
    let expected = source.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    let prepared = prepare(
        &fixture.path(),
        scan(&fixture.path(), Some(equal("n", 5))),
        true,
    );
    let initial = prepared.snapshot().memory.reserved_bytes;
    for _ in 0..2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![json!({"n":5})]);
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.columnar_fingerprint_read_bytes, source.len() as u64);
        assert_eq!(
            input.columnar_content_fingerprints,
            vec![format!("fnv64:{expected:016x}")]
        );
        assert_eq!(input.columnar_row_groups_read, 1);
        assert!(input.columnar_data_read_bytes < input.columnar_fingerprint_read_bytes);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}
