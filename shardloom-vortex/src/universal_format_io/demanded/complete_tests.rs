//! Complete local collection admission and stream lifecycle contracts.

use super::*;
use crate::{
    encode_flat_arrow_ipc_rows, encode_flat_avro_rows, encode_flat_orc_rows,
    encode_flat_parquet_rows, prepared_source_binding::local_preparation_binding_in_pool,
};
use arrow_array::{Array, Int64Array, LargeStringArray, StringArray, StringViewArray};
use shardloom_core::ScalarValue;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-complete-source-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn write(path: &Path, format: &str, columns: &[String], rows: &[Vec<(String, ScalarValue)>]) {
        let bytes = match format {
            "parquet" => encode_flat_parquet_rows(columns, rows),
            "arrow-ipc" => encode_flat_arrow_ipc_rows(columns, rows),
            "avro" => encode_flat_avro_rows(columns, rows),
            "orc" => encode_flat_orc_rows(columns, rows),
            _ => unreachable!(),
        }
        .unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    fn manifest(path: &Path, format: &str, files: &[&str], schema: &str, partitioning: &str) {
        fs::write(
            path,
            serde_json::json!({
                "version": 1,
                "format": format,
                "files": files,
                "schema": schema,
                "partitioning": partitioning,
            })
            .to_string(),
        )
        .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn int_rows(first: i64, second: i64) -> Vec<Vec<(String, ScalarValue)>> {
    [first, second]
        .into_iter()
        .map(|id| {
            vec![
                ("id".into(), ScalarValue::Int64(id)),
                ("value".into(), ScalarValue::Utf8(format!("v{id}"))),
            ]
        })
        .collect()
}

fn read_all(source: &Arc<LocalColumnarSource>, pool: &LiveMemoryPool) -> Vec<RecordBatch> {
    source
        .complete_stream(None, pool, &CancellationToken::default())
        .unwrap()
        .reader
        .map(|batch| batch.unwrap())
        .collect()
}

fn text(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    Some(
        match array.data_type() {
            DataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row),
            DataType::Utf8View => array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(row),
            DataType::LargeUtf8 => array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row),
            other => panic!("unexpected text type {other:?}"),
        }
        .to_owned(),
    )
}

#[test]
fn complete_reader_reads_every_field_and_row_for_each_binary_format() {
    let fixture = Fixture::new();
    let columns = vec!["id".to_string(), "value".to_string()];
    for (format, extension) in [
        ("parquet", "parquet"),
        ("arrow-ipc", "arrow"),
        ("avro", "avro"),
        ("orc", "orc"),
    ] {
        let directory = fixture.0.join(format!("{format}/nested"));
        Fixture::write(
            &directory.join(format!("a.{extension}")),
            format,
            &columns,
            &int_rows(1, 2),
        );
        Fixture::write(
            &directory.join(format!("b.{extension}")),
            format,
            &columns,
            &int_rows(3, 4),
        );
        let pool = LiveMemoryPool::new(64 << 20).unwrap();
        let source = LocalColumnarSource::admit_complete(
            &fixture.0.join(format),
            format,
            false,
            &pool,
            &CancellationToken::default(),
        )
        .unwrap();
        let batches = read_all(&source, &pool);
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            4,
            "{format}"
        );
        assert_eq!(
            batches[0]
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "value"],
            "{format}"
        );
        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, [1, 2, 3, 4], "{format}");
        let values = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(move |row| text(batch.column(1).as_ref(), row).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(values, ["v1", "v2", "v3", "v4"], "{format}");
        drop((batches, source));
        assert_eq!(pool.snapshot().reserved_bytes, 0, "{format}");
    }
}

#[test]
fn complete_manifest_union_by_name_aligns_nulls_in_sorted_file_order() {
    let fixture = Fixture::new();
    let format = "parquet";
    Fixture::write(
        &fixture.0.join("a.parquet"),
        format,
        &["id".into(), "a".into()],
        &[vec![
            ("id".into(), ScalarValue::Int64(1)),
            ("a".into(), ScalarValue::Utf8("left".into())),
        ]],
    );
    Fixture::write(
        &fixture.0.join("b.parquet"),
        format,
        &["id".into(), "b".into()],
        &[vec![
            ("id".into(), ScalarValue::Int64(2)),
            ("b".into(), ScalarValue::Utf8("right".into())),
        ]],
    );
    let manifest = fixture.0.join("data.manifest");
    Fixture::manifest(
        &manifest,
        format,
        &["b.parquet", "a.parquet"],
        "union_by_name",
        "none",
    );
    let pool = LiveMemoryPool::new(64 << 20).unwrap();
    let source = LocalColumnarSource::admit_complete(
        &manifest,
        format,
        false,
        &pool,
        &CancellationToken::default(),
    )
    .unwrap();
    let batches = read_all(&source, &pool);
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let schemas = batches.iter().map(RecordBatch::schema).collect::<Vec<_>>();
    assert!(schemas.iter().all(|schema| {
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            == ["id", "a", "b"]
    }));
    let ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, [1, 2]);
    let a = batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(move |row| text(batch.column(1).as_ref(), row)))
        .collect::<Vec<_>>();
    let b = batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(move |row| text(batch.column(2).as_ref(), row)))
        .collect::<Vec<_>>();
    assert_eq!(a, [Some("left".to_owned()), None]);
    assert_eq!(b, [None, Some("right".to_owned())]);
}

#[test]
fn complete_hive_manifest_materializes_partition_values_as_utf8() {
    let fixture = Fixture::new();
    Fixture::write(
        &fixture.0.join("region=north/a.parquet"),
        "parquet",
        &["id".into()],
        &[vec![("id".into(), ScalarValue::Int64(7))]],
    );
    let manifest = fixture.0.join("data.manifest");
    Fixture::manifest(
        &manifest,
        "parquet",
        &["region=north/a.parquet"],
        "strict",
        "hive",
    );
    let pool = LiveMemoryPool::new(64 << 20).unwrap();
    let source = LocalColumnarSource::admit_complete(
        &manifest,
        "parquet",
        false,
        &pool,
        &CancellationToken::default(),
    )
    .unwrap();
    let batches = read_all(&source, &pool);
    assert_eq!(
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "region"]
    );
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "north"
    );
}

#[test]
fn complete_stream_rejects_truncation_owner_and_cancellation_and_releases_credits() {
    let fixture = Fixture::new();
    let path = fixture.0.join("source.parquet");
    Fixture::write(
        &path,
        "parquet",
        &["id".into(), "value".into()],
        &int_rows(1, 2),
    );
    let pool = LiveMemoryPool::new(64 << 20).unwrap();
    let source = LocalColumnarSource::admit_complete(
        &path,
        "parquet",
        false,
        &pool,
        &CancellationToken::default(),
    )
    .unwrap();
    let other_pool = LiveMemoryPool::new(64 << 20).unwrap();
    assert!(
        source
            .complete_stream(None, &other_pool, &CancellationToken::default())
            .is_err()
    );

    let mut truncated = source
        .complete_stream(Some(1), &pool, &CancellationToken::default())
        .unwrap()
        .reader;
    assert!(truncated.next().unwrap().is_err());
    assert!(truncated.next().is_none());
    drop(truncated);

    let cancellation = CancellationToken::default();
    let mut cancelled = source
        .complete_stream(None, &pool, &cancellation)
        .unwrap()
        .reader;
    assert!(cancelled.next().unwrap().is_ok());
    cancellation.cancel();
    assert!(cancelled.next().unwrap().is_err());
    assert!(cancelled.next().is_none());
    drop(cancelled);
    drop(source);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn complete_source_rejects_changed_membership_and_conflicting_destinations() {
    let fixture = Fixture::new();
    let directory = fixture.0.join("parts");
    Fixture::write(
        &directory.join("a.parquet"),
        "parquet",
        &["id".into()],
        &[vec![("id".into(), ScalarValue::Int64(1))]],
    );
    let pool = LiveMemoryPool::new(64 << 20).unwrap();
    let source = LocalColumnarSource::admit_complete(
        &directory,
        "parquet",
        false,
        &pool,
        &CancellationToken::default(),
    )
    .unwrap();
    assert!(
        source
            .validate_output_destination(&directory.join("b.parquet"))
            .is_err()
    );
    assert!(
        source
            .validate_output_destination(&directory.join("a.parquet"))
            .is_err()
    );
    let mut reader = source
        .complete_stream(None, &pool, &CancellationToken::default())
        .unwrap()
        .reader;
    for batch in reader.by_ref() {
        batch.unwrap();
    }
    Fixture::write(
        &directory.join("b.parquet"),
        "parquet",
        &["id".into()],
        &[vec![("id".into(), ScalarValue::Int64(2))]],
    );
    assert!(
        source
            .validate_generation_controlled(&CancellationToken::default())
            .is_err()
    );
    drop(reader);
    drop(source);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn preparation_binding_uses_the_same_complete_collection_contract() {
    let fixture = Fixture::new();
    let directory = fixture.0.join("parts");
    Fixture::write(
        &directory.join("a.parquet"),
        "parquet",
        &["id".into()],
        &[vec![("id".into(), ScalarValue::Int64(1))]],
    );
    let pool = LiveMemoryPool::new(64 << 20).unwrap();
    let token = CancellationToken::default();
    let binding = local_preparation_binding_in_pool(
        &directory,
        "parquet",
        "metadata_only",
        None,
        &token,
        shardloom_core::FloatPolicy::Strict,
        &pool,
    )
    .unwrap();
    Fixture::write(
        &directory.join("b.parquet"),
        "parquet",
        &["id".into()],
        &[vec![("id".into(), ScalarValue::Int64(2))]],
    );
    let changed = local_preparation_binding_in_pool(
        &directory,
        "parquet",
        "metadata_only",
        None,
        &token,
        shardloom_core::FloatPolicy::Strict,
        &pool,
    )
    .unwrap();
    assert_ne!(binding, changed);
}
