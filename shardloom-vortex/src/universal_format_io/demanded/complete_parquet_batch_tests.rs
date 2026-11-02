//! Bulk preparation must not inherit the small query decoder's row ceiling.

use super::*;
use arrow_array::{Array, StringArray};

const ROWS: usize = 9 * 65_536 + 17;

fn bulk(fixture: &Fixture) -> PathBuf {
    let path = fixture.0.join("bulk.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                0..i64::try_from(ROWS).unwrap(),
            )),
            Arc::new(StringArray::from_iter((0..ROWS).map(|row| {
                (!row.is_multiple_of(7)).then(|| format!("row-{row}-λ"))
            }))),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(&path).unwrap(),
        schema,
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(3 * 65_536 + 17))
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path
}

fn check_values(batch: &RecordBatch, first: usize) {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let labels = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for index in 0..batch.num_rows() {
        let row = first + index;
        assert_eq!(ids.value(index), i64::try_from(row).unwrap());
        assert_eq!(labels.is_null(index), row.is_multiple_of(7));
        if !row.is_multiple_of(7) {
            assert_eq!(labels.value(index), format!("row-{row}-λ"));
        }
    }
}

#[test]
fn complete_parquet_bulk_geometry_preserves_values_order_and_arrow_export_boundary() {
    let fixture = Fixture::new();
    let input = bulk(&fixture);
    for parallelism in [1, 4] {
        let memory = LiveMemoryPool::new(256 << 20).unwrap();
        let token = CancellationToken::default();
        let source =
            LocalColumnarSource::admit_complete(&input, "parquet", false, &memory, &token).unwrap();
        let weak = Arc::downgrade(&source);
        let ingest = source
            .complete_ingest_stream_with_parallelism(None, parallelism, &memory, &token)
            .unwrap()
            .with_prefetch(parallelism, &token)
            .unwrap();
        let (metadata, _, mut reader) = ingest.into_parts();
        let drivers = metadata
            .ingest_runtime
            .as_ref()
            .unwrap()
            .start_drivers()
            .unwrap();
        let mut rows = 0;
        let mut largest = 0;
        let mut batches = 0;
        let mut retained = None;
        for batch in reader.by_ref() {
            let batch = batch.unwrap();
            check_values(&batch, rows);
            rows += batch.num_rows();
            largest = largest.max(batch.num_rows());
            batches += 1;
            retained = Some(batch);
        }
        assert_eq!(rows, ROWS);
        assert_eq!(
            largest, 65_536,
            "native bulk source inherited query-sized batches"
        );
        assert_eq!(batches, 11);
        assert_eq!(metadata.source_stream_batch_size, 65_536);
        drop(reader);
        drop(drivers);
        drop(metadata);
        // Ordinary public Arrow export keeps its existing bounded geometry.
        let mut exported = source.complete_stream(None, &memory, &token).unwrap();
        let first = exported.reader.next().unwrap().unwrap();
        assert_eq!(first.num_rows(), 8192);
        check_values(&first, 0);
        drop(first);
        drop(exported);
        drop(source);
        assert!(weak.upgrade().is_some());
        drop(retained);
        assert!(weak.upgrade().is_none());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
}

#[test]
fn complete_parquet_bulk_decoder_shrinks_and_regrows_under_shared_pressure() {
    let fixture = Fixture::new();
    let input = bulk(&fixture);
    let memory = LiveMemoryPool::new(128 << 20).unwrap();
    let token = CancellationToken::default();
    let source =
        LocalColumnarSource::admit_complete(&input, "parquet", false, &memory, &token).unwrap();
    let ingest = source
        .complete_ingest_stream(None, &memory, &token)
        .unwrap();
    let (metadata, _, mut reader) = ingest.into_parts();
    let first = reader.next().unwrap().unwrap();
    check_values(&first, 0);
    assert_eq!(first.num_rows(), 65_536);
    let mut rows = first.num_rows();
    // Keep the current row group's workspace admitted before pressure. The held
    // first output remains charged while later physical decode work shrinks.
    let next = reader.next().unwrap().unwrap();
    check_values(&next, rows);
    rows += next.num_rows();
    drop(next);
    let schema = first.schema();
    let cost =
        crate::universal_format_io::demanded::batch_work::cost(&schema, &schema, &[]).unwrap();
    let headroom = cost.fixed_bytes + cost.bytes_per_unit * 2048;
    let pressure = memory
        .reserve(memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes - headroom)
        .unwrap();
    let small = reader.next().unwrap().unwrap();
    check_values(&small, rows);
    assert!((1..=8192).contains(&small.num_rows()));
    rows += small.num_rows();
    drop(small);
    drop(pressure);
    let mut largest_after_release = 0;
    for batch in reader.by_ref() {
        let batch = batch.unwrap();
        check_values(&batch, rows);
        rows += batch.num_rows();
        largest_after_release = largest_after_release.max(batch.num_rows());
    }
    assert_eq!(rows, ROWS);
    assert!(largest_after_release > 8192);
    check_values(&first, 0);
    drop(reader);
    drop(metadata);
    drop(source);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(first);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}
