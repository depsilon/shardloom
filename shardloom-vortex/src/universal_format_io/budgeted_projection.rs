//! Scoped compatibility reads from an already admitted encoded payload.
//!
//! Decoder construction/decompression temporaries are upstream-owned and are not
//! allocator-limited here. Each returned batch is admitted before caller handoff;
//! keeping its `Budgeted` owner keeps the same source pool's credits live.

use std::{io::Cursor, path::Path};

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use bytes::Bytes;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{Budgeted, LiveMemoryPool};

use super::{
    SCOPED_COMPAT_RECORD_BATCH_ROWS, projection_header, projection_indices_for_header,
    source_schema_header,
};

/// Schema and counts from visiting a compatibility source without collecting it.
#[derive(Debug)]
pub struct ColumnarSourceVisit {
    /// Full source column names, before projection.
    pub header: Vec<String>,
    /// Requested columns present in the source schema.
    pub materialized_columns: Vec<String>,
    /// Columns decoded by the provider, including Avro's empty-projection column.
    pub reader_projection_columns: Vec<String>,
    /// Rows successfully passed to the visitor.
    pub row_count: usize,
    /// Batches successfully passed to the visitor.
    pub record_batch_count: usize,
}

struct OwnedSourceBytes(Budgeted<Vec<u8>>);

impl AsRef<[u8]> for OwnedSourceBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.value().as_slice()
    }
}

fn source_bytes(source: Budgeted<Vec<u8>>) -> Result<(Bytes, LiveMemoryPool)> {
    if source.reserved_bytes() < source.value().capacity() as u64 {
        return Err(ShardLoomError::InvalidOperation(
            "compatibility source bytes are not fully reserved; fallback execution was not attempted"
                .to_string(),
        ));
    }
    let (value, lease) = source.into_parts();
    let pool = lease.pool().clone();
    let bytes = Bytes::from_owner(OwnedSourceBytes(Budgeted::new(value, lease)));
    Ok((bytes, pool))
}

fn batch_rows(pool: &LiveMemoryPool, columns: usize, max_rows: usize) -> usize {
    let snapshot = pool.snapshot();
    let available = snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes);
    // A shaping estimate, not a bound on variable-width values or provider
    // decode allocations. Admission checks the actual delivered batch footprint.
    let row_bytes = columns.max(1).saturating_mul(64) as u64;
    let rows = usize::try_from(available / row_bytes).unwrap_or(usize::MAX);
    rows.clamp(1, max_rows.clamp(1, SCOPED_COMPAT_RECORD_BATCH_ROWS))
}

fn reader_error(path: &Path, format: &str, error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "failed to read admitted local {format} source '{}': {error}; no fallback execution was attempted",
        path.display()
    ))
}

/// Visit projected Avro batches from an encoded buffer owned by the caller's pool.
///
/// `path` is diagnostic context only; this function never opens it. The visitor
/// must retain the complete batch owner whenever it retains its arrays. Provider
/// decode temporaries and returned schema metadata are outside this byte scope.
///
/// # Errors
/// Rejects under-reserved input, invalid Avro, excessive rows, batch admission
/// failures and visitor errors. No later batch is decoded after visitor failure.
pub fn visit_budgeted_avro_source_with_projection(
    source: Budgeted<Vec<u8>>,
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
    visitor: impl FnMut(Budgeted<RecordBatch>) -> Result<()>,
) -> Result<ColumnarSourceVisit> {
    let (bytes, pool) = source_bytes(source)?;
    let full_reader = arrow_avro::reader::ReaderBuilder::new()
        .build(Cursor::new(bytes.clone()))
        .map_err(|error| reader_error(path, "Avro", error))?;
    let header = source_schema_header(path, "Avro", full_reader.schema().as_ref())?;
    let projection = projection_indices_for_header(&header, required_columns);
    let materialized_columns = projection_header(&header, &projection);
    let projection = if projection.is_empty() {
        vec![0]
    } else {
        projection
    };
    let reader_projection_columns = projection_header(&header, &projection);
    drop(full_reader);
    let reader = arrow_avro::reader::ReaderBuilder::new()
        .with_batch_size(batch_rows(&pool, projection.len(), max_rows))
        .with_projection(projection)
        .build(Cursor::new(bytes))
        .map_err(|error| reader_error(path, "Avro", error))?;
    visit_batches(
        reader,
        &pool,
        path,
        "Avro",
        max_rows,
        ColumnarSourceVisit {
            header,
            materialized_columns,
            reader_projection_columns,
            row_count: 0,
            record_batch_count: 0,
        },
        visitor,
    )
}

/// Visit projected Parquet batches using the encoded payload's existing pool.
///
/// `path` is diagnostic context only. Parquet byte slices retain the original
/// encoded owner. The visitor must retain the complete batch owner with its
/// arrays. Provider decode temporaries and schema metadata are outside this scope.
///
/// # Errors
/// Rejects under-reserved input, invalid Parquet, excessive rows, batch admission
/// failures and visitor errors. No later batch is decoded after visitor failure.
pub fn visit_budgeted_parquet_source_with_projection(
    source: Budgeted<Vec<u8>>,
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
    visitor: impl FnMut(Budgeted<RecordBatch>) -> Result<()>,
) -> Result<ColumnarSourceVisit> {
    let (bytes, pool) = source_bytes(source)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|error| reader_error(path, "Parquet", error))?;
    let header = source_schema_header(path, "Parquet", builder.schema().as_ref())?;
    let indices = projection_indices_for_header(&header, required_columns);
    let materialized_columns = projection_header(&header, &indices);
    let projection =
        parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
    let reader = builder
        .with_batch_size(batch_rows(&pool, indices.len(), max_rows))
        .with_projection(projection)
        .build()
        .map_err(|error| reader_error(path, "Parquet", error))?;
    visit_batches(
        reader,
        &pool,
        path,
        "Parquet",
        max_rows,
        ColumnarSourceVisit {
            header,
            reader_projection_columns: materialized_columns.clone(),
            materialized_columns,
            row_count: 0,
            record_batch_count: 0,
        },
        visitor,
    )
}

fn visit_batches(
    reader: impl Iterator<Item = std::result::Result<RecordBatch, ArrowError>>,
    pool: &LiveMemoryPool,
    path: &Path,
    format: &str,
    max_rows: usize,
    mut summary: ColumnarSourceVisit,
    mut visitor: impl FnMut(Budgeted<RecordBatch>) -> Result<()>,
) -> Result<ColumnarSourceVisit> {
    for batch in reader {
        let batch = batch.map_err(|error| reader_error(path, format, error))?;
        if batch.num_columns() != summary.reader_projection_columns.len() {
            return Err(reader_error(path, format, "projected column count changed"));
        }
        summary.row_count = summary
            .row_count
            .checked_add(batch.num_rows())
            .filter(|rows| *rows <= max_rows)
            .ok_or_else(|| {
                reader_error(
                    path,
                    format,
                    format!("exceeds the configured local source row budget of {max_rows}"),
                )
            })?;
        // This is the handoff boundary: upstream construction is transient and
        // excluded; retaining or observing a delivered batch requires credits.
        let lease = pool.reserve(batch.get_array_memory_size() as u64)?;
        visitor(Budgeted::new(batch, lease))?;
        summary.record_batch_count += 1;
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[derive(Clone, Copy, Debug)]
    enum Format {
        Avro,
        Parquet,
    }

    fn fixture(format: Format, rows: usize) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|v| i64::try_from(v).unwrap()),
                )),
                Arc::new(StringArray::from_iter((0..rows).map(|v| {
                    if v % 3 == 0 {
                        None
                    } else {
                        Some("retained-payload")
                    }
                }))),
            ],
        )
        .unwrap();
        match format {
            Format::Avro => {
                let mut writer =
                    arrow_avro::writer::AvroWriter::new(Vec::new(), schema.as_ref().clone())
                        .unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
                writer.into_inner()
            }
            Format::Parquet => {
                let mut bytes = Vec::new();
                let mut writer =
                    parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                bytes
            }
        }
    }

    fn admitted(bytes: &[u8], pool: &LiveMemoryPool) -> Budgeted<Vec<u8>> {
        let lease = pool.reserve(bytes.len() as u64).unwrap();
        Budgeted::new(bytes.to_vec(), lease)
    }

    fn visit(
        format: Format,
        source: Budgeted<Vec<u8>>,
        rows: usize,
        projection: &[String],
        visitor: impl FnMut(Budgeted<RecordBatch>) -> Result<()>,
    ) -> Result<ColumnarSourceVisit> {
        // A nonexistent diagnostic path proves these functions cannot reopen
        // an unowned file behind the already admitted byte payload.
        let path = Path::new("/nonexistent/shardloom-admitted-source");
        match format {
            Format::Avro => {
                visit_budgeted_avro_source_with_projection(source, path, rows, projection, visitor)
            }
            Format::Parquet => visit_budgeted_parquet_source_with_projection(
                source, path, rows, projection, visitor,
            ),
        }
    }

    #[test]
    fn encoded_slices_keep_the_original_lease_until_the_last_slice_drops() {
        let pool = LiveMemoryPool::new(6).unwrap();
        let (bytes, same_pool) = source_bytes(admitted(b"abcdef", &pool)).unwrap();
        let slice = bytes.slice(1..4);
        drop(bytes);
        assert_eq!(pool.snapshot().reserved_bytes, 6);
        assert_eq!(same_pool.snapshot().reserved_bytes, 6);
        assert_eq!(slice.as_ref(), b"bcd");
        drop(slice);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        let under_reserved = Budgeted::new(vec![1, 2, 3], pool.reserve(2).unwrap());
        assert!(source_bytes(under_reserved).is_err());
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn projected_batches_retain_the_shared_owner_and_exact_values() {
        for format in [Format::Avro, Format::Parquet] {
            let bytes = fixture(format, 9);
            let pool = LiveMemoryPool::new(1 << 20).unwrap();
            let mut retained = Vec::new();
            let result = visit(
                format,
                admitted(&bytes, &pool),
                9,
                &["label".into()],
                |batch| {
                    assert!(
                        pool.snapshot().reserved_bytes
                            >= bytes.len() as u64 + batch.reserved_bytes()
                    );
                    retained.push(batch);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(result.header, ["id", "label"]);
            assert_eq!(result.materialized_columns, ["label"]);
            assert_eq!(result.row_count, 9);
            assert_eq!(result.record_batch_count, 1);
            let values = retained[0]
                .value()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                values.iter().collect::<Vec<_>>(),
                (0..9)
                    .map(|v| {
                        if v % 3 == 0 {
                            None
                        } else {
                            Some("retained-payload")
                        }
                    })
                    .collect::<Vec<_>>()
            );
            assert!(pool.snapshot().reserved_bytes >= retained[0].reserved_bytes());
            drop(retained);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }

    #[test]
    fn shared_batch_denial_does_not_call_the_visitor_and_released_credits_allow_retry() {
        for format in [Format::Avro, Format::Parquet] {
            let bytes = fixture(format, 3);
            let pool = LiveMemoryPool::new(bytes.len() as u64 + 16_384).unwrap();
            let competing_owner = pool.reserve(16_384).unwrap();
            let mut calls = 0;
            let denied = visit(format, admitted(&bytes, &pool), 3, &["id".into()], |_| {
                calls += 1;
                Ok(())
            })
            .unwrap_err();
            assert!(denied.to_string().contains("memory reservation denied"));
            assert_eq!(calls, 0);
            assert_eq!(pool.snapshot().reserved_bytes, 16_384);
            drop(competing_owner);
            let retried = visit(format, admitted(&bytes, &pool), 3, &["id".into()], |_| {
                Ok(())
            })
            .unwrap();
            assert_eq!(retried.row_count, 3);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }

    #[test]
    fn sequential_visits_reuse_credits_and_stop_at_visitor_failure() {
        for format in [Format::Avro, Format::Parquet] {
            let bytes = fixture(format, 16_384);
            let pool = LiveMemoryPool::new(bytes.len() as u64 + 65_536).unwrap();
            let mut observed_rows = 0;
            let mut cumulative_bytes = 0;
            let result = visit(
                format,
                admitted(&bytes, &pool),
                16_384,
                &["id".into(), "label".into()],
                |batch| {
                    assert!(pool.snapshot().reserved_bytes <= pool.snapshot().limit_bytes);
                    let values = batch
                        .value()
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for value in values.values() {
                        assert_eq!(*value, observed_rows);
                        observed_rows += 1;
                    }
                    cumulative_bytes += batch.reserved_bytes();
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(observed_rows, 16_384);
            assert!(result.record_batch_count > 1);
            assert!(cumulative_bytes > 65_536);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
            let mut calls = 0;
            let failed = visit(
                format,
                admitted(&bytes, &pool),
                16_384,
                &["id".into()],
                |_| {
                    calls += 1;
                    Err(ShardLoomError::InvalidOperation(
                        "injected visitor failure".into(),
                    ))
                },
            )
            .unwrap_err();
            assert!(failed.to_string().contains("injected visitor failure"));
            assert_eq!(calls, 1);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }

    #[test]
    fn empty_projection_empty_input_and_read_failures_preserve_ownership() {
        for format in [Format::Avro, Format::Parquet] {
            let pool = LiveMemoryPool::new(1 << 20).unwrap();
            let empty = fixture(format, 0);
            let result = visit(format, admitted(&empty, &pool), 0, &["id".into()], |_| {
                Ok(())
            })
            .unwrap();
            assert_eq!(result.row_count, 0);
            assert_eq!(pool.snapshot().reserved_bytes, 0);

            let bytes = fixture(format, 3);
            let result = visit(
                format,
                admitted(&bytes, &pool),
                3,
                &["missing".into()],
                |_| Ok(()),
            )
            .unwrap();
            assert_eq!(result.row_count, 3);
            assert_eq!(result.materialized_columns, Vec::<String>::new());
            assert_eq!(pool.snapshot().reserved_bytes, 0);

            let mut calls = 0;
            let limit = visit(format, admitted(&bytes, &pool), 2, &["id".into()], |_| {
                calls += 1;
                Ok(())
            })
            .unwrap_err();
            assert!(limit.to_string().contains("row budget of 2"));
            assert_eq!(calls, 1);
            assert_eq!(pool.snapshot().reserved_bytes, 0);
            assert!(
                visit(
                    format,
                    admitted(b"invalid data", &pool),
                    3,
                    &["id".into()],
                    |_| Ok(())
                )
                .is_err()
            );
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }
}
