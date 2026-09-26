//! Bounded `ClickBench` Parquet input preparation for writer attribution profiles.
//!
//! This test-only helper uses the product source adapter for schema and source
//! identity, then replaces its unconsumed reader with one selected Parquet row
//! group. It retains at most three converted native batches and writes no files.

use std::{fs::File, path::Path, sync::Arc, time::Instant};

use super::{
    ColumnarProjectedColumn, FlatColumnarSourceShape, IngestStageTimings, NativeIngestMemory,
    record_batch_to_vortex_from_arrow_provider_profiled_with_memory,
    validate_flat_columnar_stream_source_shape,
};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use serde_json::{Value, json};

const SOURCE_ROW_LIMIT: usize = 100_000_000;
const SOURCE_ROWS: usize = 99_997_497;
const INPUT_BATCH_ROWS: usize = 131_072;
const MAX_BATCHES: usize = 3;
const NATIVE_MEMORY_BYTES: u64 = 2 << 30;
const SOURCE_COLUMNS: usize = 105;
const DERIVED_COLUMNS: usize = 112;

pub(super) struct PreparedRegion {
    pub(super) memory: NativeIngestMemory,
    pub(super) arrays: Vec<vortex::array::ArrayRef>,
    pub(super) report: Value,
}

/// Read one bounded Parquet row-group prefix using the production dictionary
/// schema, derive the existing embedded columns, and retain native arrays.
///
/// This is intended for the official 99,997,497-row `ClickBench` source. It is a
/// serial profile input path: it starts no read/conversion workers and performs
/// no output writes. The returned arrays own their native allocator credits.
#[allow(clippy::too_many_lines)] // Keep bounded source, ownership and identity proof together.
pub(super) fn prepare_region(path: &Path, row_group: usize) -> PreparedRegion {
    assert!(
        matches!(row_group, 0 | 113 | 225),
        "unexpected profile row group"
    );

    let started = Instant::now();
    let mut source =
        crate::universal_format_io::stream_flat_parquet_columnar_source_with_parallelism(
            path,
            SOURCE_ROW_LIMIT,
            1,
        )
        .expect("open production Parquet source metadata");
    assert_eq!(source.row_count_hint, Some(SOURCE_ROWS));
    assert_eq!(source.header.len(), SOURCE_COLUMNS);
    assert_eq!(source.reader_projection_columns.len(), SOURCE_COLUMNS);
    assert_eq!(
        source.source_identities.len(),
        1,
        "expected held source identity"
    );
    for identity in &source.source_identities {
        identity
            .validate()
            .expect("validate source before profile read");
    }

    // The product adapter supplies the already-admitted dictionary schema. The
    // adapter-created reader is still unconsumed and is replaced before any
    // source rows are pulled.
    let dictionary_schema = source.reader.schema();
    assert_eq!(dictionary_schema.fields().len(), SOURCE_COLUMNS);
    let source_identity_count = source.source_identities.len();
    let metadata_options = ArrowReaderOptions::new()
        .with_skip_arrow_metadata(true)
        .with_schema(Arc::clone(&dictionary_schema));
    let metadata_builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        File::open(path).expect("open selected Parquet source"),
        ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )
    .expect("read Parquet metadata for selected row group");
    let reader_metadata =
        ArrowReaderMetadata::try_new(Arc::clone(metadata_builder.metadata()), metadata_options)
            .expect("apply production dictionary schema to selected reader");
    let selected_reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
        File::open(path).expect("reopen selected Parquet source"),
        reader_metadata,
    )
    .with_row_groups(vec![row_group])
    .with_batch_size(INPUT_BATCH_ROWS)
    .build()
    .expect("build bounded selected-row-group reader");
    source.reader = Box::new(selected_reader);
    drop(metadata_builder);

    let source = crate::universal_format_io::
        with_source_native_lean_runtime_embedded_derived_columns_columnar_stream_source(source);
    assert_eq!(source.header.len(), DERIVED_COLUMNS);
    assert_eq!(source.materialized_columns.len(), DERIVED_COLUMNS);
    assert_eq!(source.reader_projection_columns.len(), DERIVED_COLUMNS);
    let shape = validate_flat_columnar_stream_source_shape(&source)
        .expect("validate production embedded-derived source shape");
    assert_eq!(shape.projected_columns.len(), DERIVED_COLUMNS);

    let memory =
        NativeIngestMemory::new(NATIVE_MEMORY_BYTES).expect("create bounded native ingest memory");
    let mut arrays = Vec::with_capacity(MAX_BATCHES);
    let mut batch_rows = Vec::with_capacity(MAX_BATCHES);
    let mut batch_nbytes = Vec::with_capacity(MAX_BATCHES);
    for batch_result in source.reader.take(MAX_BATCHES) {
        let batch = batch_result.expect("read selected Parquet row-group batch");
        assert!(
            batch.num_rows() > 0,
            "selected row group returned an empty batch"
        );
        assert_eq!(batch.num_columns(), DERIVED_COLUMNS);
        let source_shape = FlatColumnarSourceShape {
            projected_columns: shape
                .projected_columns
                .iter()
                .enumerate()
                .map(|(reader_index, column)| ColumnarProjectedColumn {
                    column: column.column.clone(),
                    reader_index,
                    dtype_hint: column.dtype_hint.clone(),
                    arrow_dtype_hint: column.arrow_dtype_hint.clone(),
                })
                .collect(),
        };
        let admission_bytes = super::arrow_ownership::batch_copy_allocation_bytes(&batch)
            .expect("size native Arrow copy admission");
        let reserve_bytes = admission_bytes
            .checked_mul(2)
            .expect("Arrow copy reservation size overflow");
        let mut lease = memory
            .pool
            .reserve(reserve_bytes)
            .expect("reserve native Arrow conversion headroom");
        let array = record_batch_to_vortex_from_arrow_provider_profiled_with_memory(
            &batch,
            &source_shape,
            &IngestStageTimings::default(),
            Some((&memory, &mut lease)),
        )
        .expect("convert profile batch through native Vortex provider");
        drop(lease);
        assert_eq!(
            array.dtype().as_struct_fields_opt().unwrap().nfields(),
            DERIVED_COLUMNS
        );
        batch_rows.push(batch.num_rows());
        batch_nbytes.push(array.nbytes());
        arrays.push(array);
    }
    assert!(!arrays.is_empty(), "selected row group returned no batches");

    for identity in &source.source_identities {
        identity
            .validate()
            .expect("validate source after profile read");
    }
    let pool = memory.pool.snapshot();
    let rows = batch_rows.iter().sum::<usize>();
    let native_array_nbytes = batch_nbytes.iter().sum::<u64>();
    let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let report = json!({
        "input_kind": "production_parquet_source_schema_and_identity_with_selected_row_group_reader",
        "source_row_count": SOURCE_ROWS,
        "source_columns": SOURCE_COLUMNS,
        "derived_columns": DERIVED_COLUMNS,
        "row_group": row_group,
        "batch_size_rows": INPUT_BATCH_ROWS,
        "max_batches": MAX_BATCHES,
        "batches_read": arrays.len(),
        "rows_read": rows,
        "batch_rows": batch_rows,
        "batch_native_nbytes": batch_nbytes,
        "native_array_nbytes": native_array_nbytes,
        "native_memory_limit_bytes": NATIVE_MEMORY_BYTES,
        "native_pool_reserved_bytes_with_arrays_retained": pool.reserved_bytes,
        "native_pool_peak_reserved_bytes": pool.peak_reserved_bytes,
        "source_identities_validated_before_and_after": source_identity_count,
        "source_dictionary_preservation_status": source.source_dictionary_preservation_status,
        "prepare_region_elapsed_micros_excluded_from_writer_timing": elapsed_micros,
        "reads_source_rows_only_from_selected_group": true,
        "output_files_written": false,
        "extra_workers_started": false,
    });

    PreparedRegion {
        memory,
        arrays,
        report,
    }
}
