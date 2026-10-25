//! Direct pinned Parquet decoding is an independent correctness reference only.
//! No production input normalization, derived-column helper or query engine runs.

use super::*;
use crate::source_identity::SourceIdentity;
use arrow_array::{Array as _, Int64Array, StringArray, UInt8Array, UInt32Array};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
        ParquetRecordBatchReaderBuilder,
    },
};
use std::io::{Read as _, Seek as _, SeekFrom};

const MAX_PARQUET_FOOTER_BYTES: u64 = 16 << 20;
const MAX_PARQUET_ROW_GROUPS: usize = 16_384;
const MAX_COMPRESSED_COLUMN_BYTES: i64 = 256 << 20;
const MAX_UNCOMPRESSED_COLUMN_BYTES: i64 = 512 << 20;

/// Complete expected output schema for this validation operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetReferenceRecipe {
    /// Exactly the source columns, preserving their names, order and types.
    SourceColumns,
    /// Source columns followed by the seven declared `ClickBench` preparation fields.
    ClickBenchPreparation,
}

#[derive(Debug, Clone, Copy)]
enum Transform {
    Identity,
    Minute,
    MinuteStart,
    ByteLength,
    Domain,
}

struct ReferenceColumn {
    input_index: usize,
    transform: Transform,
    field: Field,
}

/// Independently compare every Parquet source value and the complete expected
/// prepared schema with a held local Vortex generation. Flat scalar types use
/// lossless Arrow comparison; valid float bits and row order remain exact.
///
/// This is a read-only testing boundary. Fixed footer, row-group and column-chunk
/// bounds precede decoding. The shared limits bound batches, values, windows
/// and native work. Parquet decoder allocations are not routed through the
/// native allocator and are explicitly excluded from its accounting claim.
///
/// # Errors
/// Rejects any value/schema/row mismatch, unsupported type or recipe, source
/// generation change, resource bound, decoder error or incomplete coverage.
/// Returns no partial equality proof and writes no artifact.
pub fn compare_parquet_native_artifact(
    parquet_path: &Path,
    native_path: &Path,
    recipe: ParquetReferenceRecipe,
    limits: NativeArtifactComparisonLimits,
) -> Result<serde_json::Value> {
    limits.validate()?;
    // Keep the caller's aliases in the generation fences: resolving a symlink
    // here would hide retargeting while held descriptors still read old bytes.
    let parquet_path = std::path::absolute(parquet_path).map_err(provider_error)?;
    let native_path = std::path::absolute(native_path).map_err(provider_error)?;
    let original = SourceIdentity::capture(&parquet_path)?;
    let native_identity = SourceIdentity::capture(&native_path)?;
    let parquet_metadata = original.file.metadata().map_err(provider_error)?;
    let metadata = load_metadata(&original, limits)?;
    let native_metadata = native_identity.file.metadata().map_err(provider_error)?;
    let owner = ResidentVortexSession::new(
        limits.memory_bytes_per_source,
        limits.parallelism_per_source,
    )?;
    let prepared = owner.prepare_file(&native_path)?;
    prepared.validate_file_metadata(&native_metadata)?;
    let (_, granted_parallelism) = prepared.resource_limits();
    let scan_limits = NativeArtifactComparisonLimits {
        parallelism_per_source: granted_parallelism,
        ..limits
    };
    let memory = LiveMemoryPool::new(limits.arrow_batch_bytes * 16)?;
    let work = prepared.with_native_execution(|file, session, runtime| {
        compare(
            &original,
            &metadata,
            &Source {
                file,
                session,
                runtime,
                tasks: ScanTasks::default(),
            },
            recipe,
            scan_limits,
            &memory,
        )
    })?;
    original.validate()?;
    native_identity.validate()?;
    prepared.validate_file_metadata(&native_metadata)?;
    drop(prepared);
    let runtime = owner.snapshot();
    if memory.snapshot().reserved_bytes != 0 || runtime.memory.reserved_bytes != 0 {
        return Err(error(
            "readback retained allocation credits after completion",
        ));
    }
    Ok(serde_json::json!({
        "complete_value_equality": true,
        "compared_rows": work.rows,
        "compared_columns": work.columns,
        "compared_values": work.compared_values,
        "source_columns": metadata.schema().fields().len(),
        "derived_columns": work.columns - metadata.schema().fields().len(),
        "reference_recipe": format!("{recipe:?}"),
        "reference_parquet_batches": work.left_batches,
        "native_batches": work.right_batches,
        "equal_spans": work.equal_spans,
        "parquet": identity(&parquet_path, &parquet_metadata),
        "native": identity(&native_path, &native_metadata),
        "source_generations_validated": true,
        "native_schema_equal": true,
        "native_prepared_source_opens": runtime.prepared_source_opens,
        "native_completed_executions": runtime.completed_executions,
        "native_task_concurrency": granted_parallelism,
        "peak_native_split_tasks": work.right_peak_native_tasks,
        "native_split_tasks_after_drain": 0,
        "comparison_scope": "all_rows_all_expected_columns_in_order;original_source_fields_then_explicit_recipe_fields;exact_native_dtype_names_order_and_nullability;Arrow_ArrayData_logical_equality;valid_float_bits_exact;null_payload_and_chunk_boundaries_ignored;no_sampling_or_aggregate_surrogate",
        "reference_scope": "pinned_ParquetRecordBatchReader_with_one_projected_source_column;independent_derived_formulas;no_production_input_normalization_or_derived_helpers;no_external_query_engine",
        "generation_scope": "held_descriptors_and_pre_post_path_generation_checks_including_caller_symlink_aliases;reference_reader_reopens_checked_against_held_identity;caller_frozen_inputs_required;not_an_atomic_filesystem_snapshot",
        "boundary": "read_only_complete_artifact_validation;explicit_native_decode_and_Arrow_conversion;not_query_execution;not_fallback_execution;physical_layout_statistics_and_user_metadata_checked_separately;no_output_write;no_content_hash",
        "memory": {
            "native_limit_bytes": limits.memory_bytes_per_source,
            "native_peak_reserved_bytes": runtime.memory.peak_reserved_bytes,
            "native_final_reserved_bytes": runtime.memory.reserved_bytes,
            "arrow_work_limit_bytes": memory.snapshot().limit_bytes,
            "arrow_peak_reserved_bytes": memory.snapshot().peak_reserved_bytes,
            "arrow_final_reserved_bytes": memory.snapshot().reserved_bytes,
            "peak_arrow_batch_bytes": work.peak_arrow_batch_bytes,
            "arrow_batch_bytes": limits.arrow_batch_bytes,
            "max_value_bytes": limits.max_value_bytes,
            "parquet_batch_rows": limits.left_batch_rows,
            "native_batch_rows": limits.right_batch_rows,
            "window_rows": limits.window_rows,
            "max_parquet_footer_bytes": MAX_PARQUET_FOOTER_BYTES,
            "max_parquet_row_groups": MAX_PARQUET_ROW_GROUPS,
            "max_compressed_column_chunk_bytes": MAX_COMPRESSED_COLUMN_BYTES,
            "max_uncompressed_column_chunk_bytes": MAX_UNCOMPRESSED_COLUMN_BYTES,
            "scope": "reserved_native_source_allocator;two_Arrow_batch_work_grants_before_iteration_and_postchecked_batch_sizes;one_projected_reference_column;bounded_native_windows_and_tasks;excludes_Parquet_decoder_and_other_upstream_allocations_bypassing_host_allocator_metadata_caches_and_process_RSS;column_chunk_bounds_are_input_admission_not_allocator_enforcement"
        }
    }))
}

fn load_metadata(
    source: &SourceIdentity,
    limits: NativeArtifactComparisonLimits,
) -> Result<ArrowReaderMetadata> {
    if source.generation.len < 12 {
        return Err(error("Parquet input is too short for a footer"));
    }
    let mut file = source.open_checked()?;
    file.seek(SeekFrom::End(-8)).map_err(provider_error)?;
    let mut footer = [0_u8; 8];
    file.read_exact(&mut footer).map_err(provider_error)?;
    let footer_bytes = u64::from(u32::from_le_bytes(footer[..4].try_into().unwrap()));
    if &footer[4..] != b"PAR1"
        || footer_bytes > MAX_PARQUET_FOOTER_BYTES
        || footer_bytes > source.generation.len - 12
    {
        return Err(error("Parquet footer is unsupported or exceeds admission"));
    }
    let metadata =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::default()).map_err(provider_error)?;
    if metadata.schema().fields().is_empty()
        || metadata.schema().fields().len() > limits.max_columns
        || metadata.metadata().num_row_groups() > MAX_PARQUET_ROW_GROUPS
    {
        return Err(error("Parquet metadata dimensions exceed admission"));
    }
    for field in metadata.schema().fields() {
        if field.name().len() > 4096
            || (fixed_width(field.data_type()).is_none()
                && !matches!(field.data_type(), DataType::Utf8 | DataType::Binary))
        {
            return Err(error(
                "Parquet reference requires bounded flat scalar fields",
            ));
        }
    }
    for group in metadata.metadata().row_groups() {
        if group.num_columns() != metadata.schema().fields().len() || group.num_rows() < 0 {
            return Err(error("Parquet row-group schema or count is invalid"));
        }
        for column in group.columns() {
            if !(0..=MAX_COMPRESSED_COLUMN_BYTES).contains(&column.compressed_size())
                || !(0..=MAX_UNCOMPRESSED_COLUMN_BYTES).contains(&column.uncompressed_size())
            {
                return Err(error("Parquet column chunk exceeds readback admission"));
            }
        }
    }
    source.validate()?;
    Ok(metadata)
}

fn reference_columns(
    metadata: &ArrowReaderMetadata,
    recipe: ParquetReferenceRecipe,
) -> Result<Vec<ReferenceColumn>> {
    let schema = metadata.schema();
    let mut names = std::collections::HashSet::new();
    let mut result = Vec::new();
    for (input_index, field) in schema.fields().iter().enumerate() {
        if !names.insert(field.name().as_str()) {
            return Err(error("duplicate Parquet source field name"));
        }
        result.push(ReferenceColumn {
            input_index,
            transform: Transform::Identity,
            field: field.as_ref().clone(),
        });
    }
    if recipe == ParquetReferenceRecipe::ClickBenchPreparation {
        for (input, suffix, transform, dtype) in [
            (
                "EventTime",
                "extract_minute",
                Transform::Minute,
                DataType::UInt8,
            ),
            (
                "EventTime",
                "date_trunc_minute",
                Transform::MinuteStart,
                DataType::Int64,
            ),
            ("URL", "utf8_len", Transform::ByteLength, DataType::UInt32),
            ("URL", "url_domain", Transform::Domain, DataType::Utf8),
            (
                "Referer",
                "utf8_len",
                Transform::ByteLength,
                DataType::UInt32,
            ),
            ("Referer", "url_domain", Transform::Domain, DataType::Utf8),
            (
                "SearchPhrase",
                "utf8_len",
                Transform::ByteLength,
                DataType::UInt32,
            ),
        ] {
            let input_index = schema.index_of(input).map_err(provider_error)?;
            let required = match transform {
                Transform::Minute | Transform::MinuteStart => DataType::Int64,
                _ => DataType::Utf8,
            };
            if schema.field(input_index).data_type() != &required {
                return Err(error("derived reference input has an unsupported type"));
            }
            let name = format!("__shardloom_derived_{suffix}_{input}");
            if schema.fields().iter().any(|field| field.name() == &name) {
                return Err(error("derived reference name collides with source column"));
            }
            result.push(ReferenceColumn {
                input_index,
                transform,
                field: Field::new(name, dtype, true),
            });
        }
    }
    Ok(result)
}

// Keep the complete coverage and drain checks alongside the cursor state machine.
#[allow(clippy::too_many_lines)]
fn compare(
    original: &SourceIdentity,
    metadata: &ArrowReaderMetadata,
    native: &Source<'_>,
    recipe: ParquetReferenceRecipe,
    limits: NativeArtifactComparisonLimits,
    memory: &LiveMemoryPool,
) -> Result<Work> {
    let expected = reference_columns(metadata, recipe)?;
    let columns = columns(native, limits)?;
    if columns.len() != expected.len() {
        return Err(error("Parquet/reference and native column counts differ"));
    }
    let native_fields = native.file.dtype().as_struct_fields_opt().unwrap();
    for ((column, reference), actual_dtype) in
        columns.iter().zip(&expected).zip(native_fields.fields())
    {
        let expected_dtype = native
            .session
            .arrow()
            .from_arrow_field(&reference.field)
            .map_err(provider_error)?;
        if column.name != reference.field.name().as_str() || actual_dtype != expected_dtype {
            return Err(error(
                "Parquet/reference and native schemas differ (names/order/types/nullability)",
            ));
        }
    }
    let rows =
        u64::try_from(metadata.metadata().file_metadata().num_rows()).map_err(provider_error)?;
    if rows != native.file.row_count() {
        return Err(error("Parquet and native source row counts differ"));
    }
    let mut work = Work {
        rows,
        columns: columns.len(),
        ..Work::default()
    };
    for (column, reference) in columns.iter().zip(&expected) {
        let mut source = ReferenceCursor::new(original, metadata, reference, limits, memory)?;
        let mut start = 0;
        while start < rows {
            let end = start.saturating_add(limits.window_rows).min(rows);
            let mut target = Cursor::new(
                scan(
                    native,
                    column,
                    start..end,
                    limits.right_batch_rows,
                    limits.parallelism_per_source,
                )?,
                native.session,
                column,
                limits.right_batch_rows,
                limits,
                memory,
            )?;
            let mut offset = start;
            while target.ensure()? {
                if !source.ensure()? {
                    return Err(error("Parquet stream ended before the native row window"));
                }
                let count = target.remaining().min(source.remaining());
                let reference_slice = source.array().slice(source.offset, count);
                let native_slice = target.array().slice(target.offset, count);
                if reference_slice.to_data() != native_slice.to_data() {
                    let first = (0..count)
                        .find(|index| {
                            reference_slice.slice(*index, 1).to_data()
                                != native_slice.slice(*index, 1).to_data()
                        })
                        .ok_or_else(|| error("Parquet mismatch localization failed"))?;
                    return Err(error(&format!(
                        "value mismatch at column '{}' row {} (zero-based)",
                        column.name,
                        offset + first as u64
                    )));
                }
                source.offset += count;
                target.offset += count;
                offset = offset
                    .checked_add(count as u64)
                    .ok_or_else(|| error("readback row offset overflow"))?;
                work.compared_values = work
                    .compared_values
                    .checked_add(count as u64)
                    .ok_or_else(|| error("readback value count overflow"))?;
                work.equal_spans += 1;
                #[cfg(test)]
                AFTER_EQUAL_SPAN.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().take() {
                        hook();
                    }
                });
            }
            if offset != end {
                return Err(error(
                    "native comparison did not cover its complete row window",
                ));
            }
            work.right_batches += target.batches;
            work.peak_arrow_batch_bytes = work.peak_arrow_batch_bytes.max(target.peak_bytes);
            start = end;
        }
        if source.ensure()? {
            return Err(error(
                "Parquet stream contains rows beyond the native source",
            ));
        }
        work.left_batches += source.batches;
        work.peak_arrow_batch_bytes = work.peak_arrow_batch_bytes.max(source.peak_bytes);
        original.validate()?;
    }
    if work.compared_values
        != rows
            .checked_mul(work.columns as u64)
            .ok_or_else(|| error("expected value count overflow"))?
    {
        return Err(error("complete Parquet readback coverage failed"));
    }
    work.right_peak_native_tasks = native.tasks.0.peak.load(Ordering::Acquire);
    if native.tasks.0.active.load(Ordering::Acquire) != 0
        || work.right_peak_native_tasks > limits.parallelism_per_source
    {
        return Err(error(
            "native readback tasks failed concurrency or drain admission",
        ));
    }
    Ok(work)
}

struct ReferenceCursor<'a> {
    reader: ParquetRecordBatchReader,
    column: &'a ReferenceColumn,
    limits: NativeArtifactComparisonLimits,
    current: Option<ArrowArrayRef>,
    offset: usize,
    batches: u64,
    peak_bytes: u64,
    scheme: regex::Regex,
    _work_grant: MemoryLease,
}

impl<'a> ReferenceCursor<'a> {
    fn new(
        source: &SourceIdentity,
        metadata: &ArrowReaderMetadata,
        column: &'a ReferenceColumn,
        limits: NativeArtifactComparisonLimits,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        let grant = memory.reserve(limits.arrow_batch_bytes * 8)?;
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
            source.open_checked()?,
            metadata.clone(),
        )
        .with_projection(ProjectionMask::roots(
            metadata.metadata().file_metadata().schema_descr(),
            [column.input_index],
        ))
        .with_batch_size(limits.left_batch_rows)
        .build()
        .map_err(provider_error)?;
        Ok(Self {
            reader,
            column,
            limits,
            current: None,
            offset: 0,
            batches: 0,
            peak_bytes: 0,
            scheme: regex::Regex::new(r"^[A-Za-z][A-Za-z0-9+.-]*://").map_err(provider_error)?,
            _work_grant: grant,
        })
    }
    fn array(&self) -> &ArrowArrayRef {
        self.current
            .as_ref()
            .expect("reference cursor was advanced")
    }
    fn remaining(&self) -> usize {
        self.array().len() - self.offset
    }
    fn ensure(&mut self) -> Result<bool> {
        if self
            .current
            .as_ref()
            .is_some_and(|array| self.offset < array.len())
        {
            return Ok(true);
        }
        self.current = None;
        self.offset = 0;
        for batch in self.reader.by_ref() {
            let batch = batch.map_err(provider_error)?;
            if batch.num_columns() != 1 || batch.num_rows() > self.limits.left_batch_rows {
                return Err(error(
                    "Parquet decoder exceeded projected column or row admission",
                ));
            }
            if batch.num_rows() == 0 {
                continue;
            }
            let input = batch.column(0);
            validate_arrow(input, self.limits, self.column.field.name())?;
            self.peak_bytes = self.peak_bytes.max(input.get_array_memory_size() as u64);
            let result = derived_reference(input, self.column.transform, &self.scheme)?;
            validate_arrow(&result, self.limits, self.column.field.name())?;
            if result.len() != batch.num_rows()
                || result.data_type() != self.column.field.data_type()
            {
                return Err(error(
                    "reference derivation changed its declared type or row count",
                ));
            }
            self.peak_bytes = self.peak_bytes.max(result.get_array_memory_size() as u64);
            self.current = Some(result);
            self.batches += 1;
            return Ok(true);
        }
        Ok(false)
    }
}

fn validate_arrow(
    array: &ArrowArrayRef,
    limits: NativeArtifactComparisonLimits,
    column: &str,
) -> Result<()> {
    let bytes = array.get_array_memory_size() as u64;
    if bytes > limits.arrow_batch_bytes {
        return Err(error(&format!(
            "Parquet/reference batch exceeds admitted Arrow byte limit: column '{column}' has {bytes} retained bytes for {} rows, limit {}",
            array.len(),
            limits.arrow_batch_bytes
        )));
    }
    let excessive = match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .flatten()
            .any(|value| value.len() as u64 > limits.max_value_bytes),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap()
            .iter()
            .flatten()
            .any(|value| value.len() as u64 > limits.max_value_bytes),
        _ => false,
    };
    if excessive {
        return Err(error("Parquet/reference value exceeds byte admission"));
    }
    Ok(())
}

fn derived_reference(
    input: &ArrowArrayRef,
    transform: Transform,
    scheme: &regex::Regex,
) -> Result<ArrowArrayRef> {
    Ok(match transform {
        Transform::Identity => Arc::clone(input),
        Transform::Minute => {
            let values = input
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| error("minute reference requires signed epoch seconds"))?;
            Arc::new(UInt8Array::from_iter(values.iter().map(|value| {
                value.map(|seconds| {
                    u8::try_from(seconds.rem_euclid(3600) / 60)
                        .expect("Euclidean minute is in 0..60")
                })
            })))
        }
        Transform::MinuteStart => {
            let values = input
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| error("minute-start reference requires signed epoch seconds"))?;
            let result = values
                .iter()
                .map(|value| {
                    value
                        .map(|seconds| {
                            seconds.checked_sub(seconds.rem_euclid(60)).ok_or_else(|| {
                                error("minute-start reference is outside signed epoch range")
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(Int64Array::from(result))
        }
        Transform::ByteLength => {
            let values = input
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| error("byte-length reference requires UTF-8 values"))?;
            let result = values
                .iter()
                .map(|value| {
                    value
                        .map(|text| u32::try_from(text.len()).map_err(provider_error))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(UInt32Array::from(result))
        }
        Transform::Domain => {
            let values = input
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| error("domain reference requires UTF-8 values"))?;
            Arc::new(StringArray::from_iter(
                values
                    .iter()
                    .map(|value| value.map(|text| domain_reference(text, scheme))),
            ))
        }
    })
}

fn domain_reference<'a>(text: &'a str, scheme: &regex::Regex) -> &'a str {
    let rest = scheme.find(text).map_or_else(
        || text.strip_prefix("//").unwrap_or(text),
        |matched| &text[matched.end()..],
    );
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = if host
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("www."))
    {
        &host[4..]
    } else {
        host
    };
    if host.starts_with('[') {
        host.find(']').map_or(host, |end| &host[..=end])
    } else {
        host.split(':').next().unwrap_or("")
    }
}

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_artifact_parquet_readback_tests.rs"]
mod tests;
