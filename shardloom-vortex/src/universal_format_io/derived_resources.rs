//! Named capacities for the existing derived-column conversion boundary.
//!
//! Complete-source readers supply their existing pool. These reservations cover
//! derived schema/spec tables and Arrow builder/map/replacement capacities, not
//! the original provider's buffers, allocator bookkeeping or process RSS.

use super::super::{
    EmbeddedDerivedColumnKind, EmbeddedDerivedColumnMode, EmbeddedDerivedColumnSpec,
    FlatLocalColumnarStreamSource, append_embedded_derived_columns_to_batch,
    owned_batch::OwnedBatch, visit_embedded_derived_columns,
};
use arrow_array::{
    Array, ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    cast::AsArray as _,
};
use arrow_schema::{DataType, SchemaRef};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::sync::Arc;

pub(super) fn reserve_metadata(
    source: &FlatLocalColumnarStreamSource,
    mode: EmbeddedDerivedColumnMode,
    memory: &LiveMemoryPool,
) -> Result<Option<MemoryLease>> {
    let schema = source.reader.schema();
    let mut count = 0_u64;
    visit_embedded_derived_columns(&schema, mode, |_, _, _| count += 1);
    if count == 0 {
        // The source-native mode still appends an availability diagnostic. Its
        // replacement string remains owned by the source metadata facade.
        return if matches!(mode, EmbeddedDerivedColumnMode::SourceNativeOnly) {
            memory
                .reserve(add(
                    mul(source.source_dictionary_preservation_status.len() as u64, 2)?,
                    4096,
                )?)
                .map(Some)
        } else {
            Ok(None)
        };
    }
    // Source names may be repeated in fields/specs, three public name vectors,
    // dtype vectors and the status string. Existing fields can carry arbitrary
    // nested type/metadata storage, so a field-count-only charge is insufficient.
    let fields = schema.fields().iter().try_fold(8192_u64, |sum, field| {
        add(sum, add(mul(field.size() as u64, 16)?, 4096)?)
    })?;
    let names = source
        .header
        .iter()
        .chain(&source.materialized_columns)
        .chain(&source.reader_projection_columns)
        .try_fold(0_u64, |sum, name| add(sum, add(name.len() as u64, 64)?))?;
    let status = source.source_dictionary_preservation_status.len() as u64;
    memory
        .reserve(add(
            add(fields, mul(count, 4096)?)?,
            mul(add(names, status)?, 4)?,
        )?)
        .map(Some)
}

pub(super) fn append(
    input: OwnedBatch,
    schema: &SchemaRef,
    specs: &[EmbeddedDerivedColumnSpec],
    memory: Option<&LiveMemoryPool>,
    metadata: Option<&Arc<MemoryLease>>,
) -> Result<OwnedBatch> {
    let original_columns = input.num_columns();
    let mut credit = memory
        .map(|memory| memory.reserve(batch_capacity(&input, specs)?))
        .transpose()?;
    let mut output =
        input.map(|batch| append_embedded_derived_columns_to_batch(batch, schema, specs))?;
    if let Some(credit) = &mut credit {
        let retained = specs.iter().enumerate().try_fold(
            add(1024, column_capacity(output.num_columns(), 2)?)?,
            |sum, (index, spec)| {
                let derived = output.column(original_columns + index);
                let source = output.column(spec.source_index);
                let mut bytes = derived.get_array_memory_size() as u64;
                // A dictionary-length derivation may share the original keys. Their
                // complete allocation already has the source owner; a small slice
                // must not charge that potentially large backing allocation again.
                if let (Some(input), Some(result)) = (
                    source.as_any_dictionary_opt(),
                    derived.as_any_dictionary_opt(),
                ) {
                    let input = input.keys().to_data();
                    let result = result.keys().to_data();
                    for buffer in result.buffers() {
                        if input.buffers().iter().any(|other| buffer.ptr_eq(other)) {
                            bytes = bytes
                                .checked_sub(buffer.capacity() as u64)
                                .ok_or_else(overflow)?;
                        }
                    }
                    if let (Some(input), Some(result)) = (input.nulls(), result.nulls())
                        && input.buffer().ptr_eq(result.buffer())
                    {
                        bytes = bytes
                            .checked_sub(result.buffer().capacity() as u64)
                            .ok_or_else(overflow)?;
                    }
                }
                add(sum, bytes)
            },
        )?;
        if retained > credit.bytes() {
            return Err(ShardLoomError::InvalidOperation(
                "derived-column retained capacity exceeded its admitted construction workspace; no fallback execution was attempted".into(),
            ));
        }
        credit.resize(retained)?;
    }
    if let Some(credit) = credit {
        // Preserve standard Arrow allocations for the sealed Parquet transfer
        // proof. Native conversion retains this envelope until its own admitted
        // copies/transfers have replaced the source ownership.
        output = output.retain(Arc::new(credit));
    }
    if let Some(metadata) = metadata {
        output = output.retain(metadata.clone());
    }
    Ok(output)
}

fn batch_capacity(batch: &RecordBatch, specs: &[EmbeddedDerivedColumnSpec]) -> Result<u64> {
    let columns = batch
        .num_columns()
        .checked_add(specs.len())
        .ok_or_else(overflow)?;
    // The new column-reference vector can grow while its old allocation is
    // still live. Source payload owners are retained and are not charged here.
    specs
        .iter()
        .try_fold(add(8192, column_capacity(columns, 4)?)?, |sum, spec| {
            let source = batch
                .columns()
                .get(spec.source_index)
                .ok_or_else(overflow)?;
            let values = source
                .as_any_dictionary_opt()
                .map_or(source, |dictionary| dictionary.values());
            let rows = source.len() as u64;
            let domain = values.len() as u64;
            let bytes = match spec.kind {
                EmbeddedDerivedColumnKind::ExtractMinute
                | EmbeddedDerivedColumnKind::DateTruncMinute => {
                    // Typed values, validity and simultaneous replacement capacity.
                    mul(rows, 32)?
                }
                EmbeddedDerivedColumnKind::Utf8Length => {
                    // Keys/null rewrites, length values and the integer dictionary
                    // hash table, including old/new tables during capacity growth.
                    add(mul(rows, 64)?, mul(domain, 96)?)?
                }
                EmbeddedDerivedColumnKind::UrlDomain => {
                    // Row keys, Option<key> maps, borrowed-string hash entries,
                    // offsets/validity and old/new StringBuilder backing storage.
                    // Domain output cannot exceed its source string value spans.
                    let initial = mul(domain.min(4096), 16)?;
                    add(
                        add(mul(rows, 64)?, mul(domain, 256)?)?,
                        mul(string_bytes(values)?.max(initial), 4)?,
                    )?
                }
            };
            add(sum, add(bytes, 8192)?)
        })
}

fn column_capacity(columns: usize, copies: u64) -> Result<u64> {
    mul(
        mul(columns as u64, std::mem::size_of::<ArrayRef>() as u64)?,
        copies,
    )
}

fn string_bytes(array: &ArrayRef) -> Result<u64> {
    match array.data_type() {
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(overflow)?;
            let offsets = values.value_offsets();
            u64::try_from(i64::from(*offsets.last().ok_or_else(overflow)?) - i64::from(offsets[0]))
                .map_err(|_| overflow())
        }
        DataType::LargeUtf8 => {
            let values = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(overflow)?;
            let offsets = values.value_offsets();
            let bytes = offsets
                .last()
                .ok_or_else(overflow)?
                .checked_sub(offsets[0])
                .ok_or_else(overflow)?;
            u64::try_from(bytes).map_err(|_| overflow())
        }
        DataType::Utf8View => {
            let values = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(overflow)?;
            values
                .lengths()
                .try_fold(0_u64, |sum, length| add(sum, u64::from(length)))
        }
        _ => Err(overflow()),
    }
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or_else(overflow)
}

fn mul(left: u64, right: u64) -> Result<u64> {
    left.checked_mul(right).ok_or_else(overflow)
}

fn overflow() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "derived-column capacity or source shape is invalid; no fallback execution was attempted"
            .into(),
    )
}

#[cfg(test)]
#[path = "derived_resources_tests.rs"]
mod tests;
