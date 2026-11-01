//! Explicit union-by-name schema alignment. No value casts or schema guessing.

use super::{Result, failed};
use arrow_array::{ArrayRef, RecordBatch, StringArray, new_null_array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shardloom_exec::live_memory::LiveMemoryPool;
use std::sync::Arc;

pub(super) fn with_partitions(
    schema: &SchemaRef,
    partitions: &[(String, Option<String>)],
) -> Result<SchemaRef> {
    let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
    let mut names = std::collections::BTreeSet::new();
    if fields
        .iter()
        .any(|field| field.name().is_empty() || !names.insert(field.name()))
    {
        return Err(failed("source fields must be nonempty and unique"));
    }
    for (name, _) in partitions {
        if fields.iter().any(|field| field.name() == name) {
            return Err(failed(
                "Hive partition key collides with a stored source column",
            ));
        }
        fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

pub(super) fn merge(held: Option<&SchemaRef>, next: &SchemaRef, union: bool) -> Result<SchemaRef> {
    let Some(held) = held else {
        return Ok(Arc::clone(next));
    };
    if held.metadata() != next.metadata() {
        return Err(failed("dataset files have incompatible schema metadata"));
    }
    if !union {
        if held.fields() != next.fields() {
            return Err(failed(
                "source directory files have incompatible schemas; an explicit union_by_name manifest is required for additive evolution",
            ));
        }
        return Ok(Arc::clone(held));
    }
    let mut fields = Vec::new();
    fields
        .try_reserve_exact(
            held.fields()
                .len()
                .checked_add(next.fields().len())
                .ok_or_else(|| failed("schema width overflow"))?,
        )
        .map_err(failed)?;
    for field in held.fields() {
        let right = next
            .fields()
            .iter()
            .find(|right| right.name() == field.name());
        if let Some(right) = right
            && (field.data_type() != right.data_type() || field.metadata() != right.metadata())
        {
            return Err(failed(format!(
                "union_by_name cannot change the type or metadata of '{}'",
                field.name()
            )));
        }
        let nullable = field.is_nullable() || right.is_none_or(|right| right.is_nullable());
        fields.push(Arc::new(field.as_ref().clone().with_nullable(nullable)));
    }
    for field in next.fields() {
        if !held.fields().iter().any(|left| left.name() == field.name()) {
            fields.push(Arc::new(field.as_ref().clone().with_nullable(true)));
        }
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        held.metadata().clone(),
    )))
}

pub(super) fn local_projection(
    local: &SchemaRef,
    complete: &SchemaRef,
    requested: &[usize],
) -> Vec<usize> {
    let mut columns = local
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            requested
                .iter()
                .any(|&i| complete.field(i).name() == field.name())
                .then_some(index)
        })
        .collect::<Vec<_>>();
    // A missing-only/partition-only request still consumes real row cardinality.
    // Keep one physical column as a carrier; it is discarded by alignment.
    if columns.is_empty() && !local.fields().is_empty() {
        columns.push(0);
    }
    columns
}

pub(super) fn align(
    batch: RecordBatch,
    target: SchemaRef,
    partitions: &[(String, Option<String>)],
    memory: &LiveMemoryPool,
) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let source_schema = batch.schema();
    let mut capacity = batch.get_array_memory_size() as u64;
    for field in target.fields() {
        capacity = add(capacity, 4096)?;
        if let Some((_, value)) = partitions.iter().find(|(name, _)| name == field.name()) {
            let width = add(value.as_ref().map_or(0, |v| v.len() as u64), 16)?;
            capacity = add(capacity, mul(width, rows as u64)?)?;
        } else if source_schema.index_of(field.name()).is_err() {
            capacity = add(capacity, null_capacity(field.data_type(), rows as u64)?)?;
        }
    }
    let mut credit = memory.reserve(add(capacity, 8192)?)?;
    let mut columns: Vec<ArrayRef> = Vec::new();
    columns
        .try_reserve_exact(target.fields().len())
        .map_err(failed)?;
    for field in target.fields() {
        let column =
            if let Some((_, value)) = partitions.iter().find(|(name, _)| name == field.name()) {
                Arc::new(StringArray::from_iter(std::iter::repeat_n(
                    value.as_deref(),
                    rows,
                ))) as ArrayRef
            } else if let Ok(index) = source_schema.index_of(field.name()) {
                let column = batch.column(index);
                if column.data_type() != field.data_type() {
                    return Err(failed(
                        "decoded field type changed after source schema admission",
                    ));
                }
                Arc::clone(column)
            } else {
                if !field.is_nullable() {
                    return Err(failed("missing source field was not admitted as nullable"));
                }
                new_null_array(field.data_type(), rows)
            };
        columns.push(column);
    }
    let output = RecordBatch::try_new_with_options(
        target,
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(failed)?;
    drop(batch);
    // Retain only the delivered buffers' complete reservation after scratch is
    // released. Clones/slices carry this lease through the existing buffer owner.
    credit.resize(output.get_array_memory_size() as u64)?;
    crate::universal_format_io::text_stream::credited_batch(output, credit)
}

fn null_capacity(dtype: &DataType, rows: u64) -> Result<u64> {
    let base = add(
        mul(
            rows.checked_add(1)
                .ok_or_else(|| failed("null row count overflow"))?,
            16,
        )?,
        1024,
    )?;
    Ok(match dtype {
        DataType::Struct(fields) => fields.iter().try_fold(base, |sum, field| {
            add(sum, null_capacity(field.data_type(), rows)?)
        })?,
        DataType::FixedSizeList(field, width) => add(
            base,
            null_capacity(
                field.data_type(),
                mul(rows, u64::try_from(*width).map_err(failed)?)?,
            )?,
        )?,
        DataType::FixedSizeBinary(width) => {
            add(base, mul(rows, u64::try_from(*width).map_err(failed)?)?)?
        }
        DataType::Dictionary(key, _) => add(base, null_capacity(key, rows)?)?,
        _ => add(
            base,
            mul(rows, dtype.primitive_width().unwrap_or(16) as u64)?,
        )?,
    })
}

pub(super) fn physical_leaves(dtype: &DataType) -> usize {
    match dtype {
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| physical_leaves(field.data_type()))
            .sum(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => physical_leaves(field.data_type()),
        _ => 1,
    }
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| failed("dataset batch capacity overflow"))
}
fn mul(left: u64, right: u64) -> Result<u64> {
    left.checked_mul(right)
        .ok_or_else(|| failed("dataset batch capacity overflow"))
}
