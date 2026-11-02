//! Per-file logical identity and visibility before native batch admission.

use super::{ColumnMapping, Result, ScalarValue, Snapshot, deletes, failed, schema};
use crate::universal_format_io::owned_batch::OwnedBatch;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use shardloom_exec::{
    adaptive_work::WorkCost,
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::sync::Arc;

pub(crate) struct FileMapping {
    outputs: Vec<Option<String>>,
    equalities: Vec<(usize, Vec<Option<String>>)>,
    positions: Vec<usize>,
    _credit: MemoryLease,
}

impl FileMapping {
    #[allow(clippy::too_many_lines)]
    pub(crate) fn admit(
        snapshot: &Snapshot,
        index: usize,
        physical: &SchemaRef,
        rows: u64,
    ) -> Result<Self> {
        let file = &snapshot.files[index];
        let observed = std::fs::metadata(&file.path).map_err(failed)?;
        if file.bytes.is_some_and(|bytes| observed.len() != bytes)
            || file.records.is_some_and(|count| count != rows)
        {
            return Err(failed(
                "table data-file size/cardinality differs from selected metadata",
            ));
        }
        if matches!(snapshot.mapping, ColumnMapping::FieldId) {
            deletes::validate_ids(physical.as_ref())?;
        } else {
            deletes::validate_names(physical.as_ref())?;
        }
        let names = physical
            .fields()
            .iter()
            .map(|field| field.name().len())
            .max()
            .unwrap_or(0) as u64;
        let mut entries = snapshot.fields.len() as u64;
        for delete in &snapshot.deletes {
            if delete.applies(file) {
                let count = match &delete.content {
                    deletes::Content::Positions(_) => 1,
                    deletes::Content::Equality { fields, .. } => fields.len() as u64 + 1,
                };
                entries = entries
                    .checked_add(count)
                    .ok_or_else(|| failed("table mapping capacity overflow"))?;
            }
        }
        let capacity = names
            .checked_mul(4)
            .and_then(|width| width.checked_add(256))
            .and_then(|width| width.checked_mul(entries))
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| failed("table mapping capacity overflow"))?;
        let credit = snapshot.metadata.memory.reserve(capacity)?;
        let mut outputs = Vec::new();
        for field in &snapshot.fields {
            if file.partitions.contains_key(field.field.name())
                || field.field.data_type() == &DataType::Null
            {
                outputs.push(None);
                continue;
            }
            let index = match snapshot.mapping {
                ColumnMapping::FieldId => deletes::find_id(
                    physical.as_ref(),
                    field
                        .id
                        .ok_or_else(|| failed("logical field ID is absent"))?,
                )?,
                ColumnMapping::Name => physical.index_of(field.field.name()).ok(),
                ColumnMapping::PhysicalName => physical.index_of(&field.physical_name).ok(),
            };
            let name = if let Some(index) = index {
                let column = physical.field(index);
                if !schema::compatible(
                    column.data_type(),
                    field.field.data_type(),
                    snapshot.format == "iceberg",
                ) {
                    return Err(failed(format!(
                        "table field '{}' has incompatible physical type {:?} for {:?}",
                        field.field.name(),
                        column.data_type(),
                        field.field.data_type()
                    )));
                }
                Some(column.name().clone())
            } else {
                if !field.field.is_nullable()
                    || snapshot.required_physical.contains(field.field.name())
                {
                    return Err(failed(format!(
                        "non-null table field '{}' is absent from a selected data file",
                        field.field.name()
                    )));
                }
                None
            };
            outputs.push(name);
        }
        let mut equalities = Vec::new();
        let mut positions = Vec::new();
        for (delete_index, delete) in snapshot.deletes.iter().enumerate() {
            if !delete.applies(file) {
                continue;
            }
            delete.validate_positions(file, rows)?;
            match &delete.content {
                deletes::Content::Positions(_) => positions.push(delete_index),
                deletes::Content::Equality { fields, .. } => {
                    let mut columns = Vec::new();
                    for field in fields {
                        let column = deletes::find_id(
                            physical.as_ref(),
                            field.id.expect("equality field ID"),
                        )?;
                        let column = column
                            .map(|index| {
                                let column = physical.field(index);
                                if !schema::compatible(
                                    column.data_type(),
                                    field.field.data_type(),
                                    true,
                                ) {
                                    return Err(failed(
                                        "data and equality-delete field types are incompatible",
                                    ));
                                }
                                Ok(column.name().clone())
                            })
                            .transpose()?;
                        columns.push(column);
                    }
                    equalities.push((delete_index, columns));
                }
            }
        }
        Ok(Self {
            outputs,
            equalities,
            positions,
            _credit: credit,
        })
    }

    pub(crate) fn projection(
        &self,
        physical: &SchemaRef,
        requested: &[usize],
    ) -> Result<Vec<usize>> {
        let mut needed = std::collections::BTreeSet::new();
        for name in requested
            .iter()
            .filter_map(|&i| self.outputs[i].as_ref())
            .chain(
                self.equalities
                    .iter()
                    .flat_map(|(_, names)| names.iter().flatten()),
            )
        {
            needed.insert(physical.index_of(name).map_err(failed)?);
        }
        if needed.is_empty() && !physical.fields().is_empty() {
            needed.insert(0);
        }
        Ok(needed.into_iter().collect())
    }

    pub(crate) fn cost(
        snapshot: &Snapshot,
        file_index: usize,
        mut cost: WorkCost,
    ) -> Result<WorkCost> {
        cost.fixed_bytes = cost
            .fixed_bytes
            .checked_mul(2)
            .ok_or_else(|| failed("table batch cost overflow"))?;
        cost.bytes_per_unit = cost
            .bytes_per_unit
            .checked_mul(2)
            .and_then(|n| n.checked_add(32))
            .ok_or_else(|| failed("table batch cost overflow"))?;
        for value in snapshot.files[file_index].partitions.values() {
            let width = super::metadata::scalar_bytes(value);
            cost.bytes_per_unit = cost
                .bytes_per_unit
                .checked_add(
                    width
                        .checked_mul(4)
                        .ok_or_else(|| failed("partition width overflow"))?,
                )
                .ok_or_else(|| failed("table batch cost overflow"))?;
        }
        Ok(cost)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(crate) fn apply(
        &self,
        snapshot: &Snapshot,
        file_index: usize,
        input: OwnedBatch,
        target: SchemaRef,
        requested: &[usize],
        first_physical_row: u64,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<OwnedBatch> {
        let file = &snapshot.files[file_index];
        let rows = input.num_rows();
        let extra = file.partitions.values().try_fold(32_u64, |sum, value| {
            sum.checked_add(super::metadata::scalar_bytes(value) * 4)
                .ok_or_else(|| failed("partition capacity overflow"))
        })?;
        let capacity = (input.get_array_memory_size() as u64)
            .checked_mul(4)
            .and_then(|n| {
                n.checked_add((rows as u64).checked_mul(extra + requested.len() as u64 * 64)?)
            })
            .and_then(|n| n.checked_add(8192 + requested.len() as u64 * 4096))
            .ok_or_else(|| failed("table batch capacity overflow"))?;
        let mut credit = memory.reserve(capacity)?;
        let mut selected = Vec::new();
        selected.try_reserve_exact(rows).map_err(failed)?;
        let mut key = Vec::new();
        for row in 0..rows {
            if row % 1024 == 0 {
                cancellation.check()?;
            }
            let position = first_physical_row
                .checked_add(row as u64)
                .ok_or_else(|| failed("physical table row position overflow"))?;
            let mut deleted = self
                .positions
                .iter()
                .any(|&index| snapshot.deletes[index].position_deleted(file, position));
            if !deleted {
                for (index, columns) in &self.equalities {
                    deletes::key_batch(&input, columns, row, &mut key)?;
                    let deletes::Content::Equality { keys, .. } = &snapshot.deletes[*index].content
                    else {
                        unreachable!("equality mapping")
                    };
                    if keys.contains(&key) {
                        deleted = true;
                        break;
                    }
                }
            }
            if !deleted {
                selected.push(row);
            }
        }
        let mut columns = Vec::new();
        columns.try_reserve_exact(requested.len()).map_err(failed)?;
        for &index in requested {
            let field = &snapshot.fields[index].field;
            let column = if let Some(value) = file.partitions.get(field.name()) {
                constant(field.data_type(), value, rows)?
            } else if let Some(name) = &self.outputs[index] {
                let column = input
                    .column_by_name(name)
                    .ok_or_else(|| failed("projected table field was not decoded"))?;
                widen(column, field.data_type())?
            } else {
                arrow_array::new_null_array(field.data_type(), rows)
            };
            let column = if selected.len() == rows {
                column
            } else {
                take(&column, &selected)?
            };
            if !field.is_nullable() && column.null_count() != 0 {
                return Err(failed("table field violates its required/non-null schema"));
            }
            columns.push(column);
        }
        let batch = RecordBatch::try_new_with_options(
            target,
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(selected.len())),
        )
        .map_err(failed)?;
        credit.resize(batch.get_array_memory_size() as u64)?;
        let (_, owner) = input.into_parts();
        let output = OwnedBatch::with_owner(batch, Arc::new(credit));
        cancellation.check()?;
        Ok(if let Some(owner) = owner {
            output.retain(owner)
        } else {
            output
        })
    }
}

fn widen(column: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    use arrow_array::{Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array};
    if column.data_type() == target {
        return Ok(Arc::clone(column));
    }
    match (column.data_type(), target) {
        (DataType::Int32, DataType::Int64) => Ok(Arc::new(Int64Array::from_iter(
            column
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("dtype checked")
                .iter()
                .map(|v| v.map(i64::from)),
        ))),
        (DataType::Float32, DataType::Float64) => Ok(Arc::new(Float64Array::from_iter(
            column
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("dtype checked")
                .iter()
                .map(|v| v.map(f64::from)),
        ))),
        (DataType::Decimal128(_, _), DataType::Decimal128(precision, scale)) => Ok(Arc::new(
            column
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("dtype checked")
                .clone()
                .with_precision_and_scale(*precision, *scale)
                .map_err(failed)?,
        )),
        _ => Err(failed("unadmitted table field conversion")),
    }
}

fn take(column: &ArrayRef, indices: &[usize]) -> Result<ArrayRef> {
    let data = column.to_data();
    let mut target =
        arrow_data::transform::MutableArrayData::new(vec![&data], false, indices.len());
    let mut offset = 0;
    while offset < indices.len() {
        let begin = indices[offset];
        let mut end = begin + 1;
        offset += 1;
        while offset < indices.len() && indices[offset] == end {
            end += 1;
            offset += 1;
        }
        target.try_extend(0, begin, end).map_err(failed)?;
    }
    Ok(arrow_array::make_array(target.freeze()))
}

fn constant(dtype: &DataType, value: &ScalarValue, rows: usize) -> Result<ArrayRef> {
    use arrow_array::{
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, new_null_array,
    };
    if value.is_null() {
        return Ok(new_null_array(dtype, rows));
    }
    macro_rules! integer {
        ($ty:ty, $native:ty) => {
            if let ScalarValue::Int64(value) = value {
                let value = <$native>::try_from(*value).map_err(failed)?;
                return Ok(Arc::new(<$ty>::from_iter_values(std::iter::repeat_n(
                    value, rows,
                ))));
            }
        };
    }
    match dtype {
        DataType::Int8 => {
            integer!(Int8Array, i8);
        }
        DataType::Int16 => {
            integer!(Int16Array, i16);
        }
        DataType::Int32 => {
            integer!(Int32Array, i32);
        }
        DataType::Int64 => {
            integer!(Int64Array, i64);
        }
        DataType::Float32 => {
            if let ScalarValue::Float64(value) = value {
                #[allow(clippy::cast_possible_truncation)]
                // The partition parser already admitted f32 precision.
                let value = *value as f32;
                return Ok(Arc::new(Float32Array::from_iter_values(
                    std::iter::repeat_n(value, rows),
                )));
            }
        }
        DataType::Float64 => {
            if let ScalarValue::Float64(value) = value {
                return Ok(Arc::new(Float64Array::from_iter_values(
                    std::iter::repeat_n(*value, rows),
                )));
            }
        }
        _ => {}
    }
    let mut builder = arrow_array::builder::make_builder(dtype, rows);
    for _ in 0..rows {
        crate::universal_format_io::append_scalar_to_arrow_builder(
            builder.as_mut(),
            dtype,
            value,
            "partition",
            "local table",
        )?;
    }
    Ok(builder.finish())
}
