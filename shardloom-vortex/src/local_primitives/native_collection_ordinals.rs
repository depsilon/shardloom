//! Native lists carry selected source positions through resident and stored windows.

use super::{
    native_capacity::ReservedVec,
    native_collection_result, native_list, native_payload,
    native_relational_batch::failed,
    native_relational_expression::keys,
    native_relational_keys::Cell,
    result_batch::{self, Value},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{
    ArrayRef, IntoArray, VortexSessionExecute as _,
    arrays::ListArray,
    dtype::{DType, Nullability, PType},
    memory::HostMemorySessionExt as _,
    validity::Validity,
};

/// One flat ordinal owner and one offset per output row, independent of the
/// number of frames. Native list objects are built only for delivered batches.
pub(super) struct Resident {
    rows: ReservedVec<Option<(usize, usize)>>,
    positions: ReservedVec<usize>,
}

impl Resident {
    pub(super) fn new(rows: usize, memory: &LiveMemoryPool) -> Result<Self> {
        let mut output = ReservedVec::new(memory)?;
        output.reserve(rows)?;
        output.values.resize(rows, None);
        Ok(Self {
            rows: output,
            positions: ReservedVec::new(memory)?,
        })
    }

    pub(super) fn set(&mut self, row: usize, positions: &[usize]) -> Result<()> {
        let start = self.positions.values.len();
        self.positions.reserve(positions.len())?;
        self.positions.values.extend_from_slice(positions);
        self.rows.values[row] = (!positions.is_empty()).then_some((start, positions.len()));
        Ok(())
    }

    pub(super) fn output(
        &self,
        range: std::ops::Range<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        column(
            range.len(),
            |row| {
                self.rows.values[range.start + row]
                    .map(|(start, len)| &self.positions.values[start..start + len])
            },
            context,
        )
    }
}

pub(super) fn column<'a>(
    rows: usize,
    mut value: impl FnMut(usize) -> Option<&'a [usize]>,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let mut positions = ReservedVec::new(context.memory())?;
    let mut offsets = ReservedVec::new(context.memory())?;
    let mut valid = ReservedVec::new(context.memory())?;
    offsets.reserve(
        rows.checked_add(1)
            .ok_or_else(|| failed("collection offset count overflow"))?,
    )?;
    valid.reserve(rows)?;
    offsets.values.push(0);
    for row in 0..rows {
        context.check_cancelled()?;
        let selected = value(row).unwrap_or(&[]);
        positions.reserve(selected.len())?;
        positions.values.extend_from_slice(selected);
        offsets.values.push(positions.values.len());
        valid.values.push(!selected.is_empty());
    }
    let allocator = crate::owned_buffers::with_credit(
        context.native_session().host_allocator(),
        context
            .memory()
            .reserve(native_payload::metadata_bytes(&dtype())?)?,
    );
    let integer = DType::Primitive(PType::U64, Nullability::NonNullable);
    let elements = result_batch::build_column_with_float_policy(
        &integer,
        positions.values.len(),
        &allocator,
        context.float_policy(),
        |row| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            Ok(Value::UInt(
                u64::try_from(positions.values[row]).map_err(vortex_error)?,
            ))
        },
    )?;
    let offsets = result_batch::build_column_with_float_policy(
        &integer,
        offsets.values.len(),
        &allocator,
        context.float_policy(),
        |row| {
            Ok(Value::UInt(
                u64::try_from(offsets.values[row]).map_err(vortex_error)?,
            ))
        },
    )?;
    let validity = Validity::Array(result_batch::build_column_with_float_policy(
        &DType::Bool(Nullability::NonNullable),
        rows,
        &allocator,
        context.float_policy(),
        |row| Ok(Value::Bool(valid.values[row])),
    )?);
    let array = ListArray::try_new(
        elements,
        offsets,
        validity,
        &mut context.native_session().create_execution_ctx(),
    )
    .map(IntoArray::into_array)
    .map_err(vortex_error)?;
    crate::owned_buffers::retain_empty_array_credit(array, &allocator).map_err(vortex_error)
}

pub(super) fn dtype() -> DType {
    DType::List(
        DType::Primitive(PType::U64, Nullability::NonNullable).into(),
        Nullability::Nullable,
    )
}

#[cfg(feature = "vortex-write")]
pub(super) fn single(
    rows: &[usize],
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let values = result_batch::build_column_with_float_policy(
        &DType::Primitive(PType::U64, Nullability::NonNullable),
        rows.len(),
        &context.native_session().host_allocator(),
        context.float_policy(),
        |row| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            Ok(Value::UInt(u64::try_from(rows[row]).map_err(vortex_error)?))
        },
    )?;
    native_collection_result::build(values, &dtype(), None, context).map(Some)
}

fn transform(
    source: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
    mut output: impl FnMut(&mut [Option<usize>]) -> Result<Option<ArrayRef>>,
) -> Result<ArrayRef> {
    let mut execution = context.native_session().create_execution_ctx();
    let (elements, coordinates) =
        native_list::Column::new(source.clone(), &mut execution)?.into_indexed(&mut execution)?;
    let elements = keys(&elements, context)?;
    let mut selected = ReservedVec::new(context.memory())?;
    let mut results = ReservedVec::new(context.memory())?;
    results.reserve(source.len())?;
    for row in 0..source.len() {
        context.check_cancelled()?;
        selected.values.clear();
        let result = if let Some((start, count)) = coordinates.at(row)? {
            selected.reserve(count)?;
            for index in start..start + count {
                if index.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let Cell::NonnegativeInteger(value) = elements.raw_cell(index)? else {
                    return Err(failed(
                        "window collection position is not an unsigned integer",
                    ));
                };
                selected
                    .values
                    .push(Some(usize::try_from(value).map_err(vortex_error)?));
            }
            output(&mut selected.values)?
        } else {
            None
        };
        results.values.push(result);
    }
    native_payload::retained_column(dtype, source.len(), context, |row| {
        Ok(results.values[row].clone())
    })
}

#[cfg(feature = "vortex-write")]
pub(super) fn map(
    source: &ArrayRef,
    mut map: impl FnMut(usize) -> Result<usize>,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    transform(source, &dtype(), context, |rows| {
        let mut mapped = ReservedVec::new(context.memory())?;
        mapped.reserve(rows.len())?;
        for (index, row) in rows.iter().enumerate() {
            if index.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            mapped.values.push(map(
                row.ok_or_else(|| failed("collection position is null"))?
            )?);
        }
        single(&mapped.values, context)
    })
}

pub(super) fn gather(
    source: &ArrayRef,
    dtype: &DType,
    separator: Option<&str>,
    mut gather: impl FnMut(&[Option<usize>], &DType) -> Result<ArrayRef>,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let value_dtype = match dtype {
        DType::List(child, _) => child.as_ref(),
        dtype => dtype,
    };
    transform(source, dtype, context, |rows| {
        let values = gather(rows, value_dtype)?;
        native_collection_result::build(values, dtype, separator, context).map(Some)
    })
}
