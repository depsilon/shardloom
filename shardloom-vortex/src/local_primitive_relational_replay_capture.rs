//! Copy small physical fragments straight into bounded native record buffers.
//! No source array or foreign buffer survives a call to append.

use super::{
    ArrayRef, CopyStats, NativeExecutionContext, ReservedVec, Result, checked_add, failed,
};
use crate::{
    local_primitives::{result_batch, vortex_error},
    owned_buffers,
};
use vortex::{
    array::{
        IntoArray,
        arrays::bool::BoolArrayExt as _,
        arrays::struct_::StructArrayExt as _,
        arrays::{Bool, Primitive, Struct, StructArray},
        dtype::{DType, PType},
        memory::{HostMemorySessionExt as _, WritableHostBuffer},
        validity::Validity,
    },
    buffer::{Alignment, BitBuffer, ByteBuffer},
};

struct Column {
    dtype: DType,
    values: WritableHostBuffer,
    validity: Option<WritableHostBuffer>,
}

pub(super) struct Capture {
    dtype: DType,
    columns: ReservedVec<Column>,
    capacity: usize,
    rows: usize,
}

fn bytes(dtype: &DType, rows: usize) -> Option<usize> {
    match dtype {
        DType::Primitive(ptype, _) if *ptype != PType::F16 => rows.checked_mul(ptype.byte_width()),
        DType::Bool(_) => Some(rows.div_ceil(8)),
        _ => None,
    }
}

pub(super) fn admitted(array: &ArrayRef) -> bool {
    let Some(record) = array.as_opt::<Struct>() else {
        return false;
    };
    let width = record.struct_fields().nfields();
    if array.dtype().is_nullable() || width == 0 || width > 128 {
        return false;
    }
    (0..width).all(|index| {
        let field = record.unmasked_field(index);
        if field.len() != array.len()
            || !field.is_host()
            || bytes(field.dtype(), array.len()).is_none()
            || !(field.is::<Primitive>() || field.is::<Bool>())
        {
            return false;
        }
        match field.validity() {
            Ok(Validity::NonNullable | Validity::AllValid | Validity::AllInvalid) => true,
            Ok(Validity::Array(valid)) => {
                valid.is::<Bool>()
                    && valid.is_host()
                    && !valid.dtype().is_nullable()
                    && valid.len() == array.len()
            }
            _ => false,
        }
    })
}

impl Capture {
    pub(super) fn new(
        dtype: &DType,
        capacity: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        let fields = dtype
            .as_struct_fields_opt()
            .ok_or_else(|| failed("capture schema changed"))?;
        let schema_bytes = crate::native_payload_schema::schema_bytes(dtype)?;
        let estimate = fields
            .fields()
            .try_fold(schema_bytes.saturating_mul(2), |sum, dtype| {
                let data =
                    bytes(&dtype, capacity).ok_or_else(|| failed("capture dtype changed"))?;
                sum.checked_add(data as u64)
                    .and_then(|sum| sum.checked_add(capacity.div_ceil(8) as u64 + 1024))
                    .ok_or_else(|| failed("capture capacity overflow"))
            })?;
        let snapshot = context.memory().snapshot();
        if estimate > snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes) / 8
            || estimate > 256 * 1024
        {
            return Ok(None);
        }
        let metadata = crate::native_payload_schema::reserve_schema(dtype, context.memory())?;
        let allocator =
            owned_buffers::with_credit(context.native_session().host_allocator(), metadata);
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(fields.nfields())?;
        for dtype in fields.fields() {
            let length = bytes(&dtype, capacity).ok_or_else(|| failed("capture dtype changed"))?;
            let mut values = allocator
                .allocate(length, Alignment::new(8))
                .map_err(vortex_error)?;
            if matches!(dtype, DType::Bool(_)) {
                values.as_mut_slice().fill(0);
            }
            let validity = if dtype.is_nullable() {
                let mut valid = allocator
                    .allocate(capacity.div_ceil(8), Alignment::none())
                    .map_err(vortex_error)?;
                valid.as_mut_slice().fill(0);
                Some(valid)
            } else {
                None
            };
            columns.values.push(Column {
                dtype,
                values,
                validity,
            });
        }
        Ok(Some(Self {
            dtype: dtype.clone(),
            columns,
            capacity,
            rows: 0,
        }))
    }

    pub(super) fn remaining(&self) -> usize {
        self.capacity - self.rows
    }

    pub(super) fn append(
        &mut self,
        array: &ArrayRef,
        start: usize,
        count: usize,
        report: &mut CopyStats,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if array.dtype() != &self.dtype
            || count > self.remaining()
            || start.checked_add(count).is_none_or(|end| end > array.len())
        {
            return Err(failed("capture fragment changed its admitted domain"));
        }
        let record = array.as_::<Struct>();
        for (index, output) in self.columns.values.iter_mut().enumerate() {
            context.check_cancelled()?;
            let source = record.unmasked_field(index);
            let before =
                bytes(&output.dtype, self.rows).ok_or_else(|| failed("capture dtype changed"))?;
            let after = bytes(&output.dtype, self.rows + count)
                .ok_or_else(|| failed("capture dtype changed"))?;
            if let DType::Primitive(ptype, _) = output.dtype {
                let primitive = source.as_::<Primitive>();
                let data = vortex::array::match_each_native_ptype!(ptype, |T| {
                    primitive.to_buffer::<T>().into_byte_buffer()
                });
                let width = ptype.byte_width();
                output.values.as_mut_slice()[before..after]
                    .copy_from_slice(&data.as_slice()[start * width..(start + count) * width]);
            } else {
                let bits = source.as_::<Bool>().to_bit_buffer();
                for row in 0..count {
                    set(
                        output.values.as_mut_slice(),
                        self.rows + row,
                        bits.value(start + row),
                    );
                }
            }
            checked_add(&mut report.buffer_bytes, (after - before) as u64)?;
            if let Some(output) = &mut output.validity {
                let valid = source.validity().map_err(vortex_error)?;
                for row in 0..count {
                    let present = match &valid {
                        Validity::NonNullable | Validity::AllValid => true,
                        Validity::AllInvalid => false,
                        Validity::Array(array) => {
                            array.as_::<Bool>().to_bit_buffer().value(start + row)
                        }
                    };
                    set(output.as_mut_slice(), self.rows + row, present);
                }
                checked_add(
                    &mut report.buffer_bytes,
                    ((self.rows + count).div_ceil(8) - self.rows.div_ceil(8)) as u64,
                )?;
            }
        }
        if self.rows == 0 {
            checked_add(&mut report.batches, 1)?;
        }
        self.rows += count;
        checked_add(&mut report.rows, count as u64)?;
        report.max_batch_rows = report.max_batch_rows.max(self.rows);
        #[cfg(all(test, debug_assertions))]
        crate::local_primitives::native_relational_observation::record(|counts| {
            counts.replay_captured_fragments += 1;
            counts.replay_captured_rows += count as u64;
        });
        context.check_cancelled()
    }

    pub(super) fn finish(self, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
        context.check_cancelled()?;
        let mut children = ReservedVec::new(context.memory())?;
        children.reserve(self.columns.values.len())?;
        let (columns, _ownership) = self.columns.into_parts();
        for column in columns {
            let validity = column
                .validity
                .map(|valid| {
                    boolean(valid.freeze(), self.rows, Validity::NonNullable).map(Validity::Array)
                })
                .transpose()?
                .unwrap_or(Validity::NonNullable);
            let length =
                bytes(&column.dtype, self.rows).ok_or_else(|| failed("capture dtype changed"))?;
            let data = column.values.freeze().slice(0..length);
            let array = if let DType::Primitive(ptype, _) = column.dtype {
                result_batch::primitive(ptype, data, validity)?
            } else {
                boolean(data, self.rows, validity)?
            };
            children.values.push(array);
        }
        let (children, _ownership) = children.into_parts();
        StructArray::try_new(
            self.dtype
                .as_struct_fields_opt()
                .expect("admitted capture schema")
                .names()
                .clone(),
            children,
            self.rows,
            Validity::NonNullable,
        )
        .map(IntoArray::into_array)
        .map_err(vortex_error)
    }
}

fn set(bytes: &mut [u8], row: usize, value: bool) {
    let mask = 1 << (row % 8);
    if value {
        bytes[row / 8] |= mask;
    } else {
        bytes[row / 8] &= !mask;
    }
}

fn boolean(bytes: ByteBuffer, rows: usize, validity: Validity) -> Result<ArrayRef> {
    owned_buffers::bool_array_with_retained_buffer(BitBuffer::new(bytes, rows), validity)
        .map(IntoArray::into_array)
        .map_err(vortex_error)
}
