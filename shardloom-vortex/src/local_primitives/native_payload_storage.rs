//! Bit-preserving private movement of measures before their observation boundary.

use super::{
    ArrayRef, CopyPolicy, DType, HostAllocatorRef, NativeExecutionContext, Result, check, failed,
    vortex_error,
};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use vortex::{
    array::{
        ExecutionCtx, IntoArray as _, VortexSessionExecute as _,
        arrays::{
            BoolArray, Chunked, Constant, Decimal, DecimalArray, Dict, Masked, Primitive,
            PrimitiveArray, Slice,
            chunked::ChunkedArrayExt as _,
            decimal::DecimalArrayExt as _,
            dict::DictArraySlotsExt as _,
            masked::{MaskedArrayExt as _, MaskedArraySlotsExt as _},
            slice::SliceArraySlotsExt as _,
        },
        dtype::{PType, i256},
        matcher::Matcher,
        scalar::{DecimalValue, Scalar, ScalarValue},
        validity::Validity,
    },
    buffer::{Alignment, BitBuffer, Buffer, ByteBuffer},
};

pub(super) fn copy(
    source: &ArrayRef,
    rows: &[Option<usize>],
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let (bytes, stride) = match dtype {
        DType::Primitive(PType::F32, _) => (
            source
                .as_::<Primitive>()
                .to_buffer::<f32>()
                .into_byte_buffer(),
            4,
        ),
        DType::Primitive(PType::F64, _) => (
            source
                .as_::<Primitive>()
                .to_buffer::<f64>()
                .into_byte_buffer(),
            8,
        ),
        _ => {
            return Err(failed("private storage copy requires floating measures"));
        }
    };
    let (data, validity) = copy_buffers(source, &bytes, stride, rows, dtype, context, allocator)?;
    Ok(match dtype {
        DType::Primitive(PType::F32, _) => {
            PrimitiveArray::new(Buffer::<f32>::from_byte_buffer(data), validity).into_array()
        }
        DType::Primitive(PType::F64, _) => {
            PrimitiveArray::new(Buffer::<f64>::from_byte_buffer(data), validity).into_array()
        }
        _ => return Err(failed("private measure dtype changed during copying")),
    })
}

/// Chunked take uses the provider's precision-sized `DecimalBuilder`, which may
/// narrow an unobserved malformed value and panic. Decimal scalar construction
/// also validates precision. Resolve existing native selection wrappers and read
/// the typed buffer instead. Public copies validate selected values with the
/// fallible scalar constructor before constructing credited storage; private
/// movement preserves storage without evaluating an unobserved measure.
pub(super) fn decimal(
    source: Option<&ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    policy: CopyPolicy,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let mut execution = context.native_session().create_execution_ctx();
    decimal_with(
        source,
        rows.iter().copied(),
        dtype,
        policy,
        &mut DecimalWork {
            memory: context.memory(),
            cancellation: context.cancellation(),
            execution: &mut execution,
        },
        allocator,
    )
}

/// Key owners retain raw decimal storage until a comparison or reduction observes
/// a value. The generic canonical builder can narrow before that boundary.
pub(in crate::local_primitives) fn decimal_key(
    source: &ArrayRef,
    execution: &mut ExecutionCtx,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<DecimalArray> {
    cancellation.check()?;
    let array = source
        .clone()
        .execute_until::<DecimalStorage>(execution)
        .map_err(vortex_error)?;
    if array.dtype() != source.dtype() || array.len() != source.len() {
        return Err(failed(
            "decimal storage execution changed dtype or row count",
        ));
    }
    if let Some(decimal) = array.as_opt::<Decimal>() {
        return Ok(decimal.into_owned());
    }
    let allocator = crate::owned_buffers::with_credit(
        execution.allocator(),
        memory.reserve(super::metadata_bytes(source.dtype())?)?,
    );
    let array = decimal_with(
        Some(&array),
        (0..array.len()).map(Some),
        array.dtype(),
        CopyPolicy::PreserveUnobserved,
        &mut DecimalWork {
            memory,
            cancellation,
            execution,
        },
        &allocator,
    )?;
    Ok(array.as_::<Decimal>().into_owned())
}

struct DecimalWork<'a> {
    memory: &'a LiveMemoryPool,
    cancellation: &'a CancellationToken,
    execution: &'a mut ExecutionCtx,
}

fn decimal_with(
    source: Option<&ArrayRef>,
    rows: impl ExactSizeIterator<Item = Option<usize>>,
    dtype: &DType,
    policy: CopyPolicy,
    work: &mut DecimalWork<'_>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let DType::Decimal(decimal, _) = dtype else {
        return Err(failed("private decimal copy changed dtype"));
    };
    if policy == CopyPolicy::ValidateValues
        && !crate::native_payload_schema::admitted_decimal(*decimal)
    {
        return Err(failed(
            "decimal payload exceeds admitted precision or scale",
        ));
    }
    let length = rows.len();
    let mut values = super::ReservedVec::new(work.memory)?;
    values.reserve(length)?;
    let mut valid = super::ReservedVec::new(work.memory)?;
    valid.reserve(length)?;
    let mut wide = false;
    for (row, original) in rows.enumerate() {
        if row.is_multiple_of(1024) {
            work.cancellation.check()?;
        }
        let value = if let Some(original) = original {
            let source = source.ok_or_else(|| failed("private decimal source is absent"))?;
            decimal_value(source, original, work.cancellation, work.execution)?
        } else if dtype.is_nullable() {
            None
        } else {
            Some(DecimalValue::I128(0))
        };
        if policy == CopyPolicy::ValidateValues {
            Scalar::try_new(dtype.clone(), value.map(ScalarValue::Decimal))
                .map_err(vortex_error)?;
        }
        valid.values.push(value.is_some());
        let value = value.unwrap_or(DecimalValue::I128(0));
        wide |= value.cast::<i128>().is_none();
        values.values.push(value);
    }
    let validity = super::validity(dtype, &valid.values, allocator)?;
    let stride = if wide { 32 } else { 16 };
    let bytes = length
        .checked_mul(stride)
        .ok_or_else(|| failed("private decimal byte length overflow"))?;
    let mut data = allocator
        .allocate(bytes, Alignment::new(stride))
        .map_err(vortex_error)?;
    for (row, value) in values.values.iter().enumerate() {
        if row.is_multiple_of(1024) {
            work.cancellation.check()?;
        }
        let output = &mut data.as_mut_slice()[row * stride..(row + 1) * stride];
        if wide {
            // Pinned Vortex i256 transparently wraps Arrow's repr(C) low/high
            // fields. Each limb is copied in host order, including on big endian.
            let (low, high) = value.as_i256().to_parts();
            output[..16].copy_from_slice(&low.to_ne_bytes());
            output[16..].copy_from_slice(&high.to_ne_bytes());
        } else {
            output.copy_from_slice(
                &value
                    .cast::<i128>()
                    .ok_or_else(|| failed("private decimal width changed during copying"))?
                    .to_ne_bytes(),
            );
        }
    }
    let output = if wide {
        DecimalArray::try_new(
            Buffer::<i256>::from_byte_buffer(data.freeze()),
            *decimal,
            validity,
        )
    } else {
        DecimalArray::try_new(
            Buffer::<i128>::from_byte_buffer(data.freeze()),
            *decimal,
            validity,
        )
    };
    output
        .map(vortex::array::IntoArray::into_array)
        .map_err(vortex_error)
}

struct DecimalStorage;

impl Matcher for DecimalStorage {
    type Match<'a> = ();
    fn try_match(array: &ArrayRef) -> Option<()> {
        (array.as_opt::<Decimal>().is_some()
            || array.as_opt::<Chunked>().is_some()
            || array.as_opt::<Dict>().is_some()
            || array.as_opt::<Masked>().is_some()
            || array.as_opt::<Slice>().is_some()
            || array.as_opt::<Constant>().is_some())
        .then_some(())
    }
}

fn decimal_value(
    source: &ArrayRef,
    mut row: usize,
    cancellation: &CancellationToken,
    execution: &mut ExecutionCtx,
) -> Result<Option<DecimalValue>> {
    let mut source = source.clone();
    loop {
        cancellation.check()?;
        if row >= source.len() {
            return Err(failed("private decimal index exceeds its native owner"));
        }
        source = source
            .execute_until::<DecimalStorage>(execution)
            .map_err(vortex_error)?;
        if let Some(array) = source.as_opt::<Decimal>() {
            if !array
                .validity()
                .map_err(vortex_error)?
                .execute_is_valid(row, execution)
                .map_err(vortex_error)?
            {
                return Ok(None);
            }
            return Ok(Some(vortex::array::match_each_decimal_value_type!(
                array.values_type(),
                |D| { DecimalValue::from(array.buffer::<D>()[row]) }
            )));
        }
        if let Some(array) = source.as_opt::<Chunked>() {
            let (chunk, offset) = array.find_chunk_idx(row).map_err(vortex_error)?;
            row = offset;
            source = array.chunk(chunk).clone();
        } else if let Some(array) = source.as_opt::<Dict>() {
            let index = array
                .codes()
                .execute_scalar(row, execution)
                .map_err(vortex_error)?;
            if index.is_null() {
                return Ok(None);
            }
            row = index
                .as_primitive()
                .as_::<usize>()
                .ok_or_else(|| failed("private decimal dictionary index is invalid"))?;
            source = array.values().clone();
        } else if let Some(array) = source.as_opt::<Masked>() {
            if !array
                .masked_validity()
                .execute_is_valid(row, execution)
                .map_err(vortex_error)?
            {
                return Ok(None);
            }
            source = array.child().clone();
        } else if let Some(array) = source.as_opt::<Slice>() {
            row = row
                .checked_add(array.slice_range().start)
                .ok_or_else(|| failed("private decimal slice offset overflow"))?;
            source = array.child().clone();
        } else if let Some(array) = source.as_opt::<Constant>() {
            return match array.scalar().value() {
                Some(ScalarValue::Decimal(value)) => Ok(Some(*value)),
                None => Ok(None),
                _ => Err(failed("private decimal constant changed scalar type")),
            };
        } else {
            return Err(failed(
                "private decimal execution did not produce native decimal storage",
            ));
        }
    }
}

fn copy_buffers(
    source: &ArrayRef,
    bytes: &ByteBuffer,
    stride: usize,
    rows: &[Option<usize>],
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<(ByteBuffer, Validity)> {
    let length = rows
        .len()
        .checked_mul(stride)
        .ok_or_else(|| failed("private measure byte length overflow"))?;
    if source.len() != rows.len() || bytes.len() != length {
        return Err(failed("private measure selection changed its row count"));
    }
    let mut execution = context.native_session().create_execution_ctx();
    let valid = source
        .validity()
        .map_err(vortex_error)?
        .execute_mask(rows.len(), &mut execution)
        .map_err(vortex_error)?;
    let mut data = allocator
        .allocate(length, Alignment::new(stride))
        .map_err(vortex_error)?;
    data.as_mut_slice().fill(0);
    let mut validity = dtype
        .is_nullable()
        .then(|| {
            allocator
                .allocate(rows.len().div_ceil(8), Alignment::new(1))
                .map_err(vortex_error)
        })
        .transpose()?;
    if let Some(bits) = &mut validity {
        bits.as_mut_slice().fill(0);
    }
    for (row, original) in rows.iter().enumerate() {
        check(row, context)?;
        let present = original.is_some() && valid.value(row);
        if present {
            let range = row * stride..(row + 1) * stride;
            data.as_mut_slice()[range.clone()].copy_from_slice(&bytes[range]);
        }
        let output_valid = present || (original.is_none() && !dtype.is_nullable());
        if output_valid {
            if let Some(bits) = &mut validity {
                bits.as_mut_slice()[row / 8] |= 1 << (row % 8);
            }
        } else if !dtype.is_nullable() {
            return Err(failed("null in a nonnullable private measure"));
        }
    }
    let validity = validity.map_or(Validity::NonNullable, |bits| {
        Validity::Array(
            BoolArray::new(
                BitBuffer::new(bits.freeze(), rows.len()),
                Validity::NonNullable,
            )
            .into_array(),
        )
    });
    Ok((data.freeze(), validity))
}
