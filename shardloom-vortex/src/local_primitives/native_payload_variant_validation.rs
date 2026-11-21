//! Validate selected encoded values before any infallible Variant accessor.

use super::{NativeExecutionContext, Result, check, failed, vortex_error};
use crate::native_variant::workspace;
use parquet_variant::{Variant, VariantMetadata};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, VortexSessionExecute as _,
        arrays::{StructArray, struct_::StructArrayExt as _},
        dtype::{DType, PType},
    },
    buffer::ByteBuffer,
    encodings::parquet_variant::ParquetVariantArrayExt,
};

pub(in crate::local_primitives::native_payload) fn validate(
    source: &impl ParquetVariantArrayExt,
    rows: &[Option<usize>],
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let mut execution = context.native_session().create_execution_ctx();
    for (ordinal, row) in rows.iter().enumerate() {
        check(ordinal, context)?;
        let Some(row) = row else {
            continue;
        };
        if !source
            .parquet_variant_validity()
            .execute_is_valid(*row, &mut execution)
            .map_err(vortex_error)?
        {
            continue;
        }
        let metadata = binary(source.metadata(), *row, &mut execution)?
            .ok_or_else(|| failed("present Variant requires nonnull metadata"))?;
        let _credit = context.memory().reserve(workspace(metadata.len())?)?;
        VariantMetadata::try_new(metadata.as_slice()).map_err(vortex_error)?;
        parts(
            metadata.as_slice(),
            source.value(),
            source.typed_value(),
            *row,
            &mut execution,
            context,
            0,
        )?;
    }
    context.check_cancelled()
}

pub(super) fn binary(
    array: &ArrayRef,
    row: usize,
    execution: &mut ExecutionCtx,
) -> Result<Option<ByteBuffer>> {
    if !matches!(array.dtype(), DType::Binary(_)) {
        return Err(failed("Variant encoded child requires binary storage"));
    }
    Ok(array
        .execute_scalar(row, execution)
        .map_err(vortex_error)?
        .as_binary()
        .value()
        .cloned())
}

#[allow(clippy::too_many_arguments)]
fn parts(
    metadata: &[u8],
    value: Option<&ArrayRef>,
    typed: Option<&ArrayRef>,
    row: usize,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    depth: usize,
) -> Result<bool> {
    walk::parts(metadata, value, typed, row, execution, context, depth, 24)
}

pub(super) fn leaf(typed: &ArrayRef, row: usize, execution: &mut ExecutionCtx) -> Result<()> {
    match typed.dtype() {
        DType::Bool(_)
        | DType::Binary(_)
        | DType::Primitive(
            PType::I8 | PType::I16 | PType::I32 | PType::I64 | PType::F32 | PType::F64,
            _,
        ) => Ok(()),
        DType::Utf8(_) | DType::Decimal(..) => {
            crate::local_primitives::result_batch::scalar(typed, row, execution).map(drop)
        }
        DType::Extension(_) if shredded_temporal(typed.dtype()) => {
            let scalar = crate::local_primitives::result_batch::scalar(typed, row, execution)?;
            let value = scalar
                .as_primitive()
                .as_::<i64>()
                .ok_or_else(|| failed("invalid Variant temporal storage"))?;
            crate::native_temporal::validate_ticks(typed.dtype(), value)
        }
        _ => Err(failed(
            "Variant shredded child has an unsupported physical dtype",
        )),
    }
}

fn shredded_temporal(dtype: &DType) -> bool {
    use shardloom_core::temporal::{TemporalKind, TemporalUnit};
    crate::native_temporal::temporal_dtype(dtype).is_some_and(|dtype| {
        matches!(
            dtype.kind(),
            TemporalKind::Date32
                | TemporalKind::Time(TemporalUnit::Microseconds)
                | TemporalKind::Timestamp {
                    unit: TemporalUnit::Microseconds | TemporalUnit::Nanoseconds,
                    ..
                }
        )
    })
}

#[path = "native_payload_variant_validation_walk.rs"]
pub(super) mod walk;

#[cfg(test)]
#[path = "native_payload_variant_validation_reference.rs"]
pub(super) mod reference;
