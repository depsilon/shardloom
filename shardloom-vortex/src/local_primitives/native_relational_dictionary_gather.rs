//! Keep selected text dictionaries in independently credited receiving buffers.

use crate::{
    local_primitives::{
        AggregateDistinctValue, NativeNumericOwner, native_capacity::ReservedVec,
        native_relational_batch::failed, result_batch, vortex_error,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::{
    Result,
    native_io_decisions::{
        NativeIoCapability, NativeIoDecisionOutcome, NativeIoDecisionReason, NativeIoObservedWork,
    },
};
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::{Dict, DictArray, Primitive, PrimitiveArray, dict::DictArraySlotsExt as _},
    dtype::{DType, PType},
    memory::{HostAllocatorRef, HostMemorySessionExt as _},
};

#[cfg(test)]
#[path = "native_relational_dictionary_gather_tests.rs"]
mod tests;

const UNSEEN: usize = usize::MAX;
const METADATA_BYTES: u64 = 2048;

fn record(
    context: &NativeExecutionContext<'_>,
    rows: usize,
    outcome: NativeIoDecisionOutcome,
    reason: NativeIoDecisionReason,
) -> Result<()> {
    context.record_capability_decision(
        NativeIoCapability::RepresentationRetention,
        outcome,
        reason,
        NativeIoObservedWork {
            input_rows: Some(rows as u64),
            output_rows: (outcome == NativeIoDecisionOutcome::Executed).then_some(rows as u64),
            ..NativeIoObservedWork::default()
        },
    )
}

pub(super) fn try_copy(
    array: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    if !matches!(dtype, DType::Utf8(_)) || indices.is_empty() {
        return Ok(None);
    }
    let Some(dictionary) = array.as_opt::<Dict>() else {
        return Ok(None);
    };
    let decline = |reason| {
        record(
            context,
            indices.len(),
            NativeIoDecisionOutcome::Declined,
            reason,
        )?;
        Ok(None)
    };
    if array.dtype().as_nonnullable() != dtype.as_nonnullable()
        || (!dtype.is_nullable() && (array.dtype().is_nullable() || indices.dtype().is_nullable()))
        || !matches!(indices.dtype(), DType::Primitive(PType::U64, _))
        || !indices.is::<Primitive>()
        || !indices.is_host()
    {
        return decline(NativeIoDecisionReason::RequiredValueEvaluation);
    }
    let domain = dictionary.values();
    if domain.len() >= indices.len() {
        return decline(NativeIoDecisionReason::RequiredRepresentationCopy);
    }
    let Some(metadata) = context.memory().try_reserve(METADATA_BYTES)? else {
        return decline(NativeIoDecisionReason::InsufficientHeadroom);
    };
    let Some(mut mapping) = ReservedVec::try_with_capacity(context.memory(), domain.len())? else {
        return decline(NativeIoDecisionReason::InsufficientHeadroom);
    };
    let Some(mut referenced) = ReservedVec::try_with_capacity(context.memory(), domain.len())?
    else {
        return decline(NativeIoDecisionReason::InsufficientHeadroom);
    };
    mapping.values.resize(domain.len(), UNSEEN);
    let selected = Selection::new(dictionary.codes(), indices, domain.len(), context)?;
    for row in 0..indices.len() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        let Some(code) = selected.code(row)? else {
            continue;
        };
        if mapping.values[code] != UNSEEN {
            continue;
        }
        mapping.values[code] = referenced.values.len();
        referenced.values.push(code);
    }
    // Select before canonicalization, exactly as the existing row copy does.
    // Executing the whole domain first could validate unused malformed text.
    // First-reference order preserves the selected-value error order.
    let domain_indices = super::index_array(referenced.values.len(), false, context, |slot| {
        Ok(Some(referenced.values[slot]))
    })?;
    let mut execution = context.native_session().create_execution_ctx();
    let domain_values = super::select_column(domain, &domain_indices, dtype, &mut execution)?;
    let values = result_batch::VariableValues::try_new(&domain_values, &mut execution)?
        .ok_or_else(|| failed("dictionary gather requires selected native text values"))?;
    let allocator =
        crate::owned_buffers::with_credit(context.native_session().host_allocator(), metadata);
    let output_values = result_batch::build_column_with_float_policy(
        dtype,
        referenced.values.len(),
        &allocator,
        context.float_policy(),
        |slot| {
            if slot.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            values.value(slot)
        },
    )?;
    let output_codes = selected.copy_codes(dtype, &mapping.values, &allocator, context)?;
    let output = DictArray::try_new(output_codes, output_values)
        .map_err(vortex_error)?
        .into_array();
    context.check_cancelled()?;
    record(
        context,
        indices.len(),
        NativeIoDecisionOutcome::Executed,
        NativeIoDecisionReason::OwnedRepresentation,
    )?;
    Ok(Some(output))
}

struct Selection {
    indices: NativeNumericOwner,
    codes: NativeNumericOwner,
    domain: usize,
}

impl Selection {
    fn new(
        source_codes: &ArrayRef,
        indices: &ArrayRef,
        domain: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let mut execution = context.native_session().create_execution_ctx();
        let indices = NativeNumericOwner::new(
            indices
                .as_opt::<Primitive>()
                .expect("admitted indices")
                .into_owned(),
            &mut execution,
        )?;
        // Match the native selection boundary: all coordinates precede value
        // access. Never interpret invalid selection or codes as an optional miss.
        for (row, &position) in indices.primitive().as_slice::<u64>().iter().enumerate() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if indices.validity().value(row)
                && usize::try_from(position).map_or(true, |position| position >= source_codes.len())
            {
                return Err(failed("gather index exceeds native input"));
            }
        }
        let codes = source_codes
            .clone()
            .execute::<PrimitiveArray>(&mut execution)
            .map_err(vortex_error)?;
        if codes.dtype() != source_codes.dtype() || codes.len() != source_codes.len() {
            return Err(failed(
                "dictionary code execution changed dtype or row count",
            ));
        }
        Ok(Self {
            indices,
            codes: NativeNumericOwner::new(codes, &mut execution)?,
            domain,
        })
    }

    fn code(&self, row: usize) -> Result<Option<usize>> {
        if !self.indices.validity().value(row) {
            return Ok(None);
        }
        let source = usize::try_from(self.indices.primitive().as_slice::<u64>()[row])
            .map_err(vortex_error)?;
        let code = match self.codes.distinct_value(source)? {
            AggregateDistinctValue::Null => return Ok(None),
            AggregateDistinctValue::UInt64(code) => usize::try_from(code).map_err(vortex_error)?,
            AggregateDistinctValue::Int64(code) => usize::try_from(code).map_err(vortex_error)?,
            _ => return Err(failed("dictionary codes must be integers")),
        };
        if code >= self.domain {
            return Err(failed("dictionary code exceeds its value domain"));
        }
        Ok(Some(code))
    }

    fn copy_codes(
        &self,
        dtype: &DType,
        mapping: &[usize],
        allocator: &HostAllocatorRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let code_type = self.codes.primitive().ptype();
        let code_dtype = DType::Primitive(code_type, dtype.nullability());
        macro_rules! copy {
            ($type:ty) => {
                result_batch::build_numeric_column::<$type>(
                    &code_dtype,
                    self.indices.primitive().len(),
                    allocator,
                    |row| {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        self.code(row)?
                            .map(|code| mapping[code])
                            .map(<$type>::try_from)
                            .transpose()
                            .map_err(vortex_error)
                    },
                )
            };
        }
        match code_type {
            PType::U8 => copy!(u8),
            PType::U16 => copy!(u16),
            PType::U32 => copy!(u32),
            PType::U64 => copy!(u64),
            PType::I8 => copy!(i8),
            PType::I16 => copy!(i16),
            PType::I32 => copy!(i32),
            PType::I64 => copy!(i64),
            _ => Err(failed("dictionary codes must be integers")),
        }
    }
}
