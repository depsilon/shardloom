//! Borrow already resident flat values without executing or validating an
//! unselected domain. Join validation still observes each selected value at
//! its original stage. Unknown encodings retain the ordinary native producer.

#[cfg(test)]
#[path = "native_relational_borrowed_keys_tests.rs"]
mod tests;

use super::{
    KeyColumn, NativeNumericOwner, VariableColumn, VariableDomain, VariableHashing, failed,
    vortex_error,
};
use crate::{local_primitives::result_batch::Value, resident_session::NativeExecutionContext};
use shardloom_core::Result;
use std::borrow::Cow;
use std::ops::Range;
use vortex::array::{
    ArrayRef, VortexSessionExecute as _,
    arrays::{
        Bool, Chunked, Dict, Primitive, Slice, VarBin, VarBinView,
        chunked::ChunkedArrayExt as _,
        dict::DictArraySlotsExt as _,
        slice::{SliceArraySlotsExt as _, SliceReduce},
        varbin::{VarBinArrayExt as _, VarBinArraySlotsExt as _},
        varbinview::VarBinViewArrayExt as _,
    },
    dtype::{DType, PType},
    validity::Validity,
};

fn resident_validity(validity: &Validity) -> bool {
    match validity {
        Validity::NonNullable | Validity::AllValid | Validity::AllInvalid => true,
        Validity::Array(array) => array.is_host() && array.is::<Bool>(),
    }
}

/// Native memory scans retain metadata with single-chunk wrappers and bounded
/// slices. Resolve those coordinates without invoking an unknown provider.
fn resident_leaf(array: &ArrayRef) -> Option<(ArrayRef, Range<usize>)> {
    let mut leaf = array.clone();
    let mut range = 0..array.len();
    for _ in 0..16 {
        if range.end > leaf.len() {
            return None;
        }
        if let Some(slice) = leaf.as_opt::<Slice>() {
            range = range.start.checked_add(slice.slice_range().start)?
                ..range.end.checked_add(slice.slice_range().start)?;
            leaf = slice.child().clone();
        } else if let Some(chunks) = leaf.as_opt::<Chunked>() {
            if chunks.nchunks() != 1 {
                return None;
            }
            leaf = chunks.chunk(0).clone();
        } else {
            return Some((leaf, range));
        }
    }
    None
}

impl KeyColumn {
    /// This admission performs no value scan, dictionary-code scan, decode or
    /// deferred provider evaluation. A NULL parent cannot be silently ignored:
    /// callers pass the logical field, including its mask, to this method.
    pub(in crate::local_primitives) fn try_borrow(
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        context.check_cancelled()?;
        let Some((leaf, range)) = resident_leaf(array) else {
            return Ok(None);
        };
        if !leaf.is_host() {
            return Ok(None);
        }
        let mut execution = context.native_session().create_execution_ctx();
        let column = match array.dtype() {
            DType::Null => Self::Null(array.len()),
            DType::Primitive(..) => {
                let Some(values) = leaf.as_opt::<Primitive>() else {
                    return Ok(None);
                };
                if values.ptype() == PType::F16
                    || !resident_validity(&values.validity().map_err(vortex_error)?)
                {
                    return Ok(None);
                }
                let selected = <Primitive as SliceReduce>::slice(values, range)
                    .map_err(vortex_error)?
                    .ok_or_else(|| failed("resident primitive slice declined"))?;
                Self::Numeric(NativeNumericOwner::new_with_float_policy(
                    selected
                        .as_opt::<Primitive>()
                        .ok_or_else(|| failed("resident primitive slice changed encoding"))?
                        .into_owned(),
                    &mut execution,
                    context.float_policy(),
                )?)
            }
            DType::Bool(_) => {
                let Some(values) = leaf.as_opt::<Bool>() else {
                    return Ok(None);
                };
                let validity = values.validity().map_err(vortex_error)?;
                if !resident_validity(&validity) {
                    return Ok(None);
                }
                let selected = <Bool as SliceReduce>::slice(values, range)
                    .map_err(vortex_error)?
                    .ok_or_else(|| failed("resident boolean slice declined"))?;
                let values = selected
                    .as_opt::<Bool>()
                    .ok_or_else(|| failed("resident boolean slice changed encoding"))?;
                let valid = values
                    .validity()
                    .map_err(vortex_error)?
                    .execute_mask(values.len(), &mut execution)
                    .map_err(vortex_error)?;
                Self::Boolean {
                    values: values.into_owned(),
                    valid,
                }
            }
            DType::Utf8(_) | DType::Binary(_) => {
                let dictionary = leaf.as_opt::<Dict>();
                let (domain, domain_range) = match &dictionary {
                    Some(dict) => match resident_leaf(dict.values()) {
                        Some(values) => values,
                        None => return Ok(None),
                    },
                    None => (leaf.clone(), range.clone()),
                };
                let binary = matches!(array.dtype(), DType::Binary(_));
                let Some((values, valid)) =
                    variable_domain(&domain, domain_range, binary, context)?
                else {
                    return Ok(None);
                };
                let codes = if let Some(dictionary) = dictionary {
                    let source = dictionary.codes().slice(range).map_err(vortex_error)?;
                    let Some(Self::Numeric(codes)) = Self::try_borrow(&source, context)? else {
                        return Ok(None);
                    };
                    if !codes.is_integer() {
                        return Ok(None);
                    }
                    Some(codes)
                } else {
                    None
                };
                Self::Variable(VariableColumn {
                    values,
                    valid,
                    codes,
                    hashes: VariableHashing::OnDemand,
                    rows: array.len(),
                    binary,
                })
            }
            _ => return Ok(None),
        };
        Ok(Some(column))
    }

    /// Only observed, first-occurrence group payloads are copied. Variable
    /// values borrow their original domain during the existing output builder.
    pub(in crate::local_primitives) fn borrowed_value(&self, row: usize) -> Result<Value<'_>> {
        if let Self::Numeric(values) = self {
            // Key identity normalizes positive signed integers to unsigned.
            // Retained group payloads must keep their original declared type.
            return Ok(match values.distinct_value(row)? {
                crate::local_primitives::AggregateDistinctValue::Null => Value::Null,
                crate::local_primitives::AggregateDistinctValue::Int64(value) => Value::Int(value),
                crate::local_primitives::AggregateDistinctValue::UInt64(value) => {
                    Value::UInt(value)
                }
                crate::local_primitives::AggregateDistinctValue::Float64Bits(bits) => {
                    Value::Float(f64::from_bits(bits))
                }
                _ => return Err(failed("borrowed numeric group changed value domain")),
            });
        }
        if let Self::Variable(values) = self {
            return match values.index(row)? {
                None => Ok(Value::Null),
                Some(index) if values.binary => Ok(Value::Binary(Cow::Borrowed(
                    values.values.borrowed_bytes(index)?,
                ))),
                Some(index) => Ok(Value::Text(Cow::Borrowed(values.values.utf8_at(index)?))),
            };
        }
        Ok(match self.raw_cell(row)? {
            super::Cell::Null => Value::Null,
            super::Cell::NegativeInteger(value) => Value::Int(value),
            super::Cell::NonnegativeInteger(value) => Value::UInt(value),
            super::Cell::Float(bits) => Value::Float(f64::from_bits(bits)),
            super::Cell::Boolean(value) => Value::Bool(value),
            _ => return Err(failed("borrowed group value has an unadmitted domain")),
        })
    }
}

fn variable_domain(
    domain: &ArrayRef,
    range: Range<usize>,
    binary: bool,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<(VariableDomain, vortex::mask::Mask)>> {
    if !domain.is_host() {
        return Ok(None);
    }
    let mut execution = context.native_session().create_execution_ctx();
    if let Some(values) = domain.as_opt::<VarBinView>() {
        if (!binary && !values.has_validated_utf8())
            || !resident_validity(&values.varbinview_validity())
        {
            return Ok(None);
        }
        let selected = <VarBinView as SliceReduce>::slice(values, range)
            .map_err(vortex_error)?
            .ok_or_else(|| failed("resident variable slice declined"))?;
        let values = selected
            .as_opt::<VarBinView>()
            .ok_or_else(|| failed("resident variable slice changed encoding"))?;
        let valid = values
            .varbinview_validity()
            .execute_mask(values.len(), &mut execution)
            .map_err(vortex_error)?;
        return Ok(Some((VariableDomain::Views(values.into_owned()), valid)));
    }
    let Some(values) = domain.as_opt::<VarBin>() else {
        return Ok(None);
    };
    let validity = values.varbin_validity();
    if !resident_validity(&validity)
        || !values.bytes_handle().is_on_host()
        || !values.offsets().dtype().is_int()
        || values.offsets().dtype().is_nullable()
    {
        return Ok(None);
    }
    let source = values
        .offsets()
        .slice(
            range.start
                ..range
                    .end
                    .checked_add(1)
                    .ok_or_else(|| failed("variable offset range overflow"))?,
        )
        .map_err(vortex_error)?;
    let Some(KeyColumn::Numeric(offsets)) = KeyColumn::try_borrow(&source, context)? else {
        return Ok(None);
    };
    let valid = validity
        .slice(range.clone())
        .map_err(vortex_error)?
        .execute_mask(range.len(), &mut execution)
        .map_err(vortex_error)?;
    Ok(VariableDomain::try_offsets(
        offsets,
        values.bytes().clone(),
        range.len(),
        context.memory(),
    )?
    .map(|values| (values, valid)))
}
