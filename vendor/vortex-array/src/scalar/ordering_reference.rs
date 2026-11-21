// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Original recursive ordering from 9d8ba1b28; shallow test reference only.

use crate::dtype::{DType, StructFields};
use crate::scalar::ScalarValue;
use std::cmp::Ordering;

/// Compare two optional scalar values using `dtype` for nested tuple interpretation.
pub(super) fn partial_cmp_scalar_values(
    dtype: &DType,
    lhs: Option<&ScalarValue>,
    rhs: Option<&ScalarValue>,
) -> Option<Ordering> {
    match (lhs, rhs) {
        (None, None) => Some(Ordering::Equal),
        (None, Some(_)) => Some(Ordering::Less),
        (Some(_), None) => Some(Ordering::Greater),
        (Some(lhs), Some(rhs)) => partial_cmp_non_null_scalar_values(dtype, lhs, rhs),
    }
}

/// Compare two non-null scalar values, consulting `dtype` only for tuple-backed values.
fn partial_cmp_non_null_scalar_values(
    dtype: &DType,
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> Option<Ordering> {
    // `Scalar::validate` guarantees that a scalar's value matches its dtype. Most of the scalar
    // value variants have only 1 method of comparison, regardless of the dtype.
    match (lhs, rhs) {
        (ScalarValue::Bool(lhs), ScalarValue::Bool(rhs)) => lhs.partial_cmp(rhs),
        (ScalarValue::Primitive(lhs), ScalarValue::Primitive(rhs)) => lhs.partial_cmp(rhs),
        (ScalarValue::Decimal(lhs), ScalarValue::Decimal(rhs)) => lhs.partial_cmp(rhs),
        (ScalarValue::Utf8(lhs), ScalarValue::Utf8(rhs)) => lhs.partial_cmp(rhs),
        (ScalarValue::Binary(lhs), ScalarValue::Binary(rhs)) => lhs.partial_cmp(rhs),
        // `Tuple` is the exception here. Since it backs lists, fixed-size lists, and structs, we
        // need the dtype to know whether children share one element dtype or use per-field dtypes.
        (ScalarValue::Tuple(lhs), ScalarValue::Tuple(rhs)) => {
            partial_cmp_tuple_values(dtype, lhs, rhs)
        }
        (ScalarValue::Union(lhs), ScalarValue::Union(rhs)) => {
            if lhs.type_id() != rhs.type_id() {
                return None;
            }

            let DType::Union(variants, _) = dtype else {
                return None;
            };
            let child_index = variants.tag_to_child_index(lhs.type_id())?;
            let child_dtype = variants.variant_by_index(child_index)?;

            partial_cmp_scalar_values(&child_dtype, lhs.child_value(), rhs.child_value())
        }
        // Variant values can have a different dtype in each row, so it doesn't make sense to
        // compare them.
        (ScalarValue::Variant(_), ScalarValue::Variant(_)) => None,
        _ => None,
    }
}

/// Compare tuple values according to the list, fixed-size list, or struct dtype layout.
fn partial_cmp_tuple_values(
    dtype: &DType,
    lhs: &[Option<ScalarValue>],
    rhs: &[Option<ScalarValue>],
) -> Option<Ordering> {
    match dtype {
        DType::List(element_dtype, _) | DType::FixedSizeList(element_dtype, ..) => {
            partial_cmp_list_values(element_dtype, lhs, rhs)
        }
        DType::Struct(fields, _) => partial_cmp_struct_values(fields, lhs, rhs),
        // A map compares as the list of its `{key, value}` entry structs.
        DType::Map(map_dtype, _) => partial_cmp_list_values(&map_dtype.entries_dtype(), lhs, rhs),
        DType::Extension(ext_dtype) => {
            partial_cmp_tuple_values(ext_dtype.storage_dtype(), lhs, rhs)
        }
        _ => None,
    }
}

/// Compare list tuple values using the shared element dtype for each element.
fn partial_cmp_list_values(
    element_dtype: &DType,
    lhs: &[Option<ScalarValue>],
    rhs: &[Option<ScalarValue>],
) -> Option<Ordering> {
    for (lhs, rhs) in lhs.iter().zip(rhs.iter()) {
        match partial_cmp_scalar_values(element_dtype, lhs.as_ref(), rhs.as_ref())? {
            Ordering::Equal => continue,
            ordering => return Some(ordering),
        }
    }

    Some(lhs.len().cmp(&rhs.len()))
}

/// Compare struct tuple values using each field's dtype in field order.
fn partial_cmp_struct_values(
    fields: &StructFields,
    lhs: &[Option<ScalarValue>],
    rhs: &[Option<ScalarValue>],
) -> Option<Ordering> {
    if lhs.len() != fields.nfields() || rhs.len() != fields.nfields() {
        return None;
    }

    for ((field_dtype, lhs), rhs) in fields.field_dtypes().zip(lhs.iter()).zip(rhs.iter()) {
        match partial_cmp_scalar_values(field_dtype, lhs.as_ref(), rhs.as_ref())? {
            Ordering::Equal => continue,
            ordering => return Some(ordering),
        }
    }

    Some(Ordering::Equal)
}
