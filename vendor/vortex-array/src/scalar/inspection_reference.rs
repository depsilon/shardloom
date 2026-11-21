// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Original a67af167c scalar inspection, with recursive calls redirected here.

use crate::dtype::DType;
use crate::scalar::Scalar;
use vortex_error::vortex_panic;

fn variant_is_zero(scalar: &Scalar) -> Option<bool> {
    let inner = scalar.as_variant().value()?;
    if inner.is_null() {
        Some(true)
    } else {
        is_zero(inner)
    }
}

pub(super) fn is_zero(scalar: &Scalar) -> Option<bool> {
    let value = scalar.value()?;

    let is_zero = match scalar.dtype() {
        DType::Null => {
            vortex_panic!("non-null value somehow had `DType::Null`")
        }
        DType::Bool(_) => !value.as_bool(),
        DType::Primitive(..) => value.as_primitive().is_zero(),
        DType::Decimal(..) => value.as_decimal().is_zero(),
        DType::Utf8(_) => value.as_utf8().is_empty(),
        DType::Binary(_) => value.as_binary().is_empty(),
        DType::List(..) => value.as_list().is_empty(),
        DType::Map(..) => scalar.as_map().is_empty(),
        // A fixed-size list is zero only if it has the expected number of elements and every
        // element is itself a non-null zero value.1
        DType::FixedSizeList(_, list_size, _) => {
            let list = scalar.as_list();
            list.len() == *list_size as usize
                && (0..list.len())
                    .all(|i| list.element(i).is_some_and(|e| is_zero(&e) == Some(true)))
        }
        // A struct is zero only if every one of its fields is itself a non-null zero value.
        DType::Struct(..) => scalar
            .as_struct()
            .fields_iter()
            .is_some_and(|mut fields| fields.all(|f| is_zero(&f) == Some(true))),
        // Only the first variant of unions can be zero. Its child determines whether the
        // zero-ness is true, false, or undefined.
        DType::Union(..) => {
            let union = scalar.as_union();
            if union.child_index() != Some(0) {
                false
            } else {
                union.child().and_then(|child| is_zero(&child))?
            }
        }
        DType::Variant(_) => variant_is_zero(scalar)?,
        DType::Extension(_) => is_zero(&scalar.as_extension().to_storage_scalar())?,
    };

    Some(is_zero)
}

pub(super) fn approx_nbytes(scalar: &Scalar) -> usize {
    use crate::dtype::NativeDecimalType;
    use crate::dtype::i256;

    match scalar.dtype() {
        DType::Null => 0,
        DType::Bool(_) => 1,
        DType::Primitive(ptype, _) => ptype.byte_width(),
        DType::Decimal(dt, _) => {
            if dt.precision() <= i128::MAX_PRECISION {
                size_of::<i128>()
            } else {
                size_of::<i256>()
            }
        }
        DType::Utf8(_) => scalar
            .value()
            .map_or_else(|| 0, |value| value.as_utf8().len()),
        DType::Binary(_) => scalar
            .value()
            .map_or_else(|| 0, |value| value.as_binary().len()),
        DType::List(..) | DType::FixedSizeList(..) => scalar
            .as_list()
            .elements()
            .map(|fields| fields.into_iter().map(|f| approx_nbytes(&f)).sum::<usize>())
            .unwrap_or_default(),
        DType::Map(..) => scalar
            .as_map()
            .entries()
            .map(|(key, value)| approx_nbytes(&key) + approx_nbytes(&value))
            .sum(),
        DType::Struct(..) => scalar
            .as_struct()
            .fields_iter()
            .map(|fields| fields.into_iter().map(|f| approx_nbytes(&f)).sum::<usize>())
            .unwrap_or_default(),
        DType::Union(..) => scalar
            .as_union()
            .child()
            .map_or(0, |value| 1 + approx_nbytes(&value)),
        DType::Variant(_) => scalar.as_variant().value().map_or(0, approx_nbytes),
        DType::Extension(_) => approx_nbytes(&scalar.as_extension().to_storage_scalar()),
    }
}
