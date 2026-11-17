// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Original a67af167c scalar initializers, with only function/type names adapted.

use crate::dtype::DType;
use crate::scalar::{DecimalValue, PValue, Scalar, ScalarValue, UnionValue};
use vortex_buffer::{BufferString, ByteBuffer};
use vortex_error::VortexExpect;

pub(super) fn zero(dtype: &DType) -> Option<ScalarValue> {
    Some(match dtype {
        DType::Null => return None,
        DType::Bool(_) => ScalarValue::Bool(false),
        DType::Primitive(ptype, _) => ScalarValue::Primitive(PValue::zero(ptype)),
        DType::Decimal(dt, ..) => ScalarValue::Decimal(DecimalValue::zero(dt)),
        DType::Utf8(_) => ScalarValue::Utf8(BufferString::empty()),
        DType::Binary(_) => ScalarValue::Binary(ByteBuffer::empty()),
        DType::List(..) => ScalarValue::Tuple(vec![]),
        DType::Map(..) => ScalarValue::Tuple(vec![]),
        DType::FixedSizeList(edt, size, _) => {
            let elements = (0..*size)
                .map(|_| zero(edt).map(Some))
                .collect::<Option<Vec<_>>>()?;
            ScalarValue::Tuple(elements)
        }
        DType::Struct(fields, _) => {
            let field_values = fields
                .fields()
                .map(|f| zero(&f).map(Some))
                .collect::<Option<Vec<_>>>()?;
            ScalarValue::Tuple(field_values)
        }
        DType::Union(variants, _) => {
            let child_dtype = variants
                .variant_by_index(0)
                .vortex_expect("union must have at least one variant");
            let child_value = zero(&child_dtype)?;

            ScalarValue::Union(UnionValue::new(
                variants.child_index_to_tag(0),
                Some(child_value),
            ))
        }
        DType::Variant(_) => ScalarValue::Variant(Box::new(Scalar::null(DType::Null))),
        DType::Extension(ext_dtype) => {
            // Since we have no way to define a "zero" extension value (since we have no idea
            // what the semantics of the extension is), a best effort attempt is to just use the
            // zero storage value and try to make an extension scalar from that.
            zero(ext_dtype.storage_dtype())?
        }
    })
}

pub(super) fn default(dtype: &DType) -> Option<Option<ScalarValue>> {
    if dtype.is_nullable() {
        return Some(None);
    }

    let value = match dtype {
        DType::Null => return Some(None),
        DType::Bool(_) => ScalarValue::Bool(false),
        DType::Primitive(ptype, _) => ScalarValue::Primitive(PValue::zero(ptype)),
        DType::Decimal(dt, ..) => ScalarValue::Decimal(DecimalValue::zero(dt)),
        DType::Utf8(_) => ScalarValue::Utf8(BufferString::empty()),
        DType::Binary(_) => ScalarValue::Binary(ByteBuffer::empty()),
        DType::List(..) => ScalarValue::Tuple(vec![]),
        DType::Map(..) => ScalarValue::Tuple(vec![]),
        DType::FixedSizeList(edt, size, _) => {
            let elements = (0..*size)
                .map(|_| default(edt))
                .collect::<Option<Vec<_>>>()?;
            ScalarValue::Tuple(elements)
        }
        DType::Struct(fields, _) => {
            let field_values = fields
                .fields()
                .map(|field| default(&field))
                .collect::<Option<Vec<_>>>()?;
            ScalarValue::Tuple(field_values)
        }
        DType::Union(variants, _) => {
            let child_dtype = variants
                .variant_by_index(0)
                .vortex_expect("union must have at least one variant");
            let child_value = default(&child_dtype)?;

            ScalarValue::Union(UnionValue::new(variants.child_index_to_tag(0), child_value))
        }
        DType::Variant(_) => ScalarValue::Variant(Box::new(Scalar::null(DType::Null))),
        DType::Extension(ext_dtype) => {
            // Since we have no way to define a "default" extension value (since we have no idea
            // what the semantics of the extension is), a best effort attempt is to just use the
            // default storage value and try to make an extension scalar from that.
            default(ext_dtype.storage_dtype())??
        }
    };

    Some(Some(value))
}
