//! Partition fills share ordinary window ordering, input keys and output owners.

use super::{DType, Nullability, PType, Result, failed, field, kernel, key, validity_key};
use crate::relational_query::{
    VortexRelationalFillMethod as Method, VortexRelationalWindowExpression,
};

pub(super) fn bind(
    column: &str,
    method: Method,
    expression: &VortexRelationalWindowExpression,
    fields: &[(String, DType)],
    window: &mut kernel::Spec,
) -> Result<(kernel::fill::Spec, DType)> {
    if expression.frame.is_some() || expression.order_by.len() != 1 {
        return Err(failed(
            "temporal filling requires one ORDER BY time key and no explicit window frame",
        ));
    }
    let order = &expression.order_by[0];
    let time_dtype = field(fields, order.column.as_str())?;
    if crate::native_temporal::temporal_dtype(time_dtype).is_none() {
        return Err(failed(
            "temporal filling requires an explicitly typed temporal ordering key",
        ));
    }
    let time = key(window, fields, order.column.as_str())?;
    let source = field(fields, column)?;
    let (value, dtype) = if method == Method::Linear {
        if !matches!(
            source,
            DType::Primitive(_, _) | DType::Decimal(_, _) | DType::Null
        ) {
            return Err(failed(
                "linear interpolation requires numeric values; cast other domains explicitly",
            ));
        }
        (
            key(window, fields, column)?,
            DType::Primitive(PType::F64, Nullability::Nullable),
        )
    } else {
        // Preserve nested payloads without evaluating their children just to
        // decide whether the whole value is missing.
        (validity_key(window, fields, column)?, source.as_nullable())
    };
    Ok((
        kernel::fill::Spec {
            method,
            time,
            value,
            descending: order.descending,
        },
        dtype,
    ))
}
