//! Original binary-object classifier for shallow comparison.

use super::{DType, VortexResult, storage};

pub(crate) fn contains(dtype: &DType) -> VortexResult<bool> {
    Ok(match dtype {
        DType::Extension(_) => storage(dtype).is_some(),
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains(child)?,
        DType::Struct(fields, _) => {
            for child in fields.try_field_dtypes() {
                if contains(child?)? {
                    return Ok(true);
                }
            }
            false
        }
        _ => false,
    })
}
