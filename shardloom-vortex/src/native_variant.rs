//! Admission for the pinned native Parquet Variant provider.
//!
//! The provider owns the format and typed values. This boundary bounds its
//! validation work before invoking it and rejects ambiguous object keys.

use parquet_variant::{Variant, VariantMetadata};
use shardloom_core::{Result, ShardLoomError};
use vortex::array::dtype::DType;

#[cfg(feature = "universal-format-io")]
#[path = "native_variant_arrow.rs"]
pub(crate) mod arrow;

#[cfg(feature = "vortex-local-primitives")]
#[path = "native_variant_expression_report.rs"]
mod expression_report;
#[cfg(feature = "vortex-local-primitives")]
pub(crate) use expression_report::ExpressionWork;

/// Call after the shared schema walk has bounded recursion and metadata.
pub(crate) fn contains(dtype: &DType) -> bool {
    match dtype {
        DType::Variant(_) => true,
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains(child),
        DType::Struct(fields, _) => fields.fields().any(|child| contains(&child)),
        _ => false,
    }
}

pub(crate) fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native Variant payload: {message}; no fallback execution was attempted"
    ))
}

pub(crate) fn workspace(bytes: usize) -> Result<u64> {
    u64::try_from(bytes)
        .ok()
        .and_then(|bytes| bytes.checked_mul(64))
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| failed("validation workspace overflow"))
}

pub(crate) fn value<'m, 'v>(
    metadata: &'m [u8],
    value: &'v [u8],
    check: &dyn Fn() -> Result<()>,
) -> Result<Variant<'m, 'v>> {
    raw::value(metadata, value, check, 24)
}

#[path = "native_variant_raw.rs"]
mod raw;

#[cfg(test)]
#[path = "native_variant_reference.rs"]
mod reference;

#[cfg(test)]
#[path = "native_variant_raw_tests.rs"]
mod raw_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_variant_raw_validation_bounds_depth_and_rejects_duplicate_object_names() {
        let metadata = [1, 0, 0];
        let mut encoded = vec![0];
        for _ in 0..24 {
            let mut list = vec![3, 1, 0, u8::try_from(encoded.len()).unwrap()];
            list.extend(encoded);
            encoded = list;
        }
        assert!(value(&metadata, &encoded, &|| Ok(())).is_ok());
        let mut too_deep = vec![3, 1, 0, u8::try_from(encoded.len()).unwrap()];
        too_deep.extend(encoded);
        assert!(
            value(&metadata, &too_deep, &|| Ok(()))
                .unwrap_err()
                .to_string()
                .contains("depth")
        );
        let duplicate_metadata = [1, 2, 0, 1, 2, b'a', b'a'];
        let duplicate_object = [2, 2, 0, 1, 0, 1, 2, 0, 0];
        assert!(value(&duplicate_metadata, &duplicate_object, &|| Ok(())).is_err());
        for malformed in [&[][..], &[3][..], &[3, 1, 0, 1][..], &[2, 2, 0][..]] {
            assert!(value(&metadata, malformed, &|| Ok(())).is_err());
        }
        assert!(value(&metadata, &[0], &|| Err(failed("cancelled"))).is_err());
    }
}
