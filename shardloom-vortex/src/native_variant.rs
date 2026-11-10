//! Admission for the pinned native Parquet Variant provider.
//!
//! The provider owns the format and typed values. This boundary bounds its
//! recursive validation before invoking it and rejects ambiguous object keys.

use parquet_variant::{Variant, VariantMetadata};
use shardloom_core::{Result, ShardLoomError};
use vortex::array::dtype::DType;

#[cfg(feature = "universal-format-io")]
#[path = "native_variant_arrow.rs"]
pub(crate) mod arrow;

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
    // The upstream full validator recursively descends without a depth/work
    // limit. Inspect only container framing first; the provider still performs
    // complete byte/metadata/type validation. A valid ordinary tree has fewer
    // nodes than bytes. Repeated aliases cannot amplify validation exponentially.
    let mut remaining = value.len();
    envelope(value, 0, &mut remaining, check)?;
    let metadata = VariantMetadata::try_new(metadata).map_err(failed)?;
    let value = Variant::try_new_with_metadata(metadata, value).map_err(failed)?;
    unique_keys(&value, check)?;
    Ok(value)
}

fn integer(bytes: &[u8], start: usize, width: usize) -> Result<usize> {
    let end = start
        .checked_add(width)
        .ok_or_else(|| failed("offset overflow"))?;
    let bytes = bytes
        .get(start..end)
        .ok_or_else(|| failed("truncated container framing"))?;
    Ok(bytes.iter().enumerate().fold(0, |value, (shift, byte)| {
        value | (usize::from(*byte) << (shift * 8))
    }))
}

fn envelope(
    bytes: &[u8],
    depth: usize,
    remaining: &mut usize,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    check()?;
    if depth > 24 {
        return Err(failed("encoded value exceeds the current depth boundary"));
    }
    *remaining = remaining
        .checked_sub(1)
        .ok_or_else(|| failed("validation work exceeds its encoded byte envelope"))?;
    let header = *bytes.first().ok_or_else(|| failed("empty encoded value"))?;
    let basic = header & 3;
    if basic < 2 {
        return Ok(());
    }
    let object = basic == 2;
    let count_width = if header & if object { 64 } else { 16 } != 0 {
        4
    } else {
        1
    };
    let count = integer(bytes, 1, count_width)?;
    let offset_width = usize::from((header >> 2) & 3) + 1;
    let id_width = if object {
        usize::from((header >> 4) & 3) + 1
    } else {
        0
    };
    let offsets = count
        .checked_mul(id_width)
        .and_then(|size| size.checked_add(1 + count_width))
        .ok_or_else(|| failed("container framing overflow"))?;
    let data = count
        .checked_add(1)
        .and_then(|count| count.checked_mul(offset_width))
        .and_then(|size| size.checked_add(offsets))
        .ok_or_else(|| failed("container framing overflow"))?;
    let values = bytes
        .get(data..)
        .ok_or_else(|| failed("truncated container framing"))?;
    for index in 0..count {
        let start = integer(bytes, offsets + index * offset_width, offset_width)?;
        // Object field offsets are in key order, not storage order. Match the
        // provider's suffix access; list offsets bound each child individually.
        let end = if object {
            values.len()
        } else {
            integer(bytes, offsets + (index + 1) * offset_width, offset_width)?
        };
        let child = values
            .get(start..end)
            .ok_or_else(|| failed("invalid child offsets"))?;
        envelope(child, depth + 1, remaining, check)?;
    }
    Ok(())
}

fn unique_keys(value: &Variant<'_, '_>, check: &dyn Fn() -> Result<()>) -> Result<()> {
    check()?;
    match value {
        Variant::Object(object) => {
            let mut previous = None;
            for (name, child) in object.iter() {
                // The provider has established sorted keys, but its unsorted
                // metadata branch currently accepts adjacent duplicate names.
                if previous == Some(name) {
                    return Err(failed("object contains duplicate field names"));
                }
                previous = Some(name);
                unique_keys(&child, check)?;
            }
        }
        Variant::List(list) => {
            for child in list.iter() {
                unique_keys(&child, check)?;
            }
        }
        _ => {}
    }
    Ok(())
}

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
