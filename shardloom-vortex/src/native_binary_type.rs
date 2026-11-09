//! Versioned native binary-object descriptor storage.
//!
//! The extension's Binary cells contain descriptors, not payload bytes. Inline
//! Binary remains the representation for inline bytes; this type only admits
//! descriptor storage and does not resolve or validate descriptor contents.

use std::{
    fmt,
    hash::{Hash, Hasher},
};
use vortex::{
    array::{
        dtype::{
            DType,
            extension::{ExtDType, ExtId, ExtVTable},
        },
        scalar::ScalarValue,
    },
    error::{VortexResult, vortex_ensure},
    session::registry::CachedId,
};

pub(crate) const ID: &str = "shardloom.binary-object.v1";
const METADATA: &str = "{\"version\":1}";

/// Binary storage is an opaque descriptor cell, never the referenced bytes.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BinaryObjectType;

/// Fixed version metadata for the descriptor extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(crate) struct Metadata;

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(METADATA)
    }
}

impl PartialEq for BinaryObjectType {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for BinaryObjectType {}
impl Hash for BinaryObjectType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ID.hash(state);
    }
}

impl ExtVTable for BinaryObjectType {
    type Metadata = Metadata;
    type NativeValue<'a> = &'a ScalarValue;

    fn id(&self) -> ExtId {
        static NAME: CachedId = CachedId::new(ID);
        *NAME
    }

    fn serialize_metadata(&self, _: &Metadata) -> VortexResult<Vec<u8>> {
        Ok(METADATA.as_bytes().to_vec())
    }

    fn deserialize_metadata(&self, raw: &[u8]) -> VortexResult<Metadata> {
        vortex_ensure!(raw == METADATA.as_bytes(), "invalid binary-object metadata");
        Ok(Metadata)
    }

    fn validate_dtype(dtype: &ExtDType<Self>) -> VortexResult<()> {
        vortex_ensure!(
            matches!(dtype.storage_dtype(), DType::Binary(_)),
            "binary-object storage must be Binary"
        );
        Ok(())
    }

    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        value: &'a ScalarValue,
    ) -> VortexResult<&'a ScalarValue> {
        Ok(value)
    }
}

pub(crate) fn storage(dtype: &DType) -> Option<&DType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    extension.metadata_opt::<BinaryObjectType>()?;
    Some(extension.storage_dtype())
}

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

pub(crate) fn dtype(nullable: bool) -> VortexResult<DType> {
    Ok(DType::Extension(
        ExtDType::<BinaryObjectType>::try_new(Metadata, DType::Binary(nullable.into()))?.erased(),
    ))
}

pub(crate) fn register(session: &vortex::session::VortexSession) {
    use vortex::array::dtype::session::DTypeSessionExt as _;
    session.dtypes().register(BinaryObjectType);
    #[cfg(unix)]
    crate::binary_object::layout::register(session);
    edition::register(session);
    #[cfg(feature = "universal-format-io")]
    arrow::register(session);
}

#[path = "native_binary_edition.rs"]
mod edition;

#[cfg(feature = "universal-format-io")]
#[path = "native_binary_type_arrow.rs"]
pub(crate) mod arrow;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_object_metadata_and_storage_are_strict() {
        use vortex::array::dtype::PType;
        assert_eq!(
            BinaryObjectType.serialize_metadata(&Metadata).unwrap(),
            b"{\"version\":1}"
        );
        assert_eq!(
            BinaryObjectType
                .deserialize_metadata(b"{\"version\":1}")
                .unwrap(),
            Metadata
        );
        for invalid in [
            &b"{}"[..],
            &b"{ \"version\":1}"[..],
            &b"{\"version\":2}"[..],
            &b""[..],
        ] {
            assert!(BinaryObjectType.deserialize_metadata(invalid).is_err());
        }
        assert!(
            ExtDType::<BinaryObjectType>::try_new(
                Metadata,
                DType::Primitive(PType::I8, false.into())
            )
            .is_err()
        );
        for nullable in [false, true] {
            let ty = dtype(nullable).unwrap();
            assert_eq!(storage(&ty), Some(&DType::Binary(nullable.into())));
            assert!(contains(&ty).unwrap());
        }
    }
}
