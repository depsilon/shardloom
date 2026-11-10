//! Canonical UUID admission around the existing Vortex UUID provider.

use vortex::{
    array::dtype::{DType, Nullability, PType},
    encodings::uuid::Uuid,
};

pub(crate) const ID: &str = "vortex.uuid";

pub(crate) fn is_uuid(dtype: &DType) -> bool {
    matches!(dtype, DType::Extension(extension) if extension.id().as_ref() == ID)
}

/// Canonical Arrow UUID has no version restriction to carry provider metadata.
pub(crate) fn storage(dtype: &DType) -> Option<&DType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    if extension.metadata_opt::<Uuid>()?.version.is_some()
        || !matches!(extension.storage_dtype(), DType::FixedSizeList(child, 16, _)
            if **child == DType::Primitive(PType::U8, Nullability::NonNullable))
    {
        return None;
    }
    Some(extension.storage_dtype())
}

pub(crate) fn register(session: &vortex::session::VortexSession) {
    vortex::encodings::uuid::initialize(session);
}

#[cfg(feature = "vortex-local-primitives")]
pub(crate) fn display(bytes: [u8; 16]) -> String {
    let value = u128::from_be_bytes(bytes);
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        value >> 96,
        (value >> 80) & 0xffff,
        (value >> 64) & 0xffff,
        (value >> 48) & 0xffff,
        value & 0xffff_ffff_ffff,
    )
}

#[cfg(feature = "universal-format-io")]
#[path = "native_uuid_arrow.rs"]
pub(crate) mod arrow;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "native_uuid_values.rs"]
mod values;
#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) use values::Values;
