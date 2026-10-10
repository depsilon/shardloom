//! Shared recognition of the admitted native temporal storage domains.

use vortex::array::{
    dtype::{DType, PType},
    extension::datetime::{Date, TimeUnit, Timestamp},
};

/// Recognize provider metadata, never an extension name or its storage alone.
pub(crate) fn temporal_storage(dtype: &DType) -> Option<PType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    let storage = if extension.metadata_opt::<Date>() == Some(&TimeUnit::Days) {
        PType::I32
    } else if extension
        .metadata_opt::<Timestamp>()
        .is_some_and(|metadata| metadata.unit == TimeUnit::Microseconds && metadata.tz.is_none())
    {
        PType::I64
    } else {
        return None;
    };
    matches!(extension.storage_dtype(), DType::Primitive(ptype, _) if *ptype == storage)
        .then_some(storage)
}
