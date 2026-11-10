//! One registration boundary for `ShardLoom`'s admitted logical extensions.

pub(crate) fn register(session: &vortex::session::VortexSession) {
    crate::native_temporal::register(session);
    crate::native_uuid::register(session);
    crate::native_embedding::register(session);
    crate::native_tensor::register(session);
    crate::native_variable_tensor::register(session);
    crate::native_vector::register(session);
    #[cfg(feature = "native-binary-objects")]
    crate::native_binary_type::register(session);
}

/// Logical extensions reuse the existing structural gather/empty builders.
/// Comparison admission remains a separate typed-key contract.
#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) fn payload_storage(
    dtype: &vortex::array::dtype::DType,
) -> Option<&vortex::array::dtype::DType> {
    if let Some(storage) = crate::native_uuid::storage(dtype) {
        return Some(storage);
    }
    #[cfg(feature = "native-binary-objects")]
    if let Some(storage) = crate::native_binary_type::storage(dtype) {
        return Some(storage);
    }
    structured_payload_storage(dtype)
}

/// Logical structured payloads share intake without becoming scalar keys.
pub(crate) fn structured_payload_storage(
    dtype: &vortex::array::dtype::DType,
) -> Option<&vortex::array::dtype::DType> {
    crate::native_embedding::storage(dtype)
        .or_else(|| crate::native_tensor::storage(dtype))
        .or_else(|| crate::native_variable_tensor::storage(dtype))
        .or_else(|| crate::native_vector::storage(dtype))
}
