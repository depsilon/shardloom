//! One registration boundary for `ShardLoom`'s admitted logical extensions.

pub(crate) fn register(session: &vortex::session::VortexSession) {
    crate::native_temporal::register(session);
    crate::native_embedding::register(session);
    crate::native_tensor::register(session);
    #[cfg(feature = "native-binary-objects")]
    crate::native_binary_type::register(session);
}

/// Payload-only extensions use the existing structural gather/empty builders;
/// this does not admit their storage representation as a comparison key.
#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) fn payload_storage(
    dtype: &vortex::array::dtype::DType,
) -> Option<&vortex::array::dtype::DType> {
    #[cfg(feature = "native-binary-objects")]
    if let Some(storage) = crate::native_binary_type::storage(dtype) {
        return Some(storage);
    }
    list_payload_storage(dtype)
}

/// Logical list payloads share structural intake without becoming scalar keys.
pub(crate) fn list_payload_storage(
    dtype: &vortex::array::dtype::DType,
) -> Option<&vortex::array::dtype::DType> {
    crate::native_embedding::storage(dtype).or_else(|| crate::native_tensor::storage(dtype))
}
