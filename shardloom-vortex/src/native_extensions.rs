//! One registration boundary for `ShardLoom`'s admitted logical extensions.

pub(crate) fn register(session: &vortex::session::VortexSession) {
    crate::native_temporal::register(session);
    crate::native_embedding::register(session);
}
