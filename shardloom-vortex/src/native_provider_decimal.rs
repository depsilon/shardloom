//! Preserve private decimal storage until native selection or evaluation observes it.

use vortex::{
    array::{
        ArrayDeserialization, ArrayId, ArrayPlugin, ArrayRef, ArraySerialization,
        arrays::decimal::DecimalPlugin,
    },
    error::VortexResult,
    session::VortexSession,
};

/// Reuse the provider's wire parser and serializer while retaining the stored
/// width. Native decimal readers validate precision after ancestor validity and
/// selection; eager canonicalization would inspect unobserved private spill rows.
#[derive(Debug)]
pub(super) struct NativeDecimalStorage;

impl ArrayPlugin for NativeDecimalStorage {
    fn id(&self) -> ArrayId {
        DecimalPlugin.id()
    }

    fn read_ids(&self) -> Vec<ArrayId> {
        DecimalPlugin.read_ids()
    }

    fn write_ids(&self) -> Vec<ArrayId> {
        DecimalPlugin.write_ids()
    }

    fn serialize(
        &self,
        array: &ArrayRef,
        session: &VortexSession,
    ) -> VortexResult<Option<ArraySerialization>> {
        DecimalPlugin.serialize(array, session)
    }

    fn deserialize(
        &self,
        parts: ArrayDeserialization<'_>,
        _session: &VortexSession,
    ) -> VortexResult<ArrayRef> {
        DecimalPlugin.deserialize_preserving_storage(parts)
    }

    fn is_supported_encoding(&self, id: &ArrayId) -> bool {
        DecimalPlugin.is_supported_encoding(id)
    }
}
