//! Immutable accounting for a completed source intake, not later query work.

#[cfg(feature = "vortex-write")]
use {super::memory_error, shardloom_core::Result};

/// Work performed while constructing one native source. Reusing that source
/// leaves this receipt unchanged. Counts describe the native intake boundary;
/// they do not measure process RSS or allocations inside a foreign producer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeTransferReport {
    pub encoded_array_nodes_copied: u64,
    pub encoded_buffer_copies: u64,
    pub encoded_buffer_bytes_copied: u64,
    pub reused_array_references: u64,
    pub reused_buffer_references: u64,
    pub materialized_batches: u64,
    pub materialized_rows: u64,
    /// Credit retained for reconstructed schemas and encoding metadata.
    pub retained_metadata_bytes: u64,
    /// Largest single transfer's explicitly admitted provider scratch.
    pub provider_scratch_peak_reserved_bytes: u64,
    /// Complete input message bytes, including headers and schema.
    pub ipc_transport_bytes: u64,
    /// First-stage schema and payload copies, before retained reconstruction.
    pub ipc_buffer_bytes_copied: u64,
    /// Largest decoder workspace allowance; excludes payloads and the separate
    /// coordinate-validation, verifier, schema and transfer reservations.
    pub ipc_decode_workspace_peak_reserved_bytes: u64,
    /// Explicit coordinate-array execution calls during IPC validation. A call
    /// can reuse an already canonical array; this is not a decoded-byte count.
    pub ipc_coordinate_arrays_executed: u64,
    /// Coordinate values examined by those explicit validation passes, including
    /// repeated checks. Provider-internal validation is outside these counters.
    pub ipc_coordinate_values_checked: u64,
}

impl NativeTransferReport {
    #[cfg(feature = "vortex-write")]
    pub(super) fn combine(self, next: Self) -> Result<Self> {
        let add = |a: u64, b: u64| {
            a.checked_add(b)
                .ok_or_else(|| memory_error("native transfer accounting overflow"))
        };
        Ok(Self {
            encoded_array_nodes_copied: add(
                self.encoded_array_nodes_copied,
                next.encoded_array_nodes_copied,
            )?,
            encoded_buffer_copies: add(self.encoded_buffer_copies, next.encoded_buffer_copies)?,
            encoded_buffer_bytes_copied: add(
                self.encoded_buffer_bytes_copied,
                next.encoded_buffer_bytes_copied,
            )?,
            reused_array_references: add(
                self.reused_array_references,
                next.reused_array_references,
            )?,
            reused_buffer_references: add(
                self.reused_buffer_references,
                next.reused_buffer_references,
            )?,
            materialized_batches: add(self.materialized_batches, next.materialized_batches)?,
            materialized_rows: add(self.materialized_rows, next.materialized_rows)?,
            retained_metadata_bytes: add(
                self.retained_metadata_bytes,
                next.retained_metadata_bytes,
            )?,
            provider_scratch_peak_reserved_bytes: self
                .provider_scratch_peak_reserved_bytes
                .max(next.provider_scratch_peak_reserved_bytes),
            ipc_transport_bytes: add(self.ipc_transport_bytes, next.ipc_transport_bytes)?,
            ipc_buffer_bytes_copied: add(
                self.ipc_buffer_bytes_copied,
                next.ipc_buffer_bytes_copied,
            )?,
            ipc_decode_workspace_peak_reserved_bytes: self
                .ipc_decode_workspace_peak_reserved_bytes
                .max(next.ipc_decode_workspace_peak_reserved_bytes),
            ipc_coordinate_arrays_executed: add(
                self.ipc_coordinate_arrays_executed,
                next.ipc_coordinate_arrays_executed,
            )?,
            ipc_coordinate_values_checked: add(
                self.ipc_coordinate_values_checked,
                next.ipc_coordinate_values_checked,
            )?,
        })
    }
}
