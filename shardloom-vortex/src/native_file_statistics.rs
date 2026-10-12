//! Admission for the pinned provider's bounded file-statistics collector.

#[cfg(feature = "vortex-write")]
use shardloom_core::{Result, ShardLoomError};
#[cfg(feature = "vortex-write")]
use shardloom_exec::live_memory::LiveMemoryPool;
use shardloom_exec::live_memory::MemoryLease;
#[cfg(feature = "vortex-write")]
use vortex::array::dtype::{DType, StructFields};

/// Owns running statistics and their final footer representation. The provider
/// keeps at most five running scalar sets per field, with 64-byte string/binary
/// bounds. This allowance includes merging, final sets, serialization and a
/// second footer during validation. Input/provider compute uses its own session
/// allocator. Layout references and schema containers have separate owners.
pub(crate) struct FileStatisticsReservation {
    // Write-only builds retain this owner without reporting its size.
    #[cfg_attr(
        not(all(feature = "vortex-write", feature = "vortex-local-primitives", unix)),
        allow(dead_code)
    )]
    lease: MemoryLease,
}

impl FileStatisticsReservation {
    #[cfg(feature = "vortex-write")]
    pub(crate) fn reserve(dtype: &DType, memory: &LiveMemoryPool) -> Result<Self> {
        let fields = dtype
            .as_struct_fields_opt()
            .map_or(1, StructFields::nfields);
        let bytes = u64::try_from(fields)
            .ok()
            .and_then(|fields| fields.checked_mul(8192))
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "native file statistics reservation overflow; no fallback execution was attempted"
                        .into(),
                )
            })?;
        memory.reserve(bytes).map(|lease| Self { lease })
    }

    #[cfg(all(feature = "vortex-write", feature = "vortex-local-primitives", unix))]
    pub(crate) fn bytes(&self) -> u64 {
        self.lease.bytes()
    }
}
