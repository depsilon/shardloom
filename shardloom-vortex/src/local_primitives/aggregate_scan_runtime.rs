//! Provider progress when actual aggregate-worker admission declines.
//! Drivers share the existing runtime and are joined before its scan returns.

use super::{LocalVortexRuntime, bounded_local_vortex_worker_count};
use shardloom_core::Result;
use vortex::io::runtime::BlockingRuntime;

pub(super) trait AggregateScanRuntime: BlockingRuntime {
    type ProviderDrivers;

    /// Call only after the caller-only aggregate pool was not admitted, or was
    /// cancelled and joined for replay. A runtime already driven by a resident
    /// provider pool must never receive `worker_memory` at the scan entrypoint.
    fn provider_drivers(&self, requested: usize) -> Result<(Self::ProviderDrivers, usize)>;
}

#[cfg(not(target_arch = "wasm32"))]
impl AggregateScanRuntime for vortex::io::runtime::current::CurrentThreadRuntime {
    type ProviderDrivers = crate::resident_worker_group::ResidentWorkerGroup;

    fn provider_drivers(&self, requested: usize) -> Result<(Self::ProviderDrivers, usize)> {
        let count = bounded_local_vortex_worker_count(requested);
        let guard = crate::resident_worker_group::ResidentWorkerGroup::new(self, count)
            .map_err(super::vortex_error)?;
        Ok((guard, count))
    }
}

impl AggregateScanRuntime for LocalVortexRuntime {
    type ProviderDrivers = ();

    fn provider_drivers(&self, requested: usize) -> Result<(Self::ProviderDrivers, usize)> {
        // This cross-platform legacy runtime already owns its provider pool.
        let count = match self {
            Self::Single(_) => 0,
            Self::Current { .. } => bounded_local_vortex_worker_count(requested),
        };
        Ok(((), count))
    }
}
