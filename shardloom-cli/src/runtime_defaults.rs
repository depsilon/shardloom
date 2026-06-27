//! Shared runtime resource defaults for public local `ShardLoom` surfaces.

pub(crate) const DEFAULT_PUBLIC_LOCAL_RUNTIME_MEMORY_GB: u64 = 4;
pub(crate) const DEFAULT_PUBLIC_LOCAL_RUNTIME_MAX_PARALLELISM: usize = 2;
pub(crate) const MIN_PUBLIC_LOCAL_RUNTIME_MAX_PARALLELISM: usize = 2;

pub(crate) const PUBLIC_LOCAL_RUNTIME_MEMORY_GB_ENV: &str = "SHARDLOOM_MEMORY_GB";
pub(crate) const PUBLIC_LOCAL_RUNTIME_MAX_PARALLELISM_ENV: &str = "SHARDLOOM_MAX_PARALLELISM";

pub(crate) fn default_public_local_runtime_memory_gb() -> u64 {
    std::env::var(PUBLIC_LOCAL_RUNTIME_MEMORY_GB_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_PUBLIC_LOCAL_RUNTIME_MEMORY_GB)
}

pub(crate) fn default_public_local_runtime_max_parallelism() -> usize {
    std::env::var(PUBLIC_LOCAL_RUNTIME_MAX_PARALLELISM_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_PUBLIC_LOCAL_RUNTIME_MAX_PARALLELISM)
        .max(MIN_PUBLIC_LOCAL_RUNTIME_MAX_PARALLELISM)
}
