//! Accounted local fixture I/O. The caller supplies one owner for the complete
//! operation; a file read or writer must never create a second full grant.

use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom},
    path::Path,
    process::ExitCode,
};

use shardloom_core::{ExecutionResources, OutputFormat, ShardLoomError};
use shardloom_exec::live_memory::{Budgeted, LiveMemoryPool, MemoryLease};

/// Charge a replacement allocation while the original capacity is still live.
/// The same lease may also own nested strings and vectors.
pub(crate) fn reserve_additional<T>(
    values: &mut Vec<T>,
    additional: usize,
    lease: &mut MemoryLease,
) -> shardloom_core::Result<()> {
    let needed = values
        .len()
        .checked_add(additional)
        .ok_or_else(size_error)?;
    if needed <= values.capacity() {
        return Ok(());
    }
    let item_bytes = std::mem::size_of::<T>();
    let target = growth_target(values.capacity(), needed, item_bytes, lease)?;
    let old_bytes = byte_count(values.capacity(), item_bytes)?;
    let replacement_bytes = byte_count(target, item_bytes)?;
    let previous = lease.bytes();
    lease.resize(
        previous
            .checked_add(replacement_bytes)
            .ok_or_else(size_error)?,
    )?;
    if let Err(error) = values.try_reserve_exact(target - values.len()) {
        lease.resize(previous)?;
        return Err(ShardLoomError::InvalidOperation(format!(
            "fixture vector allocation failed: {error}"
        )));
    }
    if values.capacity() > target {
        return Err(ShardLoomError::InvalidOperation(
            "fixture allocator exceeded admitted vector capacity".into(),
        ));
    }
    lease.resize(previous - old_bytes + replacement_bytes)
}

fn byte_count(elements: usize, item_bytes: usize) -> shardloom_core::Result<u64> {
    u64::try_from(elements.checked_mul(item_bytes).ok_or_else(size_error)?)
        .map_err(|_| size_error())
}

fn growth_target(
    capacity: usize,
    needed: usize,
    item_bytes: usize,
    lease: &MemoryLease,
) -> shardloom_core::Result<usize> {
    let target = capacity.saturating_mul(2).max(needed);
    let snapshot = lease.pool().snapshot();
    let available = snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes);
    // Use exact growth near the grant instead of introducing a geometric floor.
    Ok(if byte_count(target, item_bytes)? <= available {
        target
    } else {
        needed
    })
}

fn size_error() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "fixture buffer size overflow; no fallback execution was attempted".into(),
    )
}

pub(crate) fn copy_text(value: &str, lease: &mut MemoryLease) -> shardloom_core::Result<String> {
    let bytes = byte_count(value.len(), 1)?;
    let previous = lease.bytes();
    lease.resize(previous.checked_add(bytes).ok_or_else(size_error)?)?;
    let mut text = String::new();
    if let Err(error) = text.try_reserve_exact(value.len()) {
        lease.resize(previous)?;
        return Err(ShardLoomError::InvalidOperation(format!(
            "fixture text allocation failed: {error}"
        )));
    }
    if text.capacity() > value.len() {
        drop(text);
        lease.resize(previous)?;
        return Err(ShardLoomError::InvalidOperation(
            "fixture allocator exceeded admitted text capacity".into(),
        ));
    }
    text.push_str(value);
    Ok(text)
}

/// A fallible formatting destination. The first admission error stops further
/// allocation and is returned intact by finish, even when formatters return only
/// `fmt::Error`. Text storage is released before its lease on every exit path.
pub(crate) struct Text {
    value: String,
    lease: MemoryLease,
    error: Option<ShardLoomError>,
}

impl Text {
    pub(crate) fn new(pool: &LiveMemoryPool) -> shardloom_core::Result<Self> {
        Ok(Self {
            value: String::new(),
            lease: pool.reserve(0)?,
            error: None,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.value.len()
    }

    pub(crate) fn finish(self) -> shardloom_core::Result<Budgeted<String>> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok(Budgeted::new(self.value, self.lease))
    }

    fn append(&mut self, text: &str) -> shardloom_core::Result<()> {
        let needed = self
            .value
            .len()
            .checked_add(text.len())
            .ok_or_else(size_error)?;
        if needed > self.value.capacity() {
            let target = growth_target(self.value.capacity(), needed, 1, &self.lease)?;
            let previous = self.lease.bytes();
            let replacement = byte_count(target, 1)?;
            self.lease
                .resize(previous.checked_add(replacement).ok_or_else(size_error)?)?;
            if let Err(error) = self.value.try_reserve_exact(target - self.value.len()) {
                self.lease.resize(previous)?;
                return Err(ShardLoomError::InvalidOperation(format!(
                    "fixture text allocation failed: {error}"
                )));
            }
            if self.value.capacity() > target {
                return Err(ShardLoomError::InvalidOperation(
                    "fixture allocator exceeded admitted text capacity".into(),
                ));
            }
            self.lease.resize(replacement)?;
        }
        self.value.push_str(text);
        Ok(())
    }
}

impl std::fmt::Write for Text {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        if self.error.is_some() {
            return Err(std::fmt::Error);
        }
        self.append(value).map_err(|error| {
            self.error = Some(error);
            std::fmt::Error
        })
    }
}

pub(crate) fn owner_for_command(
    resources: ExecutionResources,
    command: &str,
    format: OutputFormat,
) -> Result<LiveMemoryPool, ExitCode> {
    LiveMemoryPool::new(resources.memory_bytes()).map_err(|error| {
        crate::cli_output::emit_error(command, format, "fixture resource admission failed", &error)
    })
}

pub(crate) fn with_observation_fields(
    mut fields: Vec<(String, String)>,
    resources: ExecutionResources,
    pool: &LiveMemoryPool,
) -> Vec<(String, String)> {
    let snapshot = pool.snapshot();
    crate::execution_resources::append_declaration_fields(&mut fields, resources);
    crate::execution_resources::append_admission_fields(
        &mut fields,
        snapshot.limit_bytes,
        1,
        "shared_local_fixture_io_owner;serial_execution",
    );
    crate::execution_resources::append_memory_observation_fields(
        &mut fields,
        Some(snapshot.reserved_bytes),
        snapshot.peak_reserved_bytes,
        "fixture_owned_buffers_and_workspace_writer;excludes_uninstrumented_metadata_provider_decode_transients_reports_and_process_rss",
    );
    crate::execution_resources::append_spill_observation_fields(&mut fields, false, None);
    fields.push((
        "fixture_io_denied_reservations".into(),
        snapshot.denied_reservations.to_string(),
    ));
    fields
}

/// Reserve the exact extent before opening the file. Read into fixed capacity,
/// refusing size changes instead of growing a Vec beyond its admission.
pub(crate) fn read_bytes(
    path: &Path,
    extent: Option<(u64, u64)>,
    observed_size: u64,
    pool: &LiveMemoryPool,
) -> io::Result<Budgeted<Vec<u8>>> {
    let (offset, length) = extent.unwrap_or((0, observed_size));
    if offset
        .checked_add(length)
        .is_none_or(|end| end > observed_size)
    {
        return Err(invalid(
            "fixture byte range exceeds the inspected file length",
        ));
    }
    let length_usize = usize::try_from(length)
        .map_err(|_| invalid("fixture byte extent exceeds addressable memory"))?;
    let lease = pool.reserve(length).map_err(io::Error::other)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length_usize)
        .map_err(io::Error::other)?;
    if bytes.capacity() > length_usize {
        return Err(invalid(
            "fixture allocator exceeded its admitted buffer capacity",
        ));
    }
    bytes.resize(length_usize, 0);
    let mut file = File::open(path)?;
    let initial = file.metadata()?;
    if !initial.is_file() || initial.len() != observed_size {
        return Err(invalid("fixture file type or size changed before reading"));
    }
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut bytes)?;
    if file.metadata()?.len() != observed_size {
        return Err(invalid("fixture file size changed while reading"));
    }
    Ok(Budgeted::new(bytes, lease))
}

pub(crate) fn read_utf8(
    path: &Path,
    max_bytes: u64,
    label: &str,
    pool: &LiveMemoryPool,
) -> io::Result<Budgeted<String>> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(invalid(&format!(
            "{label} {} must be a regular file within its scoped read budget of {max_bytes} bytes",
            path.display()
        )));
    }
    let (bytes, lease) = read_bytes(path, None, metadata.len(), pool)?.into_parts();
    let text = String::from_utf8(bytes)
        .map_err(|_| invalid(&format!("{label} {} is not valid UTF-8", path.display())))?;
    Ok(Budgeted::new(text, lease))
}

pub(crate) fn write_bytes(
    workspace_root: &Path,
    path: &Path,
    allow_overwrite: bool,
    label: &str,
    content: &[u8],
    pool: &LiveMemoryPool,
) -> io::Result<shardloom_core::WorkspaceSafeLocalWriteReport> {
    let _writer = pool
        .reserve(shardloom_core::WorkspaceSafeLocalStagingWriter::buffer_capacity_bytes())
        .map_err(io::Error::other)?;
    shardloom_core::write_workspace_safe_bytes(
        workspace_root,
        path,
        allow_overwrite,
        label,
        content,
    )
    .map_err(io::Error::other)
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    #[test]
    fn text_and_nested_vectors_reserve_before_growth_and_release_on_failure() {
        let pool = LiveMemoryPool::new(64).unwrap();
        let mut lease = pool.reserve(0).unwrap();
        let mut values = Vec::new();
        reserve_additional(&mut values, 1, &mut lease).unwrap();
        values.push(copy_text("retained", &mut lease).unwrap());
        assert!(lease.bytes() >= 8 + std::mem::size_of::<String>() as u64);
        let live = pool.snapshot().reserved_bytes;
        let mut text = Text::new(&pool).unwrap();
        text.write_str("small").unwrap();
        assert_eq!(text.len(), 5);
        assert!(text.write_str(&"x".repeat(64)).is_err());
        assert!(
            text.finish()
                .unwrap_err()
                .to_string()
                .contains("memory reservation denied")
        );
        assert_eq!(pool.snapshot().reserved_bytes, live);
        drop(values);
        drop(lease);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn denied_extent_precedes_file_open_and_retained_buffers_share_credit() {
        let pool = LiveMemoryPool::new(3).unwrap();
        let absent =
            std::env::temp_dir().join(format!("shardloom-fixture-unopened-{}", std::process::id()));
        let error = read_bytes(&absent, None, 4, &pool).unwrap_err();
        assert!(error.to_string().contains("memory reservation denied"));
        assert_eq!(pool.snapshot().reserved_bytes, 0);

        let path = std::env::temp_dir().join(format!(
            "shardloom-fixture-buffer-credit-{}",
            std::process::id()
        ));
        fs::write(&path, b"abcdef").unwrap();
        let bytes = read_bytes(&path, Some((1, 3)), 6, &pool).unwrap();
        assert_eq!(bytes.value(), b"bcd");
        assert_eq!(pool.snapshot().reserved_bytes, 3);
        assert!(read_bytes(&path, Some((4, 1)), 6, &pool).is_err());
        drop(bytes);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        assert_eq!(
            read_bytes(&path, Some((4, 1)), 6, &pool).unwrap().value(),
            b"e"
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn size_changes_and_invalid_text_return_all_credit() {
        let pool = LiveMemoryPool::new(16).unwrap();
        let path = std::env::temp_dir().join(format!(
            "shardloom-fixture-buffer-shape-{}",
            std::process::id()
        ));
        fs::write(&path, b"abcd").unwrap();
        for old_size in [2, 6] {
            assert!(read_bytes(&path, None, old_size, &pool).is_err());
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
        fs::write(&path, [0xff]).unwrap();
        assert!(read_utf8(&path, 16, "fixture", &pool).is_err());
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        fs::remove_file(path).unwrap();
    }
}
