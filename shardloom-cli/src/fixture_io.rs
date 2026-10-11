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

/// Escape directly into the formatting destination, including path displays,
/// without constructing an unaccounted intermediate string.
pub(crate) struct JsonEscaped<T>(pub(crate) T);

impl<T: std::fmt::Display> std::fmt::Display for JsonEscaped<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::fmt::Write as _;

        struct EscapingWriter<'a, 'b>(&'a mut std::fmt::Formatter<'b>);
        impl std::fmt::Write for EscapingWriter<'_, '_> {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                for character in value.chars() {
                    match character {
                        '"' => self.0.write_str("\\\""),
                        '\\' => self.0.write_str("\\\\"),
                        '\n' => self.0.write_str("\\n"),
                        '\r' => self.0.write_str("\\r"),
                        '\t' => self.0.write_str("\\t"),
                        value if value.is_control() => write!(self.0, "\\u{:04x}", value as u32),
                        value => self.0.write_char(value),
                    }?;
                }
                Ok(())
            }
        }
        write!(EscapingWriter(formatter), "{}", self.0)
    }
}

pub(crate) fn owner_for_command<'a>(
    resources: ExecutionResources,
    command: &str,
    format: OutputFormat,
    context: &crate::command_context::CommandContext<'a>,
) -> Result<crate::command_context::CommandAdmission<'a>, ExitCode> {
    context.admit_for_command(resources, command, format)
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

/// Conservative workspace admission for the bounded synthetic engine/table
/// fixtures. At most 32 fixed records participate; four 4-KiB workspaces per
/// record cover their maps, clones and transition state. Caller operator text
/// may expand through projection of every fixture row, so charge 1 KiB per
/// argument byte before parsing. This estimate is not a production allocation,
/// measured live heap usage, a query spill contract or a whole-process ceiling.
pub(crate) fn fixed_fixture_workspace_bytes(arguments: &[String]) -> shardloom_core::Result<u64> {
    let bytes = arguments
        .iter()
        .try_fold(32_usize * 4 * 4096, |bytes, argument| {
            argument
                .len()
                .checked_mul(1024)
                .and_then(|additional| bytes.checked_add(additional))
                .ok_or_else(size_error)
        })?;
    u64::try_from(bytes).map_err(|_| size_error())
}

pub(crate) fn reserve_fixed_fixture_workspace(
    arguments: &[String],
    resources: ExecutionResources,
    pool: &LiveMemoryPool,
    command: &str,
    format: OutputFormat,
) -> Result<MemoryLease, ExitCode> {
    fixed_fixture_workspace_bytes(arguments)
        .and_then(|bytes| pool.reserve(bytes))
        .map_err(|error| {
            crate::cli_output::emit_error_with_fields(
                command,
                format,
                "fixture workspace admission failed",
                &error,
                with_estimated_fixture_fields(
                    Vec::new(),
                    resources,
                    pool,
                    "fixed_fixture_state_and_argument_expansion_workspace_estimate",
                ),
            )
        })
}

pub(crate) fn with_estimated_fixture_fields(
    fields: Vec<(String, String)>,
    resources: ExecutionResources,
    pool: &LiveMemoryPool,
    basis: &str,
) -> Vec<(String, String)> {
    let mut fields = with_observation_fields(fields, resources, pool);
    let snapshot = pool.snapshot();
    crate::execution_resources::append_memory_observation_fields(
        &mut fields,
        Some(snapshot.reserved_bytes),
        snapshot.peak_reserved_bytes,
        &format!(
            "{basis};conservative_reservation_not_measured_heap;excludes_caller_arguments_envelope_serialization_and_process_rss"
        ),
    );
    fields
}

/// Keep the live workspace observation on parsing and execution failures too.
pub(crate) fn emit_fixed_fixture_error(
    command: &str,
    format: OutputFormat,
    summary: &str,
    error: &ShardLoomError,
    resources: ExecutionResources,
    pool: &LiveMemoryPool,
) -> ExitCode {
    crate::cli_output::emit_error_with_fields(
        command,
        format,
        summary,
        error,
        with_estimated_fixture_fields(
            Vec::new(),
            resources,
            pool,
            "fixed_fixture_state_and_argument_expansion_workspace_estimate",
        ),
    )
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

/// Payload storage that can be checked before a fixture writer has effects.
pub(crate) trait WriteContent: AsRef<[u8]> {
    fn owned_capacity(&self) -> usize;
}

impl WriteContent for Vec<u8> {
    fn owned_capacity(&self) -> usize {
        self.capacity()
    }
}

impl WriteContent for String {
    fn owned_capacity(&self) -> usize {
        self.capacity()
    }
}

impl WriteContent for &'static str {
    fn owned_capacity(&self) -> usize {
        // Compile-time fixture bytes have no execution-owned allocation.
        0
    }
}

pub(crate) fn write_bytes<T: WriteContent>(
    workspace_root: &Path,
    path: &Path,
    allow_overwrite: bool,
    label: &str,
    content: &Budgeted<T>,
    pool: &LiveMemoryPool,
) -> io::Result<shardloom_core::WorkspaceSafeLocalWriteReport> {
    if !pool.owns(content.lease()) {
        return Err(invalid(
            "fixture write payload belongs to a different memory pool",
        ));
    }
    let capacity = u64::try_from(content.value().owned_capacity())
        .map_err(|_| invalid("fixture write payload capacity exceeds addressable memory"))?;
    if capacity > content.reserved_bytes() {
        return Err(invalid(
            "fixture write payload capacity exceeds its reservation",
        ));
    }
    let _writer = pool
        .reserve(shardloom_core::WorkspaceSafeLocalStagingWriter::buffer_capacity_bytes())
        .map_err(io::Error::other)?;
    shardloom_core::write_workspace_safe_bytes(
        workspace_root,
        path,
        allow_overwrite,
        label,
        content.value().as_ref(),
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

    fn test_workspace(label: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "shardloom-fixture-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn fixed_fixture_workspace_competes_for_credit_and_reports_an_estimate() {
        let arguments = vec!["--project".into(), "value,value,value".repeat(1024)];
        let bytes = fixed_fixture_workspace_bytes(&arguments).unwrap();
        assert!(bytes > fixed_fixture_workspace_bytes(&[]).unwrap());
        let resources = ExecutionResources::from_bytes(
            bytes,
            2,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .unwrap();
        let pool = LiveMemoryPool::new(bytes).unwrap();
        let competing = pool.reserve(1).unwrap();
        assert!(pool.reserve(bytes).is_err());
        assert_eq!(pool.snapshot().reserved_bytes, 1);
        drop(competing);
        let workspace = reserve_fixed_fixture_workspace(
            &arguments,
            resources,
            &pool,
            "fixture-test",
            OutputFormat::Json,
        )
        .unwrap();
        let fields =
            with_estimated_fixture_fields(Vec::new(), resources, &pool, "fixed_fixture_test");
        let field = |name: &str| {
            fields
                .iter()
                .find(|(key, _)| key == name)
                .unwrap()
                .1
                .as_str()
        };
        assert_eq!(
            field("execution_resource_observed_native_reserved_bytes"),
            bytes.to_string()
        );
        assert_eq!(
            field("execution_resource_observed_native_peak_reserved_bytes"),
            bytes.to_string()
        );
        assert!(
            field("execution_resource_memory_observation_scope")
                .contains("conservative_reservation_not_measured_heap")
        );
        assert_eq!(field("execution_resource_admitted_max_parallelism"), "1");
        drop(workspace);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        assert!(
            reserve_fixed_fixture_workspace(
                &arguments,
                resources,
                &pool,
                "fixture-test",
                OutputFormat::Json
            )
            .is_ok()
        );
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

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

    #[test]
    fn write_rejects_foreign_payload_before_filesystem_effects() {
        let root = test_workspace("foreign-write");
        let sentinel = root.join("sentinel.bin");
        fs::write(&sentinel, b"preserve me").unwrap();
        let absent = root.join("not-created").join("output.bin");
        let payload_pool = LiveMemoryPool::new(128 << 10).unwrap();
        let writer_pool = LiveMemoryPool::new(128 << 10).unwrap();
        let lease = payload_pool.reserve(4).unwrap();
        let payload = Budgeted::new(b"data".to_vec(), lease);

        let error = write_bytes(
            &root,
            &absent,
            false,
            "foreign payload test",
            &payload,
            &writer_pool,
        )
        .unwrap_err();
        assert!(error.to_string().contains("different memory pool"));
        assert!(!absent.parent().unwrap().exists());
        assert_eq!(fs::read(&sentinel).unwrap(), b"preserve me");

        let error = write_bytes(
            &root,
            &sentinel,
            true,
            "foreign payload overwrite test",
            &payload,
            &writer_pool,
        )
        .unwrap_err();
        assert!(error.to_string().contains("different memory pool"));
        assert_eq!(fs::read(&sentinel).unwrap(), b"preserve me");
        assert_eq!(payload_pool.snapshot().reserved_bytes, 4);
        assert_eq!(writer_pool.snapshot().reserved_bytes, 0);
        drop(payload);
        assert_eq!(payload_pool.snapshot().reserved_bytes, 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn write_rejects_vec_and_string_capacity_beyond_their_leases() {
        let root = test_workspace("under-reserved-write");
        let sentinel = root.join("sentinel.txt");
        fs::write(&sentinel, b"preserve me").unwrap();
        let pool = LiveMemoryPool::new(128 << 10).unwrap();

        let mut bytes = Vec::new();
        bytes.try_reserve_exact(32).unwrap();
        assert_eq!(bytes.capacity(), 32);
        bytes.extend_from_slice(b"data");
        let byte_lease = pool.reserve(4).unwrap();
        let bytes = Budgeted::new(bytes, byte_lease);
        let error = write_bytes(
            &root,
            &sentinel,
            true,
            "under-reserved vector test",
            &bytes,
            &pool,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("capacity exceeds its reservation")
        );
        assert_eq!(fs::read(&sentinel).unwrap(), b"preserve me");
        assert_eq!(pool.snapshot().reserved_bytes, 4);
        drop(bytes);
        assert_eq!(pool.snapshot().reserved_bytes, 0);

        let absent = root.join("string-output.txt");
        let mut text = String::new();
        text.try_reserve_exact(32).unwrap();
        assert_eq!(text.capacity(), 32);
        text.push_str("data");
        let text_lease = pool.reserve(4).unwrap();
        let text = Budgeted::new(text, text_lease);
        let error = write_bytes(
            &root,
            &absent,
            false,
            "under-reserved string test",
            &text,
            &pool,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("capacity exceeds its reservation")
        );
        assert!(!absent.exists());
        assert_eq!(pool.snapshot().reserved_bytes, 4);
        drop(text);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn write_staging_competes_with_retained_credit_and_static_text_is_unowned() {
        let root = test_workspace("write-credit");
        let target = root.join("payload.bin");
        fs::write(&target, b"original").unwrap();
        let payload_bytes = b"exact payload";
        let mut value = Vec::new();
        value.try_reserve_exact(payload_bytes.len()).unwrap();
        assert_eq!(value.capacity(), payload_bytes.len());
        value.extend_from_slice(payload_bytes);
        let payload_capacity = value.capacity() as u64;
        let writer_capacity =
            shardloom_core::WorkspaceSafeLocalStagingWriter::buffer_capacity_bytes();
        let pool = LiveMemoryPool::new(payload_capacity + writer_capacity).unwrap();
        let payload_lease = pool.reserve(payload_capacity).unwrap();
        let payload = Budgeted::new(value, payload_lease);
        let competing_credit = pool.reserve(1).unwrap();

        let error = write_bytes(
            &root,
            &target,
            true,
            "credit competition test",
            &payload,
            &pool,
        )
        .unwrap_err();
        assert!(error.to_string().contains("memory reservation denied"));
        assert_eq!(fs::read(&target).unwrap(), b"original");
        assert_eq!(pool.snapshot().reserved_bytes, payload_capacity + 1);
        drop(competing_credit);

        write_bytes(&root, &target, true, "credit release test", &payload, &pool).unwrap();
        assert_eq!(fs::read(&target).unwrap(), payload_bytes);
        assert_eq!(pool.snapshot().reserved_bytes, payload_capacity);
        drop(payload);
        assert_eq!(pool.snapshot().reserved_bytes, 0);

        let static_pool = LiveMemoryPool::new(writer_capacity).unwrap();
        let static_lease = static_pool.reserve(0).unwrap();
        let static_payload = Budgeted::new("static fixture", static_lease);
        let static_target = root.join("static.txt");
        write_bytes(
            &root,
            &static_target,
            false,
            "static payload test",
            &static_payload,
            &static_pool,
        )
        .unwrap();
        assert_eq!(fs::read(&static_target).unwrap(), b"static fixture");
        assert_eq!(static_pool.snapshot().reserved_bytes, 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn json_escaped_formats_into_text_and_releases_on_admission_failure() {
        let value = "snowman ☃ quote \" slash \\ newline\n tab\t control\u{0001} carriage\r";
        let pool = LiveMemoryPool::new(4096).unwrap();
        let mut text = Text::new(&pool).unwrap();
        write!(&mut text, "\"{}\"", JsonEscaped(value)).unwrap();
        let encoded = text.finish().unwrap();
        assert_eq!(
            serde_json::from_str::<String>(encoded.value()).unwrap(),
            value
        );
        assert_eq!(pool.snapshot().reserved_bytes, encoded.reserved_bytes());
        drop(encoded);
        assert_eq!(pool.snapshot().reserved_bytes, 0);

        let tiny = LiveMemoryPool::new(4).unwrap();
        let mut text = Text::new(&tiny).unwrap();
        assert!(write!(&mut text, "\"{}\"", JsonEscaped("long quoted input")).is_err());
        assert!(text.finish().is_err());
        assert_eq!(tiny.snapshot().reserved_bytes, 0);
    }
}
