//! Immutable native file layouts over owned memory segments, with explicit durability.
//!
//! Arrays are serialized once into one admitted Flat segment. Queries use the
//! ordinary native file scanner; durable publication reuses the segment bytes.
//! No serialized template, whole-file staging buffer, or external engine is used.

use std::{
    fmt::Write as _,
    fs,
    io::Write as _,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use futures::{FutureExt as _, future::BoxFuture};
use sha2::{Digest, Sha256};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{ArrayContext, ArrayRef, buffer::BufferHandle, dtype::DType, memory::HostAllocatorRef},
    buffer::ByteBuffer,
    editions::{ComponentKind, EditionSessionExt as _},
    error::{VortexResult, vortex_err},
    expr::Expression,
    file::{Footer, MAGIC_BYTES, OpenOptionsSessionExt as _, SegmentSpec, VortexFile},
    io::runtime::BlockingRuntime as _,
    layout::{
        LayoutContext, LayoutStrategy, LayoutWriterContext,
        layouts::flat::writer::FlatLayoutStrategy,
        segments::{SegmentFuture, SegmentId, SegmentSink, SegmentSource},
        sequence::{SequenceId, SequentialArrayStreamExt as _},
    },
    session::registry::ReadContext,
};

use crate::{
    local_primitives::{
        collect::{CollectedVortexRows, memory_certificate, render_owned_json},
        native_sink::OwnedOutput,
    },
    resident_session::{PreparedVortexProjection, PreparedVortexSource, ResidentVortexSession},
};

/// Bounds for this flat, immutable generation. Intake already admits at most
/// 65,536 rows and 64 typed fields. No generic streaming-ingest claim follows.
#[derive(Debug, Clone, Copy)]
pub struct MemoryFileGenerationBounds {
    pub max_serialized_bytes: u64,
    pub max_metadata_bytes: u64,
}

impl Default for MemoryFileGenerationBounds {
    fn default() -> Self {
        Self {
            max_serialized_bytes: 64 * 1024 * 1024,
            max_metadata_bytes: 128 * 1024,
        }
    }
}

/// Actual generation work. Counts describe calls/bytes at the named adapter
/// boundaries, not uninstrumented codec internals, allocator bypasses, or RSS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryFileGenerationEvidence {
    pub input_logical_bytes: u64,
    /// Numeric value byte copies and UTF8 payload copies at typed intake. Offset,
    /// bitmap and name construction are not included.
    pub intake_payload_bytes_copied: u64,
    pub segment_assembly_bytes_copied: u64,
    pub array_serializer_calls: u64,
    pub dictionary_build_calls: u64,
    pub memory_file_constructions: u64,
    pub source_file_opens: u64,
    pub memory_segment_requests: u64,
    pub memory_segment_bytes_returned: u64,
}

/// Completed durable publication of precisely the generation's encoded bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryFilePublication {
    pub rows: u64,
    pub file_bytes_written: u64,
    pub independent_readback_bytes: u64,
    pub output_sha256: String,
    pub validation_file_opens: u64,
    pub array_serializer_calls: u64,
    pub dictionary_build_calls: u64,
    pub footer_serializer_calls: u64,
    pub durable: bool,
    pub fallback_attempted: bool,
}

struct GenerationOwner {
    source: PreparedVortexSource,
    session: ResidentVortexSession,
    segments: Arc<MemorySegments>,
    input_logical_bytes: u64,
    intake_payload_bytes_copied: u64,
    bounds: MemoryFileGenerationBounds,
}

/// A real Vortex file whose immutable segments are backed by reservation-owned
/// memory. Clones share the generation and its cached native reader tree.
#[derive(Clone)]
pub struct MemoryFileGeneration(Arc<GenerationOwner>);

impl MemoryFileGeneration {
    pub(crate) fn build(
        session: &ResidentVortexSession,
        array: &ArrayRef,
        input_logical_bytes: usize,
        intake_payload_bytes_copied: u64,
        bounds: MemoryFileGenerationBounds,
    ) -> Result<Self> {
        if bounds.max_serialized_bytes == 0 || bounds.max_metadata_bytes < 128 * 1024 {
            return Err(generation_error(
                "generation requires positive serialized bytes and at least 128 KiB metadata",
            ));
        }
        let metadata = session.memory().reserve(bounds.max_metadata_bytes)?;
        let sink = Arc::new(MemorySegmentBuilder {
            allocator: session.native_allocator(),
            max_bytes: bounds.max_serialized_bytes,
            segments: Mutex::new(Vec::with_capacity(1)),
        });
        let file = session.with_native_session(|native, runtime| {
            let mut enabled = native.enabled_component_ids(ComponentKind::Array);
            enabled.sort();
            let context =
                ArrayContext::new(enabled.clone()).with_allowed_ids(enabled.into_iter().collect());
            let (pointer, eof) = SequenceId::root().split();
            let segments = Arc::clone(&sink);
            let layout = runtime
                .block_on(FlatLayoutStrategy::default().write_stream(
                    LayoutWriterContext::new(context.clone()),
                    segments,
                    array.to_array_stream().sequenced(pointer),
                    eof,
                    native,
                ))
                .map_err(generation_error)?;
            let owned = sink
                .segments
                .lock()
                .map_err(|_| generation_error("segment builder poisoned"))?
                .clone();
            let specs = owned.iter().map(|segment| segment.spec).collect::<Vec<_>>();
            let segments = Arc::new(MemorySegments {
                segments: owned,
                _metadata: metadata,
                requests: AtomicU64::new(0),
                returned_bytes: AtomicU64::new(0),
            });
            let footer = Footer::new(
                layout,
                specs.into(),
                None,
                ReadContext::new(context.to_ids()),
            );
            let file = VortexFile::new(footer, segments.clone(), native.clone()).with_caching();
            // Constructing the native reader validates layout/provider admission
            // before making this generation visible; it does not fetch segments.
            file.layout_reader().map_err(generation_error)?;
            Ok((file, segments))
        })?;
        let (file, segments) = file;
        Ok(Self(Arc::new(GenerationOwner {
            source: session.prepare_immutable_file(file),
            session: session.clone(),
            segments,
            input_logical_bytes: input_logical_bytes as u64,
            intake_payload_bytes_copied,
            bounds,
        })))
    }

    #[must_use]
    pub fn dtype(&self) -> &DType {
        self.0.source.dtype()
    }

    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.0.source.file().row_count()
    }

    #[must_use]
    pub fn evidence(&self) -> MemoryFileGenerationEvidence {
        MemoryFileGenerationEvidence {
            input_logical_bytes: self.0.input_logical_bytes,
            intake_payload_bytes_copied: self.0.intake_payload_bytes_copied,
            segment_assembly_bytes_copied: self
                .0
                .segments
                .segments
                .iter()
                .map(|segment| u64::from(segment.spec.length))
                .sum(),
            array_serializer_calls: self.0.segments.segments.len() as u64,
            dictionary_build_calls: 0,
            memory_file_constructions: 1,
            source_file_opens: 0,
            memory_segment_requests: self.0.segments.requests.load(Ordering::Relaxed),
            memory_segment_bytes_returned: self.0.segments.returned_bytes.load(Ordering::Relaxed),
        }
    }

    /// Bind the existing native projection/filter/ordered-limit scanner. Prepared
    /// operations retain this immutable generation even after the builder drops.
    ///
    /// # Errors
    /// Rejects nonboolean filters, invalid fields and nonpositive/excessive bounds.
    pub fn prepare_projection(
        &self,
        columns: &[&str],
        filter: Option<Expression>,
        max_rows: u64,
        max_output_bytes: u64,
    ) -> Result<PreparedVortexProjection> {
        if max_rows > 65_536 || max_output_bytes > self.0.bounds.max_serialized_bytes {
            return Err(generation_error(
                "projection bounds exceed admitted generation output",
            ));
        }
        let filter = filter
            .map(|filter| {
                filter
                    .optimize_recursive(self.dtype())
                    .and_then(|filter| filter.bind(self.dtype()))
                    .map_err(generation_error)
            })
            .transpose()?;
        if filter
            .as_ref()
            .is_some_and(|filter| !matches!(filter.dtype(), DType::Bool(_)))
        {
            return Err(generation_error(
                "generation filter must return boolean values",
            ));
        }
        Ok(self
            .0
            .source
            .prepare_projection(columns, max_rows, max_output_bytes)?
            .with_filter(filter))
    }

    /// Execute a complete bounded query and explicitly render JSON scalar values.
    /// No answer is cached and the memory source opens no filesystem object.
    ///
    /// # Errors
    /// Rejects unsupported native/JSON values and complete result bound violations.
    pub fn collect(
        &self,
        columns: &[&str],
        filter: Option<Expression>,
        max_rows: u64,
        max_output_bytes: u64,
    ) -> Result<CollectedVortexRows> {
        let filtered = filter.is_some();
        let operation = self.prepare_projection(columns, filter, max_rows, max_output_bytes)?;
        let arrays = operation.execute()?;
        let names = columns
            .iter()
            .map(|column| (*column).to_owned())
            .collect::<Vec<_>>();
        let values_json = render_owned_json(
            &arrays,
            &names,
            self.0.session.memory(),
            usize::try_from(max_output_bytes).map_err(generation_error)?,
        )?;
        let rows = arrays.row_count();
        let mut certificate = memory_certificate(rows, filtered)?;
        certificate.certificate_id = "resident.memory_file.bounded_collect.native_io".into();
        certificate.path_id = "immutable_vortex_file_segments_to_bounded_json".into();
        certificate.source_capability_report.source_kind = "immutable_vortex_file_segments".into();
        certificate.source_capability_report.adapter_id =
            "shardloom.resident_vortex.memory_file.v1".into();
        certificate.source_capability_report.schema_discovery_status =
            "validated_immutable_native_footer".into();
        certificate.source_capability_report.statistics_availability =
            "exact_footer_row_count;file_statistics_absent".into();
        certificate
            .source_capability_report
            .encoded_representation_preserved = true;
        certificate.source_capability_report.streaming_capability = true;
        certificate.source_pushdown_report.proof_basis = format!(
            "Vortex {} native file scan over immutable owned segments; exact filter before ordered result limit",
            crate::UPSTREAM_VORTEX_PROVIDER_VERSION
        );
        certificate.representation_transitions =
            vec![shardloom_core::NativeIoRepresentationTransition::new(
                shardloom_core::RepresentationState::VortexEncoded,
                shardloom_core::RepresentationState::MaterializedRows,
                true,
            )];
        for boundary in &mut certificate.materialization_boundaries {
            boundary.from_state = shardloom_core::RepresentationState::VortexEncoded;
        }
        drop(arrays);
        Ok(CollectedVortexRows {
            rows,
            projected_columns: names,
            source_order_limit: Some(usize::try_from(max_rows).map_err(generation_error)?),
            values_json,
            runtime: self.0.session.snapshot(),
            native_io_certificate: certificate,
        })
    }

    /// Persist these exact encoded segments in one complete native file. Writes,
    /// full independent hash readback, native reopen validation, atomic publication
    /// and parent directory synchronization all finish before `durable=true`.
    /// Existing memory readers keep the same immutable segment source. This
    /// explicit method enables writing only to a new target; overwrite is absent.
    /// Its parent must already be a real directory. This bounded publication
    /// does not create directory ancestry whose durability would need extra syncs.
    ///
    /// # Errors
    /// Rejects unsafe/replaced destinations, bounds, corruption and I/O failures.
    /// If parent sync fails after publication, the error explicitly reports that
    /// the output was published but durability remains unconfirmed. The final
    /// boundary validates the published file generation; it cannot prevent a
    /// different writer from changing the pathname after this method returns.
    pub fn publish(&self, target: &Path) -> Result<MemoryFilePublication> {
        self.publish_with_validation(target, |_| Ok(()))
    }

    fn publish_with_validation(
        &self,
        target: &Path,
        before_validation: impl FnOnce(&mut fs::File) -> Result<()>,
    ) -> Result<MemoryFilePublication> {
        self.publish_with_hooks(target, before_validation, || Ok(()))
    }

    fn publish_with_hooks(
        &self,
        target: &Path,
        before_validation: impl FnOnce(&mut fs::File) -> Result<()>,
        after_publication: impl FnOnce() -> Result<()>,
    ) -> Result<MemoryFilePublication> {
        let parent = target
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let (parent_directory, admitted_parent) = admit_publication_parent(parent)?;
        let _scratch = self.0.session.memory().reserve(128 * 1024)?;
        let mut output = OwnedOutput::new(target, false)?;
        let (written, expected) = self.0.session.with_native_session(|native, _| {
            let mut writer = DigestWriter {
                file: &mut output.file,
                digest: Sha256::new(),
                bytes: 0,
            };
            writer.write(&MAGIC_BYTES)?;
            for segment in &self.0.segments.segments {
                let padding = segment
                    .spec
                    .offset
                    .checked_sub(writer.bytes)
                    .ok_or_else(|| generation_error("generation segment offsets overlap"))?;
                writer.write_padding(padding)?;
                writer.write(&segment.buffer)?;
            }
            let footer = self
                .0
                .source
                .file()
                .footer()
                .clone()
                .into_serializer()
                .with_layout_context(
                    LayoutContext::default().with_allowed_ids(
                        native
                            .enabled_component_ids(ComponentKind::Layout)
                            .into_iter()
                            .collect(),
                    ),
                )
                .with_offset(writer.bytes)
                .serialize()
                .map_err(generation_error)?;
            let metadata_bytes = footer.iter().try_fold(0_u64, |bytes, buffer| {
                bytes
                    .checked_add(buffer.len() as u64)
                    .ok_or_else(|| generation_error("footer byte overflow"))
            })?;
            if metadata_bytes > self.0.bounds.max_metadata_bytes {
                return Err(generation_error(
                    "native footer exceeded admitted metadata bytes",
                ));
            }
            for buffer in footer {
                writer.write(&buffer)?;
            }
            writer.file.sync_all().map_err(generation_error)?;
            Ok((writer.bytes, hex_digest(writer.digest.finalize().into())))
        })?;
        before_validation(&mut output.file)?;
        validate_publication_parent(parent, &parent_directory, admitted_parent, false)?;
        let verified_generation = PublishedOutputGeneration::read(&output.file)?;
        let actual = output.checksum()?;
        if actual != expected || output.file.metadata().map_err(generation_error)?.len() != written
        {
            return Err(generation_error(
                "independent readback differs from immutable generation bytes",
            ));
        }
        self.validate_staged_output(&output.temporary)?;
        validate_publication_parent(parent, &parent_directory, admitted_parent, false)?;
        output.commit()?;
        let published_generation = verified_generation.after_commit(&output.file)?;
        after_publication().map_err(publication_unconfirmed)?;
        parent_directory.sync_all().map_err(publication_unconfirmed)?;
        validate_publication_parent(parent, &parent_directory, admitted_parent, true)?;
        published_generation.validate(target, &output.file)?;
        Ok(MemoryFilePublication {
            rows: self.row_count(),
            file_bytes_written: written,
            independent_readback_bytes: written,
            output_sha256: actual,
            validation_file_opens: 1,
            array_serializer_calls: 0,
            dictionary_build_calls: 0,
            footer_serializer_calls: 1,
            durable: true,
            fallback_attempted: false,
        })
    }

    fn validate_staged_output(&self, path: &Path) -> Result<()> {
        self.0.session.with_native_session(|native, runtime| {
            let file = runtime
                .block_on(native.open_options().open_path(path))
                .map_err(generation_error)?;
            if file.dtype() != self.dtype() || file.row_count() != self.row_count() {
                return Err(generation_error(
                    "durable native dtype or row count differs from generation",
                ));
            }
            Ok(())
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct PublishedOutputGeneration {
    device: u64,
    inode: u64,
    bytes: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl PublishedOutputGeneration {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt as _;
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }

    fn read(file: &fs::File) -> Result<Self> {
        file.metadata()
            .map(|metadata| Self::from_metadata(&metadata))
            .map_err(generation_error)
    }

    fn after_commit(self, file: &fs::File) -> Result<Self> {
        let published = Self::read(file).map_err(publication_unconfirmed)?;
        // The owned link/unlink changes ctime. It cannot change the verified
        // contents' size or mtime, or the held inode's identity.
        let verified_after_link = Self {
            changed: published.changed,
            ..self
        };
        if verified_after_link != published {
            return Err(publication_unconfirmed(
                "verified output changed during publication",
            ));
        }
        Ok(published)
    }

    fn validate(self, target: &Path, file: &fs::File) -> Result<()> {
        let current = fs::symlink_metadata(target).map_err(publication_unconfirmed)?;
        let held = Self::read(file).map_err(publication_unconfirmed)?;
        if !current.is_file()
            || current.file_type().is_symlink()
            || Self::from_metadata(&current) != self
            || held != self
        {
            return Err(publication_unconfirmed("published output generation changed"));
        }
        Ok(())
    }
}

fn publication_unconfirmed(error: impl std::fmt::Display) -> ShardLoomError {
    generation_error(format!(
        "output was published but durability is unconfirmed: {error}"
    ))
}

#[derive(Clone)]
struct MemorySegment {
    spec: SegmentSpec,
    buffer: ByteBuffer,
}

struct MemorySegmentBuilder {
    allocator: HostAllocatorRef,
    max_bytes: u64,
    segments: Mutex<Vec<MemorySegment>>,
}

impl SegmentSink for MemorySegmentBuilder {
    fn write<'a, 'future>(
        &'a self,
        mut sequence_id: SequenceId,
        buffers: Vec<ByteBuffer>,
    ) -> BoxFuture<'future, VortexResult<SegmentId>>
    where
        'a: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            sequence_id.collapse().await;
            let mut segments = self
                .segments
                .lock()
                .map_err(|_| vortex_err!("memory segment builder poisoned"))?;
            if !segments.is_empty() {
                return Err(vortex_err!("flat memory generation permits one segment"));
            }
            let length = buffers.iter().try_fold(0_usize, |bytes, buffer| {
                bytes
                    .checked_add(buffer.len())
                    .ok_or_else(|| vortex_err!("memory segment length overflow"))
            })?;
            let alignment = buffers
                .first()
                .map_or(vortex::buffer::Alignment::none(), ByteBuffer::alignment);
            let offset = (MAGIC_BYTES.len() as u64)
                .checked_add(*alignment as u64 - 1)
                .map(|offset| offset / *alignment as u64 * *alignment as u64)
                .ok_or_else(|| vortex_err!("memory segment offset overflow"))?;
            if offset
                .checked_add(length as u64)
                .is_none_or(|bytes| bytes > self.max_bytes)
            {
                return Err(vortex_err!(
                    "memory generation serialized byte bound exceeded"
                ));
            }
            let spec = SegmentSpec {
                offset,
                length: u32::try_from(length)
                    .map_err(|_| vortex_err!("memory segment exceeds u32 bytes"))?,
                alignment,
            };
            let mut output = self.allocator.allocate(length, alignment)?;
            let mut cursor = 0;
            for buffer in buffers {
                output.as_mut_slice()[cursor..cursor + buffer.len()].copy_from_slice(&buffer);
                cursor += buffer.len();
            }
            segments.push(MemorySegment {
                spec,
                buffer: output.freeze(),
            });
            Ok(SegmentId::from(0))
        })
    }
}

fn admit_publication_parent(parent: &Path) -> Result<(fs::File, (u64, u64))> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = fs::symlink_metadata(parent).map_err(|error| {
        generation_error(format!(
            "durable publication requires an existing real parent directory: {error}"
        ))
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(generation_error(
            "durable publication requires an existing real parent directory",
        ));
    }
    let identity = (metadata.dev(), metadata.ino());
    // Retain the directory being synchronized instead of reopening its path
    // after publication. No output preflight runs before this admission.
    let directory = fs::File::open(parent).map_err(generation_error)?;
    validate_publication_parent(parent, &directory, identity, false)?;
    Ok((directory, identity))
}

fn validate_publication_parent(
    path: &Path,
    directory: &fs::File,
    expected: (u64, u64),
    published: bool,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let valid = || -> std::io::Result<bool> {
        let held = directory.metadata()?;
        let current = fs::symlink_metadata(path)?;
        Ok(held.is_dir()
            && current.is_dir()
            && !current.file_type().is_symlink()
            && (held.dev(), held.ino()) == expected
            && (current.dev(), current.ino()) == expected)
    };
    if valid().unwrap_or(false) {
        Ok(())
    } else if published {
        Err(generation_error(
            "output was published but directory durability is unconfirmed: parent directory identity changed",
        ))
    } else {
        Err(generation_error(
            "parent directory identity changed before publication; owned staging may have moved with the admitted directory",
        ))
    }
}

struct MemorySegments {
    segments: Vec<MemorySegment>,
    _metadata: MemoryLease,
    requests: AtomicU64,
    returned_bytes: AtomicU64,
}

impl SegmentSource for MemorySegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let buffer = self
            .segments
            .get(*id as usize)
            .map(|segment| segment.buffer.clone());
        if let Some(buffer) = &buffer {
            self.returned_bytes
                .fetch_add(buffer.len() as u64, Ordering::Relaxed);
        }
        async move {
            buffer
                .map(BufferHandle::new_host)
                .ok_or_else(|| vortex_err!("unknown immutable memory segment {id}"))
        }
        .boxed()
    }
}

struct DigestWriter<'a> {
    file: &'a mut fs::File,
    digest: Sha256,
    bytes: u64,
}
impl DigestWriter<'_> {
    fn write_padding(&mut self, mut remaining: u64) -> Result<()> {
        let zeros = [0_u8; 4096];
        while remaining > 0 {
            let amount =
                usize::try_from(remaining.min(zeros.len() as u64)).map_err(generation_error)?;
            self.write(&zeros[..amount])?;
            remaining -= amount as u64;
        }
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_all(bytes).map_err(generation_error)?;
        self.digest.update(bytes);
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| generation_error("publication byte overflow"))?;
        Ok(())
    }
}

fn hex_digest(digest: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing digest hex cannot fail");
    }
    encoded
}

fn generation_error(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "immutable memory Vortex generation: {error}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "memory_file_generation_tests.rs"]
mod tests;
