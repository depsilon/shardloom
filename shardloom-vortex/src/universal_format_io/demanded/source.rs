//! Held file generations, ordered directory membership and optional full-byte
//! fingerprints. Metadata admission never decodes pages or writes preparation.

use super::{ArrowReaderMetadata, CancellationToken, OwnedLocalRead, Result, failed};
use crate::{prepared_source_binding::visit_source_paths, source_identity::SourceIdentity};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub(super) struct ColumnarFile {
    path: PathBuf,
    pub(super) identity: Arc<SourceIdentity>,
    pub(super) metadata: ArrowReaderMetadata,
}

/// One held local Parquet source, including a sorted partition directory.
/// Other formats require their own explicit reader admission.
pub struct LocalColumnarSource {
    directory: Option<PathBuf>,
    invalidated: AtomicBool,
    pub(super) files: Vec<ColumnarFile>,
    pub(super) metadata_read_bytes: u64,
    pub(super) metadata_credit: MemoryLease,
    pub(super) row_groups_total: usize,
    pub(super) fingerprint_read_bytes: u64,
    pub(super) content_fingerprints: Vec<String>,
}

impl LocalColumnarSource {
    pub(super) fn metadata(&self) -> &ArrowReaderMetadata {
        &self.files[0].metadata
    }

    pub(crate) fn validate_generation(&self) -> Result<()> {
        self.validate_generation_controlled(&CancellationToken::default())
    }

    pub(crate) fn validate_generation_controlled(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        cancellation.check()?;
        if self.invalidated.load(Ordering::Acquire) {
            return Err(failed(
                "source inventory was invalidated; prepare the source again",
            ));
        }
        let result = (|| {
            for file in &self.files {
                cancellation.check()?;
                file.identity.validate()?;
            }
            if let Some(directory) = &self.directory {
                let mut count = 0;
                visit_source_paths(directory, "parquet", cancellation, |path| {
                    if self
                        .files
                        .binary_search_by(|file| file.path.as_path().cmp(path))
                        .is_err()
                    {
                        return Err(failed(
                            "source directory membership changed; prepare the source again",
                        ));
                    }
                    count += 1;
                    Ok(())
                })?;
                if count != self.files.len() {
                    return Err(failed(
                        "source directory membership changed; prepare the source again",
                    ));
                }
            }
            cancellation.check()
        })();
        if result.is_err() && !cancellation.is_cancelled() {
            self.invalidated.store(true, Ordering::Release);
        }
        result
    }

    pub(crate) fn validate_destination(&self, path: &Path) -> Result<()> {
        use std::os::unix::fs::MetadataExt as _;
        self.validate_generation()?;
        if let Some(directory) = &self.directory
            && path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case("parquet"))
            && let Some(parent) = std::path::absolute(path).map_err(failed)?.parent()
            && parent.canonicalize().ok().as_ref()
                == Some(&directory.canonicalize().map_err(failed)?)
        {
            return Err(failed(
                "output must not change the input directory's Parquet membership",
            ));
        }
        match std::fs::metadata(path) {
            Ok(target)
                if self.files.iter().any(|file| {
                    target.dev() == file.identity.generation.device
                        && target.ino() == file.identity.generation.inode
                }) =>
            {
                Err(failed("source and output must be different files"))
            }
            Ok(_) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(failed(error)),
        }
    }

    /// Bind file/directory metadata without decoding row pages or persisting a
    /// native copy. Directory membership follows shared preparation discovery.
    /// # Errors
    /// Rejects unsupported formats, empty/mixed-schema inventories, malformed
    /// metadata, source changes, cancellation and insufficient shared capacity.
    pub fn admit(
        path: &Path,
        format: &str,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        Self::admit_with_content_fingerprint(path, format, false, memory, cancellation)
    }

    /// Optionally fingerprint every source byte under the same memory owner.
    /// Fingerprinting does not decode/validate undemanded pages. Values use the
    /// existing local FNV-64 content-digest convention, not authentication.
    /// # Errors
    /// Propagates admission, read, cancellation and generation failures.
    pub fn admit_with_content_fingerprint(
        path: &Path,
        format: &str,
        fingerprint: bool,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        cancellation.check()?;
        if format != "parquet" {
            return Err(failed("source has no admitted demand reader"));
        }
        let mut credit = memory.reserve(
            (path.as_os_str().len() as u64)
                .checked_mul(4)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| failed("source metadata capacity overflow"))?,
        )?;
        let path = std::path::absolute(path).map_err(failed)?;
        let directory = path.is_dir().then(|| path.clone());
        let mut paths = Vec::new();
        visit_source_paths(&path, format, cancellation, |path| {
            let bytes = (path.as_os_str().len() as u64)
                .checked_mul(4)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| failed("source inventory capacity overflow"))?;
            credit.resize(
                credit
                    .bytes()
                    .checked_add(bytes)
                    .ok_or_else(|| failed("source inventory capacity overflow"))?,
            )?;
            paths.try_reserve(1).map_err(failed)?;
            paths.push(path.to_owned());
            Ok(())
        })?;
        paths.sort();
        let mut files: Vec<ColumnarFile> = Vec::new();
        files.try_reserve_exact(paths.len()).map_err(failed)?;
        let mut content_fingerprints = Vec::new();
        if fingerprint {
            content_fingerprints
                .try_reserve_exact(paths.len())
                .map_err(failed)?;
        }
        let mut metadata_read_bytes = 0_u64;
        let mut fingerprint_read_bytes = 0_u64;
        let mut row_groups_total = 0_usize;
        for path in paths {
            cancellation.check()?;
            let identity = Arc::new(SourceIdentity::capture(&path)?);
            let mut file = identity.open_checked()?;
            let length = super::super::metadata::footer_length(&mut file, b"PAR1", 4)?;
            let mut footer_credit = memory.reserve(
                (length as u64)
                    .checked_mul(32)
                    .and_then(|n| n.checked_add(8192))
                    .ok_or_else(|| failed("source metadata capacity overflow"))?,
            )?;
            let reader = OwnedLocalRead::metadata(Arc::clone(&identity), memory, cancellation)?;
            let metadata = ArrowReaderMetadata::load(
                &reader,
                super::super::parquet_metadata_first_reader_options(),
            )
            .map_err(failed)?;
            if files
                .first()
                .is_some_and(|first| first.metadata.schema().fields() != metadata.schema().fields())
            {
                return Err(failed("source directory files have incompatible schemas"));
            }
            metadata_read_bytes = metadata_read_bytes
                .checked_add(reader.metrics().bytes.load(Ordering::Relaxed))
                .and_then(|n| n.checked_add(12))
                .ok_or_else(|| failed("source metadata read count overflow"))?;
            row_groups_total = row_groups_total
                .checked_add(metadata.metadata().num_row_groups())
                .ok_or_else(|| failed("source row group count overflow"))?;
            if fingerprint {
                content_fingerprints.push(content_fingerprint(&reader, cancellation)?);
                fingerprint_read_bytes = fingerprint_read_bytes
                    .checked_add(reader.len())
                    .ok_or_else(|| failed("fingerprint byte count overflow"))?;
            }
            cancellation.check()?;
            identity.validate()?;
            credit.absorb(&mut footer_credit)?;
            files.push(ColumnarFile {
                path,
                identity,
                metadata,
            });
        }
        let source = Arc::new(Self {
            directory,
            invalidated: AtomicBool::new(false),
            files,
            metadata_read_bytes,
            metadata_credit: credit,
            row_groups_total,
            fingerprint_read_bytes,
            content_fingerprints,
        });
        source.validate_generation_controlled(cancellation)?;
        cancellation.check()?;
        Ok(source)
    }
}

fn content_fingerprint(
    reader: &OwnedLocalRead,
    cancellation: &CancellationToken,
) -> Result<String> {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut position = 0;
    while position < reader.len() {
        cancellation.check()?;
        let length = usize::try_from((reader.len() - position).min(1 << 20)).map_err(failed)?;
        let bytes = reader.bytes(position, length)?;
        for byte in bytes.as_ref() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        position += length as u64;
    }
    cancellation.check()?;
    Ok(format!("fnv64:{hash:016x}"))
}
