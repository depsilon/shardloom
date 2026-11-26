//! Held file generations, ordered directory membership and optional full-byte
//! fingerprints. Metadata admission never decodes pages or writes preparation.

use super::{ArrowReaderMetadata, CancellationToken, OwnedLocalRead, Result, failed};
use crate::{source_identity::SourceIdentity, universal_format_io::local_dataset::LocalDataset};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[path = "metadata_admission.rs"]
pub(super) mod metadata_admission;

pub(super) struct ColumnarFile {
    path: PathBuf,
    pub(super) identity: Arc<SourceIdentity>,
    pub(super) metadata: ArrowReaderMetadata,
    pub(super) encoded_metadata: Option<ArrowReaderMetadata>,
}

#[derive(Clone, Copy)]
pub(super) struct PhysicalParquetFile<'a> {
    pub(super) identity: &'a Arc<SourceIdentity>,
    pub(super) metadata: &'a ArrowReaderMetadata,
    pub(super) encoded_metadata: Option<&'a ArrowReaderMetadata>,
    pub(super) partitions: &'a [(String, Option<String>)],
}

/// One held local Parquet source, including a sorted partition directory.
/// Other formats require their own explicit reader admission.
pub struct LocalColumnarSource {
    pub(super) collection: LocalDataset,
    pub(super) invalidated: AtomicBool,
    pub(super) other: Option<super::generic::Source>,
    pub(super) files: Vec<ColumnarFile>,
    pub(super) metadata_read_bytes: u64,
    pub(super) metadata_credit: MemoryLease,
    pub(super) row_groups_total: usize,
    pub(super) fingerprint_read_bytes: u64,
    pub(super) content_fingerprints: Vec<String>,
}

impl LocalColumnarSource {
    pub(super) fn schema(&self) -> arrow_schema::SchemaRef {
        self.other.as_ref().map_or_else(
            || Arc::clone(self.metadata().schema()),
            |source| Arc::clone(&source.schema),
        )
    }

    pub(super) fn inventory(&self) -> impl Iterator<Item = (&Path, &Arc<SourceIdentity>)> {
        self.files
            .iter()
            .map(|file| (file.path.as_path(), &file.identity))
            .chain(
                self.other
                    .iter()
                    .flat_map(|source| source.files.iter())
                    .map(|file| (file.path.as_path(), &file.identity)),
            )
    }

    pub(super) fn metadata(&self) -> &ArrowReaderMetadata {
        &self.files[0].metadata
    }

    /// Physical metadata remains a capability of the held source even when its
    /// logical schema requires name alignment or virtual partition columns.
    pub(super) fn parquet_metadata(&self) -> impl Iterator<Item = &ArrowReaderMetadata> {
        self.files.iter().map(|file| &file.metadata).chain(
            self.other
                .iter()
                .flat_map(|source| source.files.iter())
                .filter_map(|file| file.parquet.as_ref()),
        )
    }

    pub(super) fn is_parquet(&self) -> bool {
        self.other
            .as_ref()
            .is_none_or(|source| source.format == "parquet" && source.table.is_none())
    }

    pub(super) fn parquet_file_count(&self) -> usize {
        if let Some(other) = &self.other {
            other.files.len()
        } else {
            self.files.len()
        }
    }

    pub(super) fn parquet_file(&self, file_index: usize) -> PhysicalParquetFile<'_> {
        let partitions = self
            .collection
            .files
            .get(file_index)
            .map_or(&[][..], |file| file.partitions.as_slice());
        if let Some(other) = &self.other {
            let file = &other.files[file_index];
            PhysicalParquetFile {
                identity: &file.identity,
                metadata: file
                    .parquet
                    .as_ref()
                    .expect("wrapped Parquet file must carry Parquet metadata"),
                encoded_metadata: file.encoded_parquet.as_ref(),
                partitions,
            }
        } else {
            let file = &self.files[file_index];
            PhysicalParquetFile {
                identity: &file.identity,
                metadata: &file.metadata,
                encoded_metadata: file.encoded_metadata.as_ref(),
                partitions,
            }
        }
    }

    pub(super) fn physical_schema_discovery_read_bytes(&self) -> u64 {
        self.other
            .as_ref()
            .map_or(0, |other| other.schema_discovery_read_bytes)
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
            for (_, identity) in self.inventory() {
                cancellation.check()?;
                identity.validate()?;
            }
            self.collection.validate(cancellation)?;
            if let Some(table) = self.other.as_ref().and_then(|source| source.table.as_ref()) {
                table.validate(cancellation)?;
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
        self.collection.validate_destination(path)?;
        if let Some(table) = self.other.as_ref().and_then(|source| source.table.as_ref()) {
            table.validate_destination(path)?;
        }
        match std::fs::metadata(path) {
            Ok(target)
                if self.inventory().any(|(_, identity)| {
                    target.dev() == identity.generation.device
                        && target.ino() == identity.generation.inode
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
    #[allow(clippy::too_many_lines)] // Keep generation, metadata and credit admission in one transaction.
    pub fn admit_with_content_fingerprint(
        path: &Path,
        format: &str,
        fingerprint: bool,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        cancellation.check()?;
        if format != "parquet" {
            return super::generic::admit(path, format, fingerprint, memory, cancellation, None);
        }
        let mut credit = memory.reserve(
            (path.as_os_str().len() as u64)
                .checked_mul(4)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| failed("source metadata capacity overflow"))?,
        )?;
        let path = std::path::absolute(path).map_err(failed)?;
        let collection = LocalDataset::admit(&path, format, memory, cancellation)?;
        if collection.union_by_name
            || collection
                .files
                .iter()
                .any(|file| !file.partitions.is_empty())
        {
            return super::generic::admit_collection(
                collection,
                "parquet",
                fingerprint,
                memory,
                cancellation,
                None,
            );
        }
        for file in &collection.files {
            let bytes = (file.path.as_os_str().len() as u64)
                .checked_mul(4)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| failed("source inventory capacity overflow"))?;
            credit.resize(
                credit
                    .bytes()
                    .checked_add(bytes)
                    .ok_or_else(|| failed("source inventory capacity overflow"))?,
            )?;
        }
        let mut files: Vec<ColumnarFile> = Vec::new();
        files
            .try_reserve_exact(collection.files.len())
            .map_err(failed)?;
        let mut content_fingerprints = Vec::new();
        if fingerprint {
            content_fingerprints
                .try_reserve_exact(collection.files.len())
                .map_err(failed)?;
        }
        let mut metadata_read_bytes = 0_u64;
        let mut fingerprint_read_bytes = 0_u64;
        let mut row_groups_total = 0_usize;
        for collected in &collection.files {
            cancellation.check()?;
            let path = collected.path.clone();
            let identity = Arc::new(SourceIdentity::capture(&path)?);
            let mut file = identity.open_checked()?;
            super::super::metadata::footer_length(&mut file, b"PAR1", 4)?;
            let admission = metadata_admission::Admission::new(memory, cancellation)?;
            let reader = OwnedLocalRead::metadata(Arc::clone(&identity), memory, cancellation)?;
            let metadata = ArrowReaderMetadata::load(
                &reader,
                super::super::parquet_metadata_first_reader_options()
                    .with_metadata_memory_reservation(admission.clone()),
            )
            .map_err(metadata_admission::source_error)?;
            let mut footer_credit = admission.finish()?;
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
            let encoded_metadata = super::super::demanded_parquet_dictionary_schema_hint(
                metadata.schema().as_ref(),
                metadata.metadata().as_ref(),
            )
            .map(|hint| -> Result<ArrowReaderMetadata> {
                let bytes = super::generic::schema_bytes(&hint, (memory, cancellation).into())?;
                let mut hint_credit = memory.reserve(bytes.saturating_mul(2))?;
                let encoded = ArrowReaderMetadata::try_new(
                    Arc::clone(metadata.metadata()),
                    super::super::parquet_reader_options_with_optional_schema_hint(Some(&hint)),
                )
                .map_err(metadata_admission::source_error)?;
                credit.absorb(&mut hint_credit)?;
                Ok(encoded)
            })
            .transpose()?;
            cancellation.check()?;
            identity.validate()?;
            credit.absorb(&mut footer_credit)?;
            files.push(ColumnarFile {
                path,
                identity,
                metadata,
                encoded_metadata,
            });
        }
        let source = Arc::new(Self {
            collection,
            invalidated: AtomicBool::new(false),
            other: None,
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

pub(super) fn content_fingerprint(
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
