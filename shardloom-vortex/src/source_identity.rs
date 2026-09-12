//! Held local file generations shared by native readers and compatibility intake.

use std::{
    fs::{File, Metadata},
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    time::SystemTime,
};

use shardloom_core::{Result, ShardLoomError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileGeneration {
    pub(crate) len: u64,
    modified: SystemTime,
    pub(crate) device: u64,
    pub(crate) inode: u64,
    changed: (i64, i64),
}

impl FileGeneration {
    #[cfg(unix)]
    pub(crate) fn read(metadata: &Metadata) -> Result<Self> {
        use std::os::unix::fs::MetadataExt as _;
        Ok(Self {
            len: metadata.len(),
            modified: metadata.modified().map_err(identity_error)?,
            device: metadata.dev(),
            inode: metadata.ino(),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }

    #[cfg(not(unix))]
    pub(crate) fn read(_: &Metadata) -> Result<Self> {
        Err(identity_error(
            "resident file generation identity is not supported on this platform",
        ))
    }
}

/// Opaque held generation of a local source file.
///
/// Adapters retain this identity through EOF and output publication. Validation
/// compares the path and held descriptor's device, inode, length, modification
/// and change timestamps; it is not a content hash or a filesystem snapshot.
/// Strong local generation admission is currently available on Unix only.
pub struct SourceIdentity {
    path: PathBuf,
    pub(crate) file: File,
    pub(crate) generation: FileGeneration,
    invalidated: AtomicBool,
}

impl SourceIdentity {
    /// Capture a generation for a native path that must reopen the source.
    #[cfg(any(unix, feature = "vortex-local-primitives"))]
    pub(crate) fn capture(path: &std::path::Path) -> Result<Self> {
        let path = std::path::absolute(path).map_err(identity_error)?;
        let file = File::open(&path).map_err(identity_error)?;
        let metadata = file.metadata().map_err(identity_error)?;
        if !metadata.is_file() {
            return Err(identity_error("source must be a regular file"));
        }
        let identity = Self {
            path,
            file,
            generation: FileGeneration::read(&metadata)?,
            invalidated: AtomicBool::new(false),
        };
        identity.validate()?;
        Ok(identity)
    }

    /// Reject a changed generation, including all later uses after invalidation.
    ///
    /// # Errors
    /// Returns an error if the held file or current path changed, disappeared,
    /// became unreadable, or was previously invalidated.
    pub fn validate(&self) -> Result<()> {
        if self.invalidated.load(Ordering::Acquire) {
            return Err(identity_error(
                "prepared source generation invalidated; prepare the source again",
            ));
        }
        let result = (|| {
            let path =
                FileGeneration::read(&std::fs::metadata(&self.path).map_err(identity_error)?)?;
            let handle = FileGeneration::read(&self.file.metadata().map_err(identity_error)?)?;
            if path != self.generation || handle != self.generation {
                return Err(identity_error(
                    "prepared source changed; prepare the source again",
                ));
            }
            Ok(())
        })();
        if result.is_err() {
            self.invalidated.store(true, Ordering::Release);
        }
        result
    }

    /// Open an independent cursor only after matching it to the held generation.
    #[cfg(feature = "universal-format-io")]
    pub(crate) fn open_checked(&self) -> Result<File> {
        self.validate()?;
        let result = (|| {
            let file = File::open(&self.path).map_err(identity_error)?;
            if FileGeneration::read(&file.metadata().map_err(identity_error)?)? != self.generation {
                return Err(identity_error(
                    "prepared source changed while opening a reader; prepare the source again",
                ));
            }
            self.validate()?;
            Ok(file)
        })();
        if result.is_err() {
            self.invalidated.store(true, Ordering::Release);
        }
        result
    }
}

fn identity_error(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{error}; no fallback execution was attempted"))
}
