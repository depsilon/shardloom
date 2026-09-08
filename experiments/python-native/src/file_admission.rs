//! Stable local preflight generation around native preparation, with no payload read.

use std::{
    fs::{File, Metadata},
    io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

#[derive(Debug, PartialEq, Eq)]
struct Generation {
    device: u64,
    inode: u64,
    bytes: u64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
}
impl Generation {
    fn read(metadata: &Metadata) -> io::Result<Self> {
        if !metadata.is_file() {
            return Err(io::Error::other(
                "native experiment requires a regular file",
            ));
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            mtime: metadata.mtime(),
            mtime_nsec: metadata.mtime_nsec(),
            ctime: metadata.ctime(),
            ctime_nsec: metadata.ctime_nsec(),
        })
    }
}

pub(crate) struct FileAdmission {
    path: PathBuf,
    file: File,
    metadata: Metadata,
    generation: Generation,
}
impl FileAdmission {
    pub(crate) fn capture(path: &Path, max_bytes: u64) -> io::Result<Self> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let generation = Generation::read(&metadata)?;
        if generation.bytes > max_bytes {
            return Err(io::Error::other(
                "native experiment source exceeds its 16 MiB preflight bound",
            ));
        }
        let admitted = Self {
            path: path.to_owned(),
            file,
            metadata,
            generation,
        };
        admitted.validate()?;
        Ok(admitted)
    }

    /// Compare this admission with the provider's actual retained generation
    /// before checking the preflight descriptor and path again. A path that
    /// switched to another valid file only during prepare must not be published.
    pub(crate) fn validate_prepared<E: std::fmt::Display>(
        &self,
        validate_provider: impl FnOnce(&Metadata) -> Result<(), E>,
    ) -> io::Result<()> {
        validate_provider(&self.metadata).map_err(|error| io::Error::other(error.to_string()))?;
        self.validate()
    }

    pub(crate) fn validate(&self) -> io::Result<()> {
        if Generation::read(&std::fs::metadata(&self.path)?)? != self.generation
            || Generation::read(&self.file.metadata()?)? != self.generation
        {
            return Err(io::Error::other(
                "native experiment source generation changed during preparation; no prepared handle returned",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Write as _,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);

    pub(super) struct Directory(pub(super) PathBuf);
    impl Directory {
        pub(super) fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "shardloom-python-admission-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn preflight_descriptor_rejects_replacement_growth_and_unlink() {
        for mutation in ["replacement", "growth", "unlink"] {
            let directory = Directory::new();
            let path = directory.0.join("input");
            std::fs::write(&path, b"small").unwrap();
            let admitted = FileAdmission::capture(&path, 5).unwrap();
            match mutation {
                "replacement" => {
                    let replacement = directory.0.join("replacement");
                    std::fs::write(&replacement, b"larger file").unwrap();
                    std::fs::rename(replacement, &path).unwrap();
                }
                "growth" => File::options()
                    .append(true)
                    .open(&path)
                    .unwrap()
                    .write_all(b"!")
                    .unwrap(),
                _ => std::fs::remove_file(&path).unwrap(),
            }
            assert!(admitted.validate().is_err(), "{mutation}");
        }
    }

    #[test]
    fn stable_preflight_is_repeatable_and_initial_oversize_fails() {
        let directory = Directory::new();
        let path = directory.0.join("input");
        std::fs::write(&path, b"small").unwrap();
        let admitted = FileAdmission::capture(&path, 5).unwrap();
        admitted.validate().unwrap();
        admitted.validate().unwrap();
        assert!(FileAdmission::capture(&path, 4).is_err());
    }
}

#[cfg(test)]
#[path = "file_admission_native_tests.rs"]
mod native_tests;
