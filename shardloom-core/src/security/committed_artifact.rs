//! Writer-origin digest admission for an immediate local publication.
//!
//! The retained descriptor is never exposed or reopened from a pathname. This
//! uses the same optimistic Unix generation contract as local source admission;
//! it is not a filesystem lock or independent persisted-content readback.

use super::{
    Result, ShardLoomError, WorkspaceSafeLocalStagingWriter, WorkspaceSafeLocalWritePlan,
    first_existing_symlink_component, same_workspace_output,
};
use std::fs::{self, File, Metadata};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[cfg(all(test, unix))]
mod tests;

/// Consumed proof that a workspace-safe writer committed its accepted bytes.
///
/// Only the validated writer can construct this receipt. It is deliberately
/// neither cloneable nor serializable, and a public write report is not proof.
pub struct WorkspaceSafeCommittedArtifact {
    file: File,
    generation: Metadata,
    workspace: PathBuf,
    path: PathBuf,
    bytes_written: u64,
    digest: String,
}

impl std::fmt::Debug for WorkspaceSafeCommittedArtifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceSafeCommittedArtifact")
            .field("bytes_written", &self.bytes_written)
            .finish_non_exhaustive()
    }
}

impl WorkspaceSafeCommittedArtifact {
    /// Consume the writer proof during publication and validate the same
    /// committed generation before and after the callback. The callback receives
    /// the accepted-byte SHA-256 digest, exact length and modification time; none
    /// of these values alone can authorize subsequent digest reuse.
    ///
    /// # Errors
    /// Refuses a changed, replaced, missing, aliased or symlinked artifact, a
    /// different requested path, unavailable generation metadata, or callback
    /// failure. Does not read artifact payload bytes or replace a required
    /// independent storage readback.
    pub fn with_verified_digest<T>(
        self,
        expected_path: &Path,
        publish: impl FnOnce(&str, u64, SystemTime) -> Result<T>,
    ) -> Result<T> {
        self.validate(expected_path)?;
        let modified = self.generation.modified().map_err(|_| changed())?;
        let result = publish(&self.digest, self.bytes_written, modified)?;
        self.validate(expected_path)?;
        Ok(result)
    }

    fn validate(&self, expected_path: &Path) -> Result<()> {
        let requested = fs::symlink_metadata(expected_path).map_err(|_| changed())?;
        if requested.file_type().is_symlink()
            || fs::canonicalize(expected_path).map_err(|_| changed())? != self.path
            || first_existing_symlink_component(&self.workspace, &self.path)?.is_some()
        {
            return Err(changed());
        }
        let current = self.file.metadata().map_err(|_| changed())?;
        let published = fs::symlink_metadata(&self.path).map_err(|_| changed())?;
        if !same_workspace_output(&self.generation, &current)
            || !same_workspace_output(&current, &published)
            || !same_workspace_output(&current, &requested)
        {
            return Err(changed());
        }
        Ok(())
    }
}

pub(super) struct PendingCommittedArtifact {
    artifact: WorkspaceSafeCommittedArtifact,
}

impl PendingCommittedArtifact {
    pub(super) fn from_writer(
        writer: WorkspaceSafeLocalStagingWriter,
        plan: &WorkspaceSafeLocalWritePlan,
        staging_path: &Path,
    ) -> Result<Option<Self>> {
        #[cfg(not(unix))]
        {
            // No device/inode/change-time identity is admitted on this platform.
            // The caller must retain its independent content fingerprint path.
            let _ = (writer, plan, staging_path);
            Ok(None)
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            let bytes_written = writer.bytes_written();
            let digest = writer.output_digest();
            let file = writer.file.into_inner().map_err(|_| changed())?;
            let generation = file.metadata().map_err(|_| changed())?;
            let staging = fs::symlink_metadata(staging_path).map_err(|_| changed())?;
            if !same_workspace_output(&generation, &staging)
                || generation.nlink() != 1
                || generation.len() != bytes_written
                || generation.modified().is_err()
            {
                return Err(changed());
            }
            Ok(Some(Self {
                artifact: WorkspaceSafeCommittedArtifact {
                    file,
                    generation,
                    workspace: PathBuf::from(&plan.path_safety_report.canonical_workspace_root),
                    path: plan.target_path.clone(),
                    bytes_written,
                    digest,
                },
            }))
        }
    }

    pub(super) fn committed(mut self) -> Result<WorkspaceSafeCommittedArtifact> {
        #[cfg(not(unix))]
        {
            // from_writer cannot produce this value without Unix identity.
            let _ = &mut self.artifact;
            Err(changed())
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            let before = &self.artifact.generation;
            let current = self.artifact.file.metadata().map_err(|_| changed())?;
            // Rename/hard-link commit changes ctime. Retain descriptor identity,
            // length and mtime across that operation, then capture its committed
            // ctime for both publication checks. Never recapture from the path.
            if current.dev() != before.dev()
                || current.ino() != before.ino()
                || current.len() != before.len()
                || current.modified().ok() != before.modified().ok()
                || current.nlink() != 1
            {
                return Err(changed());
            }
            self.artifact.generation = current;
            self.artifact.validate(&self.artifact.path)?;
            Ok(self.artifact)
        }
    }
}

fn changed() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "committed workspace artifact generation changed or cannot be proven; writer checksum reuse refused; no fallback execution was attempted".into(),
    )
}
