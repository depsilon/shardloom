//! Shared output coordination. Each adapter uses its existing native writer;
//! every completed target is staged before publication begins. Local paths
//! cannot be committed as one atomic group, so late publication failures retain
//! complete published files and identify them for recovery.

use super::native_sink::OwnedOutput;
use super::{Result, ShardLoomError, VortexLocalPrimitiveRowExportFormat as Format};
use crate::resident_session::{PreparedVortexSource, ResidentVortexSession};
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::{DirBuilderExt as _, MetadataExt as _},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

/// Coordinate native primitive outputs with one retained source and resource grant.
/// # Errors
/// Rejects source/destination aliasing and propagates admission, execution and
/// publication failures. Outputs are not published when an adapter fails.
pub fn write_primitive(
    request: &super::VortexQueryPrimitiveRequest,
    targets: &[(PathBuf, Format)],
    overwrite: bool,
    policy: super::VortexLocalPrimitiveExecutionPolicy,
) -> Result<Vec<super::VortexLocalPrimitiveRowExportReport>> {
    let source = super::prepared_dispatch::prepare_source(request, policy)?;
    write_primitive_with_source(
        request,
        targets,
        overwrite,
        policy,
        &source,
        &CancellationToken::default(),
    )
}

/// Coordinate primitive outputs with a caller-owned resident session and its
/// cancellation/deadline token. Source preparation and each adapter use that same
/// operation context; target staging and publication retain their existing order.
/// # Errors
/// Rejects cancellation, source/destination aliasing and propagates admission,
/// execution and publication failures.
pub fn write_primitive_in_session(
    request: &super::VortexQueryPrimitiveRequest,
    targets: &[(PathBuf, Format)],
    overwrite: bool,
    policy: super::VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<Vec<super::VortexLocalPrimitiveRowExportReport>> {
    cancellation.check()?;
    let source = super::prepared_dispatch::prepare_source_in_session(
        request,
        policy,
        session,
        cancellation,
    )?;
    write_primitive_with_source(request, targets, overwrite, policy, &source, cancellation)
}

fn write_primitive_with_source(
    request: &super::VortexQueryPrimitiveRequest,
    targets: &[(PathBuf, Format)],
    overwrite: bool,
    policy: super::VortexLocalPrimitiveExecutionPolicy,
    source: &PreparedVortexSource,
    cancellation: &CancellationToken,
) -> Result<Vec<super::VortexLocalPrimitiveRowExportReport>> {
    let mut reports = write(
        targets,
        overwrite,
        |path| {
            cancellation.check()?;
            source.validate_generation()?;
            cancellation.check()?;
            if source.aliases_file(path)? {
                return Err(failed("source and output must be different files"));
            }
            cancellation.check()?;
            Ok(())
        },
        |path, format| {
            cancellation.check()?;
            super::prepared_dispatch::try_write_source_controlled(
                request,
                path,
                format,
                false,
                policy,
                source.clone(),
                cancellation,
            )?
            .ok_or_else(|| failed("native primitive output schema is not admitted"))
        },
    )?;
    for (report, (path, _)) in reports.iter_mut().zip(targets) {
        report.output_path = path.display().to_string();
    }
    Ok(reports)
}

/// Write a bounded set of outputs using the supplied existing native adapter.
/// Query execution is replayed once per target; no result rows are cached.
/// # Errors
/// Rejects duplicate, existing or unsafe destinations before adapter work.
/// Adapter failures leave final targets absent. A late publication failure
/// preserves any already published complete targets and reports their paths.
pub fn write<T>(
    targets: &[(PathBuf, Format)],
    overwrite: bool,
    mut validate: impl FnMut(&Path) -> Result<()>,
    mut write: impl FnMut(&Path, Format) -> Result<T>,
) -> Result<Vec<T>> {
    stage(targets, overwrite, &mut validate, &mut write)?.publish(validate)
}

/// Private completed outputs whose publication still requires caller completion.
pub(super) struct Staged<T> {
    plans: Vec<shardloom_core::WorkspaceSafeLocalWritePlan>,
    outputs: Vec<(OwnedOutput, Directory)>,
    results: Vec<T>,
}

pub(super) fn stage<T>(
    targets: &[(PathBuf, Format)],
    overwrite: bool,
    mut validate: impl FnMut(&Path) -> Result<()>,
    mut write: impl FnMut(&Path, Format) -> Result<T>,
) -> Result<Staged<T>> {
    let plans = plan_targets(targets, overwrite, &mut validate)?;
    let mut staged = Vec::with_capacity(targets.len());
    let mut results = Vec::with_capacity(targets.len());
    for (plan, (_, format)) in plans.iter().zip(targets) {
        let directory = Directory::new(&plan.parent_path)?;
        let path = directory.path.join("payload");
        let result = write(&path, *format)?;
        let output = OwnedOutput::from_completed_staging(&plan.target_path, &path)?;
        results.push(result);
        staged.push((output, directory));
    }
    Ok(Staged {
        plans,
        outputs: staged,
        results,
    })
}

impl<T> Staged<T> {
    pub(super) fn publish(
        mut self,
        mut validate: impl FnMut(&Path) -> Result<()>,
    ) -> Result<Vec<T>> {
        // Revalidate every input generation and final destination after all adapters
        // complete, before exposing any target. Publication remains create-if-absent.
        for plan in &self.plans {
            validate(&plan.target_path)?;
        }
        for (index, (output, _directory)) in self.outputs.iter_mut().enumerate() {
            if let Err(error) = output.commit() {
                let published = self.plans[..index]
                    .iter()
                    .map(|plan| plan.target_path.display().to_string())
                    .collect::<Vec<_>>();
                return Err(failed(&format!(
                    "fanout publication failed at {}: {error}; previously published complete outputs are preserved: {published:?}; inspect the failing target before retrying",
                    self.plans[index].target_path.display()
                )));
            }
        }
        Ok(self.results)
    }
}

/// Check destination declarations before an input factory is consumed.
/// This does not create directories or outputs. The writer repeats these checks
/// with source-generation validation before staging and publication.
/// # Errors
/// Rejects the same empty, excessive, duplicate, existing and unsafe destinations
/// as `write`.
pub fn preflight_targets(targets: &[(PathBuf, Format)], overwrite: bool) -> Result<()> {
    plan_targets(targets, overwrite, |_| Ok(())).map(|_| ())
}

fn plan_targets(
    targets: &[(PathBuf, Format)],
    overwrite: bool,
    mut validate: impl FnMut(&Path) -> Result<()>,
) -> Result<Vec<shardloom_core::WorkspaceSafeLocalWritePlan>> {
    if targets.is_empty() || targets.len() > 32 {
        return Err(failed("output fanout requires between 1 and 32 targets"));
    }
    let mut seen = BTreeSet::new();
    let mut plans = Vec::with_capacity(targets.len());
    for (path, _) in targets {
        validate(path)?;
        let root = shardloom_core::infer_local_output_workspace_root(path)?;
        let plan = shardloom_core::plan_workspace_safe_local_output(root, path, overwrite)?;
        if !plan.accepted() {
            return Err(failed("output path failed workspace safety validation"));
        }
        if plan.target_existed_before {
            return Err(failed(
                "atomic generation-conditional replacement is unavailable for an existing destination; choose a new output path",
            ));
        }
        if !seen.insert(plan.target_path.to_string_lossy().to_lowercase()) {
            return Err(failed("output fanout destinations must be distinct"));
        }
        plans.push(plan);
    }
    Ok(plans)
}

struct Directory {
    path: PathBuf,
    identity: (u64, u64),
}

impl Directory {
    fn new(parent: &Path) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        fs::create_dir_all(parent).map_err(|error| io_error(&error))?;
        let path = parent.join(format!(
            ".shardloom-fanout-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(|error| io_error(&error))?;
        let metadata = fs::symlink_metadata(&path).map_err(|error| io_error(&error))?;
        Ok(Self {
            path,
            identity: (metadata.dev(), metadata.ino()),
        })
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.is_dir() && (metadata.dev(), metadata.ino()) == self.identity
        }) {
            // OwnedOutput removes only the payload generation it adopted.
            // Preserve unknown files or a replaced payload, even in our directory.
            let _ = fs::remove_dir(&self.path);
        }
    }
}

fn io_error(error: &std::io::Error) -> ShardLoomError {
    failed(&error.to_string())
}
fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native output fanout: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "local_primitive_output_fanout_tests.rs"]
mod tests;
