//! Deterministic publication races, without timing-dependent concurrent writes.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-publication-race-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        files.sort();
        files
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn workspace_publication_preserves_target_created_during_production_even_with_overwrite() {
    for overwrite in [false, true] {
        let fixture = Fixture::new();
        let target = fixture.0.join("result");
        let error = write_workspace_safe_bytes_with_producer(
            &fixture.0,
            &target,
            overwrite,
            "publication race",
            |writer| {
                writer.write_all(b"candidate").unwrap();
                fs::write(&target, b"competing owner").unwrap();
                Ok(())
            },
        )
        .unwrap_err();
        assert!(
            error
                .message()
                .contains("changed or appeared before commit")
        );
        assert_eq!(fs::read(&target).unwrap(), b"competing owner");
        assert_eq!(fixture.files(), vec![target]);
    }
}

#[test]
fn workspace_publication_exclusive_create_cannot_clobber_after_final_validation() {
    let fixture = Fixture::new();
    let target = fixture.0.join("result");
    let plan = plan_workspace_safe_local_output(&fixture.0, &target, false).unwrap();
    let staging = fixture.0.join("staging");
    fs::write(&staging, b"candidate").unwrap();
    // Force the exact old check/rename race at the commit primitive.
    fs::write(&target, b"competitor after check").unwrap();
    let error = commit_workspace_safe_new_target(&plan, &staging).unwrap_err();
    assert!(error.message().contains("destination preserved"));
    assert_eq!(fs::read(&target).unwrap(), b"competitor after check");
    assert_eq!(fixture.files(), vec![target]);
}

#[test]
fn workspace_publication_replacement_rejects_changed_destination_before_commit() {
    let fixture = Fixture::new();
    let target = fixture.0.join("result");
    fs::write(&target, b"original").unwrap();
    let error = write_workspace_safe_bytes_with_validated_producer(
        &fixture.0,
        &target,
        true,
        "replacement race",
        |writer| {
            writer.write_all(b"candidate").unwrap();
            Ok(())
        },
        |()| {
            fs::write(&target, b"new independently written contents").unwrap();
            Ok(())
        },
    )
    .unwrap_err();
    assert!(
        error
            .message()
            .contains("changed or appeared before commit")
    );
    assert_eq!(
        fs::read(&target).unwrap(),
        b"new independently written contents"
    );
    assert_eq!(fixture.files(), vec![target]);
}

#[test]
fn workspace_publication_replacement_keeps_target_present_until_atomic_commit() {
    let fixture = Fixture::new();
    let target = fixture.0.join("result");
    fs::write(&target, b"original").unwrap();
    let plan = plan_workspace_safe_local_output(&fixture.0, &target, true).unwrap();
    let staging = fixture.0.join("staging");
    fs::write(&staging, b"candidate").unwrap();
    let (mode, cleanup, rollback, overwritten) =
        replace_workspace_safe_target_with_commit(&plan, &staging, |from, to| {
            // Observe the exact boundary before publication on a separate reader.
            // A backup-then-publish protocol would have removed this name already.
            std::thread::scope(|scope| {
                scope.spawn(|| assert_eq!(fs::read(to).unwrap(), b"original"));
            });
            assert_eq!(fixture.files(), vec![target.clone(), staging.clone()]);
            fs::rename(from, to)?;
            assert_eq!(fs::read(to)?, b"candidate");
            Ok(())
        })
        .unwrap();
    assert_eq!(mode, "atomic_replace_rename_same_directory");
    assert_eq!(cleanup, "no_staging_artifacts_remaining");
    assert_eq!(rollback, "not_required_atomic_replace");
    assert!(overwritten);
    assert_eq!(fixture.files(), vec![target]);
}

#[test]
fn workspace_publication_failed_atomic_replace_keeps_original_in_place() {
    let fixture = Fixture::new();
    let target = fixture.0.join("result");
    fs::write(&target, b"original").unwrap();
    let plan = plan_workspace_safe_local_output(&fixture.0, &target, true).unwrap();
    let staging = fixture.0.join("staging");
    fs::write(&staging, b"candidate").unwrap();
    let error = replace_workspace_safe_target_with_commit(&plan, &staging, |_, to| {
        assert_eq!(fs::read(to).unwrap(), b"original");
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "fixture filesystem cannot replace",
        ))
    })
    .unwrap_err();
    assert!(
        error
            .message()
            .contains("destination was not removed before commit")
    );
    assert_eq!(fs::read(&target).unwrap(), b"original");
    assert_eq!(fixture.files(), vec![target]);
}

#[test]
fn workspace_publication_real_rename_failure_keeps_original_in_place() {
    let fixture = Fixture::new();
    let target = fixture.0.join("result");
    fs::write(&target, b"original").unwrap();
    let plan = plan_workspace_safe_local_output(&fixture.0, &target, true).unwrap();
    let staging = fixture.0.join("missing-staging");
    let error = replace_workspace_safe_existing_target(&plan, &staging).unwrap_err();
    assert!(error.message().contains("failed to atomically replace"));
    assert_eq!(fs::read(&target).unwrap(), b"original");
    assert_eq!(fixture.files(), vec![target]);
}

#[cfg(unix)]
#[test]
fn workspace_publication_rejects_symlink_inserted_by_producer() {
    let fixture = Fixture::new();
    let target = fixture.0.join("result");
    let foreign = fixture.0.join("foreign");
    fs::write(&foreign, b"foreign").unwrap();
    let error = write_workspace_safe_bytes_with_producer(
        &fixture.0,
        &target,
        true,
        "symlink race",
        |writer| {
            writer.write_all(b"candidate").unwrap();
            std::os::unix::fs::symlink(&foreign, &target).unwrap();
            Ok(())
        },
    )
    .unwrap_err();
    assert!(error.message().contains("symlink component"));
    assert_eq!(fs::read(&foreign).unwrap(), b"foreign");
    assert!(
        fs::symlink_metadata(&target)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fixture.files(), vec![foreign, target]);
}
