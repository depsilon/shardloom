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
fn workspace_publication_failed_replacement_and_rollback_preserve_both_owners() {
    let fixture = Fixture::new();
    let target = fixture.0.join("result");
    fs::write(&target, b"original").unwrap();
    let plan = plan_workspace_safe_local_output(&fixture.0, &target, true).unwrap();
    let backup = fixture.0.join("backup");
    fs::rename(&target, &backup).unwrap();
    let staging = fixture.0.join("staging");
    fs::write(&staging, b"candidate").unwrap();
    fs::write(&target, b"competing owner after backup").unwrap();
    let error = finish_workspace_safe_replacement(&plan, &staging, &backup, true).unwrap_err();
    assert!(
        error
            .message()
            .contains("rollback_restored_existing_target=false")
    );
    assert!(error.message().contains("backup_retained=true"));
    assert!(error.message().contains(&backup.display().to_string()));
    assert_eq!(fs::read(&target).unwrap(), b"competing owner after backup");
    assert_eq!(fs::read(&backup).unwrap(), b"original");
    assert_eq!(fixture.files(), vec![backup, target]);
}

#[test]
fn workspace_publication_unsupported_hard_links_leave_original_destination_in_place() {
    let fixture = Fixture::new();
    let target = fixture.0.join("result");
    fs::write(&target, b"original").unwrap();
    let plan = plan_workspace_safe_local_output(&fixture.0, &target, true).unwrap();
    let before = fs::metadata(&target).unwrap();
    let staging = fixture.0.join("staging");
    fs::write(&staging, b"candidate").unwrap();
    let error = replace_workspace_safe_target_with_link_check(&plan, &staging, &before, |_, _| {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "fixture filesystem cannot link",
        ))
    })
    .unwrap_err();
    assert!(
        error
            .message()
            .contains("requires same-directory hard-link support")
    );
    assert_eq!(fs::read(&target).unwrap(), b"original");
    assert_eq!(fixture.files(), vec![target]);
}

#[test]
fn workspace_publication_changed_backup_restores_without_publishing_candidate() {
    let fixture = Fixture::new();
    let target = fixture.0.join("result");
    fs::write(&target, b"original").unwrap();
    let plan = plan_workspace_safe_local_output(&fixture.0, &target, true).unwrap();
    let backup = fixture.0.join("backup");
    fs::rename(&target, &backup).unwrap();
    let staging = fixture.0.join("staging");
    fs::write(&staging, b"candidate").unwrap();
    let error = finish_workspace_safe_replacement(&plan, &staging, &backup, false).unwrap_err();
    assert!(
        error
            .message()
            .contains("rollback_restored_existing_target=true")
    );
    assert!(error.message().contains("backup_retained=false"));
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
