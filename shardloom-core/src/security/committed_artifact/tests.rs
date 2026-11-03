use super::*;
use crate::security::{
    commit_workspace_safe_staging_file, create_workspace_safe_staging_writer,
    plan_workspace_safe_local_output, sha256_digest_bytes,
    write_workspace_safe_bytes_with_validated_producer_receipt,
};
use std::io::Write as _;

struct Fixture(PathBuf);

impl Fixture {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom_committed_receipt_{name}_{}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn target(&self) -> PathBuf {
        self.0.join("artifact.bin")
    }

    fn write(&self, overwrite: bool) -> WorkspaceSafeCommittedArtifact {
        let ((), report, receipt) = write_workspace_safe_bytes_with_validated_producer_receipt(
            &self.0,
            self.target(),
            overwrite,
            "receipt test",
            |writer| {
                writer.write_all(b"accepted").unwrap();
                Ok(())
            },
            |()| Ok(()),
        )
        .unwrap();
        assert_eq!(report.output_digest, sha256_digest_bytes(b"accepted"));
        assert_eq!(report.bytes_written, 8);
        assert!(!report.staging_path.exists());
        receipt.unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn committed_receipt_reuses_original_descriptor_for_create_and_replace() {
    let fixture = Fixture::new("create_replace");
    for overwrite in [false, true] {
        let receipt = fixture.write(overwrite);
        let result = receipt
            .with_verified_digest(&fixture.target(), |digest, bytes, modified| {
                assert_eq!(digest, sha256_digest_bytes(b"accepted"));
                assert_eq!(bytes, 8);
                assert_eq!(
                    modified,
                    fs::metadata(fixture.target()).unwrap().modified().unwrap()
                );
                Ok("published")
            })
            .unwrap();
        assert_eq!(result, "published");
    }
}

#[test]
fn committed_receipt_rejects_same_size_mutation_with_restored_mtime() {
    let fixture = Fixture::new("mutated");
    let receipt = fixture.write(false);
    let modified = fs::metadata(fixture.target()).unwrap().modified().unwrap();
    fs::write(fixture.target(), b"modified").unwrap();
    File::options()
        .write(true)
        .open(fixture.target())
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(modified))
        .unwrap();
    let error = receipt
        .with_verified_digest::<()>(&fixture.target(), |_, _, _| panic!("stale proof admitted"))
        .unwrap_err();
    assert!(error.to_string().contains("checksum reuse refused"));
}

#[test]
fn committed_receipt_rejects_replacement_and_unrelated_path() {
    let fixture = Fixture::new("replaced");
    let receipt = fixture.write(false);
    let replacement = fixture.0.join("replacement.bin");
    fs::write(&replacement, b"accepted").unwrap();
    fs::rename(&replacement, fixture.target()).unwrap();
    assert!(
        receipt
            .with_verified_digest(&fixture.target(), |_, _, _| Ok(()))
            .is_err()
    );
    let receipt = fixture.write(true);
    fs::write(&replacement, b"accepted").unwrap();
    assert!(
        receipt
            .with_verified_digest(&replacement, |_, _, _| Ok(()))
            .is_err()
    );
}

#[test]
fn committed_receipt_rejects_hardlinks_and_symlinks() {
    let fixture = Fixture::new("aliases");
    let receipt = fixture.write(false);
    let alias = fixture.0.join("alias.bin");
    fs::hard_link(fixture.target(), &alias).unwrap();
    assert!(
        receipt
            .with_verified_digest(&fixture.target(), |_, _, _| Ok(()))
            .is_err()
    );
    fs::remove_file(&alias).unwrap();
    let receipt = fixture.write(true);
    std::os::unix::fs::symlink(fixture.target(), &alias).unwrap();
    assert!(
        receipt
            .with_verified_digest(&alias, |_, _, _| Ok(()))
            .is_err()
    );
}

#[test]
fn committed_receipt_rejects_mutation_during_publication() {
    let fixture = Fixture::new("publication_race");
    let receipt = fixture.write(false);
    let error = receipt
        .with_verified_digest(&fixture.target(), |_, _, _| {
            fs::write(fixture.target(), b"modified").unwrap();
            Ok("must not be returned")
        })
        .unwrap_err();
    assert!(error.to_string().contains("checksum reuse refused"));
}

#[test]
fn committed_receipt_cannot_recapture_replaced_staging_file() {
    let fixture = Fixture::new("staging_race");
    let plan = plan_workspace_safe_local_output(&fixture.0, fixture.target(), false).unwrap();
    let (staging, mut writer) = create_workspace_safe_staging_writer(&plan).unwrap();
    writer.write_all(b"accepted").unwrap();
    writer.flush().unwrap();
    let pending = PendingCommittedArtifact::from_writer(writer, &plan, &staging)
        .unwrap()
        .unwrap();
    fs::rename(&staging, fixture.0.join("original-staging.bin")).unwrap();
    fs::write(&staging, b"accepted").unwrap();
    commit_workspace_safe_staging_file(&plan, &staging, None).unwrap();
    assert!(pending.committed().is_err());
}

#[test]
fn committed_receipt_writer_validation_failure_preserves_target() {
    let fixture = Fixture::new("validation_failure");
    fs::write(fixture.target(), b"original").unwrap();
    let result = write_workspace_safe_bytes_with_validated_producer_receipt(
        &fixture.0,
        fixture.target(),
        true,
        "receipt failure test",
        |writer| {
            writer.write_all(b"replacement").unwrap();
            Ok(())
        },
        |()| Err(ShardLoomError::new("source changed before commit")),
    );
    assert!(result.unwrap_err().to_string().contains("source changed"));
    assert_eq!(fs::read(fixture.target()).unwrap(), b"original");
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
}
