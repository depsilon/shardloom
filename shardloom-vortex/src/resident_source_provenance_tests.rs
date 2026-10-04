use super::*;
use crate::prepared_source_binding::{
    KEY, LocalPreparationIdentity, local_preparation_binding, local_preparation_identity,
};

pub(super) fn prepare(
    fixture: &Fixture,
    identity: Arc<LocalPreparationIdentity>,
    source_owned: bool,
) -> PreparedVortexRelational {
    if !source_owned {
        return prepare_relational(&fixture.scan(), policy())
            .unwrap()
            .with_preparation_sources(vec![identity])
            .unwrap();
    }
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let source = session
        .prepare_file(fixture.path())
        .unwrap()
        .with_preparation_sources(vec![identity])
        .unwrap();
    prepare_relational_from_source(
        DatasetUri::new(fixture.path().display().to_string()).unwrap(),
        source,
        policy(),
        |_| Ok(fixture.scan()),
    )
    .unwrap()
}

struct PreparedFixture {
    origin: Fixture,
    native: Fixture,
    identity: Arc<LocalPreparationIdentity>,
}

impl PreparedFixture {
    fn new() -> Self {
        let origin = Fixture::new(keyed(&[], &[]), 1);
        let csv = origin.0.join("source.csv");
        fs::write(&csv, "entity,amount\n1,10\n1,11\n").unwrap();
        fs::hard_link(&csv, origin.0.join("hard.csv")).unwrap();
        std::os::unix::fs::symlink(&csv, origin.0.join("soft.csv")).unwrap();
        let binding = local_preparation_binding(&csv, "csv", "metadata_only").unwrap();
        let native = Fixture::with_metadata(
            keyed(&[Some(1), Some(1)], &[10, 11]),
            1,
            vec![(KEY, binding.as_bytes().to_vec())],
        );
        let identity = Arc::new(local_preparation_identity(&native.path(), &binding).unwrap());
        Self {
            origin,
            native,
            identity,
        }
    }
}

#[test]
fn native_prepared_source_provenance_is_immutable_and_credits_live_through_last_clone() {
    let fixture = PreparedFixture::new();
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(fixture.native.path()).unwrap();
    let baseline = memory.snapshot().reserved_bytes;
    let error = source
        .clone()
        .with_preparation_sources(vec![Arc::clone(&fixture.identity)])
        .err()
        .unwrap();
    assert!(error.to_string().contains("before sharing"), "{error}");
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let source = source
        .with_preparation_sources(vec![Arc::clone(&fixture.identity)])
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, baseline + 131_072);
    let clone = source.clone();
    drop(source);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, baseline + 131_072);
    let error = clone
        .with_preparation_sources(vec![Arc::clone(&fixture.identity)])
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("already been attached"),
        "{error}"
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_prepared_source_provenance_rejects_capacity_and_grant_without_leaking() {
    let fixture = PreparedFixture::new();
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let source = session.prepare_file(fixture.native.path()).unwrap();
    let error = source
        .with_preparation_sources(vec![Arc::clone(&fixture.identity); 129])
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("128 compatibility sources"),
        "{error}"
    );
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    let source = session.prepare_file(fixture.native.path()).unwrap();
    let snapshot = session.memory().snapshot();
    let held = session
        .memory()
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 131_071)
        .unwrap();
    let error = source
        .with_preparation_sources(vec![Arc::clone(&fixture.identity)])
        .err()
        .unwrap();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(session.memory().snapshot().reserved_bytes, held.bytes());
    drop(held);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn native_prepared_source_provenance_checks_aliases_and_generation_on_all_reader_paths() {
    let fixture = PreparedFixture::new();
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let source = session
        .prepare_file(fixture.native.path())
        .unwrap()
        .with_preparation_sources(vec![Arc::clone(&fixture.identity)])
        .unwrap();
    for name in ["source.csv", "hard.csv", "soft.csv"] {
        let error = source
            .aliases_file(&fixture.origin.0.join(name))
            .unwrap_err();
        assert!(
            error.to_string().contains("different files"),
            "{name}: {error}"
        );
    }
    assert!(
        !source
            .aliases_file(&fixture.origin.0.join("new.jsonl"))
            .unwrap()
    );
    let metadata = fs::metadata(fixture.native.path()).unwrap();
    source.validate_file_metadata(&metadata).unwrap();
    assert_eq!(source.prepare_count().execute().unwrap(), 2);
    let direct = session.prepare_file(fixture.native.path()).unwrap();
    fs::write(fixture.origin.0.join("source.csv"), "changed\n").unwrap();
    assert!(source.validate_generation().is_err());
    assert!(source.validate_file_metadata(&metadata).is_err());
    assert!(source.prepare_count().execute().is_err());
    // Direct use of a persisted Vortex file has no implicit raw-source dependency.
    assert_eq!(direct.prepare_count().execute().unwrap(), 2);
}
