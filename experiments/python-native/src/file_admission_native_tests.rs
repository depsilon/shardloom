//! Admission races use the actual native provider for every experimental route.

use super::{FileAdmission, Generation, tests::Directory};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    local_primitives::{
        collect::{PreparedVortexCollect, prepare_rows_in_session},
        prepared_count::{PreparedVortexCountWhere, prepare_count_where_in_session},
    },
    resident_session::{PreparedVortexCount, ResidentVortexSession},
};
use std::{
    fs::Metadata,
    io::Write as _,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug)]
enum Route {
    Count,
    Filtered,
    Projection,
}
const ROUTES: [Route; 3] = [Route::Count, Route::Filtered, Route::Projection];
enum Prepared {
    Count(PreparedVortexCount),
    Filtered(PreparedVortexCountWhere),
    Projection(PreparedVortexCollect),
}
impl Prepared {
    fn open(route: Route, path: &Path, session: &ResidentVortexSession) -> Self {
        let uri = DatasetUri::new(path.display().to_string()).unwrap();
        match route {
            Route::Count => Self::Count(session.prepare_file(path).unwrap().prepare_count()),
            Route::Filtered => Self::Filtered(
                prepare_count_where_in_session(
                    &VortexQueryPrimitiveRequest::count_where(
                        uri,
                        PredicateExpr::Compare {
                            column: ColumnRef::new("value").unwrap(),
                            op: ComparisonOp::Gt,
                            value: StatValue::UInt64(2),
                        },
                    ),
                    VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap(),
                    session,
                )
                .unwrap(),
            ),
            Route::Projection => Self::Projection(
                prepare_rows_in_session(
                    &VortexQueryPrimitiveRequest::project(
                        uri,
                        ProjectionRequest::columns(vec![ColumnRef::new("value").unwrap()]),
                    )
                    .with_source_order_limit(3),
                    session,
                )
                .unwrap(),
            ),
        }
    }

    fn validate(&self, metadata: &Metadata) -> shardloom_core::Result<()> {
        match self {
            Self::Count(prepared) => prepared.validate_file_metadata(metadata),
            Self::Filtered(prepared) => prepared.validate_file_metadata(metadata),
            Self::Projection(prepared) => prepared.validate_file_metadata(metadata),
        }
    }

    fn execute(&self) {
        match self {
            Self::Count(prepared) => assert_eq!(prepared.execute().unwrap(), 5),
            Self::Filtered(prepared) => {
                let result = prepared.execute().unwrap();
                assert_eq!(result.count, 3);
                assert!(result.native_io_certificate.is_certified());
            }
            Self::Projection(prepared) => {
                let result = prepared.execute().unwrap();
                assert_eq!(
                    result.values_json.value(),
                    "[{\"value\":1},{\"value\":2},{\"value\":3}]"
                );
                assert!(result.native_io_certificate.is_certified());
            }
        }
    }
}

fn fixture(directory: &Directory, name: &str) -> PathBuf {
    let path = directory.0.join(name);
    let original = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex");
    std::fs::copy(original, &path).unwrap();
    path
}

fn switch_link(path: &Path, target: &Path) {
    let replacement = path.with_extension("next-link");
    std::os::unix::fs::symlink(target, &replacement).unwrap();
    std::fs::rename(replacement, path).unwrap();
}

fn assert_prepared_only(session: &ResidentVortexSession) {
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(session.snapshot().completed_executions, 0);
}

#[test]
fn native_file_admission_aba_symlink_prepare_rejects_all_three_retained_providers() {
    for route in ROUTES {
        let directory = Directory::new();
        let first = fixture(&directory, "first.vortex");
        let second = fixture(&directory, "second.vortex");
        let path = directory.0.join("source.vortex");
        switch_link(&path, &first);
        let limit = std::fs::metadata(&first).unwrap().len();
        let admitted = FileAdmission::capture(&path, limit).unwrap();
        switch_link(&path, &second);
        let session = ResidentVortexSession::new(1 << 30, 1).unwrap();
        let prepared = Prepared::open(route, &path, &session);
        assert_prepared_only(&session);
        switch_link(&path, &first);
        // The old path/preflight-only check passes after this exact ABA switch.
        admitted.validate().unwrap();
        let failure = admitted
            .validate_prepared(|metadata| prepared.validate(metadata))
            .unwrap_err();
        assert!(
            failure
                .to_string()
                .contains("does not match the admitted file generation"),
            "{route:?}: {failure}"
        );
        assert_prepared_only(&session);
        drop(prepared);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn native_file_admission_stable_and_initial_oversize_do_not_execute_during_prepare() {
    for route in ROUTES {
        let directory = Directory::new();
        let path = fixture(&directory, "source.vortex");
        let limit = std::fs::metadata(&path).unwrap().len();
        let session = ResidentVortexSession::new(1 << 30, 1).unwrap();
        assert!(FileAdmission::capture(&path, limit - 1).is_err());
        assert_eq!(session.snapshot().prepared_source_opens, 0);
        assert_eq!(session.snapshot().completed_executions, 0);
        let admitted = FileAdmission::capture(&path, limit).unwrap();
        let prepared = Prepared::open(route, &path, &session);
        for _ in 0..3 {
            admitted
                .validate_prepared(|metadata| prepared.validate(metadata))
                .unwrap();
            assert_prepared_only(&session);
        }
        prepared.execute();
        assert_eq!(session.snapshot().prepared_source_opens, 1);
        assert_eq!(session.snapshot().completed_executions, 1);
        drop(prepared);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn native_file_admission_actual_prepare_rejects_replacement_growth_and_unlink() {
    for route in ROUTES {
        for mutation in ["replacement", "growth", "unlink"] {
            let directory = Directory::new();
            let path = fixture(&directory, "source.vortex");
            let admitted =
                FileAdmission::capture(&path, std::fs::metadata(&path).unwrap().len()).unwrap();
            let session = ResidentVortexSession::new(1 << 30, 1).unwrap();
            let prepared = Prepared::open(route, &path, &session);
            match mutation {
                "replacement" => {
                    std::fs::rename(fixture(&directory, "replacement.vortex"), &path).unwrap()
                }
                "growth" => std::fs::File::options()
                    .append(true)
                    .open(&path)
                    .unwrap()
                    .write_all(b"!")
                    .unwrap(),
                _ => std::fs::remove_file(&path).unwrap(),
            }
            assert!(
                admitted
                    .validate_prepared(|metadata| prepared.validate(metadata))
                    .is_err(),
                "{route:?}/{mutation}"
            );
            assert_prepared_only(&session);
            drop(prepared);
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }
}

#[test]
fn native_file_admission_rejects_different_provider_even_when_its_path_is_stable() {
    let directory = Directory::new();
    let first = fixture(&directory, "first.vortex");
    let second = fixture(&directory, "second.vortex");
    let admitted =
        FileAdmission::capture(&first, std::fs::metadata(&first).unwrap().len()).unwrap();
    assert_ne!(
        Generation::read(&std::fs::metadata(&first).unwrap()).unwrap(),
        Generation::read(&std::fs::metadata(&second).unwrap()).unwrap()
    );
    for route in ROUTES {
        let session = ResidentVortexSession::new(1 << 30, 1).unwrap();
        let prepared = Prepared::open(route, &second, &session);
        assert!(
            admitted
                .validate_prepared(|metadata| prepared.validate(metadata))
                .is_err()
        );
        // An admission mismatch does not poison a correctly retained source.
        prepared
            .validate(&std::fs::metadata(&second).unwrap())
            .unwrap();
        assert_prepared_only(&session);
    }
}
