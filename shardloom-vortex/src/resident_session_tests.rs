use super::*;
use std::{
    io::{Seek as _, SeekFrom, Write as _},
    sync::atomic::AtomicUsize,
};
use vortex::{
    array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::FieldNames,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
};

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-resident-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let values = PrimitiveArray::new(
            vec![393_081_u32, 912_778, 5_719, 194_797, 616_489],
            Validity::NonNullable,
        )
        .into_array();
        let metric = PrimitiveArray::new(
            vec![-519_i64, 418_982, -719, 765_741, 832_761],
            Validity::NonNullable,
        )
        .into_array();
        let array = StructArray::try_new(
            FieldNames::from(["value", "metric"]),
            vec![values, metric],
            5,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut bytes = Vec::new();
        runtime
            .block_on(
                session
                    .write_options()
                    .write(&mut bytes, array.to_array_stream()),
            )
            .unwrap();
        std::fs::write(path.join("input.vortex"), bytes).unwrap();
        Self(path)
    }

    fn input(&self) -> PathBuf {
        self.0.join("input.vortex")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn prepared_calls_reuse_reader_and_workers_without_caching_answers() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 * 1024 * 1024, 2).unwrap();
    let source = session.prepare_file(fixture.input()).unwrap();
    let count = source.prepare_count();
    let projection = source
        .prepare_projection(&["metric", "value"], 3, 65536)
        .unwrap();
    for _ in 0..20 {
        assert_eq!(count.execute().unwrap(), 5);
        let result = projection.execute().unwrap();
        assert_eq!(result.row_count(), 3);
        assert!(!result.arrays().is_empty());
        assert!(result.logical_buffer_bytes() > 0);
    }
    let snapshot = session.snapshot();
    assert_eq!(snapshot.prepared_source_opens, 1);
    assert_eq!(snapshot.completed_executions, 40);
    assert!(snapshot.provider_background_workers <= 1);
    assert!(snapshot.memory.peak_reserved_bytes <= snapshot.memory.limit_bytes);
}

#[test]
fn executable_array_result_outlives_source_and_session() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 * 1024 * 1024, 2).unwrap();
    let memory = session.0.memory.clone();
    let source = session.prepare_file(fixture.input()).unwrap();
    let projection = source.prepare_projection(&["value"], 5, 65536).unwrap();
    let result = projection.execute().unwrap();
    drop(projection);
    drop(source);
    drop(session);
    let arrays = result.arrays().to_vec();
    drop(result);
    let native_session = VortexSession::default();
    let mut context = native_session.create_execution_ctx();
    let mut values = Vec::new();
    for array in &arrays {
        for index in 0..array.len() {
            values.push(
                array
                    .execute_scalar(index, &mut context)
                    .unwrap()
                    .to_string(),
            );
        }
    }
    assert_eq!(values.len(), 5);
    assert_ne!(values[0], values[4]);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(arrays);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn replace_mutate_truncate_and_recreate_invalidate_prepared_handles() {
    for change in ["replace", "mutate", "truncate", "recreate"] {
        let fixture = Fixture::new();
        let session = ResidentVortexSession::new(8 * 1024 * 1024, 1).unwrap();
        let source = session.prepare_file(fixture.input()).unwrap();
        let count = source.prepare_count();
        let projection = source.prepare_projection(&["value"], 5, 65536).unwrap();
        let backup = fixture.0.join("backup.vortex");
        std::fs::copy(fixture.input(), &backup).unwrap();
        match change {
            "replace" => std::fs::rename(&backup, fixture.input()).unwrap(),
            "recreate" => {
                std::fs::remove_file(fixture.input()).unwrap();
                std::fs::copy(&backup, fixture.input()).unwrap();
            }
            "mutate" => {
                let original = std::fs::metadata(fixture.input()).unwrap();
                let mut file = File::options().write(true).open(fixture.input()).unwrap();
                file.seek(SeekFrom::Start(8)).unwrap();
                file.write_all(&[42]).unwrap();
                file.set_times(
                    std::fs::FileTimes::new().set_modified(original.modified().unwrap()),
                )
                .unwrap();
            }
            "truncate" => File::options()
                .write(true)
                .open(fixture.input())
                .unwrap()
                .set_len(12)
                .unwrap(),
            _ => unreachable!(),
        }
        assert!(count.execute().is_err(), "{change}");
        assert!(projection.execute().is_err(), "{change}");
        assert!(count.execute().is_err(), "invalidation must be sticky");
    }
}

#[test]
fn invalid_projection_and_memory_limits_fail_explicitly() {
    let fixture = Fixture::new();
    let tiny = ResidentVortexSession::new(8, 1).unwrap();
    assert!(tiny.prepare_file(fixture.input()).is_err());
    assert_eq!(tiny.snapshot().memory.reserved_bytes, 0);
    let session = ResidentVortexSession::new(8 * 1024 * 1024, 1).unwrap();
    let source = session.prepare_file(fixture.input()).unwrap();
    assert!(source.prepare_projection(&["missing"], 5, 1024).is_err());
    assert!(
        source
            .prepare_projection(&["value", "value"], 5, 1024)
            .is_err()
    );
    assert!(source.prepare_projection(&["value"], 0, 1024).is_err());
    assert!(
        source
            .prepare_projection(&["value"], 5, 1)
            .unwrap()
            .execute()
            .is_err()
    );
    assert_eq!(source.prepare_count().execute().unwrap(), 5);
}

#[test]
fn concurrent_prepared_calls_share_one_session_admission_boundary() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 * 1024 * 1024, 2).unwrap();
    let source = session.prepare_file(fixture.input()).unwrap();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let source = &source;
            scope.spawn(move || {
                let count = source.prepare_count();
                for _ in 0..32 {
                    assert_eq!(count.execute().unwrap(), 5);
                }
            });
        }
    });
    assert_eq!(session.snapshot().completed_executions, 256);
    assert_eq!(session.snapshot().prepared_source_opens, 1);
}
