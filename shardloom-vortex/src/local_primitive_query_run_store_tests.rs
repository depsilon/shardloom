use super::super::{VortexLocalPrimitiveExecutionPolicy, local_vortex_runtime};
use super::*;
use vortex::{
    VortexSessionDefault as _,
    array::{IntoArray as _, VortexSessionExecute as _, arrays::PrimitiveArray},
    io::session::RuntimeSessionExt as _,
    scalar::Scalar,
};

struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-query-store-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_WORKSPACE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn policy(&self) -> QueryRunStorePolicy {
        QueryRunStorePolicy::numeric_sort(
            &crate::VortexSortSpillPolicy::new(&self.0, 32 << 20, 4 << 20).unwrap(),
        )
    }

    fn assert_empty(&self) {
        assert_eq!(fs::read_dir(&self.0).unwrap().count(), 0);
    }
}
impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn store(workspace: &Workspace, memory: &LiveMemoryPool) -> QueryRunStore {
    QueryRunStore::new(
        workspace.policy(),
        memory.clone(),
        memory.reserve(128 << 10).unwrap(),
    )
    .unwrap()
}

fn arrays(values: &[i32]) -> impl Iterator<Item = Result<ArrayRef>> + '_ {
    values
        .chunks(2)
        .map(|chunk| Ok(PrimitiveArray::from_iter(chunk.iter().copied()).into_array()))
}

fn spec(rows: usize) -> QueryRunSpec {
    QueryRunSpec {
        dtype: PrimitiveArray::from_iter([0_i32])
            .into_array()
            .dtype()
            .clone(),
        rows: rows as u64,
        block_rows: 2,
        metadata_bytes: 16 << 10,
    }
}

#[test]
fn native_array_runs_roundtrip_non_sort_schema_and_empty_without_row_serialization() {
    let workspace = Workspace::new();
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let work = Arc::new(memory.reserve(1 << 20).unwrap());
    let mut store = store(&workspace, &memory);
    for expected in [vec![i32::MIN, -1, 0, i32::MAX, 17], Vec::new()] {
        let run = store
            .write_arrays(
                &spec(expected.len()),
                arrays(&expected),
                &runtime,
                &session,
                &work,
            )
            .unwrap();
        assert_eq!(
            run.metadata.bytes(),
            spec(0).metadata_bytes + path_reservation(&run.path, 4).unwrap()
        );
        let mut reader = store
            .open(&run, &spec(0).dtype, &runtime, &session, Arc::clone(&work))
            .unwrap();
        let mut context = session.create_execution_ctx();
        let mut seen = 0;
        while let Some(block) = reader.next_block(&runtime).unwrap() {
            assert!(block.array().len() <= 2);
            for row in 0..block.array().len() {
                assert_eq!(
                    block.array().execute_scalar(row, &mut context).unwrap(),
                    Scalar::from(expected[seen])
                );
                seen += 1;
            }
        }
        assert_eq!(seen, expected.len());
        reader.validate().unwrap();
        drop(reader);
        store.remove(&run).unwrap();
    }
    assert_eq!(store.snapshot().runs_written, 2);
    assert_eq!(store.snapshot().runs_validated, 2);
    assert_eq!(store.snapshot().live_disk_bytes, MARKER_BYTE_RESERVATION);
    drop(store);
    drop(work);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.assert_empty();
}

#[test]
fn native_block_retains_metadata_work_and_path_credits_after_store_and_reader_drop() {
    let workspace = Workspace::new();
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let work = Arc::new(memory.reserve(1 << 20).unwrap());
    let mut store = store(&workspace, &memory);
    let run = store
        .write_arrays(
            &spec(2),
            arrays(&[i32::MIN, i32::MAX]),
            &runtime,
            &session,
            &work,
        )
        .unwrap();
    let mut reader = store
        .open(&run, &spec(0).dtype, &runtime, &session, Arc::clone(&work))
        .unwrap();
    let block = reader.next_block(&runtime).unwrap().unwrap();
    let retained = run.metadata.bytes() + reader.path_credit.bytes() + work.bytes();
    drop(run);
    drop(reader);
    drop(work);
    store.cleanup().unwrap();
    drop(store);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    let mut context = session.create_execution_ctx();
    assert_eq!(
        block.array().execute_scalar(0, &mut context).unwrap(),
        Scalar::from(i32::MIN)
    );
    assert_eq!(
        block.array().execute_scalar(1, &mut context).unwrap(),
        Scalar::from(i32::MAX)
    );
    drop(block);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.assert_empty();
}

#[test]
fn foreign_pool_scratch_and_work_are_rejected_before_file_side_effects() {
    let workspace = Workspace::new();
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let other = LiveMemoryPool::new(4 << 20).unwrap();
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    assert!(
        QueryRunStore::new(
            workspace.policy(),
            memory.clone(),
            other.reserve(128 << 10).unwrap()
        )
        .is_err()
    );
    assert_eq!(other.snapshot().reserved_bytes, 0);
    workspace.assert_empty();
    let work = Arc::new(memory.reserve(1 << 20).unwrap());
    let foreign = Arc::new(other.reserve(1 << 20).unwrap());
    let mut store = store(&workspace, &memory);
    assert!(
        store
            .write_arrays(&spec(2), arrays(&[1, 2]), &runtime, &session, &foreign)
            .is_err()
    );
    assert!(store.owned.is_empty());
    let run = store
        .write_arrays(&spec(2), arrays(&[1, 2]), &runtime, &session, &work)
        .unwrap();
    assert!(
        store
            .open(
                &run,
                &spec(0).dtype,
                &runtime,
                &session,
                Arc::clone(&foreign)
            )
            .is_err()
    );
    assert_eq!(store.snapshot().runs_written, 1);
    drop(run);
    drop(store);
    drop(work);
    drop(foreign);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(other.snapshot().reserved_bytes, 0);
    workspace.assert_empty();
}

#[test]
fn metadata_denial_is_before_run_creation_and_long_paths_have_separate_credit() {
    let workspace = Workspace::new();
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let work = Arc::new(memory.reserve(1 << 20).unwrap());
    let mut store = store(&workspace, &memory);
    let base = memory.snapshot().reserved_bytes;
    let mut oversized = spec(2);
    oversized.metadata_bytes = memory.snapshot().limit_bytes;
    assert!(
        store
            .write_arrays(&oversized, arrays(&[1, 2]), &runtime, &session, &work)
            .is_err()
    );
    assert!(store.owned.is_empty());
    assert_eq!(memory.snapshot().reserved_bytes, base);
    assert!(path_reservation(&PathBuf::from("x".repeat(4096)), 4).unwrap() > 16 << 10);
    drop(store);
    drop(work);
    workspace.assert_empty();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    // Retaining a short logical path with a large backing capacity must not
    // escape the same budget. Admission fails before a private directory exists.
    let mut policy = workspace.policy();
    policy.workspace.reserve(3 << 20);
    assert!(path_reservation(&policy.workspace, 2).unwrap() > 6 << 20);
    assert!(
        QueryRunStore::new(policy, memory.clone(), memory.reserve(128 << 10).unwrap()).is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.assert_empty();
}

#[test]
fn overlapping_output_quota_counts_partial_files_and_a_failed_store_cannot_resume() {
    let workspace = Workspace::new();
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let work = Arc::new(memory.reserve(1 << 20).unwrap());
    let mut store = store(&workspace, &memory);
    let run = store
        .write_arrays(&spec(4), arrays(&[1, 2, 3, 4]), &runtime, &session, &work)
        .unwrap();
    let first_bytes = store.snapshot().live_disk_bytes;
    store.policy.quota_bytes = first_bytes + run.bytes - 1;
    let error = store
        .write_arrays(&spec(4), arrays(&[1, 2, 3, 4]), &runtime, &session, &work)
        .unwrap_err();
    assert!(error.to_string().contains("quota"));
    assert!(run.path.exists());
    assert!(store.snapshot().live_disk_bytes >= first_bytes);
    assert_eq!(
        store.snapshot().live_disk_bytes,
        MARKER_BYTE_RESERVATION
            + store
                .owned
                .iter()
                .map(|path| fs::metadata(path).unwrap().len())
                .sum::<u64>()
    );
    assert!(store.snapshot().peak_disk_bytes <= store.policy.quota_bytes);
    let files = store.owned.len();
    assert!(
        store
            .write_arrays(&spec(2), arrays(&[5, 6]), &runtime, &session, &work)
            .unwrap_err()
            .to_string()
            .contains("store failed")
    );
    assert_eq!(store.owned.len(), files);
    store.cleanup().unwrap();
    assert_eq!(store.snapshot().live_disk_bytes, 0);
    drop(store);
    drop(run);
    drop(work);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.assert_empty();
}

#[test]
fn changed_and_truncated_run_bytes_are_rejected_before_native_open() {
    use std::io::{Seek as _, SeekFrom};
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    for truncate in [false, true] {
        let workspace = Workspace::new();
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let work = Arc::new(memory.reserve(1 << 20).unwrap());
        let mut store = store(&workspace, &memory);
        let run = store
            .write_arrays(&spec(2), arrays(&[1, 2]), &runtime, &session, &work)
            .unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&run.path)
            .unwrap();
        if truncate {
            file.set_len(run.bytes - 1).unwrap();
        } else {
            let mut byte = [0];
            file.read_exact(&mut byte).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            byte[0] ^= 1;
            file.write_all(&byte).unwrap();
        }
        drop(file);
        assert!(
            store
                .open(&run, &spec(0).dtype, &runtime, &session, Arc::clone(&work))
                .is_err()
        );
        drop(run);
        drop(store);
        drop(work);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        workspace.assert_empty();
    }
}

#[test]
fn schema_and_block_shape_mismatch_never_publish_a_run() {
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    for blocks in [
        vec![Ok(PrimitiveArray::from_iter([1_u64, 2]).into_array())],
        vec![Ok(PrimitiveArray::from_iter([1_i32]).into_array())],
        vec![Ok(PrimitiveArray::from_iter([1_i32, 2]).into_array())],
    ] {
        let workspace = Workspace::new();
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let work = Arc::new(memory.reserve(1 << 20).unwrap());
        let mut store = store(&workspace, &memory);
        assert!(
            store
                .write_arrays(&spec(4), blocks.into_iter(), &runtime, &session, &work)
                .is_err()
        );
        assert_eq!(store.snapshot().runs_written, 0);
        assert_eq!(store.snapshot().runs_validated, 0);
        drop(store);
        drop(work);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        workspace.assert_empty();
    }
}

#[test]
fn cancellation_stops_reader_and_mid_write_then_owned_cleanup_releases_every_credit() {
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    for during_write in [false, true] {
        let workspace = Workspace::new();
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let work = Arc::new(memory.reserve(1 << 20).unwrap());
        let mut store = store(&workspace, &memory);
        let cancellation = Arc::clone(&store.policy.cancellation);
        if during_write {
            let blocks = arrays(&[1, 2, 3, 4]).enumerate().map(|(index, block)| {
                if index == 1 {
                    cancellation.store(true, Ordering::Release);
                }
                block
            });
            assert!(
                store
                    .write_arrays(&spec(4), blocks, &runtime, &session, &work)
                    .unwrap_err()
                    .to_string()
                    .contains("cancelled")
            );
        } else {
            let run = store
                .write_arrays(&spec(4), arrays(&[1, 2, 3, 4]), &runtime, &session, &work)
                .unwrap();
            let mut reader = store
                .open(&run, &spec(0).dtype, &runtime, &session, Arc::clone(&work))
                .unwrap();
            let first = reader.next_block(&runtime).unwrap().unwrap();
            cancellation.store(true, Ordering::Release);
            assert!(
                reader
                    .next_block(&runtime)
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("cancelled")
            );
            assert!(
                reader
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("cancelled")
            );
            drop(first);
        }
        store.cleanup().unwrap();
        drop(store);
        drop(work);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        workspace.assert_empty();
    }
}

#[test]
fn closed_namespaces_and_foreign_store_runs_cannot_cross_cleanup_boundaries() {
    let workspace = Workspace::new();
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let work = Arc::new(memory.reserve(1 << 20).unwrap());
    let mut first = store(&workspace, &memory);
    let policy = QueryRunStorePolicy::exact_integer_distinct(
        workspace.0.clone(),
        32 << 20,
        Arc::new(AtomicBool::new(false)),
    );
    let mut second = QueryRunStore::new(
        policy.clone(),
        memory.clone(),
        memory.reserve(128 << 10).unwrap(),
    )
    .unwrap();
    let run = first
        .write_arrays(&spec(2), arrays(&[1, 2]), &runtime, &session, &work)
        .unwrap();
    assert!(second.remove(&run).is_err());
    assert!(
        second
            .open(&run, &spec(0).dtype, &runtime, &session, Arc::clone(&work))
            .is_err()
    );
    assert!(recover(&policy, first.directory()).is_err());
    let unknown = second.directory().join("user.txt");
    fs::write(&unknown, b"preserve me").unwrap();
    assert!(
        recover(&policy, second.directory())
            .unwrap_err()
            .to_string()
            .contains("unknown file")
    );
    assert!(unknown.exists());
    assert!(run.path.exists());
    fs::remove_file(unknown).unwrap();
    recover(&policy, second.directory()).unwrap();
    drop(second);
    drop(first);
    drop(run);
    drop(work);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.assert_empty();
}
