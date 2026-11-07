use super::*;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-object-store-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.0);
    }
}
fn entries(path: &Path) -> Vec<PathBuf> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect()
}

#[test]
fn binary_object_store_import_is_bounded_lazy_and_owned_until_last_reader() {
    let directory = Directory::new();
    let source = directory.0.join("caller-source");
    let expected: Vec<_> = (0..2 * 1024 * 1024 + 19)
        .map(|n| u8::try_from(n % 251).unwrap())
        .collect();
    fs::write(&source, &expected).unwrap();
    let session = ResidentVortexSession::new(256 << 10, 1).unwrap();
    let memory = session.shared_memory_pool();
    let store = session
        .binary_object_store(&directory.0, expected.len() as u64)
        .unwrap();
    let namespace = store.directory.path.clone();
    let token = CancellationToken::default();
    let object = store.import_file(&source, &token).unwrap();
    assert_eq!(store.snapshot().retained_bytes, expected.len() as u64);
    assert_eq!(
        session.binary_object_snapshot().unwrap().payload_read_bytes,
        0
    );
    fs::remove_file(&source).unwrap();
    assert!(
        object.read(&token).is_err(),
        "complete read exceeds the live memory grant"
    );
    let begin = CHUNK_BYTES as u64 - 7;
    let bytes = object.read_range(begin..begin + 23, &token).unwrap();
    assert_eq!(&bytes[..], &expected[CHUNK_BYTES - 7..CHUNK_BYTES + 16]);
    let observed = session.binary_object_snapshot().unwrap();
    assert_eq!(observed.payload_read_requests, 2);
    assert_eq!(observed.payload_read_bytes, 2 * CHUNK_BYTES as u64);
    assert_eq!(observed.delivered_bytes, 23);
    drop(bytes);
    let retained = object.clone();
    drop(object);
    drop(store);
    drop(session);
    assert!(namespace.is_dir());
    assert_eq!(
        retained
            .read_range(2_000_000..2_000_020, &token)
            .unwrap()
            .as_ref(),
        &expected[2_000_000..2_000_020]
    );
    drop(retained);
    assert!(!namespace.exists());
    assert_eq!(entries(&directory.0), Vec::<PathBuf>::new());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn binary_object_store_quota_deduplication_and_external_files_are_separate() {
    let directory = Directory::new();
    let session = ResidentVortexSession::new(512 << 10, 1).unwrap();
    let store = session.binary_object_store(&directory.0, 8).unwrap();
    let token = CancellationToken::default();
    let a = store.put_bytes(b"1234", &token).unwrap();
    let same = store.put_bytes(b"1234", &token).unwrap();
    assert_eq!(a.descriptor(), same.descriptor());
    assert_eq!(store.snapshot().retained_bytes, 4);
    assert_eq!(entries(&store.directory.path).len(), 1);
    let b = store.put_bytes(b"abcd", &token).unwrap();
    assert_eq!(store.snapshot().retained_bytes, 8);
    assert!(store.put_bytes(b"x", &token).is_err());
    assert_eq!(store.snapshot().retained_bytes, 8);
    drop(a);
    assert_eq!(same.read(&token).unwrap().as_ref(), b"1234");
    drop(same);
    assert_eq!(b.read(&token).unwrap().as_ref(), b"abcd");
    drop(b);
    let external = directory.0.join("external");
    fs::write(&external, b"keep me").unwrap();
    let borrowed = session
        .external_binary_object(&external, 0..7, &token)
        .unwrap();
    drop(borrowed);
    drop(store);
    drop(session);
    assert_eq!(fs::read(&external).unwrap(), b"keep me");
    fs::remove_file(external).unwrap();
    assert_eq!(entries(&directory.0), Vec::<PathBuf>::new());
}

#[test]
fn binary_object_store_cancelled_partial_copy_refunds_and_never_registers() {
    let directory = Directory::new();
    let session = ResidentVortexSession::new(512 << 10, 1).unwrap();
    let store = session
        .binary_object_store(&directory.0, 4 * CHUNK_BYTES as u64)
        .unwrap();
    let cancellation = CancellationToken::default();
    let result = session.with_native_execution_context(&cancellation, |context| {
        store.copy(3 * CHUNK_BYTES as u64, context, |start, target| {
            target.fill(0x3e);
            if start >= CHUNK_BYTES as u64 {
                cancellation.cancel();
            }
            Ok(())
        })
    });
    assert!(result.is_err());
    assert_eq!(entries(&store.directory.path), Vec::<PathBuf>::new());
    assert_eq!(store.snapshot().retained_bytes, 0);
    assert_eq!(session.binary_object_snapshot().unwrap().managed_objects, 0);
    let value = store
        .put_bytes(b"next", &CancellationToken::default())
        .unwrap();
    assert_eq!(
        value.read(&CancellationToken::default()).unwrap().as_ref(),
        b"next"
    );
    drop(value);
    drop(store);
    drop(session);
    assert_eq!(entries(&directory.0), Vec::<PathBuf>::new());
}

#[test]
fn binary_object_store_empty_and_memory_denied_intake_leave_no_partial_files() {
    let directory = Directory::new();
    let session = ResidentVortexSession::new(64 << 10, 1).unwrap();
    let store = session.binary_object_store(&directory.0, 1 << 20).unwrap();
    let token = CancellationToken::default();
    assert!(store.put_bytes(&vec![0; 2 * CHUNK_BYTES], &token).is_err());
    assert_eq!(store.snapshot().retained_bytes, 0);
    assert_eq!(entries(&store.directory.path), Vec::<PathBuf>::new());
    let empty = store.put_bytes(&[], &token).unwrap();
    assert!(empty.is_managed() && empty.is_empty());
    assert!(empty.read(&token).unwrap().is_empty());
    assert_eq!(entries(&store.directory.path).len(), 1);
    drop(empty);
    drop(store);
    drop(session);
    assert_eq!(entries(&directory.0), Vec::<PathBuf>::new());
}

#[test]
fn binary_object_store_cleanup_refuses_replaced_paths_and_unknown_entries() {
    use std::os::unix::fs::symlink;
    let directory = Directory::new();
    let outside = directory.0.join("unrelated");
    fs::write(&outside, b"untouched").unwrap();
    let session = ResidentVortexSession::new(256 << 10, 1).unwrap();
    let store = session.binary_object_store(&directory.0, 8).unwrap();
    let namespace = store.directory.path.clone();
    let quota = Arc::clone(&store.quota);
    let object = store
        .put_bytes(b"12345678", &CancellationToken::default())
        .unwrap();
    let original = entries(&namespace).pop().unwrap();
    let moved = namespace.join("unknown-owned-renamed");
    fs::rename(&original, &moved).unwrap();
    symlink(&outside, &original).unwrap();
    drop(object);
    drop(store);
    drop(session);
    assert_eq!(fs::read(&outside).unwrap(), b"untouched");
    assert!(
        fs::symlink_metadata(&original)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fs::read(&moved).unwrap(), b"12345678");
    assert_eq!(quota.bytes.load(Ordering::Acquire), 8);
    assert_eq!(quota.cleanup_failures.load(Ordering::Acquire), 1);
    fs::remove_file(original).unwrap();
    fs::remove_file(moved).unwrap();
    fs::remove_dir(namespace).unwrap();
    fs::remove_file(outside).unwrap();
}

#[test]
fn binary_object_store_renamed_payload_remains_charged_without_deleting_unknown_file() {
    let directory = Directory::new();
    let session = ResidentVortexSession::new(256 << 10, 1).unwrap();
    let store = session.binary_object_store(&directory.0, 4).unwrap();
    let namespace = store.directory.path.clone();
    let quota = Arc::clone(&store.quota);
    let object = store
        .put_bytes(b"keep", &CancellationToken::default())
        .unwrap();
    let original = entries(&namespace).pop().unwrap();
    let renamed = namespace.join("unknown");
    fs::rename(original, &renamed).unwrap();
    drop(object);
    drop(store);
    drop(session);
    assert_eq!(fs::read(&renamed).unwrap(), b"keep");
    assert_eq!(quota.bytes.load(Ordering::Acquire), 4);
    assert_eq!(quota.cleanup_failures.load(Ordering::Acquire), 1);
    fs::remove_file(renamed).unwrap();
    fs::remove_dir(namespace).unwrap();
}
