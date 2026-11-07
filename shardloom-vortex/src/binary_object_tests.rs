use super::*;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct FileFixture(PathBuf);
impl FileFixture {
    fn new(bytes: &[u8]) -> Self {
        use std::io::Write as _;
        let path = std::env::temp_dir().join(format!(
            "shardloom-binary-object-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(bytes).unwrap();
        Self(path)
    }
}
impl Drop for FileFixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn binary_object_content_identity_binds_order_length_and_chunk_geometry() {
    let one = Chunk {
        offset: 0,
        length: 3,
        digest: digest(b"abc"),
    };
    let two = Chunk {
        offset: 3,
        length: 2,
        digest: digest(b"de"),
    };
    let a = content_id(5, &[one, two]);
    assert_ne!(a, content_id(5, &[two, one]));
    assert_ne!(a, content_id(6, &[one, two]));
    assert_ne!(
        a,
        content_id(5, &[Chunk { length: 2, ..one }, Chunk { length: 3, ..two }])
    );
    assert_eq!(
        a,
        content_id(
            5,
            &[Chunk { offset: 90, ..one }, Chunk { offset: 120, ..two }]
        )
    );
    assert_ne!(content_id(0, &[]), content_id(1, &[]));
}

#[test]
fn binary_object_memory_views_retain_payload_and_credit_after_session_and_handles_drop() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.shared_memory_pool();
    let payload: Vec<_> = (0..CHUNK_BYTES * 2 + 9)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let token = CancellationToken::default();
    let object = session.binary_object(&payload, &token).unwrap();
    let other = session.binary_object(&payload, &token).unwrap();
    assert!(object.is_managed());
    assert_eq!(object.len(), payload.len() as u64);
    assert_eq!(
        descriptor::parse(object.descriptor()).unwrap().length(),
        object.len()
    );
    assert_eq!(session.binary_object_snapshot().unwrap().managed_objects, 1);
    let begin = CHUNK_BYTES as u64 - 3;
    let bytes = object.read_range(begin..begin + 11, &token).unwrap();
    assert_eq!(&bytes[..], &payload[CHUNK_BYTES - 3..CHUNK_BYTES + 8]);
    assert_eq!(object.read(&token).unwrap().as_ref(), payload);
    assert_eq!(
        session.binary_object_snapshot().unwrap().payload_read_bytes,
        0
    );
    drop(other);
    drop(object);
    drop(session);
    assert!(memory.snapshot().reserved_bytes >= payload.len() as u64);
    assert_eq!(&bytes[..], &payload[CHUNK_BYTES - 3..CHUNK_BYTES + 8]);
    drop(bytes);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn binary_object_external_ranges_are_exact_lazy_and_do_not_own_the_file() {
    let fixture = FileFixture::new(b"0123456789abcdef");
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let token = CancellationToken::default();
    let object = session
        .external_binary_object(&fixture.0, 3..13, &token)
        .unwrap();
    assert!(!object.is_managed());
    assert_eq!(
        session.binary_object_snapshot().unwrap().payload_read_bytes,
        0
    );
    assert_eq!(object.read(&token).unwrap().as_ref(), b"3456789abc");
    assert_eq!(object.read_range(2..7, &token).unwrap().as_ref(), b"56789");
    assert!(object.read_range(10..10, &token).unwrap().is_empty());
    assert!(object.read_range(10..11, &token).is_err());
    assert!(
        object
            .read_range(Range { start: 8, end: 7 }, &token)
            .is_err()
    );
    let observed = session.binary_object_snapshot().unwrap();
    assert_eq!(observed.payload_read_bytes, 15);
    assert_eq!(observed.delivered_bytes, 15);
    drop(object);
    drop(session);
    assert_eq!(fs::read(&fixture.0).unwrap(), b"0123456789abcdef");
}

#[test]
fn binary_object_missing_changed_and_truncated_external_content_never_becomes_empty() {
    let token = CancellationToken::default();
    for mutation in 0..3 {
        let fixture = FileFixture::new(b"12345678");
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let object = session
            .external_binary_object(&fixture.0, 1..7, &token)
            .unwrap();
        match mutation {
            0 => fs::remove_file(&fixture.0).unwrap(),
            1 => fs::write(&fixture.0, b"abcdefgh").unwrap(),
            _ => fs::write(&fixture.0, b"12").unwrap(),
        }
        assert!(object.read(&token).is_err());
        assert!(object.read_range(0..0, &token).is_err());
        assert_eq!(
            session.binary_object_snapshot().unwrap().payload_read_bytes,
            0
        );
    }
}

#[test]
fn binary_object_empty_values_denied_memory_and_cancellation_keep_explicit_ownership() {
    let token = CancellationToken::default();
    let session = ResidentVortexSession::new(128 << 10, 1).unwrap();
    let empty = session.binary_object(&[], &token).unwrap();
    assert!(empty.is_empty());
    assert!(empty.is_managed());
    assert!(empty.read(&token).unwrap().is_empty());
    let before = session.binary_object_snapshot().unwrap();
    assert!(session.binary_object(&vec![0; 1 << 20], &token).is_err());
    assert_eq!(session.binary_object_snapshot().unwrap(), before);
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(session.binary_object(b"denied", &cancelled).is_err());
    assert!(empty.read(&cancelled).is_err());
    assert_eq!(session.binary_object_snapshot().unwrap(), before);
    let fixture = FileFixture::new(&vec![0x5a; 256 << 10]);
    let external = session
        .external_binary_object(&fixture.0, 0..256 << 10, &token)
        .unwrap();
    assert!(external.read(&token).is_err());
    assert_eq!(
        external.read_range(17..26, &token).unwrap().as_ref(),
        &[0x5a; 9]
    );
}
