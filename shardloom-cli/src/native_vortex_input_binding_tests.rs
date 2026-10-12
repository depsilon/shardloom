use super::*;
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::{LiveMemoryPool, LiveMemorySnapshot, MemoryLease};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

const MEMORY_BYTES: u64 = 8 << 20;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-native-vortex-input-binding-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).expect("create fixture root");
        Self(path)
    }

    fn write(&self, relative: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(relative);
        fs::create_dir_all(path.parent().expect("fixture file parent"))
            .expect("create fixture parents");
        fs::write(&path, bytes).expect("write fixture file");
        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn pool() -> LiveMemoryPool {
    LiveMemoryPool::new(MEMORY_BYTES).expect("explicit 8 MiB test pool")
}

fn cancellation() -> CancellationToken {
    CancellationToken::default()
}

fn assert_released(pool: &LiveMemoryPool, denied_reservations: u64) {
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.limit_bytes, MEMORY_BYTES);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert!(snapshot.peak_reserved_bytes <= MEMORY_BYTES);
    assert_eq!(snapshot.denied_reservations, denied_reservations);
}

fn retain_almost_all(pool: &LiveMemoryPool) -> MemoryLease {
    pool.reserve(MEMORY_BYTES - 1)
        .expect("retain all but one byte of the test grant")
}

fn write_manifest(path: &Path, inputs: &[PathBuf], paths: &[PathBuf]) {
    let inputs = inputs
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let paths = paths
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let manifest = serde_json::json!({"inputs": inputs, "paths": paths});
    fs::write(
        path,
        serde_json::to_vec(&manifest).expect("serialize manifest"),
    )
    .expect("write manifest");
}

#[test]
fn one_byte_pool_denies_bind_and_collection_probe_before_missing_path_inspection() {
    let fixture = Fixture::new();
    let missing = fixture.0.join("missing.vortex");
    assert!(!missing.exists());
    let input = missing.to_string_lossy();
    let pool = LiveMemoryPool::new(1).expect("explicit one-byte pool");

    let error = is_collection(&input, &pool, &cancellation()).expect_err("workspace is denied");
    assert!(error.to_string().contains("memory reservation denied"));
    let error = bind(&input, &pool, &cancellation()).expect_err("workspace is denied");
    assert!(error.to_string().contains("memory reservation denied"));
    assert_eq!(
        pool.snapshot(),
        LiveMemorySnapshot {
            limit_bytes: 1,
            reserved_bytes: 0,
            peak_reserved_bytes: 0,
            allocation_peak_reserved_bytes: 0,
            denied_reservations: 2,
        }
    );
}

#[test]
fn cancelled_and_expired_binding_reject_missing_paths_without_retaining_credits() {
    let fixture = Fixture::new();
    let missing = fixture.0.join("missing.vortex");
    assert!(!missing.exists());
    let input = missing.to_string_lossy();
    let pool = pool();
    let cancelled = cancellation();
    cancelled.cancel();
    let expired = cancellation().with_deadline(Instant::now());

    for (token, message) in [
        (&cancelled, "execution cancelled"),
        (&expired, "execution deadline exceeded"),
    ] {
        let error = bind(&input, &pool, token).expect_err("pre-cancelled request is rejected");
        assert!(error.to_string().contains(message));
        let error = is_collection(&input, &pool, token)
            .expect_err("pre-cancelled collection probe is rejected");
        assert!(error.to_string().contains(message));
        assert_eq!(
            pool.snapshot(),
            LiveMemorySnapshot {
                limit_bytes: MEMORY_BYTES,
                reserved_bytes: 0,
                peak_reserved_bytes: 0,
                allocation_peak_reserved_bytes: 0,
                denied_reservations: 0,
            }
        );
    }
}

#[test]
fn manifest_preserves_inputs_order_and_decodes_escaped_unicode_paths_before_paths() {
    let fixture = Fixture::new();
    let first = fixture.write("sources/λ,quoted-\".vtx", b"fixture");
    let second = fixture.write("sources/second.vortex", b"fixture");
    let ignored_precedence = fixture.write("sources/not-selected.vortex", b"fixture");
    let manifest_path = fixture.0.join("inputs.json");
    write_manifest(
        &manifest_path,
        &[second.clone(), first.clone()],
        std::slice::from_ref(&ignored_precedence),
    );
    let pool = pool();

    let binding = bind(
        manifest_path.to_str().expect("UTF-8 manifest path"),
        &pool,
        &cancellation(),
    )
    .expect("valid manifest binds");
    assert_eq!(binding.mode, "manifest");
    assert_eq!(
        binding.sources,
        vec![
            second.to_string_lossy().into_owned(),
            first.to_string_lossy().into_owned(),
        ]
    );
    assert!(binding.is_partitioned());
    assert!(pool.snapshot().reserved_bytes > 0);
    drop(binding);
    assert_released(&pool, 0);
}

#[test]
fn manifest_duplicate_and_invalid_entry_errors_release_all_reserved_credits() {
    let fixture = Fixture::new();
    let source = fixture.write("sources/one.vortex", b"fixture");
    let missing = fixture.0.join("sources/missing.vortex");
    let duplicate_manifest = fixture.0.join("duplicate.manifest");
    write_manifest(&duplicate_manifest, &[source.clone(), source.clone()], &[]);
    let pool = pool();

    let error = bind(
        duplicate_manifest.to_str().expect("UTF-8 manifest path"),
        &pool,
        &cancellation(),
    )
    .expect_err("duplicate manifest entries are rejected");
    assert!(error.to_string().contains("contains duplicate entry"));
    assert_released(&pool, 0);

    let invalid_manifest = fixture.0.join("invalid.json");
    write_manifest(&invalid_manifest, &[missing], &[]);
    let error = bind(
        invalid_manifest.to_str().expect("UTF-8 manifest path"),
        &pool,
        &cancellation(),
    )
    .expect_err("missing manifest entries are rejected");
    assert!(error.to_string().contains("not an existing file"));
    assert_released(&pool, 0);
}

#[test]
fn recursive_directory_returns_complete_sorted_vortex_paths_and_ignores_other_extensions() {
    let fixture = Fixture::new();
    let root = fixture.0.join("dataset");
    fs::create_dir_all(&root).expect("create dataset root");
    let first = fixture.write("dataset/z-last.vtx", b"fixture");
    let nested = fixture.write("dataset/nested/a-first.vortex", b"fixture");
    fixture.write("dataset/nested/ignored.parquet", b"not a Vortex input");
    fixture.write("dataset/ignored.txt", b"not a Vortex input");
    let pool = pool();

    let binding = bind(
        root.to_str().expect("UTF-8 directory path"),
        &pool,
        &cancellation(),
    )
    .expect("recursive directory binds");
    assert_eq!(binding.mode, "local_directory");
    assert_eq!(
        binding.sources,
        vec![
            nested.to_string_lossy().into_owned(),
            first.to_string_lossy().into_owned(),
        ]
    );
    assert!(binding.is_partitioned());
    drop(binding);
    assert_released(&pool, 0);
}

#[test]
fn retained_grant_blocks_manifest_and_directory_expansion_then_releases_all_credits() {
    let fixture = Fixture::new();
    let first = fixture.write("sources/first.vortex", b"fixture");
    let second = fixture.write("sources/second.vtx", b"fixture");
    let manifest_path = fixture.0.join("sources.json");
    write_manifest(&manifest_path, &[first.clone(), second.clone()], &[]);
    let directory = fixture.0.join("sources");
    let expected = vec![
        first.to_string_lossy().into_owned(),
        second.to_string_lossy().into_owned(),
    ];

    for input in [
        manifest_path.to_string_lossy().into_owned(),
        directory.to_string_lossy().into_owned(),
    ] {
        let pool = pool();
        let retained = retain_almost_all(&pool);
        assert_eq!(
            pool.snapshot(),
            LiveMemorySnapshot {
                limit_bytes: MEMORY_BYTES,
                reserved_bytes: MEMORY_BYTES - 1,
                peak_reserved_bytes: MEMORY_BYTES - 1,
                allocation_peak_reserved_bytes: MEMORY_BYTES - 1,
                denied_reservations: 0,
            }
        );

        let error = bind(&input, &pool, &cancellation()).expect_err("expansion needs memory");
        assert!(error.to_string().contains("memory reservation denied"));
        assert_eq!(
            pool.snapshot(),
            LiveMemorySnapshot {
                limit_bytes: MEMORY_BYTES,
                reserved_bytes: MEMORY_BYTES - 1,
                peak_reserved_bytes: MEMORY_BYTES - 1,
                allocation_peak_reserved_bytes: MEMORY_BYTES - 1,
                denied_reservations: 1,
            }
        );
        drop(retained);
        assert_released(&pool, 1);

        let binding =
            bind(&input, &pool, &cancellation()).expect("expansion succeeds after release");
        assert_eq!(binding.sources, expected);
        assert!(pool.snapshot().reserved_bytes > 0);
        drop(binding);
        assert_released(&pool, 1);
    }
}

#[test]
fn directory_expansion_accepts_257_unique_sources_with_an_explicit_eight_mib_pool() {
    let fixture = Fixture::new();
    let root = fixture.0.join("many");
    fs::create_dir_all(&root).expect("create source directory");
    let mut expected = Vec::new();
    for index in 0..257 {
        let extension = if index % 2 == 0 { "vortex" } else { "vtx" };
        let relative = format!("many/part-{index:03}.{extension}");
        let path = fixture.write(&relative, b"tiny");
        expected.push(path.to_string_lossy().into_owned());
    }
    expected.sort_unstable();
    let pool = pool();

    let binding = bind(
        root.to_str().expect("UTF-8 directory path"),
        &pool,
        &cancellation(),
    )
    .expect("all unique input files bind");
    assert_eq!(binding.sources, expected);
    assert_eq!(binding.sources.len(), 257);
    assert!(pool.snapshot().reserved_bytes > 0);
    assert!(pool.snapshot().reserved_bytes <= MEMORY_BYTES);
    drop(binding);
    assert_released(&pool, 0);
}

#[cfg(unix)]
#[test]
fn directory_ancestor_symlink_cycle_is_deterministic_and_releases_all_credits() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let root = fixture.0.join("cycle");
    let nested = root.join("nested");
    fs::create_dir_all(&nested).expect("create cycle directories");
    symlink(&root, nested.join("back-to-ancestor")).expect("create ancestor symlink");
    let pool = pool();

    let error = bind(
        root.to_str().expect("UTF-8 directory path"),
        &pool,
        &cancellation(),
    )
    .expect_err("ancestor directory symlink is rejected");
    assert_eq!(
        error.to_string(),
        ShardLoomError::InvalidOperation(
            "native Vortex input directory contains a symbolic-link cycle; no fallback execution was attempted"
                .into()
        )
        .to_string()
    );
    assert_released(&pool, 0);
}
