//! Exercise production positional reads independently of the test-only observer.

use super::super::ResidentFileReadAt;
use super::*;
use crate::owned_buffers::ReservedHostAllocator;
use std::{path::PathBuf, sync::atomic::AtomicUsize};
use vortex::{
    buffer::Alignment,
    io::{
        VortexReadAt as _,
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
    },
};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-native-read-evidence-{}-{}.bin",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, (0..128_u8).collect::<Vec<_>>()).unwrap();
        Self(path)
    }

    fn native() -> Self {
        let fixture = Self::new();
        std::fs::copy(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/local_primitive_struct_five.vortex"),
            &fixture.0,
        )
        .unwrap();
        fixture
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn native_file_scan_evidence_counts_successful_repeated_and_overlapping_reads() {
    let fixture = Fixture::new();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let runtime = CurrentThreadRuntime::new();
    let identity = Arc::new(SourceIdentity::capture(&fixture.0).unwrap());
    let mut evidence = Evidence::default();
    let (ordinal, work) = evidence.source(&identity, &memory).unwrap();
    assert_eq!(ordinal, 0);
    let reader = ResidentFileReadAt {
        identity,
        allocator: Arc::new(ReservedHostAllocator::new(memory.clone())),
        handle: runtime.handle(),
        concurrency: 1,
        scope: None,
        reader_owner: None,
        evidence: Some(work),
    };
    // Constructing or dropping a future must not claim completed I/O.
    drop(reader.read_at(0, 128, Alignment::none()));
    for (offset, length) in [(0, 32), (16, 32), (0, 32)] {
        drop(
            runtime
                .block_on(reader.read_at(offset, length, Alignment::none()))
                .unwrap(),
        );
    }
    assert!(
        runtime
            .block_on(reader.read_at(120, 16, Alignment::none()))
            .is_err()
    );
    std::fs::write(&fixture.0, [0_u8; 127]).unwrap();
    assert!(
        runtime
            .block_on(reader.read_at(0, 32, Alignment::none()))
            .is_err()
    );
    drop(reader);
    let (report, credit) = evidence.take();
    assert_eq!(report.sources.len(), 1);
    assert_eq!(report.sources[0].completed_read_requests, 3);
    assert_eq!(report.sources[0].completed_read_bytes, 96);
    assert_eq!(report.sources[0].segment_requests, 0);
    assert_eq!(report.contracts, []);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop((report, credit));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_scan_evidence_shares_only_the_same_held_generation() {
    let first = Fixture::new();
    let second = Fixture::new();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut evidence = Evidence::default();
    let a = Arc::new(SourceIdentity::capture(&first.0).unwrap());
    let same = Arc::new(SourceIdentity::capture(&first.0).unwrap());
    let other = Arc::new(SourceIdentity::capture(&second.0).unwrap());
    let (zero, work) = evidence.source(&a, &memory).unwrap();
    let before = memory.snapshot().reserved_bytes;
    let (again, reused) = evidence.source(&same, &memory).unwrap();
    assert_eq!((zero, again), (0, 0));
    assert!(Arc::ptr_eq(&work, &reused));
    assert_eq!(memory.snapshot().reserved_bytes, before);
    assert_eq!(evidence.source(&other, &memory).unwrap().0, 1);
    drop(evidence);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_scan_evidence_direct_projection_owns_report_and_refunds_failures() {
    use super::super::ResidentVortexSession;
    use shardloom_exec::compute_pool::CancellationToken;
    let fixture = Fixture::native();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(&fixture.0).unwrap();
    assert!(
        source
            .prepare_projection(&["value"], 5, 1 << 20)
            .unwrap()
            .with_filter(Some(vortex::expr::lit(1_i64)))
            .is_err()
    );
    let projection = source.prepare_projection(&["value"], 2, 1 << 20).unwrap();
    let result = projection.execute().unwrap();
    let evidence = result.file_scan_evidence();
    assert_eq!(result.row_count(), 2);
    assert_eq!(evidence.sources.len(), 1);
    assert_eq!(evidence.contracts.len(), 1);
    assert_eq!(
        evidence.contracts[0].projected_columns,
        Some(vec!["value".into()])
    );
    assert_eq!(evidence.contracts[0].delivered_rows, 2);
    assert_eq!(evidence.contracts[0].provider_scan_invocations, Some(1));
    assert!(evidence.sources[0].completed_read_bytes > 0);
    drop(result);
    let baseline = memory.snapshot().reserved_bytes;
    let token = CancellationToken::default();
    token.cancel();
    assert!(projection.execute_with_cancellation(&token).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let mut calls = 0;
    let failed = session.with_native_execution_context(&CancellationToken::default(), |context| {
        projection.consume_in_context(context, &mut |_| {
            calls += 1;
            Err(resident_error("terminal evidence test consumer error"))
        })
    });
    assert!(failed.is_err());
    assert_eq!(calls, 1);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let result = projection.execute().unwrap();
    drop((projection, source, session));
    assert_eq!(result.file_scan_evidence().contracts[0].delivered_rows, 2);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_scan_evidence_merges_repeated_contracts_without_growing_credit() {
    use shardloom_core::native_file_scan::{NativeFileScanContract, NativeFileScanRoute};
    let fixture = Fixture::new();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut evidence = Evidence::default();
    let identity = Arc::new(SourceIdentity::capture(&fixture.0).unwrap());
    evidence.source(&identity, &memory).unwrap();
    let contract = NativeFileScanContract {
        source: 0,
        route: NativeFileScanRoute::RelationalScan,
        source_rows: 3,
        available_columns: 1,
        projected_columns: Some(vec!["key".into()]),
        output_columns: Some(vec!["key".into()]),
        pushed_predicate_columns: Some(vec!["key".into()]),
        residual_predicate_columns: Some(vec![]),
        invocations: 1,
        provider_scan_invocations: Some(1),
        delivered_rows: 2,
        whole_file_pruning_checks: 1,
        whole_file_exclusions: 0,
    };
    evidence
        .record_path(&fixture.0, contract.clone(), &memory)
        .unwrap();
    let baseline = memory.snapshot().reserved_bytes;
    for _ in 0..1000 {
        evidence
            .record_path(&fixture.0, contract.clone(), &memory)
            .unwrap();
    }
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    assert!(
        evidence
            .record_path(std::path::Path::new("/missing-source"), contract, &memory)
            .is_err()
    );
    let (report, credit) = evidence.take();
    assert_eq!(report.contracts.len(), 1);
    assert_eq!(report.contracts[0].invocations, 1001);
    assert_eq!(report.contracts[0].delivered_rows, 2002);
    drop((report, credit, evidence));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
