//! Evidence describes completed physical reads, including shared consumers.

use super::*;
use shardloom_core::native_io_decisions::{
    NativeIoCapability, NativeIoDecisionReason as Reason, NativeIoObservedWork,
};

pub(super) fn assert_regions(
    result: &CollectedVortexRelational,
    total: u64,
    excluded: u64,
    decline: Reason,
) {
    let decisions = &result.execution.native_io_certificate.capability_decisions;
    let row = decisions.get(NativeIoCapability::RegionExclusion).unwrap();
    assert_eq!(
        (row.considered, row.executed, row.declined),
        (total, excluded, total - excluded)
    );
    assert_eq!(row.admitted, excluded);
    assert_eq!(row.observed.regions, Some(total));
    assert_eq!(row.observed.excluded_regions, Some(excluded));
    assert_eq!(row.observed.input_rows, None);
    assert_eq!(row.observed.copied_buffer_bytes, None);
    if excluded > 0 {
        assert!(row.has_admission_reason(Reason::ExclusionProved));
    }
    if excluded < total {
        assert!(row.has_decline_reason(decline));
    }
}

#[test]
fn region_decisions_require_eof_and_do_not_accumulate_across_execution_or_repeated_eof() {
    let fixture = ParquetFixture::new();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.shared_memory_pool();
    let token = CancellationToken::default();
    let source = LocalColumnarSource::admit(&fixture.path(), "parquet", &memory, &token).unwrap();
    let demand = shardloom_plan::SourceDemand {
        predicates: vec![Some(equal("n", 5))],
        ..Default::default()
    };
    let prepared = source.prepare(&demand, &session, &token).unwrap();
    for complete in [false, true, true] {
        session
            .with_native_execution_context(&token, |context| {
                let mut reader = prepared.open(context)?;
                assert!(
                    context
                        .capability_decisions()?
                        .get(NativeIoCapability::RegionExclusion)
                        .is_none()
                );
                let batch = reader.next_native(&session, context)?.unwrap();
                assert_eq!(batch.row_count(), 4);
                drop(batch);
                assert!(
                    context
                        .capability_decisions()?
                        .get(NativeIoCapability::RegionExclusion)
                        .is_none()
                );
                if complete {
                    assert!(reader.next_native(&session, context)?.is_none());
                    let once = context.capability_decisions()?;
                    let row = once.get(NativeIoCapability::RegionExclusion).unwrap();
                    assert_eq!((row.considered, row.executed, row.declined), (4, 3, 1));
                    assert_eq!(row.observed.excluded_regions, Some(3));
                    for _ in 0..3 {
                        assert!(reader.next_native(&session, context)?.is_none());
                    }
                    assert_eq!(context.capability_decisions()?, once);
                }
                Ok(())
            })
            .unwrap();
    }
    drop((prepared, source, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn region_decisions_do_not_claim_execution_after_cancellation_or_resource_denial() {
    let fixture = ParquetFixture::new();
    for cancel in [false, true] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let memory = session.shared_memory_pool();
        let token = CancellationToken::default();
        let error = session
            .with_native_execution_context(&token, |context| {
                let source =
                    LocalColumnarSource::admit(&fixture.path(), "parquet", &memory, &token)?;
                let demand = shardloom_plan::SourceDemand {
                    predicates: vec![Some(equal("n", 5))],
                    ..Default::default()
                };
                let prepared = source.prepare(&demand, &session, &token)?;
                let mut reader = prepared.open(context)?;
                let snapshot = memory.snapshot();
                let _held = memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?;
                if cancel {
                    token.cancel();
                }
                let result = reader.next_native(&session, context);
                assert!(result.is_err());
                assert!(
                    context
                        .capability_decisions()?
                        .get(NativeIoCapability::RegionExclusion)
                        .is_none()
                );
                result.map(|_| ())
            })
            .unwrap_err();
        assert!(
            error.to_string().contains(if cancel {
                "cancelled"
            } else {
                "memory reservation denied"
            }),
            "{error}"
        );
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn region_decisions_report_unavailable_adapters_without_inventing_region_quantities() {
    let fixture = ParquetFixture::new();
    let rows = vec![vec![("n".into(), ScalarValue::Int64(5))]];
    for format in ["arrow-ipc", "avro"] {
        let path = fixture.0.join(format!("input.{format}"));
        let bytes = if format == "arrow-ipc" {
            crate::encode_flat_arrow_ipc_rows(&["n".into()], &rows)
        } else {
            crate::encode_flat_avro_rows(&["n".into()], &rows)
        }
        .unwrap();
        fs::write(&path, bytes).unwrap();
        let prepared = prepare_relational_with_schema(policy(), |preparation| {
            let source = LocalColumnarSource::admit(
                &path,
                format,
                &preparation.shared_memory_pool(),
                &preparation.cancellation_token(),
            )?;
            let uri = DatasetUri::new(path.display().to_string())?;
            preparation.register_local_source(uri.clone(), source)?;
            Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: uri,
                projection: ProjectionRequest::All,
                predicate: Some(equal("n", 5)),
            }))
        })
        .unwrap();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), [serde_json::json!({"n":5})]);
        let row = result
            .execution
            .native_io_certificate
            .capability_decisions
            .get(NativeIoCapability::RegionExclusion)
            .unwrap();
        assert_eq!((row.considered, row.executed, row.declined), (1, 0, 1));
        assert_eq!(row.observed, NativeIoObservedWork::default());
        assert!(row.has_decline_reason(Reason::SourceCapabilityUnavailable));
    }
}
