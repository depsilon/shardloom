use super::*;
use crate::{
    VortexLocalPrimitiveExecutionPolicy,
    local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
    relational_query::{VortexRelationalPlan, VortexRelationalScan},
    resident_memory_source::ResidentMemorySource,
    resident_session::ResidentVortexSession,
};
use arrow_array::{Array as _, cast::AsArray as _, types::Int64Type};
use shardloom_core::{DatasetUri, ExecutionResourceOrigin, ExecutionResources, ShardLoomError};
use shardloom_plan::ProjectionRequest;

fn range(rows: usize) -> (PreparedVortexRelational, ResidentVortexSession) {
    let bytes = 2 << 20;
    let session = ResidentVortexSession::new(bytes, 1).unwrap();
    let source = ResidentMemorySource::from_int64_range(&session, "value", -19, 3, rows).unwrap();
    let uri = DatasetUri::new("memory://arrow-stream-range").unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(bytes, 1, ExecutionResourceOrigin::Context).unwrap(),
    )
    .unwrap();
    let prepared = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
        schemas.register_memory_source(uri.clone(), |_| Ok(source))?;
        Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: ProjectionRequest::All,
            predicate: None,
        }))
    })
    .unwrap();
    (prepared, session)
}

#[test]
fn incremental_arrow_delivery_exceeds_resident_budget_and_verifies_every_value() {
    let rows = 1_000_017;
    let (prepared, session) = range(rows);
    let memory = session.shared_memory_pool();
    let baseline = memory.snapshot().reserved_bytes;
    assert!(
        prepared
            .execute_native(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let mut received = 0;
    let mut retained = None;
    let completed = prepared
        .for_each_arrow_batch(&CancellationToken::default(), |batch| {
            let values = batch.column(0).as_primitive::<Int64Type>();
            assert_eq!(values.null_count(), 0);
            assert!(batch.num_rows() <= 2048);
            for value in values.values() {
                assert_eq!(*value, i64::try_from(received).unwrap() * 3 - 19);
                received += 1;
            }
            if retained.is_none() {
                retained = Some(batch);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(received, rows);
    assert_eq!(completed.execution.output_rows, rows as u64);
    assert_eq!(completed.export.output_rows, rows as u64);
    assert!(completed.export.complete);
    assert!(completed.execution.native_io_certificate.is_certified());
    assert!(memory.snapshot().peak_reserved_bytes <= memory.snapshot().limit_bytes);
    drop((completed, prepared, session));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(
        retained
            .as_ref()
            .unwrap()
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0),
        -19
    );
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn incremental_arrow_consumer_error_and_cancellation_stop_demand_and_allow_retry() {
    let (prepared, session) = range(65_537);
    let memory = session.shared_memory_pool();
    let baseline = memory.snapshot().reserved_bytes;
    let mut batches = 0;
    let error = prepared
        .for_each_arrow_batch(&CancellationToken::default(), |_| {
            batches += 1;
            Err(ShardLoomError::InvalidOperation("consumer stopped".into()))
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("consumer stopped"));
    assert_eq!(batches, 1);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let token = CancellationToken::default();
    let mut cancelled_batches = 0;
    assert!(
        prepared
            .for_each_arrow_batch(&token, |_| {
                cancelled_batches += 1;
                token.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(cancelled_batches, 1);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let occupied = memory
        .reserve(memory.snapshot().limit_bytes - baseline)
        .unwrap();
    let mut denied_demand = false;
    assert!(
        prepared
            .for_each_arrow_batch(&CancellationToken::default(), |_| {
                denied_demand = true;
                Ok(())
            })
            .is_err()
    );
    assert!(!denied_demand);
    drop(occupied);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let completed = prepared
        .for_each_arrow_batch(&CancellationToken::default(), |_| Ok(()))
        .unwrap();
    assert_eq!(completed.execution.output_rows, 65_537);
    assert_eq!(session.snapshot().completed_executions, 1);
    drop((completed, prepared, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn incremental_arrow_empty_result_still_delivers_its_exact_schema() {
    let (prepared, session) = range(0);
    let mut batches = 0;
    let completed = prepared
        .for_each_arrow_batch(&CancellationToken::default(), |batch| {
            batches += 1;
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.schema().field(0).name(), "value");
            assert_eq!(
                batch.schema().field(0).data_type(),
                &arrow_schema::DataType::Int64
            );
            assert!(!batch.schema().field(0).is_nullable());
            Ok(())
        })
        .unwrap();
    assert_eq!(batches, 1);
    assert_eq!(completed.export.output_batches, 1);
    assert_eq!(completed.export.output_rows, 0);
    assert!(completed.export.complete);
    assert_eq!(session.snapshot().completed_executions, 1);
}
