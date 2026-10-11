use super::*;
use crate::{
    VortexLocalPrimitiveExecutionPolicy,
    local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
    relational_query::{VortexRelationalLimit, VortexRelationalPlan, VortexRelationalScan},
    resident_session::ResidentVortexSession,
};
use arrow_array::{Int64Array, cast::AsArray as _, types::Int64Type};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shardloom_core::{DatasetUri, ExecutionResourceOrigin, ExecutionResources, ShardLoomError};
use shardloom_plan::ProjectionRequest;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]))
}

fn batch(start: i64, rows: usize) -> RecordBatch {
    let values = (0..rows)
        .map(|row| {
            let value = start + i64::try_from(row).unwrap();
            (value % 17 != 0).then_some(value)
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn prepare(limit: Option<usize>, bytes: u64) -> (PreparedVortexRelational, ResidentVortexSession) {
    let session = ResidentVortexSession::new(bytes, 1).unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(bytes, 1, ExecutionResourceOrigin::Context).unwrap(),
    )
    .unwrap();
    let uri = DatasetUri::new("memory://columnar-stream").unwrap();
    let prepared = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
        schemas.register_arrow_batch_source(
            uri.clone(),
            schema(),
            &CancellationToken::default(),
        )?;
        let scan = VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: ProjectionRequest::All,
            predicate: None,
        });
        Ok(limit.map_or(scan.clone(), |count| {
            VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                input: scan,
                offset: 0,
                count,
            }))
        }))
    })
    .unwrap();
    (prepared, session)
}

#[test]
fn arrow_input_stream_large_batches_release_before_next_demand_and_verify_every_value() {
    let (prepared, session) = prepare(None, 3 << 20);
    let memory = session.shared_memory_pool();
    let baseline = memory.snapshot().reserved_bytes;
    let rows = 131_073;
    let mut calls = 0;
    let mut demand_memory = None;
    let mut input = || {
        let reserved = memory.snapshot().reserved_bytes;
        assert_eq!(*demand_memory.get_or_insert(reserved), reserved);
        calls += 1;
        Ok((calls <= 9).then(|| batch(i64::try_from((calls - 1) * rows).unwrap(), rows)))
    };
    let mut received = 0_i64;
    let completed = prepared
        .with_arrow_batch_input(&mut input)
        .unwrap()
        .for_each_arrow_batch(&CancellationToken::default(), |batch| {
            assert!(batch.num_rows() <= 2048);
            for value in batch.column(0).as_primitive::<Int64Type>() {
                assert_eq!(value, (received % 17 != 0).then_some(received));
                received += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(calls, 10);
    assert_eq!(received, i64::try_from(rows * 9).unwrap());
    let input = completed.execution.input.as_ref().unwrap();
    assert_eq!(input.payload_batches, 9);
    assert_eq!(input.max_batch_rows, rows);
    assert_eq!(input.max_retained_input_batches, 1);
    assert_eq!(input.intake_payload_bytes_copied, 0);
    assert!(input.intake_columnar_buffer_bytes_copied > 3 * (3 << 20));
    assert!(input.end_of_input_observed && input.output_ownership_detached);
    assert!(completed.execution.native_io_certificate.is_certified());
    assert!(completed.export.complete);
    drop(completed);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    assert!(memory.snapshot().peak_reserved_bytes <= 3 << 20);
    drop((prepared, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn arrow_input_native_collection_retains_values_after_all_query_owners_drop() {
    let (prepared, session) = prepare(None, 16 << 20);
    let memory = session.shared_memory_pool();
    let mut source = Some(batch(-37, 65_537));
    let mut input = || Ok(source.take());
    let completed = prepared
        .with_arrow_batch_input(&mut input)
        .unwrap()
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert!(
        completed
            .execution
            .input
            .as_ref()
            .unwrap()
            .end_of_input_observed
    );
    let result = Arc::new(completed.result);
    drop((completed.execution, prepared, session));
    let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
    drop(result);
    let mut expected = -37_i64;
    while let Some(batch) = stream.next_batch().unwrap() {
        for value in batch.column(0).as_primitive::<Int64Type>() {
            assert_eq!(value, (expected % 17 != 0).then_some(expected));
            expected += 1;
        }
    }
    assert_eq!(expected, -37 + 65_537);
    drop(stream);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn arrow_input_empty_batches_and_limit_still_require_complete_input() {
    for limit in [None, Some(0), Some(1)] {
        let (prepared, session) = prepare(limit, 2 << 20);
        let memory = session.shared_memory_pool();
        let baseline = memory.snapshot().reserved_bytes;
        let mut calls = 0;
        let mut input = || {
            calls += 1;
            Ok(match calls {
                1 | 3 => Some(batch(0, 0)),
                2 => Some(batch(7, 4)),
                4 => None,
                _ => unreachable!(),
            })
        };
        let completed = prepared
            .with_arrow_batch_input(&mut input)
            .unwrap()
            .execute_native(&CancellationToken::default())
            .unwrap();
        assert_eq!(calls, 4);
        assert_eq!(completed.execution.output_rows, limit.unwrap_or(4) as u64);
        assert_eq!(
            completed.execution.input.as_ref().unwrap().payload_batches,
            3
        );
        drop(completed);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);

        let mut failing_calls = 0;
        let mut failing = || {
            failing_calls += 1;
            if failing_calls == 1 {
                Ok(Some(batch(1, 4)))
            } else {
                Err(ShardLoomError::InvalidOperation(
                    "producer failed before end".into(),
                ))
            }
        };
        let error = prepared
            .with_arrow_batch_input(&mut failing)
            .unwrap()
            .execute_native(&CancellationToken::default())
            .err()
            .unwrap();
        assert!(error.to_string().contains("producer failed before end"));
        assert_eq!(failing_calls, 2);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
}

#[test]
fn arrow_input_schema_errors_cancellation_pressure_and_consumer_stop_release_all_credit() {
    let (prepared, session) = prepare(None, 2 << 20);
    let memory = session.shared_memory_pool();
    let baseline = memory.snapshot().reserved_bytes;
    for mode in ["schema", "pressure", "consumer", "cancel"] {
        let token = CancellationToken::default();
        let mut calls = 0;
        let mut input = || {
            calls += 1;
            assert_eq!(calls, 1);
            let next = match mode {
                "schema" => RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "different",
                        DataType::Int64,
                        true,
                    )])),
                    vec![Arc::new(Int64Array::from(vec![Some(1)]))],
                )
                .unwrap(),
                "pressure" => batch(0, 300_001),
                _ => batch(0, 4097),
            };
            Ok(Some(next))
        };
        let error = prepared
            .with_arrow_batch_input(&mut input)
            .unwrap()
            .for_each_arrow_batch(&token, |_| {
                if mode == "cancel" {
                    token.cancel();
                    Ok(())
                } else {
                    Err(ShardLoomError::InvalidOperation("consumer stopped".into()))
                }
            })
            .err()
            .unwrap();
        if mode == "schema" {
            assert!(error.to_string().contains("declared schema"));
        }
        assert_eq!(calls, 1);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let mut demanded = false;
    let mut no_demand = || {
        demanded = true;
        Ok(None)
    };
    assert!(
        prepared
            .with_arrow_batch_input(&mut no_demand)
            .unwrap()
            .execute_native(&cancelled)
            .is_err()
    );
    assert!(!demanded);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
}
