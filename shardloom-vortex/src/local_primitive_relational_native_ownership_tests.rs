use super::*;

#[cfg(all(feature = "vortex-write", unix))]
use crate::{
    relational_query::VortexRelationalLimit,
    resident_memory_source::{
        MemoryBatchSourceBuilder, MemoryColumn, MemoryColumnValues, ResidentMemorySource,
    },
    resident_session::{OwnedVortexResultBatch, ResidentVortexSession},
};

#[test]
fn native_result_retains_every_value_beyond_small_collection_limits() {
    let rows = 131_073_usize;
    let names = (0..9)
        .map(|column| format!("value_{column}"))
        .collect::<Vec<_>>();
    let input = StructArray::try_new(
        FieldNames::from(names.iter().map(String::as_str).collect::<Vec<_>>()),
        (0..9_i64)
            .map(|column| {
                PrimitiveArray::from_iter(
                    (0..rows).map(|row| i64::try_from(row).unwrap() * 17 - column),
                )
                .into_array()
            })
            .collect::<Vec<_>>(),
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::new(input, 2048);
    let mut allocation = policy();
    allocation.resource_envelope.memory_budget_bytes = 128 << 20;
    let prepared = prepare_relational(&fixture.scan(), allocation).unwrap();
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    assert!(prepared.execute_owned().is_err());
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(completed.result.row_count(), rows as u64);
    assert!(completed.result.logical_buffer_bytes() > 8 << 20);
    assert_eq!(completed.execution.output_rows, rows as u64);
    assert!(completed.execution.native_io_certificate.is_certified());
    assert!(
        memory.snapshot().peak_reserved_bytes <= allocation.resource_envelope.memory_budget_bytes
    );
    let mut context = completed.result.create_execution_ctx();
    let retained = completed.result.arrays().to_vec();
    drop((completed, prepared));
    assert!(memory.snapshot().reserved_bytes > 0);
    let mut start = 0;
    for batch in &retained {
        for (column, name) in names.iter().enumerate() {
            let values = crate::local_primitives::logical_field_from_native_array(batch, name)
                .unwrap()
                .execute::<PrimitiveArray>(&mut context)
                .unwrap();
            for (offset, value) in values.as_slice::<i64>().iter().enumerate() {
                assert_eq!(
                    *value,
                    i64::try_from(start + offset).unwrap() * 17 - i64::try_from(column).unwrap()
                );
            }
        }
        start += batch.len();
    }
    assert_eq!(start, rows);
    drop((retained, context));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn shared_frontend_preparation_cannot_duplicate_the_session_grant() {
    let allocation = policy();
    let session = ResidentVortexSession::new(
        allocation.resource_envelope.memory_budget_bytes,
        allocation.max_parallelism,
    )
    .unwrap();
    let fixture = Fixture::new(keyed(&[Some(1), None, Some(3)], &[5, 6, 7]), 2);
    let memory = session.memory().clone();
    let occupied = memory.reserve(memory.snapshot().limit_bytes).unwrap();
    let mut lowered = false;
    let denied = prepare_relational_with_schema_in_session(allocation, &session, |_| {
        lowered = true;
        Ok(fixture.scan())
    });
    assert!(denied.is_err());
    assert!(!lowered);
    assert_eq!(session.snapshot().prepared_source_opens, 0);
    drop(occupied);
    let prepared =
        prepare_relational_with_schema_in_session(allocation, &session, |_| Ok(fixture.scan()))
            .unwrap();
    assert!(prepared.session.same_owner(&session));
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(completed.result.row_count(), 3);
    assert_eq!(session.snapshot().completed_executions, 1);
    drop((completed, prepared, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_result_cancellation_returns_no_partial_owner_and_allows_retry() {
    let fixture = Fixture::new(keyed(&[Some(1), None], &[8, 9]), 1);
    let prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(prepared.execute_native(&cancelled).is_err());
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(completed.result.row_count(), 2);
    drop(completed);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_empty_result_keeps_authoritative_schema_after_preparation_drops() {
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
    let expected = prepared.output_dtype().unwrap();
    let result = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    drop(prepared);
    assert_eq!(result.result.row_count(), 0);
    assert_eq!(result.result.dtype(), &expected);
    assert_eq!(result.execution.output_batches, 1);
}

#[cfg(all(feature = "vortex-write", unix))]
fn nullable_u64_entity_fixture(rows: usize) -> Fixture {
    let entity = PrimitiveArray::from_option_iter(
        (0..rows).map(|row| (row % 7 != 0).then(|| u64::try_from(row).unwrap())),
    )
    .into_array();
    let amount =
        PrimitiveArray::from_iter((0..rows).map(|row| u32::try_from(row).unwrap())).into_array();
    let input = StructArray::try_new(
        FieldNames::from(["entity", "amount"]),
        vec![entity, amount],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    Fixture::new(input, 2048)
}

#[cfg(all(feature = "vortex-write", unix))]
fn memory_result_scan(uri: DatasetUri) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri,
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

#[cfg(all(feature = "vortex-write", unix))]
fn assert_native_entity_amount_values(result: &OwnedVortexResultBatch, expected_rows: usize) {
    let mut context = result.create_execution_ctx();
    let mut observed = 0_usize;
    for batch in result.arrays() {
        let entity = crate::local_primitives::logical_field_from_native_array(batch, "entity")
            .unwrap()
            .execute::<PrimitiveArray>(&mut context)
            .unwrap();
        let amount = crate::local_primitives::logical_field_from_native_array(batch, "amount")
            .unwrap()
            .execute::<PrimitiveArray>(&mut context)
            .unwrap();
        let entity_validity = entity.validity().unwrap();
        let entities = entity.as_slice::<u64>();
        let amounts = amount.as_slice::<u32>();
        assert_eq!(entities.len(), batch.len());
        assert_eq!(amounts.len(), batch.len());
        for row in 0..batch.len() {
            let global_row = observed + row;
            let expected_entity =
                (!global_row.is_multiple_of(7)).then_some(u64::try_from(global_row).unwrap());
            assert_eq!(
                entity_validity
                    .execute_is_valid(row, &mut context)
                    .unwrap()
                    .then_some(entities[row]),
                expected_entity,
                "entity row {global_row}"
            );
            assert_eq!(
                amounts[row],
                u32::try_from(global_row).unwrap(),
                "amount row {global_row}"
            );
        }
        observed += batch.len();
    }
    assert_eq!(observed, expected_rows);
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn completed_native_result_composes_after_original_owners_drop() {
    const ROWS: usize = 131_073;
    const LIMIT: usize = 65_537;
    let fixture = nullable_u64_entity_fixture(ROWS);
    let mut allocation = policy();
    allocation.resource_envelope.memory_budget_bytes = 128 << 20;
    let original_session = ResidentVortexSession::new(
        allocation.resource_envelope.memory_budget_bytes,
        allocation.max_parallelism,
    )
    .unwrap();
    let composition_session = original_session.clone();
    let memory = original_session.memory().clone();
    let prepared = prepare_relational_with_schema_in_session(allocation, &original_session, |_| {
        Ok(fixture.scan())
    })
    .unwrap();
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(completed.result.row_count(), ROWS as u64);
    let source = ResidentMemorySource::from_completed_result(
        &completed.result,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(source.row_count(), ROWS);
    let opens_before_composition = original_session.snapshot().prepared_source_opens;
    assert!(opens_before_composition > 0);
    let uri = DatasetUri::new("memory://completed-native-result").unwrap();
    let composed =
        prepare_relational_with_schema_in_session(allocation, &composition_session, |schemas| {
            schemas.register_memory_source(uri.clone(), |_| Ok(source.clone()))?;
            Ok(VortexRelationalPlan::Limit(Box::new(
                VortexRelationalLimit {
                    input: memory_result_scan(uri),
                    offset: 0,
                    count: LIMIT,
                },
            )))
        })
        .unwrap();
    drop((completed, prepared, source, original_session));
    let result = composed
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(result.result.row_count(), LIMIT as u64);
    assert_native_entity_amount_values(&result.result, LIMIT);
    assert_eq!(
        composition_session.snapshot().prepared_source_opens,
        opens_before_composition
    );
    drop((result, composed, composition_session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn native_result_from_foreign_session_is_rejected_without_poisoning_builder() {
    const ROWS: usize = 131_073;
    let fixture = nullable_u64_entity_fixture(ROWS);
    let mut allocation = policy();
    allocation.resource_envelope.memory_budget_bytes = 128 << 20;
    let foreign_session = ResidentVortexSession::new(
        allocation.resource_envelope.memory_budget_bytes,
        allocation.max_parallelism,
    )
    .unwrap();
    let foreign_prepared =
        prepare_relational_with_schema_in_session(allocation, &foreign_session, |_| {
            Ok(fixture.scan())
        })
        .unwrap();
    let foreign_result = foreign_prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    let foreign_memory = foreign_session.memory().clone();
    let foreign_reserved = foreign_memory.snapshot().reserved_bytes;

    let builder_session = ResidentVortexSession::new(
        allocation.resource_envelope.memory_budget_bytes,
        allocation.max_parallelism,
    )
    .unwrap();
    let mut builder =
        MemoryBatchSourceBuilder::new(&builder_session, CancellationToken::default()).unwrap();
    let builder_memory = builder_session.memory().clone();
    let builder_reserved = builder_memory.snapshot().reserved_bytes;
    let rejected = builder.push_native_result(&foreign_result.result);
    assert!(rejected.is_err());
    assert_eq!(foreign_memory.snapshot().reserved_bytes, foreign_reserved);
    assert_eq!(builder_memory.snapshot().reserved_bytes, builder_reserved);

    builder
        .push_columns(&[MemoryColumn {
            name: "entity",
            values: MemoryColumnValues::Int64NonNullable(&[1, 2, 3]),
        }])
        .unwrap();
    let reused = builder.finish().unwrap();
    assert_eq!(reused.row_count(), 3);
    drop((
        reused,
        foreign_result,
        foreign_prepared,
        foreign_session,
        builder_session,
    ));
    assert_eq!(foreign_memory.snapshot().reserved_bytes, 0);
    assert_eq!(builder_memory.snapshot().reserved_bytes, 0);
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn empty_native_result_composes_with_authoritative_nullable_schema() {
    let fixture = nullable_u64_entity_fixture(0);
    let mut allocation = policy();
    allocation.resource_envelope.memory_budget_bytes = 32 << 20;
    let original_session = ResidentVortexSession::new(
        allocation.resource_envelope.memory_budget_bytes,
        allocation.max_parallelism,
    )
    .unwrap();
    let composition_session = original_session.clone();
    let memory = original_session.memory().clone();
    let prepared = prepare_relational_with_schema_in_session(allocation, &original_session, |_| {
        Ok(fixture.scan())
    })
    .unwrap();
    let expected = prepared.output_dtype().unwrap();
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(completed.result.row_count(), 0);
    let source = ResidentMemorySource::from_completed_result(
        &completed.result,
        &CancellationToken::default(),
    )
    .unwrap();
    let uri = DatasetUri::new("memory://empty-completed-native-result").unwrap();
    let composed =
        prepare_relational_with_schema_in_session(allocation, &composition_session, |schemas| {
            schemas.register_memory_source(uri.clone(), |_| Ok(source.clone()))?;
            Ok(memory_result_scan(uri))
        })
        .unwrap();
    drop((completed, prepared, source, original_session));
    let result = composed
        .execute_native(&CancellationToken::default())
        .unwrap();
    let expected_nullable_schema = DType::struct_(
        [
            (
                "entity",
                DType::Primitive(PType::U64, Nullability::Nullable),
            ),
            (
                "amount",
                DType::Primitive(PType::U32, Nullability::NonNullable),
            ),
        ],
        Nullability::NonNullable,
    );
    assert_eq!(result.result.row_count(), 0);
    assert_eq!(result.result.dtype(), &expected);
    assert_eq!(result.result.dtype(), &expected_nullable_schema);
    drop((result, composed, composition_session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
