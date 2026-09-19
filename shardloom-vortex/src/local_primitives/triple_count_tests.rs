//! Complete triple keys must survive chunk boundaries and worker failures.

use super::{
    GroupedAggregateStates, VortexLocalPrimitiveExecutionPolicy,
    aggregate_count_workers::CountWorkers, shardloom_extract_minute_derived_column,
};
use crate::{
    VortexAggregateExpression, VortexAggregateOrderExpr, VortexAggregateSpillPolicy,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
};
use shardloom_core::ColumnRef;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::sync::Arc;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray},
        dtype::FieldNames,
        memory::MemorySessionExt as _,
        validity::Validity,
    },
    session::VortexSession,
};

fn columns(prepared: bool) -> Vec<String> {
    vec![
        "subject_code".into(),
        "phrase_label".into(),
        if prepared {
            shardloom_extract_minute_derived_column("event_seconds")
        } else {
            "event_seconds".into()
        },
    ]
}

#[test]
fn triple_lowering_preserves_nonnullable_raw_minutes_when_prepared_dtype_is_nullable() {
    use super::{
        aggregate_count_workers::restore_provider_drivers, aggregate_lowering::AggregateLowering,
    };
    use crate::VortexQueryPrimitiveRequest;
    use shardloom_core::{ComparisonOp, DatasetUri, PredicateExpr, StatValue};
    let aggregate = request(false);
    let query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("renamed.vortex").unwrap(),
        aggregate.clone(),
    )
    .with_source_order_limit(2);
    let mut names = columns(false);
    let hidden = shardloom_extract_minute_derived_column("event_seconds");
    names.push(hidden.clone());
    // A nullable physical field is not a nonnull proof, even when a source
    // expression is total. The raw source remains the semantic authority.
    let chunk = StructArray::try_new(
        names.into_iter().collect::<FieldNames>(),
        vec![
            integers(&[7, 7]),
            dictionary(&[0, 0], &["x"]),
            integers(&[-1, 61]),
            PrimitiveArray::new(vec![0_u8, 0], Validity::AllInvalid).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let lowering = AggregateLowering::new(&query, chunk.dtype()).unwrap();
    assert!(lowering.rewrite.rewritten_columns.is_empty());
    assert_eq!(
        lowering.rewrite.aggregate.group_expressions[0].function,
        "extract_minute"
    );
    assert!(!restore_provider_drivers(&query, chunk.dtype()));
    let expected = serial_values(&aggregate, std::slice::from_ref(&chunk), false, 2);
    assert_eq!(
        worker_values(
            &lowering.rewrite.aggregate,
            std::slice::from_ref(&chunk),
            false,
            2,
            3
        ),
        expected
    );

    // An explicit nullable prepared-key request is still declined.
    let prepared_query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("renamed.vortex").unwrap(),
        request(true),
    )
    .with_source_order_limit(2);
    assert!(restore_provider_drivers(&prepared_query, chunk.dtype()));

    // The shortcut must not bypass existing predicate rewrites.
    let mut filtered = query;
    filtered.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("subject_code").unwrap(),
        op: ComparisonOp::Gt,
        value: StatValue::Int64(0),
    });
    assert!(!super::triple_count_workers::preserve_raw_minute_input(
        &filtered,
        chunk.dtype()
    ));
    assert_eq!(
        AggregateLowering::new(&filtered, chunk.dtype())
            .unwrap()
            .rewrite
            .rewritten_columns,
        vec![hidden]
    );
}

fn request(prepared: bool) -> VortexSimpleAggregateRequest {
    let columns = columns(prepared);
    let mut request = VortexSimpleAggregateRequest::grouped(
        columns[..if prepared { 3 } else { 2 }]
            .iter()
            .map(|name| ColumnRef::new(name).unwrap())
            .collect(),
        vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "frequency".into(),
        )],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("frequency", true)]);
    if !prepared {
        request = request.with_group_expressions(vec![VortexAggregateExpression::new(
            "minute_slot".into(),
            ColumnRef::new("event_seconds").unwrap(),
            "extract_minute",
        )]);
    }
    request
}

fn integers(values: &[i64]) -> ArrayRef {
    PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array()
}

fn dictionary(codes: &[u8], values: &[&str]) -> ArrayRef {
    DictArray::try_new(
        PrimitiveArray::new(codes.to_vec(), Validity::NonNullable).into_array(),
        VarBinViewArray::from_iter_str(values.iter().copied()).into_array(),
    )
    .unwrap()
    .into_array()
}

fn chunk(prepared: bool, numbers: ArrayRef, text: ArrayRef, minutes: ArrayRef) -> ArrayRef {
    let rows = numbers.len();
    StructArray::try_new(
        columns(prepared).into_iter().collect::<FieldNames>(),
        vec![numbers, text, minutes],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn values(states: &GroupedAggregateStates<'_>, limit: usize) -> serde_json::Value {
    let (_, summary) = states.result_row_count_and_summary(Some(limit)).unwrap();
    serde_json::from_str::<serde_json::Value>(&summary).unwrap()["values"].clone()
}

fn serial_values(
    request: &VortexSimpleAggregateRequest,
    chunks: &[ArrayRef],
    prepared: bool,
    limit: usize,
) -> serde_json::Value {
    let columns = columns(prepared);
    let mut states =
        GroupedAggregateStates::new(request, Some(limit), &columns, false, false).unwrap();
    for chunk in chunks {
        assert!(
            states
                .update_compact_direct_from_chunk(chunk, &columns, None)
                .unwrap()
        );
    }
    values(&states, limit)
}

fn worker_values(
    request: &VortexSimpleAggregateRequest,
    chunks: &[ArrayRef],
    prepared: bool,
    limit: usize,
    parallelism: usize,
) -> serde_json::Value {
    let columns = columns(prepared);
    let mut states =
        GroupedAggregateStates::new(request, Some(limit), &columns, false, false).unwrap();
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let session = VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    let mut workers = CountWorkers::admit(
        &states,
        chunks[0].dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
        &session,
        &memory,
    )
    .unwrap()
    .expect("complete triple-key workers admitted");
    assert!(matches!(&workers, CountWorkers::Triple(_)));
    for chunk in chunks {
        workers.before_next(&mut states).unwrap();
        assert!(workers.submit(chunk, &mut states).unwrap());
    }
    workers.finish(&mut states).unwrap();
    let (_, mut summary) = states.result_row_count_and_summary(Some(limit)).unwrap();
    workers.annotate_summary(&mut summary).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    let result = summary["values"].clone();
    assert_eq!(
        summary["aggregate_workers_family"],
        "complete_numeric_minute_string_partitions"
    );
    assert_eq!(
        summary["aggregate_workers_submitted_chunks"],
        summary["aggregate_workers_joined_chunks"]
    );
    assert_eq!(
        summary["aggregate_workers_triple_rows"],
        chunks.iter().map(|chunk| chunk.len() as u64).sum::<u64>()
    );
    assert!(memory.snapshot().peak_reserved_bytes > 0);
    assert!(memory.snapshot().peak_reserved_bytes <= memory.snapshot().limit_bytes);
    drop((workers, states, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    result
}

#[test]
fn triple_workers_reconcile_changed_dictionary_domains_and_cross_chunk_keys() {
    let chunks = [
        chunk(
            false,
            integers(&[7, 7, -2, 7]),
            dictionary(&[0, 1, 2, 3], &["same", "東京", "", "same", "unused"]),
            integers(&[60, 60, -1, 61]),
        ),
        chunk(
            false,
            integers(&[-2, 7, 7, 7]),
            dictionary(&[2, 1, 0, 1], &["東京", "same", "", "unused"]),
            integers(&[-60, 62, 61, 63]),
        ),
    ];
    let request = request(false);
    let expected = serial_values(&request, &chunks, false, 8);
    assert_eq!(
        expected,
        serde_json::json!([
            {"subject_code":7,"phrase_label":"same","minute_slot":1,"frequency":4},
            {"subject_code":-2,"phrase_label":"","minute_slot":59,"frequency":2},
            {"subject_code":7,"phrase_label":"東京","minute_slot":1,"frequency":2}
        ])
    );
    for parallelism in [1, 3] {
        assert_eq!(
            worker_values(&request, &chunks, false, 8, parallelism),
            expected
        );
    }
}

#[test]
fn triple_workers_retain_a_global_winner_absent_from_every_chunk_top_one() {
    let chunks = (0_i64..3)
        .map(|local| {
            chunk(
                false,
                integers(&[vec![local; 6], vec![99; 5]].concat()),
                dictionary(
                    &[vec![u8::from(local != 1); 6], vec![u8::from(local == 1); 5]].concat(),
                    if local == 1 {
                        &["local", "shared"]
                    } else {
                        &["shared", "local"]
                    },
                ),
                integers(&[60; 11]),
            )
        })
        .collect::<Vec<_>>();
    let request = request(false);
    for chunk in &chunks {
        let local = serial_values(&request, std::slice::from_ref(chunk), false, 1);
        assert_eq!(local[0]["frequency"], 6);
        assert_eq!(local[0]["phrase_label"], "local");
    }
    let expected = serial_values(&request, &chunks, false, 1);
    assert_eq!(expected[0]["subject_code"], 99);
    assert_eq!(expected[0]["frequency"], 15);
    assert_eq!(worker_values(&request, &chunks, false, 1, 3), expected);
}

#[test]
fn triple_workers_preserve_signed_extrema_and_negative_raw_timestamp_minutes() {
    let chunks = [chunk(
        false,
        integers(&[i64::MIN, i64::MAX, i64::MIN, i64::MAX, 0]),
        dictionary(&[0, 0, 0, 0, 1], &["edge", "origin"]),
        integers(&[-1, -3601, -60, -3660, -3600]),
    )];
    let request = request(false);
    let expected = serial_values(&request, &chunks, false, 3);
    assert_eq!(
        expected,
        serde_json::json!([
            {"subject_code":i64::MIN,"phrase_label":"edge","minute_slot":59,"frequency":2},
            {"subject_code":i64::MAX,"phrase_label":"edge","minute_slot":59,"frequency":2},
            {"subject_code":0,"phrase_label":"origin","minute_slot":0,"frequency":1}
        ])
    );
    assert_eq!(worker_values(&request, &chunks, false, 3, 3), expected);
}

#[test]
fn triple_workers_preserve_prepared_native_u8_minutes_without_extracting_twice() {
    let chunks = [chunk(
        true,
        integers(&[7, 7, -2, 7]),
        dictionary(&[0, 0, 1, 0], &["same", "other"]),
        PrimitiveArray::new(vec![59_u8, 59, 0, 59], Validity::NonNullable).into_array(),
    )];
    let request = request(true);
    let expected = serial_values(&request, &chunks, true, 2);
    let minute = shardloom_extract_minute_derived_column("event_seconds");
    assert_eq!(expected[0]["frequency"], 3);
    assert_eq!(expected[0][&minute], 59);
    assert_eq!(expected[1][&minute], 0);
    assert_eq!(worker_values(&request, &chunks, true, 2, 3), expected);
}

#[test]
fn triple_workers_preserve_full_tie_order_before_offset_and_limit() {
    let chunks = [chunk(
        false,
        integers(&[7, -2, -2, -2, -2]),
        dictionary(&[0, 2, 1, 0, 3], &["a", "z", "a", "last"]),
        integers(&[0, 60, 0, 0, 120]),
    )];
    let request = request(false).with_offset(1);
    let expected = serial_values(&request, &chunks, false, 2);
    assert_eq!(
        expected,
        serde_json::json!([
            {"subject_code":-2,"phrase_label":"z","minute_slot":0,"frequency":1},
            {"subject_code":-2,"phrase_label":"a","minute_slot":1,"frequency":1}
        ])
    );
    assert_eq!(worker_values(&request, &chunks, false, 2, 3), expected);
}

#[test]
fn triple_workers_accept_empty_chunks_without_creating_groups() {
    let empty = chunk(
        false,
        integers(&[]),
        dictionary(&[], &["unused"]),
        integers(&[]),
    );
    let request = request(false);
    assert_eq!(
        worker_values(&request, std::slice::from_ref(&empty), false, 2, 3),
        serde_json::json!([])
    );
    let chunks = [
        empty.clone(),
        chunk(
            false,
            integers(&[4]),
            dictionary(&[0], &["kept"]),
            integers(&[-1]),
        ),
        empty,
    ];
    assert_eq!(
        worker_values(&request, &chunks, false, 2, 3),
        serial_values(&request, &chunks, false, 2)
    );
}

#[test]
fn triple_workers_bound_large_result_windows_by_actual_groups() {
    let empty = chunk(
        false,
        integers(&[]),
        dictionary(&[], &["unused"]),
        integers(&[]),
    );
    let tiny = chunk(
        false,
        integers(&[7, 7]),
        dictionary(&[0, 0], &["only"]),
        integers(&[1, 1]),
    );
    for (chunk, offset, limit) in [
        (empty, 0, 1 << 30),
        (tiny.clone(), 0, 1 << 30),
        (tiny, 1 << 30, 1),
    ] {
        let request = request(false).with_offset(offset);
        let chunks = std::slice::from_ref(&chunk);
        let expected = serial_values(&request, chunks, false, limit);
        assert_eq!(
            expected.as_array().unwrap().len(),
            usize::from(!chunk.is_empty() && offset == 0)
        );
        // The worker helper has only a 16 MiB pool: reserving the requested
        // billion-entry window instead of the actual group count must fail.
        assert_eq!(worker_values(&request, chunks, false, limit, 3), expected);
    }
}

#[test]
fn triple_workers_decline_nullable_roles_and_explicit_spill() {
    let columns = columns(false);
    let request = request(false);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = VortexSession::default();
    for arrays in [
        [
            PrimitiveArray::new(vec![1_i64], Validity::AllValid).into_array(),
            dictionary(&[0], &["x"]),
            integers(&[0]),
        ],
        [
            integers(&[1]),
            VarBinViewArray::from_iter_nullable_str([Some("x")]).into_array(),
            integers(&[0]),
        ],
        [
            integers(&[1]),
            dictionary(&[0], &["x"]),
            PrimitiveArray::new(vec![0_i64], Validity::AllValid).into_array(),
        ],
    ] {
        let [numeric, text, minute] = arrays;
        let chunk = chunk(false, numeric, text, minute);
        let states =
            GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
        assert!(
            CountWorkers::admit(
                &states,
                chunk.dtype(),
                &columns,
                VortexLocalPrimitiveExecutionPolicy::new(3).unwrap(),
                &session,
                &memory,
            )
            .unwrap()
            .is_none()
        );
    }
    let request = request.with_spill(
        VortexAggregateSpillPolicy::new(
            std::env::temp_dir().join("shardloom-triple-admission-only"),
            8 << 20,
            4 << 20,
        )
        .unwrap(),
    );
    let chunk = chunk(
        false,
        integers(&[1]),
        dictionary(&[0], &["x"]),
        integers(&[0]),
    );
    let states = GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
    assert!(
        CountWorkers::admit(
            &states,
            chunk.dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(3).unwrap(),
            &session,
            &memory,
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn triple_workers_reject_invalid_prepared_minutes_in_a_losing_group() {
    let columns = columns(true);
    let request = request(true);
    let chunks = [
        chunk(
            true,
            integers(&[1, 1, 1]),
            dictionary(&[0, 0, 0], &["winner"]),
            integers(&[4, 4, 4]),
        ),
        chunk(
            true,
            integers(&[99]),
            dictionary(&[0], &["loser"]),
            integers(&[60]),
        ),
    ];
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let session = VortexSession::default();
    let mut states =
        GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
    let mut workers = CountWorkers::admit(
        &states,
        chunks[0].dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
        &session,
        &memory,
    )
    .unwrap()
    .expect("prepared triple admitted");
    assert!(matches!(&workers, CountWorkers::Triple(_)));
    assert!(workers.submit(&chunks[0], &mut states).unwrap());
    let error = workers
        .submit(&chunks[1], &mut states)
        .expect_err("invalid prepared minute must fail");
    assert!(error.to_string().contains("minute"), "{error}");
    assert!(
        states.numeric_minute_string_count_groups.is_none(),
        "failed workers must not install a partial result"
    );
    drop((workers, states, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn triple_worker_source_precheck_admits_raw_and_prepared_renamed_roles() {
    use super::aggregate_count_workers::{request_may_be_admitted, restore_provider_drivers};
    use crate::VortexQueryPrimitiveRequest;
    use shardloom_core::DatasetUri;

    for prepared in [false, true] {
        let chunk = chunk(
            prepared,
            integers(&[7]),
            dictionary(&[0], &["x"]),
            integers(&[1]),
        );
        let query = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new("renamed-triples.vortex").unwrap(),
            request(prepared),
        )
        .with_source_order_limit(1);
        assert!(request_may_be_admitted(&query), "prepared={prepared}");
        assert!(
            !restore_provider_drivers(&query, chunk.dtype()),
            "prepared={prepared}"
        );
    }
}

#[test]
fn triple_worker_cancellation_after_committed_updates_releases_every_lease() {
    let columns = columns(false);
    let request = request(false);
    let chunk = chunk(
        false,
        integers(&[7, 7]),
        dictionary(&[0, 0], &["x"]),
        integers(&[1, 1]),
    );
    for parallelism in [1, 3] {
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let session = VortexSession::default();
        let mut states =
            GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
        let mut workers = CountWorkers::admit(
            &states,
            chunk.dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
            &session,
            &memory,
        )
        .unwrap()
        .unwrap();
        assert!(workers.submit(&chunk, &mut states).unwrap());
        let CountWorkers::Triple(triple) = &mut workers else {
            panic!("triple workers required");
        };
        triple.drain(&mut states).unwrap();
        assert!(triple.has_committed_groups());
        assert!(workers.submit(&chunk, &mut states).unwrap());
        let CountWorkers::Triple(triple) = &workers else {
            panic!("triple workers required");
        };
        // Cancellation must reject final publication even if the pending
        // receipt completed before cancellation; no scheduling race is assumed.
        assert!(memory.snapshot().reserved_bytes > 0);
        triple.cancel_for_test();
        let error = workers
            .finish(&mut states)
            .expect_err("cancelled work must not finalize");
        assert!(error.to_string().contains("cancel"), "{error}");
        assert!(states.numeric_minute_string_count_groups.is_none());
        drop((workers, states, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn triple_worker_buffer_pressure_after_committed_updates_fails_without_serial_replay() {
    let columns = columns(false);
    let request = request(false);
    let chunk = chunk(
        false,
        integers(&[7, 7]),
        dictionary(&[0, 0], &["x"]),
        integers(&[1, 1]),
    );
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let session = VortexSession::default();
    let mut states =
        GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
    let mut workers = CountWorkers::admit(
        &states,
        chunk.dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
        &session,
        &memory,
    )
    .unwrap()
    .unwrap();
    assert!(workers.submit(&chunk, &mut states).unwrap());
    let CountWorkers::Triple(triple) = &workers else {
        panic!("triple workers required");
    };
    assert!(triple.has_committed_groups());
    let snapshot = memory.snapshot();
    let pressure = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
        .unwrap();
    let error = workers
        .submit(&chunk, &mut states)
        .expect_err("capacity denial must not request serial replay");
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert!(memory.snapshot().denied_reservations > 0);
    assert!(states.numeric_minute_string_count_groups.is_none());
    drop((workers, states, session, pressure));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn triple_worker_state_growth_denial_after_committed_updates_is_fatal_and_releases_leases() {
    let columns = columns(false);
    let request = request(false);
    let first = chunk(
        false,
        integers(&[7, 7]),
        dictionary(&[0, 0], &["x"]),
        integers(&[1, 1]),
    );
    let growing = chunk(
        false,
        integers(&(100_i64..228).collect::<Vec<_>>()),
        dictionary(&[0; 128], &["x"]),
        integers(&[1; 128]),
    );
    for parallelism in [1, 3] {
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let session = VortexSession::default();
        let mut states =
            GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
        let mut workers = CountWorkers::admit(
            &states,
            first.dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
            &session,
            &memory,
        )
        .unwrap()
        .unwrap();
        assert!(workers.submit(&first, &mut states).unwrap());
        let CountWorkers::Triple(triple) = &mut workers else {
            panic!("triple workers required");
        };
        triple.drain(&mut states).unwrap();
        assert!(triple.has_committed_groups());
        assert_eq!(memory.snapshot().denied_reservations, 0);
        // Fault is consumed only when a persistent partition needs to grow.
        // Buffer reservation must succeed before this failure is exercised.
        triple.deny_next_state_growth_for_test();
        let error = match workers.submit(&growing, &mut states) {
            Err(error) => error,
            Ok(true) => workers
                .finish(&mut states)
                .expect_err("worker state growth denial must fail completion"),
            Ok(false) => panic!("committed triple state must not replay into serial state"),
        };
        assert!(
            error.to_string().contains("memory reservation denied"),
            "{error}"
        );
        assert!(memory.snapshot().denied_reservations > 0);
        assert!(states.numeric_minute_string_count_groups.is_none());
        drop((workers, states, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
