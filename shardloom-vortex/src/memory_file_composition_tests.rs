//! Complete native composition values, ownership and admission acceptance.

use super::*;
use crate::{
    VortexAggregateOrderExpr, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveRowExportFormat, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest,
    local_primitives::prepared_aggregate::{ExecutedVortexAggregate, prepare_memory_aggregate},
    memory_file_generation::{MemoryFileCompositionBounds, MemoryFileGeneration},
};
use serde_json::{Value, json};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use vortex::{
    array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::{Nullability, PType, StructFields},
        validity::Validity,
    },
    buffer::{Alignment, Buffer},
};

fn schema() -> DType {
    DType::Struct(
        StructFields::new(
            ["key"].into(),
            vec![DType::Primitive(PType::U64, Nullability::NonNullable)],
        ),
        Nullability::NonNullable,
    )
}

fn owned_keys(
    session: &ResidentVortexSession,
    batches: usize,
    rows: usize,
) -> OwnedVortexResultBatch {
    let lease = session
        .memory()
        .reserve((batches * std::mem::size_of::<ArrayRef>()) as u64)
        .unwrap();
    let mut arrays = Vec::with_capacity(batches);
    assert_eq!(arrays.capacity(), batches);
    for batch in 0..batches {
        let mut values = session
            .native_allocator()
            .allocate(rows * 8, Alignment::new(8))
            .unwrap();
        for (row, bytes) in values.as_mut_slice().chunks_exact_mut(8).enumerate() {
            let key = u64::MAX - ((batch * rows + row) % 3) as u64;
            bytes.copy_from_slice(&key.to_ne_bytes());
        }
        let keys = PrimitiveArray::new(
            Buffer::<u64>::from_byte_buffer(values.freeze()),
            Validity::NonNullable,
        )
        .into_array();
        arrays.push(
            StructArray::try_new(["key"].into(), vec![keys], rows, Validity::NonNullable)
                .unwrap()
                .into_array(),
        );
    }
    OwnedVortexResultBatch {
        dtype: schema(),
        arrays: Budgeted::new(arrays, lease),
        runtime: Arc::clone(&session.0),
        rows: (batches * rows) as u64,
        logical_buffer_bytes: (batches * rows * 8) as u64,
    }
}

fn request(generation: &MemoryFileGeneration, grouped: bool) -> VortexQueryPrimitiveRequest {
    let measures = vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())];
    let aggregate = if grouped {
        VortexSimpleAggregateRequest::grouped(vec![ColumnRef::new("key").unwrap()], measures)
            .with_order_by(vec![
                VortexAggregateOrderExpr::new("n", true),
                VortexAggregateOrderExpr::new("key", false),
            ])
    } else {
        VortexSimpleAggregateRequest::new(measures)
    };
    let request =
        VortexQueryPrimitiveRequest::simple_aggregate(generation.source_uri().clone(), aggregate);
    if grouped {
        request.with_source_order_limit(8)
    } else {
        request
    }
}

fn values(executed: &ExecutedVortexAggregate) -> Value {
    let summary = executed.report.result_summary.as_deref().unwrap();
    serde_json::from_str::<Value>(summary.rsplit_once(" values=").unwrap().1).unwrap()["values"]
        .clone()
}

fn rejected<T>(result: Result<T>, reason: &str) {
    let error = result.err().expect("operation must reject").to_string();
    assert!(error.contains(reason), "expected {reason}: {error}");
}

fn assert_memory_provenance(executed: &ExecutedVortexAggregate, uri: &DatasetUri) {
    let certificate = &executed.native_io_certificate;
    assert!(certificate.is_certified());
    assert!(!certificate.fallback_attempted && !executed.report.has_errors());
    assert_eq!(executed.runtime.prepared_source_opens, 0);
    assert_eq!(
        certificate.source_capability_report.source_kind,
        "immutable_vortex_file_segments"
    );
    assert_eq!(
        certificate.source_capability_report.adapter_id,
        "shardloom.resident_vortex.memory_file.v1"
    );
    assert_eq!(
        certificate.source_capability_report.schema_discovery_status,
        "validated_immutable_native_footer"
    );
    let proof = &certificate.source_pushdown_report.proof_basis;
    for marker in [
        "immutable_generation_owner_retained=true",
        "source_specific_file_opens=0",
        "construction_excluded_from_query_work=true",
        "construction_native_materialization_calls=",
        "construction_native_materialization_rows=",
        "no_zero_copy_composition_claim=true",
    ] {
        assert!(proof.contains(marker), "missing {marker}: {proof}");
    }
    assert!(proof.contains(&format!("memory_generation_uri={}", uri.as_str())));
}

#[test]
fn composition_multiple_owned_batches_feed_repeated_exact_native_aggregation() {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let input = owned_keys(&session, 3, 32_768);
    assert_eq!(input.row_count(), 98_304);
    let generation = MemoryFileGeneration::from_owned(
        input,
        MemoryFileCompositionBounds::default(),
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(generation.row_count(), 98_304);
    assert_eq!(generation.dtype(), &schema());
    assert_eq!(generation.evidence().source_file_opens, 0);
    assert_eq!(session.snapshot().completed_executions, 0);
    let uri = generation.source_uri().clone();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let good = request(&generation, true);
    let mut mismatched = good.clone();
    mismatched.source_uri = Some(DatasetUri::new("memory://wrong/0.vortex").unwrap());
    assert!(generation.prepare_aggregate(&mismatched, policy).is_err());
    assert_eq!(generation.evidence().memory_segment_requests, 0);
    let prepared = generation.prepare_aggregate(&good, policy).unwrap();
    drop(generation);
    drop(session);
    for completed in 1..=3 {
        let executed = prepared.execute().unwrap();
        assert_eq!(
            values(&executed),
            json!([
                {"key": u64::MAX - 2, "n": 32_768},
                {"key": u64::MAX - 1, "n": 32_768},
                {"key": u64::MAX, "n": 32_768},
            ])
        );
        assert_memory_provenance(&executed, &uri);
        assert_eq!(executed.runtime.completed_executions, completed);
    }
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

struct OutputDirectory(std::path::PathBuf);
impl OutputDirectory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-composition-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for OutputDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn composition_typed_empty_result_aggregates_and_native_sink_preserve_schema() {
    let output = OutputDirectory::new();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let input = owned_keys(&session, 0, 0);
    assert!(input.arrays().is_empty());
    let generation = MemoryFileGeneration::from_owned(
        input,
        MemoryFileCompositionBounds::default(),
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(generation.dtype(), &schema());
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let scalar = generation
        .prepare_aggregate(&request(&generation, false), policy)
        .unwrap();
    let executed = scalar.execute().unwrap();
    assert_eq!(values(&executed), json!({"n": 0}));
    assert_memory_provenance(&executed, generation.source_uri());
    let grouped = generation
        .prepare_aggregate(&request(&generation, true), policy)
        .unwrap();
    let completed = grouped.execute_owned().unwrap();
    assert_eq!(completed.result.row_count(), 0);
    assert_eq!(
        completed
            .result
            .dtype()
            .as_struct_fields_opt()
            .unwrap()
            .field("key"),
        Some(DType::Primitive(PType::U64, Nullability::NonNullable))
    );
    let dtype = completed.result.dtype().clone();
    let target = output.0.join("empty.vortex");
    completed
        .write(&target, VortexLocalPrimitiveRowExportFormat::Vortex, false)
        .unwrap();
    let reader = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let reopened = reader.prepare_file(&target).unwrap();
    assert_eq!(reopened.dtype(), &dtype);
    assert_eq!(reopened.prepare_count().execute().unwrap(), 0);
    #[cfg(feature = "universal-format-io")]
    {
        let target = output.0.join("empty.arrow");
        grouped
            .execute_owned()
            .unwrap()
            .write(
                &target,
                VortexLocalPrimitiveRowExportFormat::ArrowIpc,
                false,
            )
            .unwrap();
        let reader =
            arrow_ipc::reader::FileReader::try_new(std::fs::File::open(target).unwrap(), None)
                .unwrap();
        assert_eq!(
            reader.schema().field_with_name("key").unwrap().data_type(),
            &arrow_schema::DataType::UInt64
        );
        assert_eq!(
            reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
            0
        );
    }
    drop(scalar);
    drop(grouped);
    drop(generation);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the one-grant lifecycle and its rejection proofs together.
fn composition_p1_borrows_one_context_and_rejects_foreign_or_metadata_grants() {
    let session =
        ResidentVortexSession::with_serving_policy(16 << 20, 1, ResidentServingPolicy::default())
            .unwrap();
    let memory = session.memory().clone();
    let seed = MemoryFileGeneration::from_owned(
        owned_keys(&session, 1, 1),
        MemoryFileCompositionBounds::default(),
        &CancellationToken::default(),
    )
    .unwrap();
    let source = seed.retained_source();
    let input = owned_keys(&session, 3, 4);
    let before = session.admission_snapshot().unwrap().admitted_calls;
    let generation = source
        .with_native_execution_controlled(&CancellationToken::default(), |_, context| {
            let generation = MemoryFileGeneration::from_owned_in_context(
                &input,
                MemoryFileCompositionBounds::default(),
                context,
            )?;
            let mut filtered = request(&generation, true);
            filtered.predicate = Some(PredicateExpr::And(vec![
                PredicateExpr::Compare {
                    column: ColumnRef::new("key").unwrap(),
                    op: ComparisonOp::GtEq,
                    value: StatValue::UInt64(u64::MAX - 2),
                },
                PredicateExpr::Compare {
                    column: ColumnRef::new("key").unwrap(),
                    op: ComparisonOp::LtEq,
                    value: StatValue::UInt64(u64::MAX - 1),
                },
            ]));
            let prepared = prepare_memory_aggregate(
                &filtered,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
                &generation,
                Some(context),
            )?;
            let executed = prepared.execute_in_context(context)?;
            assert_eq!(
                values(&executed),
                json!([{"key": u64::MAX - 2, "n": 4}, {"key": u64::MAX - 1, "n": 4}])
            );
            assert_memory_provenance(&executed, generation.source_uri());
            assert_eq!(executed.runtime.completed_executions, 0);
            Ok(generation)
        })
        .unwrap();
    assert_eq!(session.snapshot().completed_executions, 1);
    assert_eq!(
        session.admission_snapshot().unwrap().admitted_calls,
        before + 1
    );
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let request = request(&generation, true);
    let prepared = generation.prepare_aggregate(&request, policy).unwrap();
    let reads = generation.evidence().memory_segment_requests;
    let other = ResidentVortexSession::new(16 << 20, 1).unwrap();
    other
        .with_native_execution_context(&CancellationToken::default(), |context| {
            rejected(
                MemoryFileGeneration::from_owned_in_context(
                    &input,
                    MemoryFileCompositionBounds::default(),
                    context,
                ),
                "different session",
            );
            rejected(
                prepare_memory_aggregate(&request, policy, &generation, Some(context)),
                "different session",
            );
            rejected(prepared.execute_in_context(context), "different session");
            Ok(())
        })
        .unwrap();
    {
        let metadata = session
            .0
            .enter(CallClass::Metadata, CancellationToken::default())
            .unwrap();
        rejected(
            MemoryFileGeneration::from_owned_in_context(
                &input,
                MemoryFileCompositionBounds::default(),
                &metadata,
            ),
            "requires a general operation grant",
        );
        rejected(
            prepared.execute_in_context(&metadata),
            "requires a general operation grant",
        );
    }
    assert_eq!(generation.evidence().memory_segment_requests, reads);
    assert_eq!(session.snapshot().completed_executions, 1);
    assert_eq!(other.snapshot().completed_executions, 0);
    drop(prepared);
    drop(generation);
    drop(input);
    drop(source);
    drop(seed);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn composition_caps_and_precancellation_release_consumed_owned_inputs() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    for kind in 0..5 {
        let mut bounds = MemoryFileCompositionBounds::default();
        match kind {
            0 => bounds.max_rows = 15,
            1 => bounds.max_batches = 1,
            2 => bounds.max_columns = 0,
            3 => bounds.storage.max_metadata_bytes = 1,
            _ => bounds.storage.max_serialized_bytes = 1,
        }
        assert!(
            MemoryFileGeneration::from_owned(
                owned_keys(&session, 2, 8),
                bounds,
                &CancellationToken::default()
            )
            .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(session.snapshot().completed_executions, 0);
        assert_eq!(session.snapshot().prepared_source_opens, 0);
    }
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    rejected(
        MemoryFileGeneration::from_owned(
            owned_keys(&session, 2, 8),
            MemoryFileCompositionBounds::default(),
            &cancelled,
        ),
        "cancel",
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn owned_fixed_bytes<const N: usize>(
    session: &ResidentVortexSession,
    values: &[[u8; N]],
) -> vortex::buffer::ByteBuffer {
    let mut buffer = session
        .native_allocator()
        .allocate(values.len() * N, Alignment::new(N))
        .unwrap();
    for (target, value) in buffer.as_mut_slice().chunks_exact_mut(N).zip(values) {
        target.copy_from_slice(value);
    }
    buffer.freeze()
}

fn owned_root_validity(session: &ResidentVortexSession, valid: &[bool]) -> Validity {
    use vortex::{array::arrays::BoolArray, buffer::BitBuffer};
    let mut buffer = session
        .native_allocator()
        .allocate(valid.len().div_ceil(8), Alignment::none())
        .unwrap();
    buffer.as_mut_slice().fill(0);
    for (index, valid) in valid.iter().enumerate() {
        if *valid {
            buffer.as_mut_slice()[index / 8] |= 1 << (index % 8);
        }
    }
    Validity::Array(
        BoolArray::new(
            BitBuffer::new(buffer.freeze(), valid.len()),
            Validity::NonNullable,
        )
        .into_array(),
    )
}

#[test]
#[allow(clippy::too_many_lines)] // Parent nulls, child nulls and producer/output ownership form one proof.
fn composition_nullable_struct_batches_preserve_logical_fields_and_mixed_widths() {
    use crate::resident_memory_source::OwnedMemoryColumn;
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let vectors = session
        .memory()
        .reserve((2 * std::mem::size_of::<ArrayRef>()) as u64)
        .unwrap();
    let mut batches = Vec::with_capacity(2);
    assert_eq!(batches.capacity(), 2);
    for (small, wide, offsets, text, text_valid, root_valid) in [
        (
            [i16::MIN, 17, i16::MAX],
            [0_u32, u32::MAX, 17],
            [0_u64, 2, 8, 8],
            "λhidden",
            [true, true, false],
            [true, false, true],
        ),
        (
            [6_i16, -7, 8],
            [19_u32, 23, u32::MAX],
            [0_u64, 0, 6, 6],
            "東京",
            [true, true, false],
            [true, true, false],
        ),
    ] {
        let small = PrimitiveArray::new(
            Buffer::<i16>::from_byte_buffer(owned_fixed_bytes(
                &session,
                &small.map(i16::to_ne_bytes),
            )),
            Validity::NonNullable,
        )
        .into_array();
        let wide = PrimitiveArray::new(
            Buffer::<u32>::from_byte_buffer(owned_fixed_bytes(
                &session,
                &wide.map(u32::to_ne_bytes),
            )),
            Validity::NonNullable,
        )
        .into_array();
        let text = OwnedMemoryColumn::utf8(
            &session,
            "text",
            offsets.to_vec(),
            text.as_bytes().to_vec(),
            Some(text_valid.to_vec()),
        )
        .unwrap();
        batches.push(
            StructArray::try_new(
                ["small", "wide", "text"].into(),
                vec![small, wide, text.array().clone()],
                3,
                owned_root_validity(&session, &root_valid),
            )
            .unwrap()
            .into_array(),
        );
    }
    let producer_owners = [batches[0].clone(), batches[1].clone()];
    let input = OwnedVortexResultBatch {
        dtype: batches[0].dtype().clone(),
        rows: 6,
        logical_buffer_bytes: batches.iter().map(|batch| batch.nbytes()).sum(),
        arrays: Budgeted::new(batches, vectors),
        runtime: Arc::clone(&session.0),
    };
    assert!(input.dtype().is_nullable());
    let generation = MemoryFileGeneration::from_owned(
        input,
        MemoryFileCompositionBounds::default(),
        &CancellationToken::default(),
    )
    .unwrap();
    let expected_dtype = DType::Struct(
        StructFields::new(
            ["small", "wide", "text"].into(),
            vec![
                DType::Primitive(PType::I16, Nullability::Nullable),
                DType::Primitive(PType::U32, Nullability::Nullable),
                DType::Utf8(Nullability::Nullable),
            ],
        ),
        Nullability::NonNullable,
    );
    assert_eq!(generation.dtype(), &expected_dtype);
    assert!(
        generation
            .evidence()
            .construction_native_materialization_calls
            > 0
    );
    assert!(
        generation
            .evidence()
            .construction_native_materialization_rows
            >= 6
    );
    let result = generation
        .prepare_projection(&["small", "wide", "text"], None, 6, 64 << 10)
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(result.dtype(), &expected_dtype);
    drop(generation);
    drop(session);
    let json = result
        .to_bounded_json(&["small".into(), "wide".into(), "text".into()], 64 << 10)
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(json.value()).unwrap(),
        json!([
            {"small": i16::MIN, "wide": 0, "text": "λ"},
            {"small": null, "wide": null, "text": null},
            {"small": i16::MAX, "wide": 17, "text": null},
            {"small": 6, "wide": 19, "text": ""},
            {"small": -7, "wide": 23, "text": "東京"},
            {"small": null, "wide": null, "text": null},
        ])
    );
    drop(json);
    drop(result);
    assert!(
        memory.snapshot().reserved_bytes > 0,
        "producer clones retain their payload credits"
    );
    drop(producer_owners);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn contains_dictionary(array: &ArrayRef) -> bool {
    array.is::<vortex::array::arrays::Dict>() || array.children_iter().any(contains_dictionary)
}

#[test]
fn composition_dictionary_domain_obeys_serialized_cap_and_retains_native_values() {
    use crate::resident_memory_source::OwnedMemoryColumn;
    use vortex::array::arrays::DictArray;
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let mut offsets = vec![0_u64];
    let mut bytes = Vec::new();
    for index in 0..1024 {
        bytes.extend_from_slice(format!("{index:04}:{}", "x".repeat(123)).as_bytes());
        offsets.push(bytes.len() as u64);
    }
    let domain = OwnedMemoryColumn::utf8(&session, "domain", offsets, bytes, None).unwrap();
    let codes = PrimitiveArray::new(
        Buffer::<u32>::from_byte_buffer(owned_fixed_bytes(
            &session,
            &[7_u32, 63].map(u32::to_ne_bytes),
        )),
        Validity::NonNullable,
    )
    .into_array();
    let dictionary = DictArray::try_new(codes, domain.array().clone())
        .unwrap()
        .into_array();
    let array = StructArray::try_new(["text"].into(), vec![dictionary], 2, Validity::NonNullable)
        .unwrap()
        .into_array();
    let retained = memory.snapshot().reserved_bytes;
    let make_input = || {
        let lease = session
            .memory()
            .reserve(std::mem::size_of::<ArrayRef>() as u64)
            .unwrap();
        let arrays = vec![array.clone()];
        assert_eq!(arrays.capacity(), 1);
        OwnedVortexResultBatch {
            dtype: array.dtype().clone(),
            rows: 2,
            logical_buffer_bytes: array.nbytes(),
            arrays: Budgeted::new(arrays, lease),
            runtime: Arc::clone(&session.0),
        }
    };
    let mut tight = MemoryFileCompositionBounds::default();
    tight.storage.max_serialized_bytes = 4096;
    rejected(
        MemoryFileGeneration::from_owned(make_input(), tight, &CancellationToken::default()),
        "serialized byte bound exceeded",
    );
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    assert_eq!(session.snapshot().completed_executions, 0);
    let generation = MemoryFileGeneration::from_owned(
        make_input(),
        MemoryFileCompositionBounds::default(),
        &CancellationToken::default(),
    )
    .unwrap();
    assert!(generation.evidence().segment_assembly_bytes_copied >= 128 * 1024);
    assert_eq!(generation.evidence().dictionary_build_calls, 0);
    drop(array);
    drop(domain);
    drop(session);
    let result = generation
        .prepare_projection(&["text"], None, 2, 512 << 10)
        .unwrap()
        .execute()
        .unwrap();
    assert!(
        result.arrays().iter().any(contains_dictionary),
        "serialized generation must retain the native dictionary"
    );
    let json = result.to_bounded_json(&["text".into()], 4096).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(json.value()).unwrap(),
        json!([
            {"text": format!("0007:{}", "x".repeat(123))}, {"text": format!("0063:{}", "x".repeat(123))},
        ])
    );
    drop(json);
    drop(result);
    drop(generation);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
