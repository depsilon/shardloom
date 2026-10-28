use super::*;
use crate::{
    VortexLocalPrimitiveExecutionPolicy,
    local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
    relational_query::{VortexRelationalPlan, VortexRelationalScan},
    resident_memory_source::MemoryBatchSourceBuilder,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, StringArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array, types::Int16Type,
};
use arrow_schema::{DataType, Field, Schema};
use shardloom_core::{DatasetUri, ExecutionResourceOrigin, ExecutionResources};
use shardloom_plan::ProjectionRequest;
use vortex::array::arrays::PrimitiveArray;

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let schema = Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(
        Arc::new(schema),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap()
}

#[test]
fn direct_columnar_intake_keeps_exact_types_nulls_and_endpoint_values() {
    let input = rich_columnar_intake_fixture();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let source = ResidentMemorySource::from_arrow_record_batch(
        &session,
        input.clone(),
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(source.intake_payload_bytes_copied(), 0);
    assert_eq!(
        source.intake_columnar_buffer_bytes_copied(),
        arrow_ownership::batch_copy_buffer_bytes(&input).unwrap()
    );
    let expected_dtype = session
        .with_native_session(|native, _| {
            native
                .arrow()
                .from_arrow_schema(input.schema().as_ref())
                .map_err(native_error)
        })
        .unwrap();
    assert_eq!(source.dtype(), &expected_dtype);
    let names = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();
    let result = source
        .prepare_projection(&names, None, None)
        .unwrap()
        .execute()
        .unwrap();
    let values: serde_json::Value = serde_json::from_str(result.values_json.value()).unwrap();
    assert_eq!(
        values[0],
        serde_json::json!({
            "i8":i8::MIN,"i16":i16::MIN,"i32":i32::MIN,"i64":i64::MIN,
            "u8":u8::MAX,"u16":u16::MAX,"u32":u32::MAX,"u64":u64::MAX,
            "f32":-0.0,"f64":1.25,"flag":true,"text":"λ\0猫","binary":"00ff41",
            "decimal":"decimal128(38,2):12345678901234567890123456789012345678",
            "date":i32::MIN,"instant":i64::MAX,"items":[i16::MIN,null,i16::MAX]
        })
    );
    assert!(
        values[1]
            .as_object()
            .unwrap()
            .values()
            .all(serde_json::Value::is_null)
    );
    drop((source, result));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

fn rich_columnar_intake_fixture() -> RecordBatch {
    batch(vec![
        ("i8", Arc::new(Int8Array::from(vec![Some(i8::MIN), None]))),
        (
            "i16",
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None])),
        ),
        (
            "i32",
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
        ),
        (
            "i64",
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
        ),
        ("u8", Arc::new(UInt8Array::from(vec![Some(u8::MAX), None]))),
        (
            "u16",
            Arc::new(UInt16Array::from(vec![Some(u16::MAX), None])),
        ),
        (
            "u32",
            Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])),
        ),
        (
            "u64",
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
        ),
        ("f32", Arc::new(Float32Array::from(vec![Some(-0.0), None]))),
        ("f64", Arc::new(Float64Array::from(vec![Some(1.25), None]))),
        ("flag", Arc::new(BooleanArray::from(vec![Some(true), None]))),
        (
            "text",
            Arc::new(StringArray::from(vec![Some("λ\0猫"), None])),
        ),
        (
            "binary",
            Arc::new(BinaryArray::from(vec![Some(&b"\0\xffA"[..]), None])),
        ),
        (
            "decimal",
            Arc::new(
                Decimal128Array::from(vec![
                    Some(12_345_678_901_234_567_890_123_456_789_012_345_678_i128),
                    None,
                ])
                .with_precision_and_scale(38, 2)
                .unwrap(),
            ),
        ),
        (
            "date",
            Arc::new(Date32Array::from(vec![Some(i32::MIN), None])),
        ),
        (
            "instant",
            Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MAX), None])),
        ),
        (
            "items",
            Arc::new(ListArray::from_iter_primitive::<Int16Type, _, _>([
                Some(vec![Some(i16::MIN), None, Some(i16::MAX)]),
                None,
            ])),
        ),
    ])
}

#[test]
fn direct_columnar_large_batches_execute_all_values_with_one_resource_owner() {
    let rows = 131_073_i64;
    let resources =
        ExecutionResources::from_bytes(64 << 20, 1, ExecutionResourceOrigin::ExecutionCall)
            .unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(resources).unwrap();
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let mut builder =
        MemoryBatchSourceBuilder::new(&session, CancellationToken::default()).unwrap();
    for range in [0..65_539, 65_539..rows] {
        let values = Int64Array::from_iter_values(range.map(|n| n * 17 - 9));
        builder
            .push_arrow(batch(vec![("n", Arc::new(values))]))
            .unwrap();
    }
    let source = builder.finish().unwrap();
    assert_eq!(source.row_count(), usize::try_from(rows).unwrap());
    assert_eq!(
        source.intake_columnar_buffer_bytes_copied(),
        u64::try_from(rows).unwrap() * 8
    );
    let uri = DatasetUri::new("memory://columnar-large").unwrap();
    let prepared = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
        schemas.register_memory_source(uri.clone(), |_| Ok(source.clone()))?;
        Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: ProjectionRequest::All,
            predicate: None,
        }))
    })
    .unwrap();
    let result = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(result.result.row_count(), u64::try_from(rows).unwrap());
    assert_eq!(result.execution.runtime.prepared_source_opens, 0);
    let mut context = result.result.create_execution_ctx();
    let memory = session.memory().clone();
    drop((prepared, source, session));
    let mut row = 0_i64;
    for batch in result.result.arrays() {
        let values = crate::local_primitives::logical_field_from_native_array(batch, "n")
            .unwrap()
            .execute::<PrimitiveArray>(&mut context)
            .unwrap();
        for value in values.as_slice::<i64>() {
            assert_eq!(*value, row * 17 - 9);
            row += 1;
        }
    }
    assert_eq!(row, rows);
    drop((result, context));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn direct_columnar_slice_and_child_alias_retain_schema_and_buffer_credit() {
    for slice in [(1, 2), (1, 0)] {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let input = batch(vec![(
            "n",
            Arc::new(Int64Array::from(vec![Some(7), None, Some(9), Some(11)])),
        )])
        .slice(slice.0, slice.1);
        let input_pointer = input.column(0).to_data().buffers()[0].as_ptr();
        let source = ResidentMemorySource::from_arrow_record_batch(
            &session,
            input,
            &CancellationToken::default(),
        )
        .unwrap();
        let field = source.0.array.slots()[1].as_ref().unwrap().clone();
        assert_ne!(field.buffers()[0].as_ptr(), input_pointer);
        let hidden = field.buffers()[0].clone();
        let witness = Arc::downgrade(source.0.metadata.as_ref().unwrap());
        drop((source, field, session));
        assert!(witness.upgrade().is_some());
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(hidden);
        assert!(witness.upgrade().is_none());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn direct_columnar_borrowed_context_rejects_another_session_before_intake() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let foreign = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let input = batch(vec![("n", Arc::new(Int64Array::from(vec![Some(7), None])))]);
    let error = foreign
        .with_native_execution_context(&CancellationToken::default(), |context| {
            ResidentMemorySource::from_arrow_in_context(&session, input, context, true)
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("different session"));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    assert_eq!(foreign.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn direct_columnar_denial_and_cancellation_release_credits_and_allow_retry() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let valid = || batch(vec![("n", Arc::new(Int64Array::from(vec![Some(7), None])))]);
    let invalid = [
        batch(vec![(
            "n",
            Arc::new(Float64Array::from(vec![f64::INFINITY])),
        )]),
        batch(vec![(
            "n",
            Arc::new(
                Decimal128Array::from(vec![123])
                    .with_precision_and_scale(2, 0)
                    .unwrap(),
            ),
        )]),
        batch(vec![(
            "n",
            Arc::new(TimestampMicrosecondArray::from(vec![1]).with_timezone("ShardLoom/Invalid")),
        )]),
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, true)
                    .with_metadata([("ARROW:extension:name".into(), "unsupported".into())].into()),
            ])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap(),
    ];
    for input in invalid {
        assert!(
            ResidentMemorySource::from_arrow_record_batch(
                &session,
                input,
                &CancellationToken::default()
            )
            .is_err()
        );
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(ResidentMemorySource::from_arrow_record_batch(&session, valid(), &cancelled).is_err());
    let occupied = session.memory().reserve(1 << 20).unwrap();
    assert!(
        ResidentMemorySource::from_arrow_record_batch(
            &session,
            valid(),
            &CancellationToken::default()
        )
        .is_err()
    );
    drop(occupied);
    let source = ResidentMemorySource::from_arrow_record_batch(
        &session,
        valid(),
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(source.row_count(), 2);
    drop(source);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn direct_columnar_decimal_validation_ignores_unreachable_nested_values() {
    let values = Decimal128Array::from(vec![123, 9, -12])
        .with_precision_and_scale(2, 0)
        .unwrap();
    let structure = arrow_array::StructArray::new(
        vec![Field::new("amount", DataType::Decimal128(2, 0), false)].into(),
        vec![Arc::new(values)],
        Some(vec![false, true, true].into()),
    );
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let source = ResidentMemorySource::from_arrow_record_batch(
        &session,
        batch(vec![("record", Arc::new(structure))]),
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(source.row_count(), 3);
    drop(source);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
