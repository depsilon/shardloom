use super::*;
use crate::{
    VortexLocalPrimitiveExecutionPolicy,
    local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
    relational_query::{VortexRelationalPlan, VortexRelationalScan},
    resident_memory_source::ResidentMemorySource,
    resident_session::ResidentVortexSession,
};
use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int8Array, Int16Array,
    Int64Array, ListArray, StringArray, TimestampMicrosecondArray, UInt64Array, types::Int16Type,
};
use shardloom_core::{DatasetUri, ExecutionResourceOrigin, ExecutionResources};
use shardloom_plan::ProjectionRequest;

fn batch(columns: Vec<(&str, ArrowArrayRef)>, nullable: bool) -> RecordBatch {
    let fields = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), nullable))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap()
}

fn native(input: RecordBatch, bytes: u64) -> (Arc<OwnedVortexResultBatch>, ResidentVortexSession) {
    let session = ResidentVortexSession::new(bytes, 1).unwrap();
    let source = ResidentMemorySource::from_arrow_record_batch(
        &session,
        input,
        &CancellationToken::default(),
    )
    .unwrap();
    let resources =
        ExecutionResources::from_bytes(bytes, 1, ExecutionResourceOrigin::ExecutionCall).unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(resources).unwrap();
    let uri = DatasetUri::new("memory://native-arrow-result").unwrap();
    let prepared = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
        schemas.register_memory_source(uri.clone(), |_| Ok(source.clone()))?;
        Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: ProjectionRequest::All,
            predicate: None,
        }))
    })
    .unwrap();
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(completed.execution.runtime.prepared_source_opens, 0);
    (Arc::new(completed.result), session)
}

#[test]
fn native_arrow_export_keeps_rich_types_and_full_domain_values_without_rows() {
    let input = rich_type_arrow_fixture();
    let (result, session) = native(input.clone(), 64 << 20);
    let memory = session.memory().clone();
    let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
    assert_eq!(stream.schema(), input.schema());
    let exported = stream.next_batch().unwrap().unwrap();
    assert_eq!(exported, input);
    assert!(stream.next_batch().unwrap().is_none());
    let report = stream.report();
    assert!(report.complete && report.shared_native_buffer_bytes > 0);
    assert_eq!(report.output_rows, 3);
    assert_eq!(report.output_batches, 1);
    assert!(report.converted_buffer_capacity_bytes > 0);
    drop((stream, result, session));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(exported, input);
    assert_eq!(
        exported
            .column(4)
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(0)
            .to_bits(),
        (-0.0_f64).to_bits()
    );
    drop(exported);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn rich_type_arrow_fixture() -> RecordBatch {
    let long_text = "λ".repeat(65_537);
    let long_name = "exact_nested_field_".repeat(20);
    let structure = arrow_array::StructArray::new(
        vec![Field::new(&long_name, DataType::Int16, true)].into(),
        vec![Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            Some(i16::MAX),
            None,
        ]))],
        Some(vec![true, true, false].into()),
    );
    batch(
        vec![
            (
                "i8",
                Arc::new(Int8Array::from(vec![Some(i8::MIN), Some(i8::MAX), None])),
            ),
            (
                "i64",
                Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
            ),
            (
                "u64",
                Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(0), None])),
            ),
            (
                "flag",
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            ),
            (
                "real",
                Arc::new(Float64Array::from(vec![
                    Some(-0.0),
                    Some(f64::MIN_POSITIVE),
                    None,
                ])),
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![
                    Some(long_text.as_str()),
                    Some("\0猫"),
                    None,
                ])),
            ),
            (
                "binary",
                Arc::new(BinaryArray::from(vec![
                    Some(&b"\0\xffA"[..]),
                    Some(&b""[..]),
                    None,
                ])),
            ),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(10_i128.pow(38) - 1),
                        Some(1 - 10_i128.pow(38)),
                        None,
                    ])
                    .with_precision_and_scale(38, 6)
                    .unwrap(),
                ),
            ),
            (
                "date",
                Arc::new(Date32Array::from(vec![
                    Some(i32::MIN),
                    Some(i32::MAX),
                    None,
                ])),
            ),
            (
                "time",
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(i64::MIN),
                    Some(i64::MAX),
                    None,
                ])),
            ),
            (
                "list",
                Arc::new(ListArray::from_iter_primitive::<Int16Type, _, _>([
                    Some(vec![Some(i16::MIN), None, Some(i16::MAX)]),
                    Some(vec![]),
                    None,
                ])),
            ),
            ("record", Arc::new(structure)),
        ],
        true,
    )
}

#[test]
fn native_arrow_export_shares_large_primitive_results_and_checks_every_value() {
    let rows = 131_073_i64;
    let names = (0..9)
        .map(|column| format!("value_{column}"))
        .collect::<Vec<_>>();
    let fields = names
        .iter()
        .enumerate()
        .map(|(column, name)| {
            (
                name.as_str(),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| row * 17 - i64::try_from(column).unwrap()),
                )) as ArrowArrayRef,
            )
        })
        .collect::<Vec<_>>();
    let (result, session) = native(batch(fields, false), 128 << 20);
    assert!(result.logical_buffer_bytes() > 8 << 20);
    let memory = session.memory().clone();
    let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
    let mut all = Vec::new();
    while let Some(batch) = stream.next_batch().unwrap() {
        all.push(batch);
    }
    let report = stream.report();
    assert!(report.complete);
    assert_eq!(report.output_rows, u64::try_from(rows).unwrap());
    assert_eq!(report.converted_buffer_capacity_bytes, 0);
    assert_eq!(
        report.shared_native_buffer_bytes,
        u64::try_from(rows).unwrap() * 9 * 8
    );
    assert_eq!(
        report.referenced_arrow_buffer_bytes,
        report.shared_native_buffer_bytes
    );
    drop((stream, result, session));
    assert!(memory.snapshot().reserved_bytes > 0);
    let mut row = 0_i64;
    for batch in &all {
        for (column, array) in batch.columns().iter().enumerate() {
            let values = array.as_primitive::<arrow_array::types::Int64Type>();
            for (offset, value) in values.values().iter().enumerate() {
                assert_eq!(
                    *value,
                    (row + i64::try_from(offset).unwrap()) * 17 - i64::try_from(column).unwrap()
                );
            }
        }
        row += i64::try_from(batch.num_rows()).unwrap();
    }
    assert_eq!(row, rows);
    drop(all);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_arrow_export_empty_slices_keep_metadata_and_buffer_owners() {
    for rows in [0, 11] {
        for boolean in [false, true] {
            let input: ArrowArrayRef = if boolean {
                Arc::new(BooleanArray::from_iter(
                    (0..rows).map(|row| Some(row % 2 == 0)),
                ))
            } else {
                Arc::new(Int64Array::from_iter_values(0..rows))
            };
            let (result, session) = native(batch(vec![("value", input)], false), 1 << 20);
            let memory = session.memory().clone();
            let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
            let output = stream.next_batch().unwrap().unwrap();
            assert_eq!(output.num_rows(), usize::try_from(rows).unwrap());
            assert!(stream.next_batch().unwrap().is_none());
            let field = output.column(0).slice(0, 0);
            let hidden = field.to_data().buffers()[0].clone();
            drop((field, output, stream, result, session));
            assert!(
                memory.snapshot().reserved_bytes > 0,
                "rows={rows}, boolean={boolean}"
            );
            drop(hidden);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_arrow_export_resource_denial_and_cancellation_allow_new_export() {
    let (result, session) = native(
        batch(
            vec![("n", Arc::new(Int64Array::from_iter_values(0..4099)))],
            false,
        ),
        2 << 20,
    );
    let memory = session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
    let available = memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes;
    let occupied = memory.reserve(available).unwrap();
    assert!(stream.next_batch().is_err());
    assert_eq!(stream.report().output_batches, 0);
    assert!(stream.next_batch().is_err());
    drop((stream, occupied));
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let token = CancellationToken::default();
    let mut stream = result.arrow_batches(token.clone()).unwrap();
    let first = stream.next_batch().unwrap().unwrap();
    token.cancel();
    assert!(stream.next_batch().is_err());
    assert!(!stream.report().complete);
    drop((stream, first));
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let mut retry = result.arrow_batches(CancellationToken::default()).unwrap();
    let mut rows = 0;
    while let Some(batch) = retry.next_batch().unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 4099);
    assert!(retry.report().complete);
    drop((retry, result, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_arrow_borrowed_export_rejects_a_foreign_execution_owner() {
    let (result, session) = native(
        batch(
            vec![("n", Arc::new(Int64Array::from(vec![1, 2, 3])))],
            false,
        ),
        2 << 20,
    );
    let foreign = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
    let error = foreign
        .with_native_execution_context(&CancellationToken::default(), |context| {
            stream.next_batch_in_context(context)
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("different session"));
    assert_eq!(stream.report().output_batches, 0);
    assert_eq!(session.snapshot().completed_executions, 1);
    assert_eq!(foreign.shared_memory_pool().snapshot().reserved_bytes, 0);
}
