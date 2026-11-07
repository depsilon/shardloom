use super::*;
use arrow_array::{
    Array as _, FixedSizeListArray, Float32Array, Int32Array, LargeListArray, ListArray,
    RecordBatch, StructArray, cast::AsArray as _, types::Float32Type,
};
use arrow_schema::Schema;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use vortex::{VortexSessionDefault as _, array::VortexSessionExecute as _};

fn target(array: &ArrowArray, metadata: &str) -> Field {
    Field::new("t", array.data_type().clone(), true).with_metadata(HashMap::from([
        (NAME.into(), super::super::ARROW_ID.into()),
        (META.into(), metadata.into()),
    ]))
}

fn input(
    data: Vec<Option<Vec<Option<f32>>>>,
    dimensions: Vec<Option<i32>>,
    rank: i32,
    parent: Option<Vec<bool>>,
    shape: Option<Vec<bool>>,
) -> ArrowArray {
    let rows = data.len();
    let data: ArrowArray = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(data));
    let shape: ArrowArray = Arc::new(
        FixedSizeListArray::try_new_with_length(
            Arc::new(Field::new("item", DataType::Int32, true)),
            rank,
            Arc::new(Int32Array::from(dimensions)),
            shape.map(Into::into),
            rows,
        )
        .unwrap(),
    );
    Arc::new(StructArray::new(
        vec![
            Field::new("data", data.data_type().clone(), true),
            Field::new("shape", shape.data_type().clone(), true),
        ]
        .into(),
        vec![data, shape],
        parent.map(Into::into),
    ))
}

fn batch(field: Field, array: ArrowArray) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
}

fn intake(batch: RecordBatch) -> shardloom_core::Result<()> {
    let session = crate::resident_session::ResidentVortexSession::new(4 << 20, 1).unwrap();
    let result = crate::resident_memory_source::ResidentMemorySource::from_arrow_record_batch(
        &session,
        batch,
        &CancellationToken::default(),
    )
    .map(drop);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    result
}

#[test]
fn variable_tensor_arrow_parent_masks_and_slices_control_shape_validation() {
    let tensor = input(
        vec![Some(vec![Some(7.0)]), Some(vec![Some(1.0), Some(2.0)])],
        vec![Some(-1), Some(1), Some(1), Some(2)],
        2,
        None,
        None,
    );
    let field = target(&tensor, "{}");
    assert!(
        intake(batch(field.clone(), tensor.clone()))
            .unwrap_err()
            .to_string()
            .contains("nonnegative")
    );
    intake(batch(field.clone(), tensor.slice(1, 1))).unwrap();
    intake(batch(field.clone(), tensor.slice(0, 0))).unwrap();
    for visible in [false, true] {
        let nulls = Some(vec![visible, true].into());
        let structure: ArrowArray = Arc::new(StructArray::new(
            vec![field.clone()].into(),
            vec![tensor.clone()],
            nulls.clone(),
        ));
        let list: ArrowArray = Arc::new(ListArray::new(
            Arc::new(field.clone()),
            arrow_buffer::OffsetBuffer::new(vec![0_i32, 1, 2].into()),
            tensor.clone(),
            nulls.clone(),
        ));
        let large: ArrowArray = Arc::new(LargeListArray::new(
            Arc::new(field.clone()),
            arrow_buffer::OffsetBuffer::new(vec![0_i64, 1, 2].into()),
            tensor.clone(),
            nulls.clone(),
        ));
        let fixed: ArrowArray = Arc::new(FixedSizeListArray::new(
            Arc::new(field.clone()),
            1,
            tensor.clone(),
            nulls,
        ));
        for parent in [structure, list, large, fixed] {
            let parent_field = Field::new("parent", parent.data_type().clone(), true);
            assert_eq!(
                intake(batch(parent_field.clone(), parent.clone())).is_ok(),
                !visible
            );
            intake(batch(parent_field, parent.slice(1, 1))).unwrap();
        }
    }
    let hidden = input(
        vec![Some(vec![Some(7.0)]), Some(vec![Some(1.0), Some(2.0)])],
        vec![None, Some(-1), Some(1), Some(2)],
        2,
        Some(vec![false, true]),
        None,
    );
    intake(batch(target(&hidden, "{}"), hidden)).unwrap();
}

#[test]
fn variable_tensor_arrow_rejects_visible_nulls_mismatch_and_uniform_violation() {
    for (data, dimensions, parent, shape) in [
        (vec![None], vec![Some(1)], None, None),
        (
            vec![Some(vec![Some(1.0)])],
            vec![Some(1)],
            None,
            Some(vec![false]),
        ),
        (vec![Some(vec![Some(1.0)])], vec![None], None, None),
        (vec![Some(vec![Some(1.0)])], vec![Some(2)], None, None),
    ] {
        let array = input(data, dimensions, 1, parent, shape);
        assert!(intake(batch(target(&array, "{}"), array)).is_err());
    }
    let scalar = input(
        vec![Some(vec![Some(7.0)]), None],
        vec![],
        0,
        Some(vec![true, false]),
        None,
    );
    intake(batch(target(&scalar, r#"{"uniform_shape":[]}"#), scalar)).unwrap();
    let empty = input(
        vec![Some(vec![])],
        vec![Some(i32::MAX), Some(i32::MAX), Some(i32::MAX), Some(0)],
        4,
        None,
        None,
    );
    intake(batch(target(&empty, "{}"), empty.clone())).unwrap();
    assert!(
        intake(batch(
            target(&empty, r#"{"uniform_shape":[null,null,null,1]}"#),
            empty
        ))
        .is_err()
    );
}

#[test]
fn variable_tensor_arrow_roundtrip_preserves_schema_slices_and_float_bits() {
    let array = input(
        vec![
            Some(vec![Some(3.0)]),
            Some(vec![
                Some(f32::from_bits(0xff80_0042)),
                Some(-0.0),
                None,
                Some(f32::INFINITY),
                Some(f32::NEG_INFINITY),
                Some(1.25),
            ]),
        ],
        vec![Some(1), Some(1), Some(1), Some(2), Some(1), Some(3)],
        3,
        None,
        None,
    );
    let target = target(
        &array,
        r#"{"dim_names":["Ω","h","w"],"permutation":[2,0,1],"uniform_shape":[null,1,null]}"#,
    );
    let session = VortexSession::default();
    crate::native_extensions::register(&session);
    for input in [array.clone(), array.slice(1, 1), array.slice(2, 0)] {
        let native = session
            .arrow()
            .from_arrow_array(input.clone(), &target)
            .unwrap();
        assert_eq!(
            session.arrow().to_arrow_field("t", native.dtype()).unwrap(),
            target
        );
        let output = session
            .arrow()
            .execute_arrow(native, Some(&target), &mut session.create_execution_ctx())
            .unwrap();
        assert_eq!(input.data_type(), output.data_type());
        assert_eq!(input.len(), output.len());
        for row in 0..input.len() {
            let a = input.as_struct().column(0).as_list::<i32>().value(row);
            let b = output.as_struct().column(0).as_list::<i32>().value(row);
            let a = a.as_any().downcast_ref::<Float32Array>().unwrap();
            let b = b.as_any().downcast_ref::<Float32Array>().unwrap();
            assert_eq!(a.nulls(), b.nulls());
            for i in 0..a.len() {
                assert_eq!(a.value(i).to_bits(), b.value(i).to_bits());
            }
            assert_eq!(
                input
                    .as_struct()
                    .column(1)
                    .as_fixed_size_list()
                    .value(row)
                    .to_data(),
                output
                    .as_struct()
                    .column(1)
                    .as_fixed_size_list()
                    .value(row)
                    .to_data()
            );
        }
    }
}

#[test]
fn variable_tensor_arrow_metadata_workspace_and_cancellation_release() {
    let array = input(vec![Some(vec![Some(1.0)])], vec![Some(1)], 1, None, None);
    let input = batch(target(&array, "{}"), array);
    let denied = LiveMemoryPool::new(256).unwrap();
    assert!(validate_batch(&input, Some(&denied), &|| Ok(())).is_err());
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    let memory = LiveMemoryPool::new(64 << 10).unwrap();
    let token = CancellationToken::default();
    token.cancel();
    assert!(validate_batch(&input, Some(&memory), &|| token.check()).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    validate_batch(&input, Some(&memory), &|| Ok(())).unwrap();
    assert!(memory.snapshot().peak_reserved_bytes > 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn variable_tensor_parquet_required_shape_inherits_null_struct_visibility() {
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

    for width in [0, 2] {
        let shape = FixedSizeListArray::try_new_with_length(
            Arc::new(Field::new("item", DataType::Int32, false)),
            width,
            Arc::new(Int32Array::from_iter_values(0..3 * width)),
            None,
            3,
        )
        .unwrap();
        let parent = StructArray::new(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("shape", shape.data_type().clone(), false),
            ]
            .into(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), Arc::new(shape)],
            Some(vec![true, false, true].into()),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "parent",
            parent.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(parent)]).unwrap();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buffer))
            .unwrap()
            .with_batch_size(2)
            .build()
            .unwrap();
        let mut start = 0;
        for actual in reader {
            let actual = actual.unwrap();
            assert_eq!(actual.schema(), schema);
            actual.column(0).to_data().validate_full().unwrap();
            assert_eq!(actual, batch.slice(start, actual.num_rows()));
            start += actual.num_rows();
        }
        assert_eq!(start, 3);
    }
}
