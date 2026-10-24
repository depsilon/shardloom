//! Immediate Parquet views retain admitted native owners and the persisted schema.

use super::*;
use arrow_array::{
    Array as _, RecordBatch, StringArray, StringViewArray, StructArray as ArrowStruct,
};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        VortexSessionExecute as _,
        arrays::varbinview::BinaryView,
        dtype::{FieldNames, Nullability},
        memory::{HostAllocator as _, HostMemorySessionExt as _},
        validity::Validity,
    },
    arrow::{ArrowExportOptions, ArrowSessionExt as _, CompactBuffers},
    buffer::{Alignment, Buffer},
    session::VortexSession,
};

const TEXT: &str = "an outlined native value with λ and 猫";
const ROWS: usize = 128;

fn fixture(
    backing_bytes: usize,
) -> (
    ArrayRef,
    SchemaRef,
    LiveMemoryPool,
    VortexSession,
    *const u8,
) {
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let allocator = Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
        memory.clone(),
    ));
    let session = VortexSession::default().with_host_allocator(allocator.clone());
    let mut payload = allocator
        .allocate(backing_bytes, Alignment::of::<u8>())
        .unwrap();
    payload.as_mut_slice().fill(b'x');
    payload.as_mut_slice()[..TEXT.len()].copy_from_slice(TEXT.as_bytes());
    let payload = payload.freeze();
    let pointer = payload.as_ptr();
    let views = Buffer::from_iter((0..ROWS).map(|_| BinaryView::make_view(TEXT.as_bytes(), 0, 0)));
    let text = VarBinViewArray::try_new(
        views,
        Arc::from([payload]),
        DType::Utf8(Nullability::Nullable),
        Validity::from_iter((0..ROWS).map(|row| row % 3 != 1)),
        &mut session.create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["text"]),
        vec![text],
        ROWS,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("text", DataType::Utf8, true)
                .with_metadata([("meaning".into(), "source text".into())].into()),
        ],
        [("dataset".into(), "immediate export".into())].into(),
    ));
    (array, schema, memory, session, pointer)
}

fn admit(array: &ArrayRef, session: &VortexSession) -> (ArrayRef, u64) {
    let check = || Ok(());
    let limits = ConversionLimits {
        string_bytes: 64 * 1024,
        arrow_batch_bytes: 8 << 20,
        check: &check,
    };
    let mut expanded = 0;
    let array = canonical(
        array,
        &mut session.create_execution_ctx(),
        &limits,
        &mut expanded,
    )
    .unwrap();
    (array, expanded)
}

#[test]
fn immediate_parquet_views_share_payload_keep_credit_and_reopen_declared_schema() {
    let (source, schema, memory, session, pointer) = fixture(512);
    let (canonical, expanded) = admit(&source, &session);
    let (view_schema, admitted) = immediate_parquet_target(&canonical, &schema, expanded, 8 << 20)
        .unwrap()
        .expect("bounded immediate view target");
    assert!(admitted > expanded);
    assert_eq!(view_schema.field(0).data_type(), &DataType::Utf8View);
    assert_eq!(view_schema.field(0).metadata(), schema.field(0).metadata());
    assert_eq!(view_schema.metadata(), schema.metadata());
    let options = ArrowExportOptions::default().with(CompactBuffers(false));
    let arrow = session
        .arrow()
        .exporter(&options)
        .execute_arrow(
            canonical,
            Some(&Field::new(
                "",
                DataType::Struct(view_schema.fields().clone()),
                false,
            )),
            &mut session.create_execution_ctx(),
        )
        .unwrap();
    let structure = arrow.as_any().downcast_ref::<ArrowStruct>().unwrap();
    let batch = RecordBatch::try_new(view_schema, structure.columns().to_vec()).unwrap();
    let views = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(views.data_buffers()[0].as_ptr(), pointer);
    assert!((batch.get_array_memory_size() as u64) <= admitted);
    for row in 0..ROWS {
        assert_eq!(views.is_null(row), row % 3 == 1);
        if !views.is_null(row) {
            assert_eq!(views.value(row), TEXT);
        }
    }
    let before = memory.snapshot().reserved_bytes;
    assert!(before >= 512);
    drop((source, arrow, session));
    assert!(memory.snapshot().reserved_bytes >= 512);

    let mut encoded = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut encoded, schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.flush().unwrap();
    writer.close().unwrap();
    drop(batch);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(encoded),
    )
    .unwrap();
    assert_eq!(reader.schema(), &schema);
    let batches = reader
        .build()
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let mut rows = 0;
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for value in values {
            assert_eq!(value, (rows % 3 != 1).then_some(TEXT));
            rows += 1;
        }
    }
    assert_eq!(rows, ROWS);
}

#[test]
fn immediate_parquet_views_keep_compaction_for_large_backing_and_tight_budgets() {
    let (source, schema, _, session, _) = fixture(2 << 20);
    let (canonical, expanded) = admit(&source, &session);
    assert!(
        immediate_parquet_target(&canonical, &schema, expanded, 8 << 20)
            .unwrap()
            .is_none()
    );
    let (source, schema, _, session, _) = fixture(512);
    let (canonical, expanded) = admit(&source, &session);
    assert!(
        immediate_parquet_target(&canonical, &schema, expanded, expanded)
            .unwrap()
            .is_none()
    );
}

#[test]
fn immediate_parquet_views_leave_contiguous_and_nested_text_on_existing_targets() {
    use vortex::array::arrays::VarBinArray;
    let (source, _, _, session, _) = fixture(512);
    let dtype = DType::Utf8(Nullability::NonNullable);
    let contiguous =
        VarBinArray::from_iter_nonnull(std::iter::repeat_n(TEXT, ROWS), dtype).into_array();
    let array = StructArray::try_new(
        FieldNames::from(["contiguous", "nested"]),
        vec![contiguous, source],
        ROWS,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let DataType::Struct(fields) = data_type(array.dtype()).unwrap() else {
        panic!("struct");
    };
    let schema = Arc::new(Schema::new(fields));
    let (canonical, expanded) = admit(&array, &session);
    assert!(
        immediate_parquet_target(&canonical, &schema, expanded, 8 << 20)
            .unwrap()
            .is_none()
    );
}

#[test]
fn immediate_parquet_views_preserve_binary_nulls_beside_nested_text() {
    use arrow_array::{BinaryArray, BinaryViewArray};
    let (source, _, _, session, _) = fixture(512);
    let binary_value = [0xffu8; 32];
    let binary = VarBinViewArray::from_iter(
        (0..ROWS).map(|row| (row % 3 != 1).then_some(binary_value)),
        DType::Binary(Nullability::Nullable),
    )
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["binary", "nested"]),
        vec![binary, source],
        ROWS,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let DataType::Struct(fields) = data_type(array.dtype()).unwrap() else {
        panic!("struct");
    };
    let schema = Arc::new(Schema::new(fields));
    let (canonical, expanded) = admit(&array, &session);
    let (view_schema, _) = immediate_parquet_target(&canonical, &schema, expanded, 8 << 20)
        .unwrap()
        .expect("binary views admitted");
    assert_eq!(view_schema.field(0).data_type(), &DataType::BinaryView);
    assert_eq!(view_schema.field(1), schema.field(1));
    let arrow = session
        .arrow()
        .exporter(&ArrowExportOptions::default().with(CompactBuffers(false)))
        .execute_arrow(
            canonical,
            Some(&Field::new(
                "",
                DataType::Struct(view_schema.fields().clone()),
                false,
            )),
            &mut session.create_execution_ctx(),
        )
        .unwrap();
    let structure = arrow.as_any().downcast_ref::<ArrowStruct>().unwrap();
    assert!(structure.column(0).as_any().is::<BinaryViewArray>());
    let batch = RecordBatch::try_new(view_schema, structure.columns().to_vec()).unwrap();
    let mut encoded = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut encoded, schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(encoded),
    )
    .unwrap();
    assert_eq!(reader.schema(), &schema);
    let mut row = 0;
    for batch in reader.build().unwrap() {
        let batch = batch.unwrap();
        let binary = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let nested = batch
            .column(1)
            .as_any()
            .downcast_ref::<ArrowStruct>()
            .unwrap();
        let text = nested
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (binary, text) in binary.iter().zip(text.iter()) {
            assert_eq!(binary, (row % 3 != 1).then_some(binary_value.as_slice()));
            assert_eq!(text, (row % 3 != 1).then_some(TEXT));
            row += 1;
        }
    }
    assert_eq!(row, ROWS);
}
