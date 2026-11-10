//! Variant values cross the same native query and persistence boundaries as
//! other retained payloads. Compare complete encodings, not a JSON projection.

use super::*;
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use parquet_variant::{Variant, VariantBuilder};

fn structure(items: Vec<(&str, ArrowArrayRef)>) -> ArrowArrayRef {
    let fields = items
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect::<Vec<_>>();
    Arc::new(arrow_array::StructArray::new(
        fields.into(),
        items.into_iter().map(|(_, array)| array).collect(),
        None,
    ))
}

fn raw_fixture() -> RecordBatch {
    let encoded = [
        Variant::Int64(i64::MIN),
        Variant::Int64(i64::MAX),
        Variant::Null,
        Variant::BooleanTrue,
        Variant::Double(-0.0),
        Variant::Float(f32::from_bits(0x7fc0_0042)),
        Variant::Double(f64::from_bits(0x7ff8_0000_0000_0042)),
        Variant::Binary(&[0, 255, 128]),
        Variant::from("東京\nquoted\""),
        Variant::Null,
    ]
    .into_iter()
    .map(|value| VariantBuilder::new().with_value(value).finish())
    .collect::<Vec<_>>();
    let field = crate::native_variant::arrow::field("payload", true);
    let DataType::Struct(fields) = field.data_type() else {
        unreachable!()
    };
    let array: ArrowArrayRef = Arc::new(arrow_array::StructArray::new(
        fields.clone(),
        vec![
            Arc::new(BinaryArray::from_iter_values(
                encoded.iter().map(|row| row.0.as_slice()),
            )),
            Arc::new(BinaryArray::from_iter_values(
                encoded.iter().map(|row| row.1.as_slice()),
            )),
        ],
        Some(
            (0..encoded.len())
                .map(|row| row + 1 != encoded.len())
                .collect(),
        ),
    ));
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
}

#[test]
fn native_variant_shredded_lists_and_nested_null_ancestors_keep_complete_values() {
    let items = structure(vec![
        (
            "value",
            Arc::new(BinaryArray::from(vec![
                None,
                Some(&[0][..]),
                None,
                Some(&[255][..]),
                Some(&[255][..]),
                Some(&[255][..]),
            ])),
        ),
        (
            "typed_value",
            Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(-7),
                None,
                None,
                None,
            ])),
        ),
    ]);
    let typed: ArrowArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", items.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 3, 3, 5, 6])),
        items,
        Some(vec![true, true, false, true].into()),
    ));
    let storage = structure(vec![
        (
            "metadata",
            Arc::new(BinaryArray::from_vec(vec![&[1, 0, 0][..]; 4])),
        ),
        ("typed_value", typed),
    ]);
    // Metadata is required by the extension. A containing null struct hides
    // the malformed last row before the Variant validator or provider sees it.
    let payload_field =
        crate::native_variant::arrow::field("payload", true).with_data_type(DataType::Struct(
            vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new(
                    "typed_value",
                    storage.as_struct().column(1).data_type().clone(),
                    true,
                ),
            ]
            .into(),
        ));
    let payload: ArrowArrayRef = Arc::new(arrow_array::StructArray::new(
        match payload_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        storage.as_struct().columns().to_vec(),
        None,
    ));
    let containing: ArrowArrayRef = Arc::new(arrow_array::StructArray::new(
        vec![payload_field].into(),
        vec![payload],
        Some(vec![true, true, true, false].into()),
    ));
    let input = batch(vec![("detail", containing)], true);
    let (result, session) = native(input, 32 << 20);
    let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
    let output = stream.next_batch().unwrap().unwrap();
    let detail = output.column(0).as_struct();
    assert!(crate::native_variant::arrow::is_variant(
        &detail.fields()[0]
    ));
    let payload = detail.column(0).as_struct();
    let decode = |row| {
        Variant::try_new(
            payload.column(0).as_binary::<i32>().value(row),
            payload.column(1).as_binary::<i32>().value(row),
        )
        .unwrap()
    };
    assert_eq!(
        decode(0).as_list().unwrap().iter().collect::<Vec<_>>(),
        vec![Variant::Int64(1), Variant::Null, Variant::Int64(-7)]
    );
    assert!(decode(1).as_list().unwrap().is_empty());
    assert!(payload.is_valid(2) && payload.column(1).is_null(2));
    assert!(detail.is_null(3));
    drop((stream, output, result));
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)]
fn native_variant_raw_write_reopen_arrow_and_parquet_preserve_every_byte_and_empty_type() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    use crate::local_primitives::prepared_relational::{
        prepare_relational, prepare_relational_with_schema_in_session,
    };
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "shardloom-variant-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&directory).unwrap();
    let input = raw_fixture();
    for count in [input.num_rows(), 0] {
        let input = input.slice(0, count);
        let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
        let source = ResidentMemorySource::from_arrow_record_batch(
            &session,
            input.clone(),
            &CancellationToken::default(),
        )
        .unwrap();
        let resources =
            ExecutionResources::from_bytes(64 << 20, 1, ExecutionResourceOrigin::ExecutionCall)
                .unwrap();
        let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(resources).unwrap();
        let uri = DatasetUri::new("memory://variant-write").unwrap();
        let prepared = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
            schemas.register_memory_source(uri.clone(), |_| Ok(source))?;
            Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: uri,
                projection: ProjectionRequest::All,
                predicate: None,
            }))
        })
        .unwrap();
        for format in [Format::Vortex, Format::ArrowIpc, Format::Parquet] {
            let path = directory.join(format!("{count}.{}", format.as_str()));
            let written = prepared
                .write(&path, format, false)
                .unwrap_or_else(|error| panic!("{format:?} {count}: {error}"));
            assert_eq!(written.output.rows_written, count as u64);
            assert!(written.execution.native_io_certificate.is_certified());
            let batches = match format {
                Format::Vortex => {
                    let reopened = prepare_relational(
                        &VortexRelationalPlan::Scan(VortexRelationalScan {
                            source_uri: DatasetUri::new(path.display().to_string()).unwrap(),
                            projection: ProjectionRequest::All,
                            predicate: None,
                        }),
                        policy,
                    )
                    .unwrap();
                    assert_eq!(reopened.output_dtype(), prepared.output_dtype());
                    let result = Arc::new(
                        reopened
                            .execute_native(&CancellationToken::default())
                            .unwrap()
                            .result,
                    );
                    let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
                    assert_eq!(stream.schema(), input.schema());
                    let mut batches = Vec::new();
                    while let Some(batch) = stream.next_batch().unwrap() {
                        batches.push(batch);
                    }
                    batches
                }
                Format::ArrowIpc => {
                    let reader = arrow_ipc::reader::FileReader::try_new(
                        fs::File::open(&path).unwrap(),
                        None,
                    )
                    .unwrap();
                    assert_eq!(reader.schema(), input.schema());
                    reader.collect::<std::result::Result<Vec<_>, _>>().unwrap()
                }
                Format::Parquet => {
                    let builder =
                        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                            fs::File::open(&path).unwrap(),
                        )
                        .unwrap();
                    assert_eq!(builder.schema(), &input.schema());
                    builder
                        .build()
                        .unwrap()
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .unwrap()
                }
                _ => unreachable!(),
            };
            let mut offset = 0;
            for actual in batches {
                assert_eq!(
                    actual,
                    input.slice(offset, actual.num_rows()),
                    "{format:?} {count}"
                );
                offset += actual.num_rows();
            }
            assert_eq!(offset, count);
        }
        drop(prepared);
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
    fs::remove_dir_all(directory).unwrap();
}
