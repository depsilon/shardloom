//! UUID keeps canonical logical identity across native and terminal owners.

use super::*;
use arrow_array::FixedSizeBinaryArray;

fn fixture() -> RecordBatch {
    let values = [
        Some([42; 16]),
        Some([0; 16]),
        Some([255; 16]),
        None,
        Some(0x01234567_89ab_cdef_fedc_ba9876543210_u128.to_be_bytes()),
        Some(0x01234567_89ab_cdef_fedc_ba9876543211_u128.to_be_bytes()),
        Some([0; 16]),
        Some([42; 16]),
    ];
    let array =
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 16).unwrap();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![crate::native_uuid::arrow::field(
            "id", true,
        )])),
        vec![Arc::new(array)],
    )
    .unwrap()
    .slice(1, 6)
}

#[test]
fn native_uuid_arrow_slice_nulls_and_final_owner_preserve_all_bytes() {
    let input = fixture();
    for count in [0, input.num_rows()] {
        let expected = input.slice(0, count);
        let (result, session) = native(expected.clone(), 32 << 20);
        let memory = session.memory().clone();
        let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
        assert_eq!(stream.schema(), expected.schema());
        let output = stream.next_batch().unwrap();
        if count == 0 {
            assert!(output.as_ref().is_none_or(|batch| batch.num_rows() == 0));
        } else {
            let output = output.unwrap();
            assert_eq!(output, expected);
            let retained = output.column(0).clone();
            drop((stream, output, result, session));
            assert!(memory.snapshot().reserved_bytes > 0);
            assert_eq!(retained.as_fixed_size_binary().value(1), &[255; 16]);
            assert!(retained.is_null(2));
            drop(retained);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            continue;
        }
        drop((stream, output, result, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_uuid_arrow_empty_invalid_declarations_fail_before_publication() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let canonical = crate::native_uuid::arrow::field("id", true);
    let mut versioned = canonical.metadata().clone();
    versioned.insert("ARROW:extension:metadata".into(), "4".into());
    let mut foreign = canonical.metadata().clone();
    foreign.insert("ARROW:extension:name".into(), "other.uuid".into());
    for field in [
        canonical.clone().with_metadata(versioned),
        canonical.clone().with_metadata(foreign),
        canonical
            .clone()
            .with_data_type(DataType::FixedSizeBinary(15)),
        Field::new("id", DataType::FixedSizeBinary(16), true),
    ] {
        let column = arrow_array::new_empty_array(field.data_type());
        let input = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![column]).unwrap();
        assert!(
            ResidentMemorySource::from_arrow_record_batch(
                &session,
                input,
                &CancellationToken::default()
            )
            .is_err()
        );
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![canonical])),
        vec![Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [None::<[u8; 16]>; 3].into_iter(),
                16,
            )
            .unwrap(),
        )],
    )
    .unwrap();
    let (result, owner) = native(input.clone(), 8 << 20);
    let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
    assert_eq!(stream.next_batch().unwrap().unwrap(), input);
    drop((stream, result));
    assert_eq!(owner.memory().snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)]
fn native_uuid_write_reopen_vortex_arrow_and_parquet_keep_values_and_empty_type() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    use crate::local_primitives::prepared_relational::prepare_relational;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "shardloom-uuid-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&directory).unwrap();
    for count in [6, 0] {
        let input = fixture().slice(0, count);
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
        let uri = DatasetUri::new("memory://uuid-write").unwrap();
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
            let written = prepared.write(&path, format, false).unwrap();
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
