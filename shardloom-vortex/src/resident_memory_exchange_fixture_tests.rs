//! Complete independent inputs for the finite transport prerequisite.
//! This checks real consumers and codec controls, without a transport candidate.

use super::*;
use crate::{
    VortexLocalPrimitiveExecutionPolicy,
    local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
    relational_query::{VortexRelationalPlan, VortexRelationalProject, VortexRelationalScan},
};
use arrow_array::Array as _;
use arrow_flight::{
    FlightData,
    decode::FlightRecordBatchStream,
    encode::{DictionaryHandling, FlightDataEncoderBuilder},
};
use arrow_ipc::{
    CompressionType,
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt as _;
use prost::Message as _;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use shardloom_core::{
    ColumnRef, DatasetUri, ExecutionResourceOrigin, ExecutionResources, ExprId, Expression,
    ExpressionKind,
};
use shardloom_plan::ProjectionRequest;
use std::{io::Cursor, path::Path};
use vortex::array::{
    ArrayRef, VortexSessionExecute as _,
    arrays::{Chunked, Struct, chunked::ChunkedArrayExt as _, struct_::StructArraySlotsExt as _},
    dtype::DType,
};

const GRANT: u64 = 256 << 20;

fn hash(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut text = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(text, "{byte:02x}").unwrap();
    }
    text
}

fn ordered_hash<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> String {
    use std::fmt::Write as _;
    let mut state = Sha256::new();
    for part in parts {
        state.update(u64::try_from(part.len()).unwrap().to_le_bytes());
        state.update(part);
    }
    let mut text = String::with_capacity(64);
    for byte in state.finalize() {
        write!(text, "{byte:02x}").unwrap();
    }
    text
}

fn input(root: &Path, record: &Value) -> Vec<u8> {
    let path = root.join(record["file"].as_str().unwrap());
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(bytes.len() as u64, record["bytes"].as_u64().unwrap());
    assert_eq!(hash(&bytes), record["sha256"].as_str().unwrap());
    bytes
}

fn projections(source: &ResidentMemorySource, case: &Value) -> Vec<Value> {
    let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(GRANT, 1, ExecutionResourceOrigin::Context).unwrap(),
    )
    .unwrap();
    let uri = DatasetUri::new("memory://independent-exchange-fixture").unwrap();
    let queries = case["queries"].as_array().unwrap();
    assert_eq!(queries.len(), 3);
    let prepared = queries
        .iter()
        .map(|query| {
            prepare_relational_with_schema_in_session(policy, &source.0.session, |schemas| {
                schemas.register_memory_source(uri.clone(), |_| Ok(source.clone()))?;
                let input = VortexRelationalPlan::Scan(VortexRelationalScan {
                    source_uri: uri.clone(),
                    projection: ProjectionRequest::All,
                    predicate: None,
                });
                Ok(VortexRelationalPlan::Project(Box::new(
                    VortexRelationalProject {
                        input,
                        expressions: query["projection"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .enumerate()
                            .map(|(index, field)| {
                                (
                                    field["alias"].as_str().unwrap().to_owned(),
                                    Expression::new(
                                        ExprId::new(format!("exchange-field-{index}")).unwrap(),
                                        ExpressionKind::Column(
                                            ColumnRef::new(field["source"].as_str().unwrap())
                                                .unwrap(),
                                        ),
                                    ),
                                )
                            })
                            .collect(),
                    },
                )))
            })
            .unwrap()
        })
        .collect::<Vec<_>>();
    let before = source.0.session.snapshot().completed_executions;
    let mut records = Vec::new();
    for index in [0, 1, 2, 1] {
        let expected = &queries[index];
        let result = prepared[index]
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(result.execution.output_rows, case["rows"].as_u64().unwrap());
        let actual_schema: Value = serde_json::from_str(result.result_schema_json.value()).unwrap();
        assert_eq!(actual_schema, expected["dtype"]);
        let actual = result.result_jsonl.value().as_bytes();
        assert_eq!(
            actual.len() as u64,
            expected["jsonl_bytes"].as_u64().unwrap()
        );
        assert_eq!(
            hash(actual),
            expected["jsonl_sha256"].as_str().unwrap(),
            "{}/{}",
            case["id"],
            expected["id"]
        );
        let certificate = &result.execution.native_io_certificate;
        assert!(certificate.is_certified());
        assert!(!certificate.side_effects.fallback_attempted);
        records.push(
            json!({"query": expected["id"], "rows": result.execution.output_rows,
            "complete_jsonl_sha256": hash(actual), "dtype": actual_schema,
            "certified": true, "fallback_attempted": false}),
        );
    }
    assert_eq!(source.0.session.snapshot().completed_executions, before + 4);
    records
}

fn target_type(dtype: &DType, path: &[String], preserving: &[Value]) -> DataType {
    if let Some(field) = preserving.iter().find(|field| field["path"] == json!(path)) {
        let value = crate::native_arrow_conversion::data_type(dtype).unwrap();
        return match field["arrow"].as_str().unwrap() {
            "Dictionary" => {
                assert_eq!(field["keys"], "UInt16");
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(value))
            }
            "RunEndEncoded" => {
                assert_eq!(field["ends"], "Int32");
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", value, dtype.is_nullable())),
                )
            }
            other => panic!("unregistered preserving control {other}"),
        };
    }
    if let DType::Struct(fields, _) = dtype {
        return DataType::Struct(
            fields
                .names()
                .iter()
                .zip(fields.fields())
                .map(|(name, dtype)| {
                    let mut path = path.to_vec();
                    path.push(name.to_string());
                    Field::new(
                        name.to_string(),
                        target_type(&dtype, &path, preserving),
                        dtype.is_nullable(),
                    )
                })
                .collect::<Vec<_>>()
                .into(),
        );
    }
    crate::native_arrow_conversion::data_type(dtype).unwrap()
}

fn export(source: &ResidentMemorySource, case: &Value) -> RecordBatch {
    let target = target_type(
        source.dtype(),
        &[],
        case["arrow_preserving_fields"].as_array().unwrap(),
    );
    let DataType::Struct(fields) = target.clone() else {
        panic!("expected record schema")
    };
    let schema = Arc::new(Schema::new(fields));
    source
        .0
        .session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let target = Field::new("", target, false);
            let array = context
                .native_session()
                .arrow()
                .execute_arrow(source.0.array.clone(), Some(&target), &mut execution)
                .map_err(native_error)?;
            array.to_data().validate_full().map_err(native_error)?;
            let structure = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap();
            RecordBatch::try_new(schema, structure.columns().to_vec()).map_err(native_error)
        })
        .unwrap()
}

fn encodings_at_path(array: &ArrayRef, path: &[Value]) -> Vec<String> {
    if let Some(chunked) = array.as_opt::<Chunked>() {
        return chunked
            .chunks()
            .iter()
            .flat_map(|chunk| encodings_at_path(chunk, path))
            .collect();
    }
    let Some((head, tail)) = path.split_first() else {
        return vec![array.encoding_id().as_str().to_owned()];
    };
    let structure = array.as_::<Struct>();
    let index = array
        .dtype()
        .as_struct_fields()
        .names()
        .iter()
        .position(|name| name.as_ref() == head.as_str().unwrap())
        .unwrap();
    encodings_at_path(&structure.fields()[index], tail)
}

fn preserving_ids(source: &ResidentMemorySource, case: &Value) -> Vec<Value> {
    case["arrow_preserving_fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| {
            let path = field["path"].as_array().unwrap();
            let actual = encodings_at_path(&source.0.array, path);
            let expected = match field["arrow"].as_str().unwrap() {
                "Dictionary" => "vortex.dict",
                "RunEndEncoded" => "vortex.runend",
                other => panic!("unknown preserving encoding {other}"),
            };
            assert_ne!(actual, Vec::<String>::new());
            assert!(
                actual.iter().all(|id| id == expected),
                "{actual:?} != {expected}"
            );
            json!({"path": path, "native_encodings_below_chunk_containers": actual})
        })
        .collect()
}

fn native_ids(source: &ResidentMemorySource, case: &Value) -> Vec<Value> {
    case["native_preserving_fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| {
            let path = field["path"].as_array().unwrap();
            let actual = encodings_at_path(&source.0.array, path);
            let expected = field["encoding"].as_str().unwrap();
            assert_ne!(actual, Vec::<String>::new());
            assert!(
                actual.iter().all(|id| id == expected),
                "{actual:?} != {expected}"
            );
            json!({"path": path, "native_encodings_below_chunk_containers": actual})
        })
        .collect()
}

fn native_ipc_control(source: &ResidentMemorySource, case: &Value) -> Value {
    use vortex::ipc::messages::{EncoderMessage, MessageEncoder};
    let messages = source
        .0
        .session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut encoder = MessageEncoder::new(context.native_session().clone());
            let dtype = encoder
                .encode(EncoderMessage::DType(source.dtype()))
                .map_err(native_error)?;
            let array = encoder
                .encode(EncoderMessage::Array(&source.0.array))
                .map_err(native_error)?;
            Ok([dtype, array])
        })
        .unwrap();
    let bytes = messages
        .iter()
        .map(|parts| parts.concat())
        .collect::<Vec<_>>();
    assert!(bytes.iter().map(Vec::len).sum::<usize>() <= 64 << 20);
    let receipt = messages
        .iter()
        .zip(&bytes)
        .map(|(parts, bytes)| {
            json!({"bytes": bytes.len(), "sha256": hash(bytes),
            "provider_part_lengths": parts.iter().map(bytes::Bytes::len).collect::<Vec<_>>()})
        })
        .collect::<Vec<_>>();
    let ordered_payload_sha256 = ordered_hash(bytes.iter().map(Vec::as_slice));
    let payload_bytes = bytes.iter().map(Vec::len).sum::<usize>();
    let received = ResidentMemorySource::from_vortex_ipc(
        &source.0.session,
        &bytes[0],
        &bytes[1],
        &CancellationToken::default(),
    )
    .unwrap();
    drop((messages, bytes));
    assert_eq!(received.dtype(), source.dtype());
    let preserved = native_ids(&received, case);
    let queries = projections(&received, case);
    json!({"messages": receipt, "ordered_payload_sha256": ordered_payload_sha256,
        "payload_bytes": payload_bytes, "preserved_encodings": preserved, "queries": queries})
}

fn codec_controls(source: &ResidentMemorySource, case: &Value) -> Vec<Value> {
    let exported = export(source, case);
    let schema = exported.schema();
    let mut records = Vec::new();
    for (name, compression) in [
        ("none", None),
        ("lz4", Some(CompressionType::LZ4_FRAME)),
        ("zstd", Some(CompressionType::ZSTD)),
    ] {
        let options = IpcWriteOptions::default()
            .try_with_compression(compression)
            .unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
            writer.write(&exported).unwrap();
            writer.finish().unwrap();
        }
        let wire_bytes = bytes.len();
        let wire_sha256 = hash(&bytes);
        let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.schema(), schema);
        assert!(reader.next().is_none());
        let received = ResidentMemorySource::from_arrow_record_batch(
            &source.0.session,
            batch,
            &CancellationToken::default(),
        )
        .unwrap();
        assert_eq!(received.dtype(), source.dtype());
        let preserved = preserving_ids(&received, case);
        let queries = projections(&received, case);
        records.push(
            json!({"codec": name, "ipc_stream_bytes": wire_bytes, "ipc_stream_sha256": wire_sha256,
            "preserved_encodings": preserved, "queries": queries}),
        );
    }
    records
}

fn frame_admission(frame: &FlightData) -> Value {
    assert!(frame.encoded_len() + (16 << 10) <= 2 << 20);
    assert!(frame.data_header.len() <= 64 << 10);
    assert!(frame.app_metadata.is_empty());
    assert!(frame.flight_descriptor.is_none());
    let message = arrow_ipc::root_as_message(&frame.data_header).unwrap();
    assert_eq!(
        usize::try_from(message.bodyLength()).unwrap(),
        frame.data_body.len()
    );
    let batch = message.header_as_record_batch().or_else(|| {
        message.header_as_dictionary_batch().map(|dictionary| {
            assert!(!dictionary.isDelta());
            dictionary.data().unwrap()
        })
    });
    let mut decoded_bytes = 0_u64;
    let mut buffers = 0;
    let mut nodes = 0;
    if let Some(batch) = batch {
        assert!(usize::try_from(batch.length()).unwrap() <= 65_536);
        nodes = batch.nodes().unwrap().len();
        assert!(nodes <= 4096);
        for node in batch.nodes().unwrap() {
            assert!(node.length() >= 0);
            assert!((0..=node.length()).contains(&node.null_count()));
        }
        for buffer in batch.buffers().unwrap() {
            buffers += 1;
            let start = usize::try_from(buffer.offset()).unwrap();
            let len = usize::try_from(buffer.length()).unwrap();
            let end = start.checked_add(len).unwrap();
            let data = frame.data_body.get(start..end).unwrap();
            let bytes = if batch.compression().is_some() && !data.is_empty() {
                let length = i64::from_le_bytes(data[..8].try_into().unwrap());
                if length == -1 {
                    u64::try_from(data.len() - 8).unwrap()
                } else {
                    u64::try_from(length).unwrap()
                }
            } else {
                u64::try_from(data.len()).unwrap()
            };
            decoded_bytes = decoded_bytes.checked_add(bytes).unwrap();
        }
        assert!(buffers <= 4096 && decoded_bytes <= 64 << 20);
    } else {
        assert_eq!(message.header_type(), arrow_ipc::MessageHeader::Schema);
        assert!(frame.data_body.is_empty());
    }
    json!({"encoded_bytes": frame.encoded_len(), "protobuf_sha256": hash(&frame.encode_to_vec()),
        "header_bytes": frame.data_header.len(), "header_sha256": hash(&frame.data_header),
        "body_bytes": frame.data_body.len(), "body_sha256": hash(&frame.data_body),
        "header_kind": message.header_type().variant_name().unwrap(),
        "declared_decoded_buffer_bytes": decoded_bytes, "buffers": buffers, "nodes": nodes})
}

fn flight_controls(source: &ResidentMemorySource, case: &Value) -> Vec<Value> {
    let exported = export(source, case);
    let mut records = Vec::new();
    for (name, compression) in [
        ("none", None),
        ("lz4", Some(CompressionType::LZ4_FRAME)),
        ("zstd", Some(CompressionType::ZSTD)),
    ] {
        let options = IpcWriteOptions::default()
            .try_with_compression(compression)
            .unwrap();
        let frames = futures::executor::block_on(
            FlightDataEncoderBuilder::new()
                .with_schema(exported.schema())
                .with_dictionary_handling(DictionaryHandling::Resend)
                .with_max_flight_data_size(1 << 20)
                .with_options(options)
                .build(futures::stream::iter(vec![Ok(exported.clone())]))
                .try_collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(!frames.is_empty() && frames.len() <= 256);
        let admission = frames.iter().map(frame_admission).collect::<Vec<_>>();
        let wire_bytes: usize = frames.iter().map(prost::Message::encoded_len).sum();
        let ordered_payload_sha256 = ordered_hash(
            frames
                .iter()
                .flat_map(|frame| [frame.data_header.as_ref(), frame.data_body.as_ref()]),
        );
        let payload_bytes = frames
            .iter()
            .map(|frame| frame.data_header.len() + frame.data_body.len())
            .sum::<usize>();
        assert!(wire_bytes <= 64 << 20);
        let decoded_sum = admission
            .iter()
            .map(|value| value["declared_decoded_buffer_bytes"].as_u64().unwrap())
            .sum::<u64>();
        assert!(decoded_sum <= 64 << 20);
        let mut received = super::super::MemoryBatchSourceBuilder::new(
            &source.0.session,
            CancellationToken::default(),
        )
        .unwrap();
        let received_schema = futures::executor::block_on(async {
            let mut decoder = FlightRecordBatchStream::new_from_flight_data(futures::stream::iter(
                frames.into_iter().map(Ok),
            ));
            let mut batches = 0;
            while let Some(batch) = decoder.try_next().await.unwrap() {
                assert_eq!(batch.schema(), exported.schema());
                received.push_arrow(batch).unwrap();
                batches += 1;
            }
            let schema = decoder.schema().unwrap().clone();
            if batches == 0 {
                assert_eq!(case["rows"], 0);
                received
                    .push_arrow(RecordBatch::new_empty(schema.clone()))
                    .unwrap();
            }
            schema
        });
        assert_eq!(received_schema, exported.schema());
        let received = received.finish().unwrap();
        assert_eq!(received.dtype(), source.dtype());
        let preserved = preserving_ids(&received, case);
        let queries = projections(&received, case);
        records.push(json!({"codec": name, "dictionary_handling": "Resend", "max_flight_data_size": 1 << 20,
            "standard_flight_protobuf_bytes": wire_bytes, "declared_decoded_buffer_bytes": decoded_sum,
            "ordered_payload_sha256": ordered_payload_sha256, "payload_bytes": payload_bytes,
            "frames": admission, "preserved_encodings": preserved, "queries": queries}));
    }
    records
}

#[test]
#[ignore = "requires independently generated local fixtures and the sequential storage/process guard"]
fn independent_exchange_fixtures_and_encoded_arrow_controls() {
    let root = std::env::var_os("SHARDLOOM_EXCHANGE_FIXTURES").expect("fixture root required");
    let root = Path::new(&root);
    let manifest_bytes = std::fs::read(root.join("fixture-manifest.json")).unwrap();
    let manifest: Value = serde_json::from_slice(&manifest_bytes).unwrap();
    let cases = manifest["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 13);
    let mut results = Vec::new();
    for case in cases {
        let dtype = input(root, &case["dtype_message"]);
        let array = input(root, &case["array_message"]);
        let session = ResidentVortexSession::new(GRANT, 1).unwrap();
        let source = ResidentMemorySource::from_vortex_ipc(
            &session,
            &dtype,
            &array,
            &CancellationToken::default(),
        )
        .unwrap();
        drop((dtype, array));
        assert_eq!(source.row_count() as u64, case["rows"].as_u64().unwrap());
        assert_eq!(serde_json::to_value(source.dtype()).unwrap(), case["dtype"]);
        let preserved = native_ids(&source, case);
        let intake = source.intake_transfer_report();
        assert_eq!(intake.materialized_batches, 0);
        assert_eq!(intake.materialized_rows, 0);
        // Freeze ordinary provider serialization before executing any query on
        // the original source; query-time caches must not define a wire input.
        let native_ipc = native_ipc_control(&source, case);
        let native = projections(&source, case);
        let arrow = codec_controls(&source, case);
        let flight = flight_controls(&source, case);
        assert_eq!(source.intake_transfer_report(), intake);
        drop(source);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        let record = json!({"id": case["id"], "native_preserved_encodings": preserved,
            "native_queries": native, "arrow_ipc_controls": arrow, "final_reserved_bytes": 0,
            "ordinary_native_ipc_control": native_ipc,
            "arrow_flight_controls": flight,
            "native_intake": {"complete_report": format!("{intake:?}"),
                "materialized_batches": intake.materialized_batches, "materialized_rows": intake.materialized_rows,
                "wire_bytes": intake.ipc_transport_bytes, "ipc_copy_bytes": intake.ipc_buffer_bytes_copied,
                "retained_copy_bytes": intake.encoded_buffer_bytes_copied,
                "reused_buffer_references": intake.reused_buffer_references}});
        println!("accepted fixture {}", case["id"]);
        results.push(record);
    }
    let report = json!({"version": 1, "scope": "independent fixture and encoded Arrow IPC prerequisite; no transport or performance claim",
        "fixture_manifest_sha256": hash(&manifest_bytes), "cases": results});
    let report_path =
        std::env::var_os("SHARDLOOM_EXCHANGE_FIXTURE_REPORT").expect("new report path required");
    let mut output = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(report_path)
        .unwrap();
    serde_json::to_writer_pretty(&mut output, &report).unwrap();
}
