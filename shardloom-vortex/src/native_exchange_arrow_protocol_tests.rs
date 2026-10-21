// SPDX-License-Identifier: Apache-2.0
use super::super::tests::{binding, change};
use super::*;
use arrow_array::{
    DictionaryArray, Int16Array, Int64Array, RecordBatch, StringArray, types::Int16Type,
};
use arrow_flight::{
    decode::FlightRecordBatchStream,
    encode::{DictionaryHandling, FlightDataEncoderBuilder},
};
use futures::TryStreamExt as _;
use std::sync::Arc;
use wire::ArrowPosition;

fn fixture_batch(empty: bool) -> RecordBatch {
    let dictionary = DictionaryArray::<Int16Type>::try_new(
        Int16Array::from(vec![
            Some(0),
            None,
            Some(1),
            Some(2),
            Some(2),
            Some(0),
            Some(1),
        ]),
        Arc::new(StringArray::from(vec![Some("λ\0猫"), None, Some("")])),
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "event_key",
            Arc::new(Int64Array::from(
                (0..7).map(|row| (1_i64 << 53) + row).collect::<Vec<_>>(),
            )) as arrow_array::ArrayRef,
        ),
        ("category", Arc::new(dictionary) as arrow_array::ArrayRef),
    ])
    .unwrap();
    if empty {
        RecordBatch::new_empty(batch.schema())
    } else {
        batch
    }
}

fn fixture(mode: Mode, empty: bool) -> (Binding, Expected, Vec<FlightData>, RecordBatch) {
    let batch = fixture_batch(empty);
    let compression = match mode {
        Mode::ArrowNone => None,
        Mode::ArrowLz4 => Some(arrow_ipc::CompressionType::LZ4_FRAME),
        Mode::ArrowZstd => Some(arrow_ipc::CompressionType::ZSTD),
        _ => panic!(),
    };
    let frames = futures::executor::block_on(
        FlightDataEncoderBuilder::new()
            .with_schema(batch.schema())
            .with_dictionary_handling(DictionaryHandling::Resend)
            .with_max_flight_data_size(1 << 20)
            .with_options(
                arrow_ipc::writer::IpcWriteOptions::default()
                    .try_with_compression(compression)
                    .unwrap(),
            )
            .build(futures::stream::iter([Ok(batch.clone())]))
            .try_collect::<Vec<_>>(),
    )
    .unwrap();
    let mut ordered = Sha256::new();
    let expected_frames = frames
        .iter()
        .map(|frame| {
            for bytes in [&frame.data_header, &frame.data_body] {
                ordered.update((bytes.len() as u64).to_le_bytes());
                ordered.update(bytes);
            }
            ExpectedFrame {
                header_bytes: frame.data_header.len(),
                header_sha256: hash(&frame.data_header),
                body_bytes: frame.data_body.len(),
                body_sha256: hash(&frame.data_body),
                metadata: inspect(frame, mode).unwrap(),
            }
        })
        .collect();
    let expected = Expected {
        frames: expected_frames,
        ordered_sha256: Bytes::copy_from_slice(&ordered.finalize()),
    };
    let mut binding = binding();
    binding.rows = batch.num_rows() as u64;
    let total = frames
        .iter()
        .map(|frame| frame.data_header.len() + frame.data_body.len())
        .sum::<usize>() as u64;
    let count = frames.len();
    let frames = frames
        .into_iter()
        .enumerate()
        .map(|(index, mut frame)| {
            let ack = (index == 0).then(|| Ack {
                profile: Some(profile()),
                mode: mode as i32,
                expected_payload_sha256: expected.ordered_sha256.clone(),
                expected_payload_bytes: total,
                expected_messages: u32::try_from(count).unwrap(),
            });
            let complete = (index + 1 == count).then(|| Complete {
                frames: count as u64,
                messages: u32::try_from(count).unwrap(),
                rows: binding.rows,
                payload_bytes: total,
                ordered_payload_sha256: expected.ordered_sha256.clone(),
            });
            frame.app_metadata = Frame {
                binding: Some(binding.clone()),
                sequence: index as u64,
                body: Some(Body::Arrow(ArrowPosition { ack, complete })),
            }
            .encode_to_vec()
            .into();
            frame
        })
        .collect();
    (binding, expected, frames, batch)
}

#[test]
fn all_standard_codecs_and_schema_only_empty_frames_keep_exact_values() {
    for mode in [Mode::ArrowNone, Mode::ArrowLz4, Mode::ArrowZstd] {
        for empty in [false, true] {
            let (binding, expected, frames, batch) = fixture(mode, empty);
            if empty {
                assert_eq!(frames.len(), 1);
            }
            let mut state = ArrowStream::new(binding, mode, expected).unwrap();
            for data in &frames {
                state.accept(data).unwrap();
            }
            state.finish(true).unwrap();
            // Application metadata stays ordinary Flight metadata. The unmodified
            // provider decoder consumes the unchanged header and body bytes.
            let mut decoder = FlightRecordBatchStream::new_from_flight_data(futures::stream::iter(
                frames.into_iter().map(Ok),
            ));
            let received = futures::executor::block_on(async {
                let mut batches = Vec::new();
                while let Some(batch) = decoder.try_next().await.unwrap() {
                    batches.push(batch);
                }
                batches
            });
            assert_eq!(decoder.schema().unwrap().as_ref(), batch.schema().as_ref());
            if empty {
                assert_eq!(received, [] as [RecordBatch; 0]);
            } else {
                assert_eq!(received, [batch]);
            }
        }
    }
}

#[test]
fn changed_compressed_bytes_and_identities_fail_before_allocating_decoder_admission() {
    for mode in [Mode::ArrowNone, Mode::ArrowLz4, Mode::ArrowZstd] {
        let (binding, expected, frames, _) = fixture(mode, false);
        let index = frames
            .iter()
            .position(|frame| !frame.data_body.is_empty())
            .unwrap();
        let mut corrupt = frames[index].clone();
        let mut bytes = corrupt.data_body.to_vec();
        bytes[0] ^= 1;
        corrupt.data_body = bytes.into();
        let wrong_identity = change(&frames[index], |frame| {
            frame.binding.as_mut().unwrap().principal = "reader2".into();
        });
        let wrong_order = change(&frames[index], |frame| frame.sequence += 1);
        let wrong_alternative = change(&frames[index], |frame| {
            frame.body = Some(Body::Complete(Complete::default()));
        });
        for changed in [corrupt, wrong_identity, wrong_order, wrong_alternative] {
            let mut state = ArrowStream::new(binding.clone(), mode, expected.clone()).unwrap();
            for data in &frames[..index] {
                state.accept(data).unwrap();
            }
            assert!(state.accept(&changed).is_err());
            assert!(state.accept(&frames[index]).is_err());
            assert!(state.finish(true).is_err());
        }
    }
}

#[test]
fn arrow_ack_completion_truncation_and_trailing_frames_are_strict() {
    let mode = Mode::ArrowLz4;
    let (binding, expected, frames, _) = fixture(mode, false);
    for prefix in 0..frames.len() {
        let mut state = ArrowStream::new(binding.clone(), mode, expected.clone()).unwrap();
        for data in &frames[..prefix] {
            state.accept(data).unwrap();
        }
        assert!(state.finish(true).is_err());
    }
    for successful_rpc in [false, true] {
        let mut state = ArrowStream::new(binding.clone(), mode, expected.clone()).unwrap();
        for data in &frames {
            state.accept(data).unwrap();
        }
        if successful_rpc {
            assert!(state.accept(frames.last().unwrap()).is_err());
        }
        assert!(state.finish(successful_rpc).is_err());
    }
    let first_changes: [fn(&mut Frame); 3] = [
        |frame| {
            let Some(Body::Arrow(position)) = &mut frame.body else {
                panic!()
            };
            position.ack = None;
        },
        |frame| {
            let Some(Body::Arrow(position)) = &mut frame.body else {
                panic!()
            };
            position.ack.as_mut().unwrap().mode = Mode::ArrowNone as i32;
        },
        |frame| {
            let Some(Body::Arrow(position)) = &mut frame.body else {
                panic!()
            };
            position.complete = Some(Complete::default());
        },
    ];
    for mutate in first_changes {
        let mut state = ArrowStream::new(binding.clone(), mode, expected.clone()).unwrap();
        assert!(state.accept(&change(&frames[0], mutate)).is_err());
    }
    let last_changes: [fn(&mut Frame); 3] = [
        |frame| {
            let Some(Body::Arrow(position)) = &mut frame.body else {
                panic!()
            };
            position.complete = None;
        },
        |frame| {
            let Some(Body::Arrow(position)) = &mut frame.body else {
                panic!()
            };
            position.complete.as_mut().unwrap().frames += 1;
        },
        |frame| {
            let Some(Body::Arrow(position)) = &mut frame.body else {
                panic!()
            };
            position.ack = Some(Ack::default());
        },
    ];
    for mutate in last_changes {
        let mut state = ArrowStream::new(binding.clone(), mode, expected.clone()).unwrap();
        for data in &frames[..frames.len() - 1] {
            state.accept(data).unwrap();
        }
        assert!(
            state
                .accept(&change(frames.last().unwrap(), mutate))
                .is_err()
        );
    }
}

fn metadata_frame(
    rows: i64,
    node: (i64, i64),
    range: (i64, i64),
    body: Vec<u8>,
    compressed: bool,
) -> FlightData {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let nodes = builder.create_vector(&[arrow_ipc::FieldNode::new(node.0, node.1)]);
    let buffers = builder.create_vector(&[arrow_ipc::Buffer::new(range.0, range.1)]);
    let compression = compressed.then(|| {
        arrow_ipc::BodyCompression::create(
            &mut builder,
            &arrow_ipc::BodyCompressionArgs {
                codec: arrow_ipc::CompressionType::LZ4_FRAME,
                method: arrow_ipc::BodyCompressionMethod::BUFFER,
            },
        )
    });
    let batch = arrow_ipc::RecordBatch::create(
        &mut builder,
        &arrow_ipc::RecordBatchArgs {
            length: rows,
            nodes: Some(nodes),
            buffers: Some(buffers),
            compression,
            ..Default::default()
        },
    );
    let message = arrow_ipc::Message::create(
        &mut builder,
        &arrow_ipc::MessageArgs {
            version: arrow_ipc::MetadataVersion::V5,
            header_type: arrow_ipc::MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: i64::try_from(body.len()).unwrap(),
            ..Default::default()
        },
    );
    builder.finish(message, None);
    FlightData {
        data_header: builder.finished_data().to_vec().into(),
        data_body: body.into(),
        ..Default::default()
    }
}

#[test]
fn arrow_metadata_refuses_negative_ranges_invalid_nodes_and_unbounded_declarations() {
    let ordinary = metadata_frame(7, (7, 0), (0, 8), vec![0; 8], false);
    assert_eq!(
        inspect(&ordinary, Mode::ArrowNone).unwrap().decoded_bytes,
        8
    );
    for frame in [
        metadata_frame(65_537, (7, 0), (0, 8), vec![0; 8], false),
        metadata_frame(-1, (7, 0), (0, 8), vec![0; 8], false),
        metadata_frame(7, (-1, 0), (0, 8), vec![0; 8], false),
        metadata_frame(7, (7, 8), (0, 8), vec![0; 8], false),
        metadata_frame(7, (7, 0), (-1, 8), vec![0; 8], false),
        metadata_frame(7, (7, 0), (0, -1), vec![0; 8], false),
        metadata_frame(7, (7, 0), (1, 8), vec![0; 8], false),
    ] {
        assert!(inspect(&frame, Mode::ArrowNone).is_err());
    }
    for body in [
        vec![0; 7],
        (-2_i64).to_le_bytes().to_vec(),
        i64::try_from(MAX_PAYLOAD + 1)
            .unwrap()
            .to_le_bytes()
            .to_vec(),
    ] {
        let frame = metadata_frame(
            7,
            (7, 0),
            (0, i64::try_from(body.len()).unwrap()),
            body,
            true,
        );
        assert!(inspect(&frame, Mode::ArrowLz4).is_err());
    }
    let mut raw = (-1_i64).to_le_bytes().to_vec();
    raw.extend_from_slice(b"rawbytes");
    let raw = metadata_frame(7, (7, 0), (0, i64::try_from(raw.len()).unwrap()), raw, true);
    assert_eq!(inspect(&raw, Mode::ArrowLz4).unwrap().decoded_bytes, 8);
    assert!(inspect(&raw, Mode::ArrowZstd).is_err());
    assert!(inspect(&raw, Mode::ArrowNone).is_err());
    let mut wrong_length = ordinary;
    wrong_length.data_body = Bytes::new();
    assert!(inspect(&wrong_length, Mode::ArrowNone).is_err());
}
