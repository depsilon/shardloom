// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    reader::{decoder_memory_tests::Memory, read_schema_with_memory},
    writer::*,
};
use arrow_array::{types::Int8Type, *};

fn batch(values: &[&str], keys: Vec<Option<i8>>) -> RecordBatch {
    let column = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(keys),
        Arc::new(StringArray::from(values.to_vec())),
    )
    .unwrap();
    RecordBatch::try_from_iter([("value", Arc::new(column) as ArrayRef)]).unwrap()
}

fn message(encoded: EncodedData, options: &IpcWriteOptions) -> (crate::Block, Buffer) {
    let mut output = Vec::new();
    let (metadata, body) = write_message(&mut output, encoded, options).unwrap();
    (
        crate::Block::new(
            0,
            i32::try_from(metadata).unwrap(),
            i64::try_from(body).unwrap(),
        ),
        Buffer::from_vec(output),
    )
}

#[test]
fn owned_dictionary_delta_replacement_and_decoder_drop_keep_complete_prior_batches() {
    let options = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let batches = [
        batch(&["first", "second"], vec![Some(1), None, Some(0)]),
        batch(&["first", "second", "third"], vec![Some(2), Some(0), None]),
        batch(&["replacement", "other"], vec![Some(0), None, Some(1)]),
    ];
    let encoded = generator.schema_to_bytes_with_dictionary_tracker(
        &batches[0].schema(),
        &mut tracker,
        &options,
    );
    let header = crate::root_as_message(&encoded.ipc_message).unwrap();
    let memory = Memory::new(1 << 20);
    let schema = read_schema_with_memory(header.header_as_schema().unwrap(), &memory).unwrap();
    let mut decoder = OwnedFileDecoder::new(
        schema,
        header.version(),
        Some(&[0, 0]),
        Arc::new(memory.clone()),
    )
    .unwrap();
    let mut outputs = Vec::new();
    let mut modes = Vec::new();
    for batch in &batches {
        let (dictionaries, encoded) = generator
            .encode(batch, &mut tracker, &options, &mut Default::default())
            .unwrap();
        assert_eq!(dictionaries.len(), 1);
        for dictionary in dictionaries {
            modes.push(
                crate::root_as_message(&dictionary.ipc_message)
                    .unwrap()
                    .header_as_dictionary_batch()
                    .unwrap()
                    .isDelta(),
            );
            let (block, data) = message(dictionary, &options);
            decoder.read_dictionary(&block, &data).unwrap();
        }
        let (block, data) = message(encoded, &options);
        outputs.push(decoder.read_record_batch(&block, &data).unwrap().unwrap());
    }
    assert_eq!(modes, vec![false, true, false]);
    drop(decoder);
    drop(encoded);
    for (output, expected) in outputs.iter().zip(&batches) {
        assert_eq!(output.batch(), &expected.project(&[0, 0]).unwrap());
    }
    assert!(memory.current() > 0);
    drop(outputs);
    assert_eq!(memory.current(), 0);
}

#[test]
fn failed_dictionary_delta_is_atomic_and_retry_succeeds_under_same_owner() {
    let options = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let first = batch(&["first"], vec![Some(0)]);
    let next = batch(&["first", "next"], vec![Some(1)]);
    let schema_message =
        generator.schema_to_bytes_with_dictionary_tracker(&first.schema(), &mut tracker, &options);
    let header = crate::root_as_message(&schema_message.ipc_message).unwrap();
    let memory = Memory::new(1 << 20);
    let schema = read_schema_with_memory(header.header_as_schema().unwrap(), &memory).unwrap();
    let mut decoder =
        OwnedFileDecoder::new(schema, header.version(), None, Arc::new(memory.clone())).unwrap();
    let (dictionaries, encoded) = generator
        .encode(&first, &mut tracker, &options, &mut Default::default())
        .unwrap();
    for dictionary in dictionaries {
        let (block, data) = message(dictionary, &options);
        decoder.read_dictionary(&block, &data).unwrap();
    }
    let (first_block, first_data) = message(encoded, &options);
    let (dictionaries, next_encoded) = generator
        .encode(&next, &mut tracker, &options, &mut Default::default())
        .unwrap();
    let (delta_block, delta_data) = message(dictionaries.into_iter().next().unwrap(), &options);
    let held = memory.current();
    memory.set_limit(held);
    assert!(
        decoder
            .read_dictionary(&delta_block, &delta_data)
            .unwrap_err()
            .to_string()
            .contains("denied")
    );
    assert_eq!(memory.current(), held);
    memory.set_limit(1 << 20);
    assert_eq!(
        decoder
            .read_record_batch(&first_block, &first_data)
            .unwrap()
            .unwrap()
            .batch(),
        &first
    );
    decoder.read_dictionary(&delta_block, &delta_data).unwrap();
    let (block, data) = message(next_encoded, &options);
    assert_eq!(
        decoder
            .read_record_batch(&block, &data)
            .unwrap()
            .unwrap()
            .batch(),
        &next
    );
    drop(decoder);
    assert_eq!(memory.current(), 0);
}

#[test]
fn separate_message_inputs_preserve_delta_values_and_reject_extent_and_version_mismatch() {
    let options = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let batches = [
        batch(&["first"], vec![Some(0), None]),
        batch(&["first", "second"], vec![Some(1), None, Some(0)]),
    ];
    let encoded_schema = generator.schema_to_bytes_with_dictionary_tracker(
        &batches[0].schema(),
        &mut tracker,
        &options,
    );
    let header = crate::root_as_message(&encoded_schema.ipc_message).unwrap();
    let memory = Memory::new(1 << 20);
    let schema = read_schema_with_memory(header.header_as_schema().unwrap(), &memory).unwrap();
    let mut wrong = OwnedFileDecoder::new(
        schema.clone(),
        crate::MetadataVersion::V4,
        None,
        Arc::new(memory.clone()),
    )
    .unwrap();
    let mut decoder =
        OwnedFileDecoder::new(schema, header.version(), None, Arc::new(memory.clone())).unwrap();
    let mut outputs = Vec::new();
    for expected in &batches {
        let (dictionaries, encoded) = generator
            .encode(expected, &mut tracker, &options, &mut Default::default())
            .unwrap();
        for dictionary in dictionaries {
            let metadata = crate::root_as_message(&dictionary.ipc_message).unwrap();
            let body = Buffer::from_vec(dictionary.arrow_data);
            let held = memory.current();
            assert!(
                wrong
                    .read_dictionary_message(&metadata, &body)
                    .unwrap_err()
                    .to_string()
                    .contains("versions mismatch")
            );
            assert!(
                decoder
                    .read_dictionary_message(&metadata, &Buffer::from_vec(Vec::<u8>::new()))
                    .unwrap_err()
                    .to_string()
                    .contains("body length mismatch")
            );
            assert_eq!(memory.current(), held);
            decoder.read_dictionary_message(&metadata, &body).unwrap();
        }
        let metadata = crate::root_as_message(&encoded.ipc_message).unwrap();
        let body = Buffer::from_vec(encoded.arrow_data);
        let held = memory.current();
        assert!(
            wrong
                .read_record_batch_message(&metadata, &body)
                .unwrap_err()
                .to_string()
                .contains("versions mismatch")
        );
        assert!(
            decoder
                .read_record_batch_message(&metadata, &Buffer::from_vec(Vec::<u8>::new()))
                .unwrap_err()
                .to_string()
                .contains("body length mismatch")
        );
        assert_eq!(memory.current(), held);
        outputs.push(
            decoder
                .read_record_batch_message(&metadata, &body)
                .unwrap()
                .unwrap(),
        );
    }
    drop(decoder);
    drop(wrong);
    for (output, expected) in outputs.iter().zip(&batches) {
        assert_eq!(output.batch(), expected);
    }
    assert!(memory.current() > 0);
    drop(outputs);
    assert_eq!(memory.current(), 0);
}
