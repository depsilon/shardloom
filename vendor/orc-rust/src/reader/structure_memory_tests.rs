// SPDX-License-Identifier: Apache-2.0
use crate::{
    reader::{
        memory::tests::{Counters, Memory},
        metadata::read_metadata,
        ChunkReader,
    },
    ArrowReaderBuilder, ArrowWriterBuilder,
};
use arrow::{
    array::{ArrayRef, Int64Array},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use std::{
    io::Cursor,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
struct Observed {
    bytes: Bytes,
    reads: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl ChunkReader for Observed {
    type T = Cursor<Bytes>;
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn get_read(&self, offset: u64) -> std::io::Result<Self::T> {
        Ok(Cursor::new(self.bytes.slice(offset as usize..)))
    }
    fn get_bytes(&self, offset: u64, length: u64) -> std::io::Result<Bytes> {
        self.reads.lock().unwrap().push((offset, length));
        Ok(self
            .bytes
            .slice(offset as usize..(offset + length) as usize))
    }
}

fn input(name: &str) -> Observed {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![3, 5, 8, 13, 21]));
    let batch = RecordBatch::try_from_iter([(name, values)]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, batch.schema())
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Observed {
        bytes: bytes.into(),
        reads: Arc::default(),
    }
}

fn budget(limit: usize) -> (Arc<Memory>, Arc<Mutex<Counters>>) {
    let counters = Arc::new(Mutex::new(Counters::default()));
    (
        Arc::new(Memory {
            counters: Arc::clone(&counters),
            limit,
        }),
        counters,
    )
}

#[test]
fn reader_structure_is_admitted_before_projection_or_payload_reads() {
    let mut input = input("id");
    let metadata = Arc::new(read_metadata(&mut input).unwrap());
    let reads = input.reads.lock().unwrap().len();
    let (memory, counters) = budget(1);
    let result = ArrowReaderBuilder::new_with_metadata(input.clone(), metadata)
        .with_decoder_memory(memory)
        .try_build();
    assert!(
        matches!(result, Err(error) if error.to_string().contains("test shared memory denied"))
    );
    assert_eq!(input.reads.lock().unwrap().len(), reads);
    assert_eq!(counters.lock().unwrap().peak, 0);
    assert_eq!(counters.lock().unwrap().live, 0);
}

#[test]
fn long_schema_names_deny_stripe_structure_before_stream_payload() {
    let name = "wide".repeat(16_384);
    let mut input = input(&name);
    let metadata = read_metadata(&mut input).unwrap();
    let stripe = &metadata.stripe_metadatas()[0];
    input.reads.lock().unwrap().clear();
    let (memory, counters) = budget(128 * 1024);
    let result = crate::stripe::Stripe::new_with_memory(
        &mut input,
        &metadata,
        metadata.root_data_type(),
        stripe,
        Some(memory),
    );
    assert!(
        matches!(result, Err(error) if error.to_string().contains("test shared memory denied"))
    );
    assert_eq!(
        *input.reads.lock().unwrap(),
        vec![(stripe.footer_offset(), stripe.footer_length())]
    );
    assert!(counters.lock().unwrap().peak > 0);
    assert_eq!(counters.lock().unwrap().live, 0);
}

#[test]
fn shared_schema_and_column_clones_retain_structural_credit() {
    let mut input = input("id");
    let metadata = read_metadata(&mut input).unwrap();
    let root = metadata.root_data_type().clone();
    assert!(std::ptr::eq(
        root.children().as_ptr(),
        metadata.root_data_type().children().as_ptr()
    ));
    let (memory, counters) = budget(1 << 20);
    let stripe = crate::stripe::Stripe::new_with_memory(
        &mut input,
        &metadata,
        &root,
        &metadata.stripe_metadatas()[0],
        Some(memory),
    )
    .unwrap();
    let original = &stripe.columns()[0];
    let copy = original.clone();
    assert!(std::ptr::eq(original.data_type(), copy.data_type()));
    let retained = counters.lock().unwrap().live;
    assert!(retained > 0);
    drop(stripe);
    assert_eq!(copy.name(), "id");
    assert_eq!(counters.lock().unwrap().live, retained);
    drop(copy);
    assert_eq!(counters.lock().unwrap().live, 0);
}

#[test]
fn controlled_reader_values_keep_payload_credit_after_reader_drop() {
    let input = input("id");
    let (memory, counters) = budget(1 << 20);
    let mut reader = ArrowReaderBuilder::try_new(input)
        .unwrap()
        .with_decoder_memory(memory)
        .with_batch_size(2)
        .try_build()
        .unwrap();
    let mut batches = Vec::new();
    for batch in reader.by_ref() {
        batches.push(batch.unwrap());
    }
    drop(reader);
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![3, 5, 8, 13, 21]);
    assert!(counters.lock().unwrap().live > 0);
    drop(batches);
    assert_eq!(counters.lock().unwrap().live, 0);
}
