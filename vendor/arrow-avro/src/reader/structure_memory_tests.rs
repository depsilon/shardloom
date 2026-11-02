// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom named-expansion and retained-schema controls.

use super::*;
use crate::reader::{DecoderMemory, ReaderBuilder};
use arrow_schema::ArrowError;
use std::{
    io::Cursor,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Debug, Default)]
struct Counts {
    live: AtomicUsize,
    denied: AtomicUsize,
}
#[derive(Debug)]
struct Factory {
    counts: Arc<Counts>,
    limit: usize,
}
#[derive(Debug)]
struct Credit {
    counts: Arc<Counts>,
    bytes: usize,
}
impl DecoderMemoryReservation for Credit {}
impl Drop for Credit {
    fn drop(&mut self) {
        self.counts.live.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}
impl DecoderMemory for Factory {
    fn reserve(&self, bytes: usize) -> Result<Box<dyn DecoderMemoryReservation>, ArrowError> {
        let mut current = self.counts.live.load(Ordering::SeqCst);
        loop {
            let Some(next) = current
                .checked_add(bytes)
                .filter(|total| *total <= self.limit)
            else {
                self.counts.denied.fetch_max(bytes, Ordering::SeqCst);
                return Err(ArrowError::MemoryError("structure denied".into()));
            };
            match self.counts.live.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        Ok(Box::new(Credit {
            counts: self.counts.clone(),
            bytes,
        }))
    }
}

fn controlled(limit: usize) -> (Arc<Counts>, ReaderBuilder) {
    let counts = Arc::new(Counts::default());
    let builder = ReaderBuilder::new().with_decoder_memory(Arc::new(Factory {
        counts: counts.clone(),
        limit,
    }));
    (counts, builder)
}

fn container(schema: &str) -> Vec<u8> {
    let mut bytes = b"Obj\x01\x02\x16avro.schema".to_vec();
    let mut n = schema.len() * 2;
    while n > 127 {
        bytes.push((n as u8 & 127) | 128);
        n >>= 7;
    }
    bytes.push(n as u8);
    bytes.extend_from_slice(schema.as_bytes());
    bytes.extend_from_slice(&[0; 17]);
    bytes
}

#[test]
fn compact_named_graph_is_admitted_before_decoder_expansion() {
    let mut fields = vec![serde_json::json!({"name":"seed", "type":{
        "type":"record", "name":"n0", "fields":[]}})];
    for index in 1..=10 {
        let prior = format!("n{}", index - 1);
        fields.push(serde_json::json!({"name": format!("v{index}"), "type":{
            "type":"record", "name":format!("n{index}"), "fields":[
                {"name":"left", "type":prior}, {"name":"right", "type":prior}]}}));
    }
    let raw = serde_json::json!({"type":"record", "name":"root", "fields":fields}).to_string();
    assert!(raw.len() < 2048);
    let schema: Schema<'_> = serde_json::from_str(&raw).unwrap();
    let (counts, builder) = controlled(64 << 10);
    let error = builder
        .make_record_decoder(&schema, None, false)
        .unwrap_err();
    assert!(matches!(error, AvroError::ArrowError(error)
        if matches!(*error, ArrowError::MemoryError(ref message) if message == "structure denied")));
    assert!(counts.denied.load(Ordering::SeqCst) > 1 << 20);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[test]
fn schema_owner_survives_reader_and_retains_actual_fields() {
    let raw = r#"{"type":"record","name":"root","fields":[{"name":"id","type":"long"},{"name":"nested","type":{"type":"record","name":"child","fields":[{"name":"tag","type":"string"}]}}]}"#;
    let (counts, builder) = controlled(16 << 20);
    let reader = builder.build(Cursor::new(container(raw))).unwrap();
    let schema = reader.schema();
    let owner = reader.schema_memory_reservation().unwrap();
    let copy = owner.clone();
    let with_header = counts.live.load(Ordering::SeqCst);
    drop(reader);
    let without_header = counts.live.load(Ordering::SeqCst);
    assert!(without_header > 0 && without_header < with_header);
    assert_eq!(schema.field(0).name(), "id");
    let arrow_schema::DataType::Struct(fields) = schema.field(1).data_type() else {
        panic!("nested schema")
    };
    assert_eq!(fields[0].name(), "tag");
    drop(schema);
    drop(owner);
    assert_eq!(counts.live.load(Ordering::SeqCst), without_header);
    drop(copy);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[test]
fn typed_shape_counts_defaults_metadata_and_projection_skippers() {
    let writer = r#"{"type":"record","name":"r","fields":[{"name":"keep","type":"long"},{"name":"skip","type":{"type":"array","items":{"type":"map","values":"string"}}}]}"#;
    let writer: Schema<'_> = serde_json::from_str(writer).unwrap();
    let reader_raw = format!(
        r#"{{"type":"record","name":"r","fields":[{{"name":"keep","type":"long"}},{{"name":"default","type":"string","default":"{}"}}]}}"#,
        "payload".repeat(100)
    );
    let reader: Schema<'_> = serde_json::from_str(&reader_raw).unwrap();
    let mut shapes = Shapes {
        names: Values::new(),
    };
    let writer_shape = shapes.schema(&writer, None).unwrap();
    let reader_shape = shapes.schema(&reader, None).unwrap();
    assert!(reader_shape.heap > 700);
    assert!(
        capacity(writer_shape, Some(reader_shape), false).unwrap()
            > capacity(writer_shape, None, false).unwrap()
    );
    let (counts, builder) = controlled(16 << 20);
    let mut decoder = builder
        .make_record_decoder(&writer, Some(&reader), false)
        .unwrap();
    // keep=42, and the skipped array is empty. Defaults still produce complete values.
    assert_eq!(decoder.decode(&[84, 0], 1).unwrap(), 2);
    let batch = decoder.flush().unwrap();
    let owner = decoder.structure_memory();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(0),
        "payload".repeat(100)
    );
    drop(decoder);
    drop(batch);
    drop(owner);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[test]
fn schema_shape_overflow_does_not_wrap_to_a_small_grant() {
    let shape = Shape {
        nodes: usize::MAX,
        ..Shape::default()
    };
    assert!(capacity(shape, None, false).is_err());
    let shape = Shape {
        heap: usize::MAX,
        ..Shape::default()
    };
    assert!(capacity(Shape::default(), Some(shape), false).is_err());
}

#[test]
fn flat_named_chain_cannot_bypass_the_controlled_construction_depth_limit() {
    let mut fields = vec![serde_json::json!({"name":"seed", "type":{
        "type":"record", "name":"n0", "fields":[]}})];
    for index in 1..=128 {
        fields.push(serde_json::json!({"name":format!("v{index}"), "type":{
            "type":"record", "name":format!("n{index}"), "fields":[
                {"name":"child", "type":format!("n{}", index - 1)}]}}));
    }
    // The source JSON stays shallow. Named references add the depth only after
    // parsing; their checked shape is inspected without expanding the graph.
    let raw = serde_json::json!({"type":"record", "name":"root", "fields":fields}).to_string();
    let schema: Schema<'_> = serde_json::from_str(&raw).unwrap();
    let (counts, builder) = controlled(usize::MAX);
    let error = builder
        .make_record_decoder(&schema, None, false)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("schema expansion exceeds depth 128")
    );
    assert_eq!(counts.denied.load(Ordering::SeqCst), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);

    let boundary = Shape {
        height: 128,
        ..Shape::default()
    };
    assert!(capacity(boundary, None, false).is_ok());
    assert!(
        capacity(
            Shape {
                height: 129,
                ..boundary
            },
            None,
            false
        )
        .is_err()
    );
}
