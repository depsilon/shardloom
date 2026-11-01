// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom schema-stage admission and complete-value controls.

use super::*;
use crate::reader::{DecoderMemory, DecoderMemoryReservation, ReaderBuilder, header::read_header};
use arrow_array::{Int64Array, StringArray};
use arrow_schema::ArrowError;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct State {
    live: usize,
    peak: usize,
    calls: usize,
    cancelled: bool,
}

#[derive(Debug)]
struct Pool {
    state: Mutex<State>,
    limit: usize,
}

#[derive(Debug)]
struct Factory(Arc<Pool>);
#[derive(Debug)]
struct Credit {
    pool: Arc<Pool>,
    bytes: usize,
}
impl DecoderMemoryReservation for Credit {}
impl Drop for Credit {
    fn drop(&mut self) {
        self.pool.state.lock().unwrap().live -= self.bytes;
    }
}
impl DecoderMemory for Factory {
    fn reserve(&self, bytes: usize) -> Result<Box<dyn DecoderMemoryReservation>, ArrowError> {
        let mut state = self.0.state.lock().unwrap();
        state.calls += 1;
        if state.cancelled {
            return Err(ArrowError::MemoryError("schema cancelled".into()));
        }
        let next = state
            .live
            .checked_add(bytes)
            .filter(|total| *total <= self.0.limit)
            .ok_or_else(|| ArrowError::MemoryError("schema denied".into()))?;
        state.live = next;
        state.peak = state.peak.max(next);
        Ok(Box::new(Credit {
            pool: self.0.clone(),
            bytes,
        }))
    }
}

fn controlled(limit: usize) -> (Arc<Pool>, ReaderBuilder) {
    let pool = Arc::new(Pool {
        state: Mutex::new(State::default()),
        limit,
    });
    let builder = ReaderBuilder::new().with_decoder_memory(Arc::new(Factory(pool.clone())));
    (pool, builder)
}

fn header(schema: &str) -> crate::reader::header::Header {
    fn length(mut n: usize, out: &mut Vec<u8>) {
        n *= 2;
        while n > 127 {
            out.push((n as u8 & 127) | 128);
            n >>= 7;
        }
        out.push(n as u8);
    }
    let mut out = b"Obj\x01\x02".to_vec();
    length(11, &mut out);
    out.extend_from_slice(b"avro.schema");
    length(schema.len(), &mut out);
    out.extend_from_slice(schema.as_bytes());
    out.extend_from_slice(&[0; 17]);
    read_header(out.as_slice()).unwrap().0
}

#[test]
fn schema_admission_precedes_parsing_and_preserves_typed_errors() {
    let header = header("{");
    assert!(matches!(
        ReaderBuilder::new().make_decoder(Some(&header), None),
        Err(AvroError::ParseError(_))
    ));
    for cancelled in [false, true] {
        let (pool, builder) = controlled(0);
        pool.state.lock().unwrap().cancelled = cancelled;
        let error = builder.make_decoder(Some(&header), None).unwrap_err();
        let AvroError::ArrowError(error) = error else {
            panic!("wrong typed error")
        };
        let expected = if cancelled {
            "schema cancelled"
        } else {
            "schema denied"
        };
        assert!(matches!(*error, ArrowError::MemoryError(ref value) if value == expected));
        let state = pool.state.lock().unwrap();
        assert_eq!((state.calls, state.live), (1, 0));
    }
}

#[test]
fn projected_reader_defaults_preserve_values_and_release_parser_scratch() {
    let header = header(r#"{"type":"record","name":"r","fields":[{"name":"id","type":"int"}]}"#);
    let reader = AvroSchema::new(r#"{"type":"record","name":"r","fields":[{"name":"tag","type":"string","default":"東京"},{"name":"id","type":"long"}]}"#.into());
    let (pool, builder) = controlled(16 << 20);
    let builder = builder.with_projection(vec![1, 0]);
    let mut decoder = builder.make_decoder(Some(&header), Some(&reader)).unwrap();
    {
        let state = pool.state.lock().unwrap();
        assert!(state.peak > state.live);
    }
    assert_eq!(decoder.active_decoder.decode(&[84], 1).unwrap(), 1);
    let batch = decoder.active_decoder.flush().unwrap();
    let schema_owner = decoder.schema_memory_reservation();
    assert_eq!(batch.schema().fields()[0].name(), "id");
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "東京"
    );
    drop(decoder);
    assert!(pool.state.lock().unwrap().live > 0);
    drop(batch);
    assert!(pool.state.lock().unwrap().live > 0);
    drop(schema_owner);
    assert_eq!(pool.state.lock().unwrap().live, 0);
}

#[test]
fn schema_errors_keep_original_semantics_and_refund_workspace() {
    let cases = [
        r#"{"type":"record","name":"r","fields":[{"name":"x","type":"notDefined"}]}"#.to_string(),
        format!("{}\"long\"{}", "[".repeat(150), "]".repeat(150)),
    ];
    for raw in cases {
        let header = header(&raw);
        let expected = ReaderBuilder::new()
            .make_decoder(Some(&header), None)
            .unwrap_err();
        let (pool, builder) = controlled(64 << 20);
        let actual = builder.make_decoder(Some(&header), None).unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string());
        assert_eq!(pool.state.lock().unwrap().live, 0);
    }
    let header = header(r#"{"type":"record","name":"r","fields":[{"name":"x","type":"long"}]}"#);
    let expected = ReaderBuilder::new()
        .with_projection(vec![0, 0])
        .make_decoder(Some(&header), None)
        .unwrap_err();
    let (pool, builder) = controlled(16 << 20);
    let actual = builder
        .with_projection(vec![0, 0])
        .make_decoder(Some(&header), None)
        .unwrap_err();
    assert_eq!(actual.to_string(), expected.to_string());
    assert_eq!(pool.state.lock().unwrap().live, 0);
}

#[test]
fn schema_capacity_counts_nesting_without_treating_string_contents_as_nodes() {
    let quoted = JsonShape::inspect(br#"{"doc":"[[[ { \\\" : ]]]","fields":[]}"#).unwrap();
    assert_eq!(quoted.objects, 1);
    assert_eq!(quoted.entries, 2);
    assert_eq!(quoted.max_depth, 2);
    let nested = br#"{"doc":[[[{"x":0}]]],"fields":[]}"#;
    assert!(
        json_workspace(nested).unwrap() > json_workspace(br#"{"doc":"x","fields":[]}"#).unwrap()
    );
    let overflow = JsonShape {
        tokens: usize::MAX,
        ..JsonShape::default()
    };
    assert!(matches!(
        overflow.parser_bytes(0),
        Err(AvroError::ParseError(_))
    ));
    assert!(times(usize::MAX, 2).is_err());
    assert!(add(usize::MAX, 1).is_err());
}
