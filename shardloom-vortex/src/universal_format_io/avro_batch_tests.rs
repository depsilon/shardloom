//! Literal OCF fixtures keep framing checks independent of the provider writer.

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_avro::{
    compression::CompressionCodec,
    reader::{BlockMemoryReservation, DecoderMemory, DecoderMemoryReservation, ReaderBuilder},
};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    io::{BufReader, Cursor},
    sync::{Arc, Mutex},
};

const SYNC: [u8; 16] = *b"0123456789abcdef";
const LONG: &str = r#"{"type":"record","name":"r","fields":[{"name":"id","type":"long"}]}"#;
const NULL: &str = r#"{"type":"record","name":"r","fields":[{"name":"id","type":"null"}]}"#;

fn long(value: i64, out: &mut Vec<u8>) {
    let mut encoded = u64::from_ne_bytes(((value << 1) ^ (value >> 63)).to_ne_bytes());
    while encoded >= 128 {
        out.push(u8::try_from(encoded & 127).unwrap() | 128);
        encoded >>= 7;
    }
    out.push(u8::try_from(encoded).unwrap());
}

fn bytes(value: &[u8], out: &mut Vec<u8>) {
    long(i64::try_from(value.len()).unwrap(), out);
    out.extend_from_slice(value);
}

fn header(schema: &str) -> Vec<u8> {
    let mut out = b"Obj\x01".to_vec();
    long(1, &mut out);
    bytes(b"avro.schema", &mut out);
    bytes(schema.as_bytes(), &mut out);
    long(0, &mut out);
    out.extend_from_slice(&SYNC);
    out
}

fn block(count: i64, payload: &[u8], out: &mut Vec<u8>) {
    long(count, out);
    bytes(payload, out);
    out.extend_from_slice(&SYNC);
}

#[test]
fn avro_zero_byte_records_preserve_declared_rows_across_blocks() {
    let mut input = header(NULL);
    block(5, &[], &mut input);
    block(0, &[], &mut input);
    block(7, &[], &mut input);
    let reader = ReaderBuilder::new()
        .with_batch_size(3)
        .build(Cursor::new(input))
        .unwrap();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 12);
    assert!(batches.iter().all(|batch| batch.num_rows() <= 3));
    for batch in batches {
        assert_eq!(batch.column(0).logical_null_count(), batch.num_rows());
    }
}

#[test]
fn avro_partial_block_at_eof_is_an_error() {
    let prefix = header(LONG);
    let mut complete = prefix.clone();
    block(1, &[2], &mut complete);
    for end in prefix.len() + 1..complete.len() {
        let reader = ReaderBuilder::new()
            .build(Cursor::new(complete[..end].to_vec()))
            .unwrap();
        assert!(reader.collect::<Result<Vec<_>, _>>().is_err(), "end={end}");
    }
    let mut partial_count = prefix;
    partial_count.push(128);
    assert!(
        ReaderBuilder::new()
            .build(Cursor::new(partial_count))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .is_err()
    );
}

#[test]
fn avro_block_sync_mismatch_is_an_error() {
    let mut input = header(LONG);
    block(1, &[2], &mut input);
    *input.last_mut().unwrap() ^= 1;
    let reader = ReaderBuilder::new().build(Cursor::new(input)).unwrap();
    assert!(reader.collect::<Result<Vec<_>, _>>().is_err());
}

#[test]
fn avro_fragmented_blocks_preserve_all_values() {
    let mut input = header(LONG);
    for start in [0, 11] {
        let mut payload = Vec::new();
        for value in start..start + 11 {
            long(value, &mut payload);
        }
        block(11, &payload, &mut input);
    }
    for capacity in [1, 2, 3, 7, 16, 17, 8192] {
        let reader = ReaderBuilder::new()
            .with_batch_size(5)
            .build(BufReader::with_capacity(capacity, Cursor::new(&input)))
            .unwrap();
        assert_eq!(reader.avro_header().sync(), SYNC);
        let mut expected = 0;
        for batch in reader {
            let batch = batch.unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for &value in values.values() {
                assert_eq!(value, expected);
                expected += 1;
            }
        }
        assert_eq!(expected, 22);
    }
}

#[test]
fn avro_batch_controls_shrink_real_buffers_and_preserve_held_rows() {
    let mut input = header(LONG);
    for start in [0, 2500] {
        let mut payload = Vec::new();
        for value in start..start + 2500 {
            long(value, &mut payload);
        }
        block(2500, &payload, &mut input);
    }
    let mut reader = ReaderBuilder::new()
        .with_batch_size(2048)
        .build(BufReader::with_capacity(3, Cursor::new(input)))
        .unwrap();
    assert!(reader.set_batch_size(0).is_err());
    let first = reader.next().unwrap().unwrap();
    reader.set_batch_size(4).unwrap();
    let second = reader.next().unwrap().unwrap();
    reader.set_batch_size(64).unwrap();
    let third = reader.next().unwrap().unwrap();
    assert_eq!(
        (first.num_rows(), second.num_rows(), third.num_rows()),
        (2048, 4, 64)
    );
    assert!(second.get_array_memory_size() < first.get_array_memory_size() / 4);
    assert!(third.get_array_memory_size() > second.get_array_memory_size());
    let mut batches = vec![first, second, third];
    for limit in [1, 100, 2, usize::MAX] {
        reader.set_batch_size(limit).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert!(batch.num_rows() <= limit.min(2048));
        batches.push(batch);
    }
    batches.extend(reader.collect::<Result<Vec<_>, _>>().unwrap());
    let mut expected = 0;
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for &value in values.values() {
            assert_eq!(value, expected);
            expected += 1;
        }
    }
    assert_eq!(expected, 5000);
}

#[test]
fn avro_inconsistent_record_counts_fail_and_reader_does_not_retry() {
    for (count, payload) in [(0, vec![2]), (1, vec![2, 4]), (2, vec![2])] {
        let mut input = header(LONG);
        block(count, &payload, &mut input);
        let mut reader = ReaderBuilder::new().build(Cursor::new(input)).unwrap();
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
        assert!(reader.set_batch_size(1).is_err());
    }
    let mut reader = ReaderBuilder::new()
        .with_batch_size(3)
        .build(Cursor::new(header(LONG)))
        .unwrap();
    assert!(reader.set_batch_size(0).is_err());
    reader.set_batch_size(usize::MAX).unwrap();
    assert!(reader.next().is_none());
    assert!(
        ReaderBuilder::new()
            .with_batch_size(0)
            .build(Cursor::new(header(LONG)))
            .is_err()
    );
}

#[derive(Debug)]
struct Credit(Mutex<MemoryLease>);

#[derive(Debug)]
struct Records(LiveMemoryPool);

#[derive(Debug)]
struct RecordCredit {
    _credit: MemoryLease,
}

impl DecoderMemoryReservation for RecordCredit {}

impl DecoderMemory for Records {
    fn reserve(
        &self,
        bytes: usize,
    ) -> Result<Box<dyn DecoderMemoryReservation>, arrow_schema::ArrowError> {
        self.0
            .reserve(u64::try_from(bytes).unwrap())
            .map(|credit| {
                Box::new(RecordCredit { _credit: credit }) as Box<dyn DecoderMemoryReservation>
            })
            .map_err(|error| arrow_schema::ArrowError::ExternalError(Box::new(error)))
    }
}

#[test]
fn avro_record_payload_and_block_owners_remain_independent() {
    for codec in [
        None,
        Some(CompressionCodec::Deflate),
        Some(CompressionCodec::Snappy),
    ] {
        let input = compressed(codec);
        let pool = LiveMemoryPool::new(4 << 20).unwrap();
        let mut reader = ReaderBuilder::new()
            .with_batch_size(1)
            .with_block_memory_reservation(Arc::new(Credit(Mutex::new(pool.reserve(0).unwrap()))))
            .with_decoder_memory(Arc::new(Records(pool.clone())))
            .build(Cursor::new(input))
            .unwrap();
        let header = reader.avro_header().clone();
        let schema = reader.schema();
        let schema_owner = reader.schema_memory_reservation().unwrap();
        let structure_bytes = pool.snapshot().reserved_bytes;
        assert_eq!(reader.prepare_next_block().unwrap(), 4);
        let block_bytes = pool.snapshot().reserved_bytes - structure_bytes;
        let mut held = Vec::new();
        for _ in 0..4 {
            let batch = reader.next().unwrap().unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.value(0), "large 東京 value".repeat(3000));
            held.push(batch);
        }
        let with_block = pool.snapshot().reserved_bytes;
        assert!(with_block > block_bytes + structure_bytes);
        drop(reader);
        assert_eq!(pool.snapshot().reserved_bytes, with_block - block_bytes);
        drop(header);
        assert!(pool.snapshot().reserved_bytes < with_block - block_bytes);
        let slice = held[0].slice(0, 1);
        drop(held);
        assert!(pool.snapshot().reserved_bytes > 0);
        assert_eq!(
            slice
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "large 東京 value".repeat(3000)
        );
        drop(slice);
        assert!(pool.snapshot().reserved_bytes > 0);
        assert_eq!(schema.field(0).name(), "text");
        drop((schema, schema_owner));
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn avro_record_growth_denial_is_terminal_and_precedes_later_field_error() {
    let schema = r#"{"type":"record","name":"r","fields":[{"name":"id","type":"long"},{"name":"later","type":"string"}]}"#;
    let mut input = header(schema);
    block(1, &[2], &mut input); // The first field is valid; the later string is truncated.
    let pool = LiveMemoryPool::new(1 << 20).unwrap();
    let mut reader = ReaderBuilder::new()
        .with_decoder_memory(Arc::new(Records(pool.clone())))
        .build(Cursor::new(input))
        .unwrap();
    // Admit header/schema preparation first, then leave the record builder its
    // original one-byte headroom. This remains a record-growth admission test.
    let snapshot = pool.snapshot();
    let pressure = pool
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1)
        .unwrap();
    assert!(
        reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("memory reservation denied")
    );
    assert!(reader.next().is_none());
    drop(reader);
    assert_eq!(pool.snapshot().reserved_bytes, pressure.bytes());
    drop(pressure);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

impl BlockMemoryReservation for Credit {
    fn resize(&self, bytes: usize) -> Result<(), arrow_schema::ArrowError> {
        self.0
            .lock()
            .unwrap()
            .resize(u64::try_from(bytes).unwrap())
            .map_err(|error| arrow_schema::ArrowError::ExternalError(Box::new(error)))
    }
}

fn compressed(codec: Option<CompressionCodec>) -> Vec<u8> {
    let batch = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec![
            "large 東京 value".repeat(3000);
            4
        ])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let mut writer = arrow_avro::writer::WriterBuilder::new(batch.schema().as_ref().clone())
        .with_compression(codec)
        .build::<_, arrow_avro::writer::format::AvroOcfFormat>(Vec::new())
        .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    writer.into_inner()
}

#[test]
fn avro_block_storage_is_owned_across_small_batches_and_released_at_end() {
    for codec in [
        None,
        Some(CompressionCodec::Deflate),
        Some(CompressionCodec::Snappy),
    ] {
        let input = compressed(codec);
        let pool = LiveMemoryPool::new(2 << 20).unwrap();
        let credit = Arc::new(Credit(Mutex::new(pool.reserve(0).unwrap())));
        let mut reader = ReaderBuilder::new()
            .with_batch_size(4)
            .with_block_memory_reservation(credit.clone())
            .build(BufReader::with_capacity(4096, Cursor::new(input)))
            .unwrap();
        assert_eq!(reader.prepare_next_block().unwrap(), 4);
        let retained = pool.snapshot().reserved_bytes;
        assert!(retained >= 4 * 3000 * 14, "{codec:?}: {retained}");
        assert_eq!(reader.prepare_next_block().unwrap(), 4);
        assert_eq!(pool.snapshot().reserved_bytes, retained);
        let mut batches = Vec::new();
        for remaining in (1..=4).rev() {
            assert_eq!(reader.prepare_next_block().unwrap(), remaining);
            reader.set_batch_size(1).unwrap();
            batches.push(reader.next().unwrap().unwrap());
            assert_eq!(pool.snapshot().reserved_bytes, retained);
        }
        assert_eq!(reader.prepare_next_block().unwrap(), 0);
        assert!(reader.next().is_none());
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        drop((reader, credit));
        for batch in batches {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.len(), 1);
            assert_eq!(values.value(0), "large 東京 value".repeat(3000));
        }
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn avro_indivisible_block_denial_returns_no_partial_rows_or_retries() {
    for codec in [
        None,
        Some(CompressionCodec::Deflate),
        Some(CompressionCodec::Snappy),
    ] {
        let input = compressed(codec);
        let pool = LiveMemoryPool::new(16 << 10).unwrap();
        let credit = Arc::new(Credit(Mutex::new(pool.reserve(0).unwrap())));
        let mut reader = ReaderBuilder::new()
            .with_batch_size(1)
            .with_block_memory_reservation(credit.clone())
            .build(BufReader::with_capacity(4096, Cursor::new(input)))
            .unwrap();
        let error = reader.prepare_next_block().unwrap_err().to_string();
        assert!(
            error.contains("memory reservation denied"),
            "{codec:?}: {error}"
        );
        assert!(reader.next().is_none());
        assert!(reader.prepare_next_block().is_err());
        assert!(pool.snapshot().peak_reserved_bytes <= 16 << 10);
        drop((reader, credit));
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}
