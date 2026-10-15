// SPDX-License-Identifier: Apache-2.0
//! Opt-in complete compression/serialization/reopen cost screen.
//! The identical harness also runs against the pre-change provider.

use super::*;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::time::Instant;
use vortex::array::arrays::{Primitive, VarBinView};

const GRANT: u64 = 64 << 20;
const SAMPLES: usize = 3;
const OPERATIONS: usize = 25;

enum Expected {
    Integers(Vec<Option<i64>>),
    Bytes(Vec<Option<Vec<u8>>>),
}

struct Fixture {
    input: ArrayRef,
    expected: Expected,
}

fn fixture(kind: &str, rows: usize) -> Fixture {
    if kind == "nullable_i64" {
        let expected: Vec<_> = (0..rows)
            .map(|index| {
                let signed = i64::try_from(index).unwrap();
                (index % 17 != 0).then_some(match index % 19 {
                    0 => i64::MIN,
                    1 => i64::MAX,
                    _ => signed.wrapping_mul(0x1234_5678_9abc) ^ (signed / 97),
                })
            })
            .collect();
        return Fixture {
            input: PrimitiveArray::from_option_iter(expected.iter().copied()).into_array(),
            expected: Expected::Integers(expected),
        };
    }
    let expected: Vec<_> = (0..rows)
        .map(|index| {
            (index % 13 != 0).then(|| {
                if index % 29 == 0 {
                    return Vec::new();
                }
                let mut value = format!(
                    "native λ 🎉 group {:03} value {:06} ",
                    index % 37,
                    index % 997
                )
                .repeat(1 + index % 3)
                .into_bytes();
                if kind == "nullable_binary" {
                    value.extend_from_slice(&[0, 0x80, 0xff, u8::try_from(index % 256).unwrap()]);
                }
                value
            })
        })
        .collect();
    let input = if kind == "nullable_binary" {
        VarBinViewArray::from_iter_nullable_bin(expected.iter().map(Option::as_deref))
    } else {
        VarBinViewArray::from_iter_nullable_str(expected.iter().map(|value| {
            value
                .as_deref()
                .map(|bytes| std::str::from_utf8(bytes).unwrap())
        }))
    };
    Fixture {
        input: input.into_array(),
        expected: Expected::Bytes(expected),
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
struct Observation {
    serialized_bytes: usize,
    serialized_sha256: String,
    producer_peak_bytes: u64,
    producer_retained_bytes: u64,
    consumer_peak_bytes: u64,
}

fn operation(fixture: &Fixture, encoding: &str, frame_rows: usize) -> Observation {
    let producer_memory = LiveMemoryPool::new(GRANT).unwrap();
    let producer = session(&producer_memory);
    let mut ctx = producer.create_execution_ctx();
    let encoded = if encoding == "canonical" {
        fixture.input.clone()
    } else if let Some(values) = fixture.input.as_opt::<Primitive>() {
        let values = values.into_owned();
        let data = if encoding == "zstd_dictionary" {
            ZstdData::from_primitive(&values, 0, frame_rows, &mut ctx)
        } else {
            ZstdData::from_primitive_without_dict(&values, 0, frame_rows, &mut ctx)
        }
        .unwrap();
        Zstd::try_new(values.dtype().clone(), data, values.validity().unwrap())
            .unwrap()
            .into_array()
    } else {
        let values = fixture.input.as_::<VarBinView>().into_owned();
        if encoding == "zstd_dictionary" {
            Zstd::from_var_bin_view(&values, 0, frame_rows, &mut ctx)
        } else {
            Zstd::from_var_bin_view_without_dict(&values, 0, frame_rows, &mut ctx)
        }
        .unwrap()
        .into_array()
    };
    if encoding != "canonical" {
        let dictionary_present = encoded
            .as_::<Zstd>()
            .data()
            .clone()
            .into_parts(encoded.validity().unwrap())
            .dictionary
            .is_some();
        assert_eq!(dictionary_present, encoding == "zstd_dictionary");
    }
    let producer_retained_bytes = producer_memory.snapshot().reserved_bytes;
    let context = ArrayContext::empty();
    let mut serialized = ByteBufferMut::empty();
    for buffer in encoded
        .serialize(&context, &producer, &SerializeOptions::default())
        .unwrap()
    {
        serialized.extend_from_slice(buffer.as_ref());
    }
    let serialized = serialized.freeze();
    let serialized_bytes = serialized.len();
    let mut serialized_sha256 = String::with_capacity(64);
    for byte in Sha256::digest(&serialized) {
        write!(&mut serialized_sha256, "{byte:02x}").unwrap();
    }
    drop(encoded);
    drop(ctx);
    drop(producer);
    assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
    let producer_peak_bytes = producer_memory.snapshot().peak_reserved_bytes;

    let consumer_memory = LiveMemoryPool::new(GRANT).unwrap();
    let consumer = session(&consumer_memory);
    let mut ctx = consumer.create_execution_ctx();
    let reopened = SerializedArray::try_from(serialized)
        .unwrap()
        .decode(
            fixture.input.dtype(),
            fixture.input.len(),
            &ReadContext::new(context.to_ids()),
            &consumer,
        )
        .unwrap();
    assert_eq!(reopened.is::<Zstd>(), encoding != "canonical");
    assert_eq!(reopened.dtype(), fixture.input.dtype());
    assert_eq!(reopened.len(), fixture.input.len());
    verify_values(reopened, fixture, &mut ctx);
    drop(ctx);
    drop(consumer);
    assert_eq!(consumer_memory.snapshot().reserved_bytes, 0);
    Observation {
        serialized_bytes,
        serialized_sha256,
        producer_peak_bytes,
        producer_retained_bytes,
        consumer_peak_bytes: consumer_memory.snapshot().peak_reserved_bytes,
    }
}

fn verify_values(reopened: ArrayRef, fixture: &Fixture, ctx: &mut ExecutionCtx) {
    match &fixture.expected {
        Expected::Integers(expected) => {
            let output = reopened.execute::<PrimitiveArray>(ctx).unwrap();
            let mask = output
                .validity()
                .unwrap()
                .execute_mask(output.len(), ctx)
                .unwrap();
            let values = output.as_slice::<i64>();
            for (index, expected) in expected.iter().enumerate() {
                assert_eq!(mask.value(index), expected.is_some());
                if let Some(expected) = expected {
                    assert_eq!(values[index], *expected);
                }
            }
        }
        Expected::Bytes(expected) => {
            let output = reopened.execute::<VarBinViewArray>(ctx).unwrap();
            let mask = output
                .validity()
                .unwrap()
                .execute_mask(output.len(), ctx)
                .unwrap();
            for (index, expected) in expected.iter().enumerate() {
                assert_eq!(mask.value(index), expected.is_some());
                if let Some(expected) = expected {
                    assert_eq!(output.bytes_at(index).as_slice(), expected);
                }
            }
        }
    }
}

#[test]
#[ignore = "explicit frozen compression lifecycle cost screen; not a routine unit test"]
fn native_zstd_complete_compression_cost_screen() {
    assert_eq!(
        std::env::var("SHARDLOOM_COMPRESSION_COST").as_deref(),
        Ok("frozen-v1")
    );
    for kind in ["nullable_i64", "nullable_utf8", "nullable_binary"] {
        for rows in [4096, 65_536] {
            let fixture = fixture(kind, rows);
            let frame_rows = if rows == 4096 { 128 } else { 1024 };
            for encoding in ["canonical", "zstd_no_dictionary", "zstd_dictionary"] {
                let started = Instant::now();
                let reference = operation(&fixture, encoding, frame_rows);
                println!(
                    "COMPRESSION_COST {}",
                    serde_json::json!({
                        "phase": "warmup", "kind": kind, "rows": rows,
                        "encoding": encoding, "frame_rows": frame_rows,
                        "grant_bytes": GRANT, "nanoseconds": [started.elapsed().as_nanos()],
                        "observation": reference,
                    })
                );
                for sample in 0..SAMPLES {
                    let mut nanoseconds = Vec::with_capacity(OPERATIONS);
                    for _ in 0..OPERATIONS {
                        let started = Instant::now();
                        let observed = operation(&fixture, encoding, frame_rows);
                        let elapsed = started.elapsed().as_nanos();
                        assert_eq!(observed, reference);
                        nanoseconds.push(elapsed);
                    }
                    println!(
                        "COMPRESSION_COST {}",
                        serde_json::json!({
                            "phase": "measured", "sample": sample,
                            "kind": kind, "rows": rows, "encoding": encoding,
                            "frame_rows": frame_rows, "grant_bytes": GRANT,
                            "nanoseconds": nanoseconds, "observation": reference,
                        })
                    );
                }
            }
        }
    }
}
