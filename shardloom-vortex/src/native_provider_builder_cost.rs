//! Guarded lifecycle cost screen for native Chunked byte-buffer admission.

use super::*;
use sha2::{Digest as _, Sha256};
use std::{fmt::Write as _, hint::black_box, time::Instant};

const SHAPES: [&str; 10] = [
    "i64",
    "i64_nullable",
    "f64_nullable",
    "bool_nullable",
    "decimal38_nullable",
    "decimal76",
    "bitpacked_i32",
    "constant_i64",
    "zstd_i64_nullable",
    "utf8_nullable",
];
const ROWS: [usize; 3] = [32, 4096, 65536];
const CHUNKS: [usize; 2] = [2, 16];
const PAIRS: usize = 9;
const WARMUP: usize = 8;
const GRANT: u64 = 64 << 20;

fn fixture(shape: &str, rows: usize, count: usize) -> (ArrayRef, ArrayRef) {
    let integers = || {
        PrimitiveArray::from_option_iter(
            (0..rows)
                .map(|row| (!row.is_multiple_of(7)).then_some(i64::try_from(row).unwrap() - 17)),
        )
    };
    let expected = match shape {
        "i64" => PrimitiveArray::from_iter((0..rows).map(|row| i64::try_from(row).unwrap() - 17))
            .into_array(),
        "i64_nullable" | "zstd_i64_nullable" => integers().into_array(),
        "f64_nullable" => PrimitiveArray::from_option_iter((0..rows).map(|row| {
            (!row.is_multiple_of(7)).then_some(f64::from(u32::try_from(row).unwrap()) / 8.0 - 128.0)
        }))
        .into_array(),
        "bool_nullable" => BoolArray::from_iter(
            (0..rows + 3).map(|row| (!row.is_multiple_of(7)).then_some(row.is_multiple_of(3))),
        )
        .into_array()
        .slice(3..rows + 3)
        .unwrap(),
        "decimal38_nullable" => DecimalArray::from_option_iter(
            (0..rows).map(|row| {
                (!row.is_multiple_of(7)).then_some(100_000_000_000_000_000_000i128 + row as i128)
            }),
            DecimalDType::new(38, 4),
        )
        .into_array(),
        "decimal76" => DecimalArray::new(
            (0..rows)
                .map(|row| {
                    let value = i256::from_parts(row as u128 + 17, 1 << 70);
                    if row.is_multiple_of(2) { value } else { -value }
                })
                .collect::<vortex::buffer::Buffer<_>>(),
            DecimalDType::new(76, -2),
            Validity::NonNullable,
        )
        .into_array(),
        "bitpacked_i32" => {
            PrimitiveArray::from_iter((0..rows).map(|row| i32::try_from(row % 13).unwrap()))
                .into_array()
        }
        "constant_i64" => PrimitiveArray::from_iter(std::iter::repeat_n(-37i64, rows)).into_array(),
        "utf8_nullable" => VarBinViewArray::from_iter_nullable_str((0..rows).map(|row| {
            (!row.is_multiple_of(7)).then(|| format!("native builder λ value {row:06}"))
        }))
        .into_array(),
        _ => panic!("unrecognized frozen builder fixture"),
    };
    let mut ctx = VortexSession::default().create_execution_ctx();
    let source = match shape {
        "bitpacked_i32" => BitPackedData::encode(&expected, 4, &mut ctx)
            .unwrap()
            .into_array(),
        "constant_i64" => ConstantArray::new(-37i64, rows).into_array(),
        "zstd_i64_nullable" => {
            let values = expected.as_::<Primitive>().into_owned();
            let data = ZstdData::from_primitive_without_dict(&values, 0, 1024, &mut ctx).unwrap();
            Zstd::try_new(values.dtype().clone(), data, values.validity().unwrap())
                .unwrap()
                .into_array()
        }
        _ => expected.clone(),
    };
    let input = chunks(
        (0..count)
            .map(|chunk| {
                source
                    .slice(chunk * rows / count..(chunk + 1) * rows / count)
                    .unwrap()
            })
            .collect(),
    );
    (input, expected)
}

fn fingerprint(array: &ArrayRef) -> String {
    fn update(hash: &mut Sha256, array: &ArrayRef) {
        hash.update(array.encoding_id().to_string().as_bytes());
        hash.update(array.dtype().to_string().as_bytes());
        hash.update(array.len().to_le_bytes());
        let buffers = array.buffers();
        hash.update(buffers.len().to_le_bytes());
        for buffer in buffers {
            hash.update(buffer.len().to_le_bytes());
            hash.update(buffer.as_slice());
        }
        for child in array.children() {
            update(hash, &child);
        }
    }
    let mut hash = Sha256::new();
    update(&mut hash, array);
    let mut result = String::with_capacity(64);
    for byte in hash.finalize() {
        write!(&mut result, "{byte:02x}").unwrap();
    }
    result
}

fn complete(input: &ArrayRef, ctx: &mut ExecutionCtx) -> ArrayRef {
    let result = input
        .clone()
        .execute::<Canonical>(ctx)
        .unwrap()
        .into_array();
    assert_eq!(result.dtype(), input.dtype());
    assert_eq!(result.len(), input.len());
    result
}

fn verify(input: &ArrayRef, expected: &ArrayRef, ctx: &mut ExecutionCtx) {
    let result = complete(input, ctx);
    assert_eq!(result.dtype(), expected.dtype());
    for row in 0..expected.len() {
        assert_eq!(
            result.execute_scalar(row, ctx).unwrap(),
            expected.execute_scalar(row, ctx).unwrap(),
            "builder fixture row {row}"
        );
    }
}

#[test]
#[ignore = "native builder lifecycle screen; guarded serial release execution required"]
#[allow(clippy::assertions_on_constants, clippy::too_many_lines)]
fn native_builder_lifecycle_cost_screen() {
    assert!(!cfg!(debug_assertions));
    assert_eq!(
        std::env::var("SHARDLOOM_BUILDER_COST_SCREEN").as_deref(),
        Ok("1")
    );
    let reverse = match std::env::var("SHARDLOOM_BUILDER_COST_ORDER").as_deref() {
        Ok("forward") => false,
        Ok("reverse") => true,
        _ => panic!("explicit frozen screen order required"),
    };
    let mut cell_index = 0usize;
    for shape in SHAPES {
        for rows in ROWS {
            for count in CHUNKS {
                let (input, expected) = fixture(shape, rows, count);
                let input_sha256 = fingerprint(&input);
                let reference_sha256 = fingerprint(&expected);
                let pools = [
                    LiveMemoryPool::new(GRANT).unwrap(),
                    LiveMemoryPool::new(GRANT).unwrap(),
                ];
                let baseline = VortexSession::default().with_allocator(Arc::new(
                    crate::owned_buffers::ReservedHostAllocator::new(pools[0].clone()),
                ));
                let admitted = session(&pools[1]);
                let mut contexts = [
                    baseline.create_execution_ctx(),
                    admitted.create_execution_ctx(),
                ];
                for index in 0..2 {
                    verify(&input, &expected, &mut contexts[index]);
                    for _ in 0..WARMUP {
                        drop(black_box(complete(&input, &mut contexts[index])));
                    }
                    assert_eq!(pools[index].snapshot().reserved_bytes, 0);
                }
                let iterations = match rows {
                    32 => 1024,
                    4096 => 128,
                    65536 => 16,
                    _ => unreachable!(),
                };
                for pair in 0..PAIRS {
                    let order = if (cell_index + pair + usize::from(reverse)).is_multiple_of(2) {
                        [0, 1]
                    } else {
                        [1, 0]
                    };
                    for (order_position, index) in order.into_iter().enumerate() {
                        verify(&input, &expected, &mut contexts[index]);
                        assert_eq!(pools[index].snapshot().reserved_bytes, 0);
                        let start = Instant::now();
                        for _ in 0..iterations {
                            drop(black_box(complete(&input, &mut contexts[index])));
                        }
                        let nanos = start.elapsed().as_nanos();
                        assert_eq!(pools[index].snapshot().reserved_bytes, 0);
                        verify(&input, &expected, &mut contexts[index]);
                        assert_eq!(pools[index].snapshot().reserved_bytes, 0);
                        println!(
                            "SHARDLOOM_BUILDER_COST_SAMPLE={}",
                            serde_json::json!({
                                "shape": shape, "rows": rows, "chunks": count,
                                "pair": pair, "order_position": order_position,
                                "variant": if index == 0 { "native" } else { "admitted" },
                                "iterations": iterations, "batch_nanos": nanos,
                                "peak_reserved_bytes": pools[index].snapshot().peak_reserved_bytes,
                                "reserved_bytes_after": 0, "complete_reference_checks": 2,
                                "input_sha256": input_sha256, "reference_sha256": reference_sha256,
                            })
                        );
                    }
                }
                assert_eq!(fingerprint(&input), input_sha256);
                assert_eq!(fingerprint(&expected), reference_sha256);
                cell_index += 1;
            }
        }
    }
    assert_eq!(cell_index, SHAPES.len() * ROWS.len() * CHUNKS.len());
    println!(
        "SHARDLOOM_BUILDER_COST_COMPLETE={}",
        serde_json::json!({
            "schema_version": "shardloom.native_builder_lifecycle_cost.v1",
            "cells": cell_index, "pairs_per_cell": PAIRS, "warmup": WARMUP,
            "reverse": reverse, "budget_bytes_per_variant": GRANT,
            "all_complete_values_equal": true, "all_result_credits_released": true,
            "scope": "native concatenation, typed shape checks and output destruction; fixture/context construction, complete oracle checks and logging excluded",
            "comparison": "same executable, native append providers and allocator; ProviderMemory admission installed only in admitted variant",
        })
    );
}
