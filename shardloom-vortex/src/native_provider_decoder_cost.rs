//! One externally paired, complete native array-lifecycle timing sample.
//! The same fixture and verifier are compiled against baseline and candidate.

use super::*;
use std::{hint::black_box, time::Instant};
use vortex::{
    array::{
        arrays::{DictArray, Patched},
        patches::Patches,
    },
    buffer::Buffer,
    encodings::{
        alp::{ALP, ALPRD, Exponents},
        zigzag::zigzag_encode,
    },
};

const SHAPES: [&str; 18] = [
    "i64",
    "i64_nullable",
    "utf8_nullable",
    "constant_i64",
    "bitpacked_i32",
    "for_bitpacked_u64",
    "delta_i32",
    "rle_i32",
    "zigzag_i32",
    "runend_i32",
    "sequence_i32",
    "alp_f64_patches",
    "alprd_f64_patches",
    "patched_i32",
    "sparse_i32",
    "dict_bool",
    "dict_decimal",
    "dict_i64",
];
const ROWS: [usize; 3] = [32, 4096, 65536];
const CHUNKS: [usize; 2] = [2, 16];
const GRANT: u64 = 64 << 20;
const WARMUP: usize = 8;

fn reference(shape: &str, rows: usize) -> ArrayRef {
    match shape {
        "i64" | "i64_nullable" | "utf8_nullable" | "constant_i64" | "bitpacked_i32" => {
            cost::fixture(shape, rows, 2).1
        }
        "for_bitpacked_u64" => {
            PrimitiveArray::from_iter((0..rows).map(|row| 10_000 + row as u64 % 13)).into_array()
        }
        "delta_i32" | "sequence_i32" => {
            PrimitiveArray::from_iter((0..rows).map(|row| i32::try_from(row).unwrap() * 3 - 37))
                .into_array()
        }
        "rle_i32" | "runend_i32" => {
            PrimitiveArray::from_iter((0..rows).map(|row| i32::try_from(row / 7).unwrap()))
                .into_array()
        }
        "zigzag_i32" => PrimitiveArray::from_iter((0..rows).map(|row| {
            let value = i32::try_from(row).unwrap();
            if row.is_multiple_of(2) { value } else { -value }
        }))
        .into_array(),
        "alp_f64_patches" => PrimitiveArray::from_iter((0..rows).map(|row| {
            let value = f64::from(u32::try_from(row).unwrap());
            if row.is_multiple_of(17) {
                -value - 0.125
            } else {
                value
            }
        }))
        .into_array(),
        "alprd_f64_patches" => PrimitiveArray::from_iter(
            (0..rows).map(|row| f64::from(u32::try_from(row % 65).unwrap()) / 8.0 - 4.0),
        )
        .into_array(),
        "patched_i32" => PrimitiveArray::from_iter((0..rows).map(|row| {
            if row.is_multiple_of(17) {
                99_999
            } else {
                i32::try_from(row % 13).unwrap()
            }
        }))
        .into_array(),
        "sparse_i32" => PrimitiveArray::from_iter((0..rows).map(|row| {
            if row.is_multiple_of(17) {
                i32::try_from(row).unwrap() + 7
            } else {
                0
            }
        }))
        .into_array(),
        "dict_bool" => BoolArray::new(
            (0..rows).map(|row| row % 3 != 1).collect(),
            Validity::NonNullable,
        )
        .into_array(),
        "dict_decimal" => DecimalArray::new(
            (0..rows).map(|row| [123i128, -456, 789][row % 3]).collect(),
            DecimalDType::new(18, 2),
            Validity::NonNullable,
        )
        .into_array(),
        "dict_i64" => {
            PrimitiveArray::from_iter((0..rows).map(|row| [7i64, -17, 91][row % 3])).into_array()
        }
        _ => panic!("unrecognized frozen decoder fixture"),
    }
}

fn fixture(shape: &str, rows: usize, count: usize) -> (ArrayRef, ArrayRef) {
    let expected = reference(shape, rows);
    let mut ctx = VortexSession::default().create_execution_ctx();
    let leaves = (0..count)
        .map(|chunk| {
            let start = chunk * rows / count;
            let stop = (chunk + 1) * rows / count;
            let values = expected.slice(start..stop).unwrap();
            physical_leaf(shape, start, stop, values, &mut ctx)
        })
        .collect();
    (chunks(leaves), expected)
}

fn physical_leaf(
    shape: &str,
    start: usize,
    stop: usize,
    values: ArrayRef,
    ctx: &mut ExecutionCtx,
) -> ArrayRef {
    match shape {
        "i64" | "i64_nullable" | "utf8_nullable" => values,
        "constant_i64" => ConstantArray::new(-37i64, stop - start).into_array(),
        "bitpacked_i32" => BitPackedData::encode(&values, 4, ctx).unwrap().into_array(),
        "for_bitpacked_u64" => {
            let deltas =
                PrimitiveArray::from_iter((start..stop).map(|row| row as u64 % 13)).into_array();
            let packed = BitPackedData::encode(&deltas, 4, ctx).unwrap().into_array();
            FoR::try_new(packed, 10_000u64.into()).unwrap().into_array()
        }
        "delta_i32" => {
            Delta::try_from_primitive_array(&values.as_::<Primitive>().into_owned(), ctx)
                .unwrap()
                .into_array()
        }
        "rle_i32" => RLE::encode(values.as_::<Primitive>(), ctx)
            .unwrap()
            .into_array(),
        "zigzag_i32" => zigzag_encode(values.as_::<Primitive>())
            .unwrap()
            .into_array(),
        "runend_i32" => RunEnd::encode(values, ctx).unwrap().into_array(),
        "sequence_i32" => Sequence::try_new_typed(
            i32::try_from(start).unwrap() * 3 - 37,
            3,
            Nullability::NonNullable,
            stop - start,
        )
        .unwrap()
        .into_array(),
        "alp_f64_patches" => {
            let patches = patches(&values, start, |row| row.is_multiple_of(17), ctx);
            let encoded =
                PrimitiveArray::from_iter((start..stop).map(|row| i64::try_from(row).unwrap()))
                    .into_array();
            ALP::try_new(encoded, Exponents { e: 0, f: 0 }, patches)
                .unwrap()
                .into_array()
        }
        "alprd_f64_patches" => alprd(&values),
        "patched_i32" => {
            let patches = patches(&values, start, |row| row.is_multiple_of(17), ctx);
            let inner = PrimitiveArray::from_iter(
                (start..stop).map(|row| i32::try_from(row % 13).unwrap()),
            )
            .into_array();
            let packed = BitPackedData::encode(&inner, 4, ctx).unwrap().into_array();
            patches.map_or(packed.clone(), |patches| {
                Patched::from_array_and_patches(packed, &patches, ctx)
                    .unwrap()
                    .into_array()
            })
        }
        "sparse_i32" => {
            // Sparse requires at least one patch. A zero-valued patch keeps
            // small chunks without exceptions on the same physical route.
            let mut indices: Vec<_> = (start..stop)
                .enumerate()
                .filter_map(|(index, row)| {
                    row.is_multiple_of(17)
                        .then_some(u32::try_from(index).unwrap())
                })
                .collect();
            if indices.is_empty() {
                indices.push(0);
            }
            let patch_values = PrimitiveArray::from_iter(
                indices
                    .iter()
                    .map(|index| values.as_::<Primitive>().as_slice::<i32>()[*index as usize]),
            )
            .into_array();
            let patches = Patches::new(
                stop - start,
                0,
                PrimitiveArray::from_iter(indices).into_array(),
                patch_values,
                None,
            )
            .unwrap();
            Sparse::try_new_from_patches(patches, 0i32.into())
                .unwrap()
                .into_array()
        }
        "dict_bool" | "dict_decimal" | "dict_i64" => {
            let dictionary = reference(shape, 3);
            let codes =
                PrimitiveArray::from_iter((start..stop).map(|row| u32::try_from(row % 3).unwrap()))
                    .into_array();
            DictArray::try_new(codes, dictionary).unwrap().into_array()
        }
        _ => panic!("unrecognized frozen decoder leaf"),
    }
}

fn patches(
    values: &ArrayRef,
    start: usize,
    include: impl Fn(usize) -> bool,
    ctx: &mut ExecutionCtx,
) -> Option<Patches> {
    let indices: Vec<_> = (0..values.len())
        .filter(|index| include(start + index))
        .map(|index| u32::try_from(index).unwrap())
        .collect();
    if indices.is_empty() {
        return None;
    }
    let codes = PrimitiveArray::from_iter(indices).into_array();
    let selected = values
        .take(codes.clone())
        .unwrap()
        .execute::<Canonical>(ctx)
        .unwrap()
        .into_array();
    Some(Patches::new(values.len(), 0, codes, selected, None).unwrap())
}

fn alprd(values: &ArrayRef) -> ArrayRef {
    let bits: Vec<_> = values
        .as_::<Primitive>()
        .as_slice::<f64>()
        .iter()
        .map(|v| v.to_bits())
        .collect();
    let left: Vec<_> = bits
        .iter()
        .map(|v| u16::try_from(v >> 48).unwrap())
        .collect();
    let patches = Patches::new(
        values.len(),
        0,
        PrimitiveArray::from_iter((0..values.len()).map(|v| u32::try_from(v).unwrap()))
            .into_array(),
        Buffer::copy_from(&left).into_array(),
        None,
    )
    .unwrap();
    ALPRD::try_new(
        values.dtype().clone(),
        PrimitiveArray::from_iter(std::iter::repeat_n(0u16, values.len())).into_array(),
        buffer![left[0]],
        PrimitiveArray::from_iter(bits.into_iter().map(|b| b & ((1u64 << 48) - 1))).into_array(),
        48,
        Some(patches),
    )
    .unwrap()
    .into_array()
}

#[test]
fn native_decoder_cost_fixtures_match_complete_references() {
    for shape in SHAPES {
        for rows in [32, 4096] {
            for count in CHUNKS {
                let (input, expected) = fixture(shape, rows, count);
                let memory = LiveMemoryPool::new(GRANT).unwrap();
                cost::verify(
                    &input,
                    &expected,
                    &mut session(&memory).create_execution_ctx(),
                );
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
#[ignore = "native decoder lifecycle screen; guarded serial release execution required"]
#[allow(clippy::assertions_on_constants)]
fn native_decoder_lifecycle_cost_sample() {
    assert!(!cfg!(debug_assertions));
    assert_eq!(
        std::env::var("SHARDLOOM_DECODER_COST_SCREEN").as_deref(),
        Ok("1")
    );
    let shape = std::env::var("SHARDLOOM_DECODER_COST_SHAPE").unwrap();
    let rows: usize = std::env::var("SHARDLOOM_DECODER_COST_ROWS")
        .unwrap()
        .parse()
        .unwrap();
    let count: usize = std::env::var("SHARDLOOM_DECODER_COST_CHUNKS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(SHAPES.contains(&shape.as_str()) && ROWS.contains(&rows) && CHUNKS.contains(&count));
    let iterations = match rows {
        32 => 1024,
        4096 => 128,
        65536 => 16,
        _ => unreachable!(),
    };
    let (input, expected) = fixture(&shape, rows, count);
    let input_hash = cost::fingerprint(&input);
    let reference_hash = cost::fingerprint(&expected);
    let memory = LiveMemoryPool::new(GRANT).unwrap();
    let mut ctx = session(&memory).create_execution_ctx();
    cost::verify(&input, &expected, &mut ctx);
    for _ in 0..WARMUP {
        drop(black_box(cost::complete(&input, &mut ctx)));
    }
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let start = Instant::now();
    for _ in 0..iterations {
        drop(black_box(cost::complete(&input, &mut ctx)));
    }
    let nanos = start.elapsed().as_nanos();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    cost::verify(&input, &expected, &mut ctx);
    assert_eq!(cost::fingerprint(&input), input_hash);
    assert_eq!(cost::fingerprint(&expected), reference_hash);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    println!(
        "SHARDLOOM_DECODER_COST_SAMPLE={}",
        serde_json::json!({
            "shape": shape, "rows": rows, "chunks": count, "iterations": iterations,
            "warmup": WARMUP, "grant": GRANT, "batch_nanos": nanos,
            "peak_reserved_bytes": memory.snapshot().peak_reserved_bytes,
            "reserved_bytes_after": 0, "complete_reference_checks": 2,
            "input_sha256": input_hash, "reference_sha256": reference_hash,
            "complete_result_lifecycle": true,
        })
    );
}
