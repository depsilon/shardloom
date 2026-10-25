// SPDX-License-Identifier: Apache-2.0
//! Frozen complete text/binary decode, selection, concatenation and transfer screen.
//! Compile this identical harness against the baseline and candidate providers.

use super::*;
use sha2::{Digest as _, Sha256};
use std::{fmt::Write as _, time::Instant};
use vortex::{
    array::{
        ArrayContext, ExecutionCtx,
        arrays::VarBinViewArray,
        serde::{SerializeOptions, SerializedArray},
    },
    buffer::ByteBufferMut,
    encodings::zstd::Zstd,
    mask::Mask,
    session::registry::ReadContext,
};

const GRANT: u64 = 64 << 20;
const SAMPLES: usize = 3;
const OPERATIONS: usize = 25;

struct Fixture {
    bytes: ByteBuffer,
    context: ArrayContext,
    dtype: DType,
    rows: usize,
    encoding: String,
    filter: Mask,
    take: ArrayRef,
    expected: Vec<Option<Vec<u8>>>,
}

fn fingerprint(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut value, "{byte:02x}").unwrap();
    }
    value
}

fn serialize(array: &ArrayRef, session: &VortexSession) -> (ByteBuffer, ArrayContext) {
    let context = ArrayContext::empty();
    let mut bytes = ByteBufferMut::empty();
    for buffer in array
        .serialize(&context, session, &SerializeOptions::default())
        .unwrap()
    {
        bytes.extend_from_slice(buffer.as_ref());
    }
    (bytes.freeze(), context)
}

fn value(kind: &str, row: usize) -> Option<Vec<u8>> {
    let run = row / 4;
    if run.is_multiple_of(13) {
        return None;
    }
    let mut bytes = match run % 17 {
        0 => Vec::new(),
        1 => b"tiny".to_vec(),
        _ => format!("shared λ 猫 buffer {run:06} value ").into_bytes(),
    };
    if kind == "binary" && run % 17 > 1 {
        bytes.extend_from_slice(&[0, 0x80, 0xff]);
    }
    Some(bytes)
}

fn plain(kind: &str, values: &[Option<Vec<u8>>]) -> VarBinViewArray {
    if kind == "binary" {
        VarBinViewArray::from_iter_nullable_bin(values.iter().map(Option::as_deref))
    } else {
        VarBinViewArray::from_iter_nullable_str(values.iter().map(|bytes| {
            bytes
                .as_deref()
                .map(|bytes| std::str::from_utf8(bytes).unwrap())
        }))
    }
}

fn fixture(kind: &str, rows: usize, encoding: &str) -> Fixture {
    let values: Vec<_> = (0..rows).map(|row| value(kind, row)).collect();
    let input = plain(kind, &values);
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();
    let encoded = match encoding {
        "canonical" => input.clone().into_array(),
        "fsst" => {
            let input = input.clone().into_array();
            let compressor = fsst_train_compressor(&input, &mut ctx).unwrap();
            fsst_compress(&input, &compressor, &mut ctx)
                .unwrap()
                .into_array()
        }
        "zstd" => Zstd::from_var_bin_view_without_dict(
            &input,
            0,
            if rows == 32 { 8 } else { 1024 },
            &mut ctx,
        )
        .unwrap()
        .into_array(),
        "runend" => {
            let run_values: Vec<_> = values.iter().step_by(4).cloned().collect();
            let ends = PrimitiveArray::from_iter(
                (4..=rows).step_by(4).map(|end| u32::try_from(end).unwrap()),
            );
            RunEnd::try_new(
                ends.into_array(),
                plain(kind, &run_values).into_array(),
                &mut ctx,
            )
            .unwrap()
            .into_array()
        }
        _ => panic!("unknown frozen buffer-table fixture"),
    };
    let (bytes, context) = serialize(&encoded, &session);
    let filter = Mask::from_iter((0..rows).map(|row| row % 3 != 1));
    let selected: Vec<_> = values
        .into_iter()
        .enumerate()
        .filter_map(|(row, value)| (row % 3 != 1).then_some(value))
        .rev()
        .collect();
    let take = PrimitiveArray::from_iter(
        (0..selected.len())
            .rev()
            .map(|row| u32::try_from(row).unwrap()),
    )
    .into_array();
    Fixture {
        bytes,
        context,
        dtype: input.dtype().clone(),
        rows,
        encoding: encoded.encoding_id().to_string(),
        filter,
        take,
        expected: selected[1..selected.len() - 1].to_vec(),
    }
}

fn compose(fixture: &Fixture, chunks: usize, session: &VortexSession) -> ArrayRef {
    let mut ctx = session.create_execution_ctx();
    let input = SerializedArray::try_from(fixture.bytes.clone())
        .unwrap()
        .decode(
            &fixture.dtype,
            fixture.rows,
            &ReadContext::new(fixture.context.to_ids()),
            session,
        )
        .unwrap();
    assert_eq!(input.encoding_id().to_string(), fixture.encoding);
    let decoded = input.execute::<VarBinViewArray>(&mut ctx).unwrap();
    let filtered = decoded
        .filter(fixture.filter.clone())
        .unwrap()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    let taken = filtered
        .take(fixture.take.clone())
        .unwrap()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    let sliced = taken.slice(1..taken.len() - 1).unwrap();
    let leaves: Vec<_> = (0..chunks)
        .map(|chunk| {
            sliced
                .slice(chunk * sliced.len() / chunks..(chunk + 1) * sliced.len() / chunks)
                .unwrap()
        })
        .collect();
    let output = ChunkedArray::try_new(leaves, fixture.dtype.clone())
        .unwrap()
        .into_array()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    record(output.into_array())
}

fn verify(array: ArrayRef, fixture: &Fixture, ctx: &mut ExecutionCtx) {
    let values = array
        .nth_child(0)
        .unwrap()
        .clone()
        .execute::<VarBinViewArray>(ctx)
        .unwrap();
    assert_eq!(values.dtype(), &fixture.dtype);
    assert_eq!(values.len(), fixture.expected.len());
    let mask = values
        .validity()
        .unwrap()
        .execute_mask(values.len(), ctx)
        .unwrap();
    for (row, expected) in fixture.expected.iter().enumerate() {
        assert_eq!(mask.value(row), expected.is_some(), "validity at {row}");
        if let Some(expected) = expected {
            assert_eq!(values.bytes_at(row).as_slice(), expected, "value at {row}");
        }
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
struct Observation {
    serialized_bytes: usize,
    serialized_sha256: String,
    output_rows: usize,
    producer_peak_bytes: u64,
    receiver_peak_bytes: u64,
    retained_metadata_bytes: u64,
}

fn operation(fixture: &Fixture, chunks: usize) -> Observation {
    let producer = ResidentVortexSession::new(GRANT, 1).unwrap();
    let source_memory = producer.shared_memory_pool();
    let source = producer
        .with_native_execution_context(&CancellationToken::default(), |context| {
            Ok(compose(fixture, chunks, context.native_session()))
        })
        .unwrap();
    let receiver = ResidentVortexSession::new(GRANT, 1).unwrap();
    let receiving_memory = receiver.shared_memory_pool();
    let copied = copy(&source, &receiver);
    assert_eq!(copied.report.materialized_rows, 0);
    let output = copied.arrays.values[0].0.clone();
    let (serialized_bytes, serialized_sha256) = receiver
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let (bytes, _) = serialize(&output, context.native_session());
            Ok((bytes.len(), fingerprint(bytes.as_ref())))
        })
        .unwrap();
    drop((source, producer));
    assert_eq!(source_memory.snapshot().reserved_bytes, 0);
    receiver
        .with_native_execution_context(&CancellationToken::default(), |context| {
            verify(
                output,
                fixture,
                &mut context.native_session().create_execution_ctx(),
            );
            Ok(())
        })
        .unwrap();
    let retained_metadata_bytes = copied.report.retained_metadata_bytes;
    drop((copied, receiver));
    assert_eq!(receiving_memory.snapshot().reserved_bytes, 0);
    Observation {
        serialized_bytes,
        serialized_sha256,
        output_rows: fixture.expected.len(),
        producer_peak_bytes: source_memory.snapshot().peak_reserved_bytes,
        receiver_peak_bytes: receiving_memory.snapshot().peak_reserved_bytes,
        retained_metadata_bytes,
    }
}

#[test]
#[ignore = "frozen complete buffer-table lifecycle screen; guarded serial release execution only"]
#[allow(clippy::assertions_on_constants)] // This opt-in screen must use an optimized build.
fn native_varbin_metadata_complete_lifecycle_cost_screen() {
    assert!(!cfg!(debug_assertions));
    assert_eq!(
        std::env::var("SHARDLOOM_VARBIN_METADATA_COST").as_deref(),
        Ok("frozen-v1")
    );
    let mut cells = 0usize;
    for kind in ["utf8", "binary"] {
        for rows in [32, 65_536] {
            for encoding in ["canonical", "fsst", "zstd", "runend"] {
                let fixture = fixture(kind, rows, encoding);
                let input_sha256 = fingerprint(fixture.bytes.as_ref());
                for chunks in [2, 16] {
                    let start = Instant::now();
                    let reference = operation(&fixture, chunks);
                    println!(
                        "VARBIN_METADATA_COST {}",
                        serde_json::json!({
                            "phase": "warmup", "kind": kind, "rows": rows,
                            "encoding": encoding, "chunks": chunks, "grant_bytes": GRANT,
                            "nanoseconds": [start.elapsed().as_nanos()], "observation": reference,
                            "input_sha256": input_sha256,
                        })
                    );
                    for sample in 0..SAMPLES {
                        let mut nanoseconds = Vec::with_capacity(OPERATIONS);
                        for _ in 0..OPERATIONS {
                            let start = Instant::now();
                            let observed = operation(&fixture, chunks);
                            let elapsed = start.elapsed().as_nanos();
                            assert_eq!(observed, reference);
                            nanoseconds.push(elapsed);
                        }
                        println!(
                            "VARBIN_METADATA_COST {}",
                            serde_json::json!({
                                "phase": "measured", "sample": sample, "kind": kind, "rows": rows,
                                "encoding": encoding, "chunks": chunks, "grant_bytes": GRANT,
                                "nanoseconds": nanoseconds, "observation": reference,
                                "input_sha256": input_sha256,
                            })
                        );
                    }
                    assert_eq!(fingerprint(fixture.bytes.as_ref()), input_sha256);
                    cells += 1;
                }
            }
        }
    }
    assert_eq!(cells, 32);
    println!(
        "VARBIN_METADATA_COST_COMPLETE {}",
        serde_json::json!({
            "cells": cells, "samples_per_cell": SAMPLES, "operations_per_sample": OPERATIONS,
            "recorded_warmups_per_cell": 1, "all_complete_values_equal": true,
            "all_source_and_receiving_credits_released": true,
        })
    );
}
