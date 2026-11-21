// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::process::Command;
use std::thread;

use flatbuffers::{FlatBufferBuilder, Verifiable, VerifierOptions};

use super::*;

#[path = "array_verify_reference.rs"]
mod reference;

fn fixtures() -> Vec<(Vec<u8>, bool)> {
    let mut fixtures = Vec::new();
    for mask in 0..16 {
        for defaults in [false, true] {
            for array in [false, true] {
                let mut builder = FlatBufferBuilder::new();
                builder.force_defaults(defaults);
                let metadata = (mask & 1 != 0).then(|| builder.create_vector(&[0u8, 255, 12]));
                let children = (mask & 2 != 0).then(|| {
                    let leaf = fb::ArrayNode::create(&mut builder, &fb::ArrayNodeArgs::default());
                    let empty = builder.create_vector::<flatbuffers::WIPOffset<fb::ArrayNode>>(&[]);
                    let second = fb::ArrayNode::create(
                        &mut builder,
                        &fb::ArrayNodeArgs {
                            children: Some(empty),
                            encoding: 65535,
                            ..Default::default()
                        },
                    );
                    builder.create_vector(&[leaf, second, leaf])
                });
                let buffers = (mask & 4 != 0).then(|| builder.create_vector(&[0u16, 65535, 17]));
                let stats = (mask & 8 != 0).then(|| {
                    let min = builder.create_vector(&[0u8, 1, 255]);
                    let empty = builder.create_vector::<u8>(&[]);
                    fb::ArrayStats::create(
                        &mut builder,
                        &fb::ArrayStatsArgs {
                            min: Some(min),
                            min_precision: fb::Precision::Exact,
                            max: Some(empty),
                            max_precision: fb::Precision(255),
                            sum: Some(min),
                            is_sorted: Some(true),
                            is_strict_sorted: Some(false),
                            is_constant: Some(true),
                            null_count: Some(0),
                            uncompressed_size_in_bytes: Some(u64::MAX),
                            nan_count: Some(17),
                        },
                    )
                });
                let root = fb::ArrayNode::create(
                    &mut builder,
                    &fb::ArrayNodeArgs {
                        encoding: if defaults { 0 } else { 65535 },
                        metadata,
                        children,
                        buffers,
                        stats,
                    },
                );
                if array {
                    let buffers = builder.create_vector(&[
                        fb::Buffer::new(0, 0, fb::Compression::None, 0),
                        fb::Buffer::new(7, 6, fb::Compression(255), 65535),
                    ]);
                    let root = fb::Array::create(
                        &mut builder,
                        &fb::ArrayArgs {
                            root: Some(root),
                            buffers: Some(buffers),
                        },
                    );
                    builder.finish_minimal(root);
                } else {
                    builder.finish_minimal(root);
                }
                fixtures.push((builder.finished_data().to_vec(), array));
            }
        }
    }
    // Optional outer fields are not made logically required by the verifier.
    let mut builder = FlatBufferBuilder::new();
    let root = fb::Array::create(&mut builder, &fb::ArrayArgs::default());
    builder.finish_minimal(root);
    fixtures.push((builder.finished_data().to_vec(), true));
    fixtures
}

fn outcome(bytes: &[u8], options: &VerifierOptions, original: bool, array: bool) -> Result<()> {
    let mut verifier = Verifier::new(options, bytes);
    if original {
        reference::verify_root(&mut verifier, array).map_err(reference::normalize_marker_alignment)
    } else if array {
        <ForwardsUOffset<fb::Array>>::run_verifier(&mut verifier, 0)
    } else {
        <ForwardsUOffset<fb::ArrayNode>>::run_verifier(&mut verifier, 0)
    }
}

fn compare(bytes: &[u8], options: &VerifierOptions, array: bool) {
    assert_eq!(
        outcome(bytes, options, false, array),
        outcome(bytes, options, true, array)
    );
}

#[test]
fn physical_array_verifier_matches_original_mutations_and_truncations() {
    for (index, (bytes, array)) in fixtures().into_iter().enumerate() {
        let options = VerifierOptions::default();
        assert_eq!(outcome(&bytes, &options, true, array), Ok(()));
        compare(&bytes, &options, array);
        for end in 0..bytes.len() {
            compare(&bytes[..end], &options, array);
        }
        for position in 0..bytes.len() {
            for replacement in [0, 1, 0x7f, 0x80, 0xff] {
                let mut changed = bytes.clone();
                changed[position] = replacement;
                assert_eq!(
                    outcome(&changed, &options, false, array),
                    outcome(&changed, &options, true, array),
                    "fixture {index}, byte {position} = {replacement}"
                );
            }
        }
    }
}

fn limit(options: &mut VerifierOptions, axis: usize, value: usize) {
    match axis {
        0 => options.max_depth = value,
        1 => options.max_tables = value,
        2 => options.max_apparent_size = value,
        _ => unreachable!(),
    }
}

#[test]
fn physical_array_verifier_preserves_independent_budget_thresholds() {
    for (bytes, array) in fixtures() {
        for axis in 0..3 {
            let mut low = 0;
            let mut high = match axis {
                0 => 64,
                1 => 1_000_000,
                _ => 1 << 31,
            };
            while low < high {
                let middle = low + (high - low) / 2;
                let mut options = VerifierOptions::default();
                limit(&mut options, axis, middle);
                compare(&bytes, &options, array);
                if outcome(&bytes, &options, true, array).is_ok() {
                    high = middle;
                } else {
                    low = middle + 1;
                }
            }
            for value in [0, low.saturating_sub(1), low, low + 1] {
                let mut options = VerifierOptions::default();
                limit(&mut options, axis, value);
                compare(&bytes, &options, array);
                assert_eq!(
                    outcome(&bytes, &options, false, array).is_ok(),
                    value >= low
                );
            }
        }
    }
}

#[test]
fn aliased_children_consume_expanded_table_and_byte_budgets() {
    let mut builder = FlatBufferBuilder::new();
    let mut root = fb::ArrayNode::create(&mut builder, &fb::ArrayNodeArgs::default());
    for _ in 0..10 {
        let children = builder.create_vector(&[root, root]);
        root = fb::ArrayNode::create(
            &mut builder,
            &fb::ArrayNodeArgs {
                children: Some(children),
                ..Default::default()
            },
        );
    }
    builder.finish_minimal(root);
    let bytes = builder.finished_data();
    for (options, expected) in [
        (
            VerifierOptions {
                max_tables: 100,
                ..Default::default()
            },
            InvalidFlatbuffer::TooManyTables,
        ),
        (
            VerifierOptions {
                max_apparent_size: 4096,
                ..Default::default()
            },
            InvalidFlatbuffer::ApparentSizeTooLarge,
        ),
    ] {
        compare(bytes, &options, false);
        assert_eq!(outcome(bytes, &options, false, false), Err(expected));
    }
}

#[test]
fn deep_physical_array_verifier_on_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_PHYSICAL_ARRAY_VERIFIER";
    const COMPLETED: &str = "deep physical array verifier completed";
    if env::var_os(SELECTOR).is_some() {
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let mut builder = FlatBufferBuilder::new();
                let mut root = fb::ArrayNode::create(&mut builder, &fb::ArrayNodeArgs::default());
                for _ in 0..8192 {
                    let children = builder.create_vector(&[root]);
                    root = fb::ArrayNode::create(
                        &mut builder,
                        &fb::ArrayNodeArgs {
                            children: Some(children),
                            ..Default::default()
                        },
                    );
                }
                builder.finish_minimal(root);
                let bytes = builder.finished_data();
                assert_eq!(
                    outcome(bytes, &VerifierOptions::default(), false, false),
                    Err(InvalidFlatbuffer::DepthLimitReached)
                );
                let options = VerifierOptions {
                    max_depth: 9000,
                    max_tables: 9000,
                    max_apparent_size: 1 << 24,
                    ..Default::default()
                };
                let mut current =
                    flatbuffers::root_with_opts::<fb::ArrayNode>(&options, bytes).unwrap();
                for _ in 0..8192 {
                    current = current.children().unwrap().get(0);
                }
                assert!(current.children().is_none());
                let location = current._tab.loc();
                let mut corrupt = bytes.to_vec();
                corrupt[location..location + 4].copy_from_slice(&i32::MIN.to_le_bytes());
                match outcome(&corrupt, &options, false, false).unwrap_err() {
                    InvalidFlatbuffer::SignedOffsetOutOfBounds { error_trace, .. } => {
                        assert_eq!(error_trace.as_ref().len(), 8192 * 2)
                    }
                    _ => panic!("wrong deep physical array corruption error"),
                }
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{COMPLETED}");
        return;
    }
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "flatbuffers::array_verify::tests::deep_physical_array_verifier_on_small_stack",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(SELECTOR, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line == COMPLETED)
    );
}
