// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use crate::encoding::{self, DecodeContext, WireType};
use crate::{DecodeError, Message};

#[test]
fn explicit_depth_is_bounded_and_siblings_keep_their_parent_budget() {
    for budget in [0, 1, 2, 100, 8192] {
        let root = DecodeContext::with_recursion_limit(budget);
        let mut child = root.clone();
        for _ in 0..budget {
            assert!(child.check_recursion_limit().is_ok());
            child = child.enter_recursion_checked().unwrap();
        }
        let error = child.check_recursion_limit().unwrap_err();
        assert_eq!(
            error.to_string(),
            "failed to decode Protobuf message: recursion limit reached"
        );
        assert_eq!(child.enter_recursion_checked().unwrap_err(), error);
        // Failed child entry never changes the parent or the next sibling.
        assert_eq!(root.check_recursion_limit().is_ok(), budget != 0);
        assert_eq!(root.enter_recursion_checked().is_ok(), budget != 0);
    }
}

#[test]
fn default_depth_retains_the_feature_contract() {
    let mut context = DecodeContext::default();
    for _ in 0..100 {
        context = context.enter_recursion_checked().unwrap();
    }
    #[cfg(not(feature = "no-recursion-limit"))]
    assert!(context.enter_recursion_checked().is_err());
    #[cfg(feature = "no-recursion-limit")]
    for _ in 0..8192 {
        context = context.enter_recursion_checked().unwrap();
    }
}

fn compare_skip(wire: WireType, bytes: &[u8], budget: u32) {
    let mut actual = bytes;
    let mut original = bytes;
    let context = DecodeContext::with_recursion_limit(budget);
    let expected = crate::decode_reference::skip_field(wire, 3, &mut original, context.clone());
    let result = encoding::skip_field(wire, 3, &mut actual, context);
    assert_eq!(
        result, expected,
        "wire={wire:?} bytes={bytes:?} budget={budget}"
    );
    assert_eq!(
        actual, original,
        "wire={wire:?} bytes={bytes:?} budget={budget}"
    );
}

#[test]
fn unknown_fields_keep_original_errors_and_consumption() {
    let fixtures: &[&[u8]] = &[
        &[],
        &[0],
        &[0x80],
        &[0xff; 11],
        &[0x1c],
        &[0x24],
        &[0x1b, 0x1c, 0x1c],
        &[0x1b, 8, 7, 0x1c, 0x1c],
        &[0x1b, 0x23, 0x24, 0x1c, 0x1c],
        &[3, 1, 2, 3, 4],
        &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1],
    ];
    for wire in [
        WireType::Varint,
        WireType::ThirtyTwoBit,
        WireType::SixtyFourBit,
        WireType::LengthDelimited,
        WireType::StartGroup,
        WireType::EndGroup,
    ] {
        for budget in [0, 1, 2, 3, 100] {
            for bytes in fixtures {
                for end in 0..=bytes.len() {
                    compare_skip(wire, &bytes[..end], budget);
                }
                for index in 0..bytes.len() {
                    for byte in 0..=u8::MAX {
                        let mut changed = bytes.to_vec();
                        changed[index] = byte;
                        compare_skip(wire, &changed, budget);
                    }
                }
            }
        }
    }
}

fn merge_continued(
    wire: WireType,
    value: &mut u32,
    input: &mut &[u8],
    context: DecodeContext,
) -> Result<(), DecodeError> {
    let frame = encoding::message::DecodeContinuation::new(wire, input, context)?;
    while let Some((tag, wire)) = frame.next_field(input)? {
        value.merge_field(tag, wire, input, frame.context())?;
    }
    Ok(())
}

fn compare_message(wire: WireType, bytes: &[u8], budget: u32) {
    let mut actual = bytes;
    let mut original = bytes;
    let mut a = 17;
    let mut b = 17;
    let context = DecodeContext::with_recursion_limit(budget);
    let expected = encoding::message::merge(wire, &mut b, &mut original, context.clone());
    let result = merge_continued(wire, &mut a, &mut actual, context);
    assert_eq!(
        result, expected,
        "wire={wire:?} bytes={bytes:?} budget={budget}"
    );
    assert_eq!(
        actual, original,
        "wire={wire:?} bytes={bytes:?} budget={budget}"
    );
    assert_eq!(a, b, "wire={wire:?} bytes={bytes:?} budget={budget}");
}

#[test]
fn message_continuation_keeps_checks_partial_updates_and_overrun_order() {
    let fixtures: &[&[u8]] = &[
        &[],
        &[0],
        &[0x80],
        &[0xff; 11],
        &[2, 8, 1],
        &[1, 8, 1],          // A complete field consumes beyond the declared message.
        &[3, 8, 7, 8, 0x80], // Partial update before malformed second value.
        &[3, 0x1b, 0x1c, 8, 9],
        &[5, 8, 1],
        &[0, 8, 17],
    ];
    for wire in [
        WireType::Varint,
        WireType::LengthDelimited,
        WireType::StartGroup,
    ] {
        for budget in [0, 1, 2, 100] {
            for bytes in fixtures {
                for end in 0..=bytes.len() {
                    compare_message(wire, &bytes[..end], budget);
                }
                for index in 0..bytes.len() {
                    for byte in 0..=u8::MAX {
                        let mut changed = bytes.to_vec();
                        changed[index] = byte;
                        compare_message(wire, &changed, budget);
                    }
                }
            }
        }
    }
}

#[test]
fn deep_unknown_groups_finish_fail_and_release_on_a_small_stack() {
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(|| {
            const DEPTH: usize = 8192;
            // The outer StartGroup key is already consumed by the caller.
            let mut bytes = vec![0x1b; DEPTH - 1];
            bytes.extend(core::iter::repeat_n(0x1c, DEPTH));
            for valid in [true, false] {
                let mut input = bytes.as_slice();
                let budget = if valid { DEPTH } else { DEPTH - 1 };
                let result = encoding::skip_field(
                    WireType::StartGroup,
                    3,
                    &mut input,
                    DecodeContext::with_recursion_limit(budget as u32),
                );
                assert_eq!(result.is_ok(), valid);
                if valid {
                    assert!(input.is_empty());
                } else {
                    assert_eq!(input.len(), DEPTH);
                }
            }
            let mut wrong_end = bytes;
            wrong_end[DEPTH - 1] = 0x24;
            let mut input = wrong_end.as_slice();
            assert_eq!(
                encoding::skip_field(
                    WireType::StartGroup,
                    3,
                    &mut input,
                    DecodeContext::with_recursion_limit(DEPTH as u32)
                )
                .unwrap_err()
                .to_string(),
                "failed to decode Protobuf message: unexpected end group tag"
            );
            assert_eq!(input.len(), DEPTH - 1);
        })
        .unwrap()
        .join()
        .unwrap();
}
