// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use super::*;
use crate::FlatBufferBuilder;
use std::panic::{catch_unwind, AssertUnwindSafe};

struct Child;
impl Verifiable for Child {
    fn run_verifier(v: &mut Verifier, pos: usize) -> Result<()> {
        v.visit_table(pos)?
            .visit_field::<u32>("value", 4, true)?
            .finish();
        Ok(())
    }
}

fn fixture() -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let child = builder.start_table();
    builder.push_slot_always(4, 37_u32);
    let child = builder.end_table(child);
    let parent = builder.start_table();
    builder.push_slot_always(4, child);
    builder.push_slot_always(6, 9_u8);
    let parent = builder.end_table(parent);
    builder.finish_minimal(parent);
    builder.finished_data().to_vec()
}

fn recursive(v: &mut Verifier, pos: usize) -> Result<()> {
    v.visit_table(pos)?
        .visit_field::<ForwardsUOffset<Child>>("child", 4, false)?
        .visit_field::<u8>("after", 6, false)?
        .finish();
    Ok(())
}

fn continued(v: &mut Verifier, pos: usize) -> Result<()> {
    let mut table = v.visit_table(pos)?;
    if let Some(child) = table.deref(4)? {
        let pending = table.suspend();
        append_trace(
            <ForwardsUOffset<Child>>::run_verifier(v, child),
            ErrorTraceDetail::TableField {
                field_name: "child".into(),
                position: child,
            },
        )?;
        table = pending.resume(v);
    }
    table.visit_field::<u8>("after", 6, false)?.finish();
    Ok(())
}

fn run(bytes: &[u8], opts: &VerifierOptions, iterative: bool) -> (Result<()>, [usize; 3]) {
    let mut v = Verifier::new(opts, bytes);
    let result = (|| {
        let pos = v.get_uoffset(0)? as usize;
        if iterative {
            continued(&mut v, pos)
        } else {
            recursive(&mut v, pos)
        }
    })();
    (result, [v.depth, v.num_tables, v.apparent_size])
}

#[test]
fn suspend_resume_preserves_every_counter_and_error_trace() {
    let bytes = fixture();
    let opts = VerifierOptions::default();
    let expected = run(&bytes, &opts, false);
    assert!(expected.0.is_ok());
    assert_eq!(expected.1[0], 0);
    assert_eq!(expected.1[1], 2);
    assert_eq!(expected, run(&bytes, &opts, true));
    for end in 0..=bytes.len() {
        assert_eq!(
            run(&bytes[..end], &opts, false),
            run(&bytes[..end], &opts, true)
        );
    }
    for position in 0..bytes.len() {
        for replacement in [0, 1, 0x7f, 0x80, 0xff] {
            let mut changed = bytes.clone();
            changed[position] = replacement;
            assert_eq!(run(&changed, &opts, false), run(&changed, &opts, true));
        }
    }
    for depth in 0..=3 {
        for tables in 0..=3 {
            for apparent in 0..=expected.1[2] + 1 {
                let opts = VerifierOptions {
                    max_depth: depth,
                    max_tables: tables,
                    max_apparent_size: apparent,
                    ..VerifierOptions::default()
                };
                assert_eq!(run(&bytes, &opts, false), run(&bytes, &opts, true));
            }
        }
    }
}

#[test]
fn suspended_tables_reject_misordered_and_mismatched_resumption() {
    let bytes = fixture();
    let other_bytes = bytes.clone();
    let opts = VerifierOptions::default();
    let other_opts = opts.clone();
    for misuse in 0..4 {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut v = Verifier::new(&opts, &bytes);
            let pos = v.get_uoffset(0).unwrap() as usize;
            let pending = v.visit_table(pos).unwrap().suspend();
            match misuse {
                0 => {
                    let _unfinished_child = v.visit_table(pos).unwrap().suspend();
                    pending.resume(&mut v).finish();
                }
                1 => {
                    v.reset();
                    pending.resume(&mut v).finish();
                }
                2 => {
                    let mut other = Verifier::new(&opts, &other_bytes);
                    let _other_table = other.visit_table(pos).unwrap().suspend();
                    pending.resume(&mut other).finish();
                }
                3 => {
                    let mut other = Verifier::new(&other_opts, &bytes);
                    let _other_table = other.visit_table(pos).unwrap().suspend();
                    pending.resume(&mut other).finish();
                }
                _ => unreachable!(),
            }
        }));
        assert!(result.is_err(), "invalid continuation accepted: {}", misuse);
    }
}

#[test]
fn checked_vector_range_and_deferred_trace_match_ordinary_verification() {
    let mut builder = FlatBufferBuilder::new();
    let vector = builder.create_vector(&[1_u32, 2, 3]);
    builder.finish_minimal(vector);
    let bytes = builder.finished_data();
    let opts = VerifierOptions::default();
    let mut direct = Verifier::new(&opts, bytes);
    <ForwardsUOffset<Vector<'_, u32>>>::run_verifier(&mut direct, 0).unwrap();
    let mut deferred = Verifier::new(&opts, bytes);
    let position = deferred.get_uoffset(0).unwrap() as usize;
    let range = verify_vector_range::<u32>(&mut deferred, position).unwrap();
    assert_eq!(range.len(), 12);
    assert_eq!(direct.apparent_size, deferred.apparent_size);

    let data_error: Result<()> = InvalidFlatbuffer::new_missing_required("value");
    let traced = append_trace(
        data_error.clone(),
        ErrorTraceDetail::VectorElement {
            index: 1,
            position: 8,
        },
    );
    assert_ne!(traced, data_error);
    for error in [
        InvalidFlatbuffer::DepthLimitReached,
        InvalidFlatbuffer::TooManyTables,
        InvalidFlatbuffer::ApparentSizeTooLarge,
    ] {
        assert_eq!(
            append_trace::<()>(
                Err(error.clone()),
                ErrorTraceDetail::TableField {
                    field_name: "value".into(),
                    position: 8
                }
            ),
            Err(error)
        );
    }
}
