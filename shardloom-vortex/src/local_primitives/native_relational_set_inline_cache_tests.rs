//! Independent first-occurrence references cover complete composite outputs.

use super::*;
use crate::{
    local_primitives::native_relational_keys::{Cell, KeyColumn},
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{BoolArray, DictArray, NullArray, PrimitiveArray, StructArray, VarBinViewArray},
    dtype::{FieldNames, Nullability},
    validity::Validity,
};

fn record(arrays: Vec<ArrayRef>) -> ArrayRef {
    let rows = arrays[0].len();
    let names = (0..arrays.len())
        .map(|column| format!("k{column}"))
        .collect::<Vec<_>>();
    StructArray::try_new(
        FieldNames::from(names.iter().map(String::as_str).collect::<Vec<_>>()),
        arrays,
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn verify_batches(arrays: Vec<Vec<ArrayRef>>, expect_cache: &[bool]) {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let fields = arrays[0]
        .iter()
        .enumerate()
        .map(|(column, array)| (format!("k{column}"), array.dtype().clone()))
        .collect::<Vec<_>>();
    let names = fields
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    assert_eq!(arrays.len(), expect_cache.len());
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut set = RowSet::new(&fields, &names, context.memory())?;
            let mut first: Vec<Vec<Cell>> = Vec::new();
            for (sources, expect_cache) in arrays.iter().zip(expect_cache) {
                let mut execution = context.native_session().create_execution_ctx();
                let reference = sources
                    .iter()
                    .map(|source| {
                        KeyColumn::values(
                            source,
                            &mut execution,
                            context.memory(),
                            context.cancellation(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                // The complete tuple is compared linearly; this reference does not
                // use the candidate identity, its hash or the native RowIndex.
                let expected = (0..sources[0].len())
                    .map(|row| {
                        let values = reference
                            .iter()
                            .map(|key| key.cell(row))
                            .collect::<Result<Vec<_>>>()
                            .unwrap();
                        first
                            .iter()
                            .position(|prior| *prior == values)
                            .unwrap_or_else(|| {
                                first.push(values);
                                first.len() - 1
                            })
                    })
                    .collect::<Vec<_>>();
                #[cfg(debug_assertions)]
                let observation = super::super::native_relational_observation::Session::start();
                let actual = set.intern_batch(record(sources.clone()), context, 7)?;
                assert_eq!(actual.values, expected);
                assert_eq!(set.rows(), first.len());
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    assert_eq!(counts.key_inline_cache_batches > 0, *expect_cache);
                    if *expect_cache {
                        assert!(counts.key_inline_cache_hits > 0);
                        assert!(counts.key_exact_lookups < sources[0].len() as u64);
                    } else {
                        assert_eq!(counts.key_inline_cache_hits, 0);
                    }
                    assert_eq!(counts.group_ordinal_rows, sources[0].len() as u64);
                }
            }
            let rows = (0..first.len()).map(Some).collect::<Vec<_>>();
            let outputs = fields
                .iter()
                .map(|(name, dtype)| set.gather(&rows, context)?.column(name, dtype, context))
                .collect::<Result<Vec<_>>>()?;
            drop(set);
            drop(arrays);
            let mut execution = context.native_session().create_execution_ctx();
            for (column, output) in outputs.iter().enumerate() {
                let output = KeyColumn::values(
                    output,
                    &mut execution,
                    context.memory(),
                    context.cancellation(),
                )?;
                for (row, expected) in first.iter().enumerate() {
                    assert_eq!(output.cell(row)?, expected[column]);
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn inline_cache_complete_text_and_binary_results_survive_changed_batches() {
    let domains = [
        [
            None,
            Some(""),
            Some("a"),
            Some("a\0"),
            Some("é"),
            Some("abcdefghijkl"),
        ],
        [
            Some("b"),
            Some("a\0"),
            None,
            Some("東京"),
            Some(""),
            Some("a"),
        ],
    ];
    for binary in [false, true] {
        let arrays = domains.map(|domain| {
            let values = (0..2049).map(|row| domain[row * 7 % domain.len()]);
            vec![if binary {
                VarBinViewArray::from_iter_nullable_bin(
                    values.map(|value| value.map(str::as_bytes)),
                )
                .into_array()
            } else {
                VarBinViewArray::from_iter_nullable_str(values).into_array()
            }]
        });
        verify_batches(Vec::from(arrays), &[true, true]);
    }
    let bytes: [Option<&[u8]>; 5] = [
        None,
        Some(&[]),
        Some(&[0, 255]),
        Some(&[0]),
        Some(&[255; 12]),
    ];
    verify_batches(
        vec![vec![
            VarBinViewArray::from_iter_nullable_bin((0..1025).map(|row| bytes[row % bytes.len()]))
                .into_array(),
            VarBinViewArray::from_iter_nullable_bin(
                (0..1025).map(|row| bytes[(row / 5) % bytes.len()]),
            )
            .into_array(),
        ]],
        &[true],
    );
}

#[test]
fn inline_cache_complete_composite_integer_keys_preserve_all_widths_and_limits() {
    macro_rules! check {
        ($($ty:ty),+) => { $(
            let mut batches = Vec::new();
            for offset in [0usize, 1] {
                let values = [None, Some(<$ty>::MIN), Some(<$ty>::MAX), Some(0), Some(1)];
                batches.push(vec![
                    PrimitiveArray::from_option_iter((0..1025).map(|row| values[(row + offset) % values.len()])).into_array(),
                    VarBinViewArray::from_iter_nullable_str((0..1025).map(|row| [Some("a"), None, Some(""), Some("b\0")][(row / 5 + offset) % 4])).into_array(),
                ]);
            }
            verify_batches(batches, &[true, true]);
        )+ };
    }
    check!(u8, u16, u32, u64, i8, i16, i32, i64);
    verify_batches(
        vec![vec![
            BoolArray::from_iter((0..1025).map(|row| [None, Some(false), Some(true)][row % 3]))
                .into_array(),
            NullArray::new(1025).into_array(),
        ]],
        &[true],
    );
}

#[test]
fn inline_cache_repeat_prefix_cannot_hide_unique_or_long_tail_values() {
    let array = VarBinViewArray::from_iter_nullable_str((0..1025).map(|row| {
        if row < 256 {
            Some("repeat".into())
        } else if row % 11 == 0 {
            None
        } else if row % 7 == 0 {
            Some(format!("long-outlined-value-{row}"))
        } else {
            Some(format!("key-{row}"))
        }
    }))
    .into_array();
    verify_batches(vec![vec![array]], &[true]);
    let unique =
        VarBinViewArray::from_iter_str((0..1025).map(|row| format!("key-{row}"))).into_array();
    verify_batches(vec![vec![unique]], &[false]);
    let tiny = VarBinViewArray::from_iter_nullable_str([None, Some(""), Some("x")]).into_array();
    verify_batches(
        vec![vec![tiny.clone()], vec![tiny.slice(1..1).unwrap()]],
        &[false, false],
    );
}

#[test]
fn inline_cache_membership_keeps_null_and_semi_anti_policies() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let source = record(vec![
        VarBinViewArray::from_iter_nullable_str(
            (0..1025).map(|row| [None, Some(""), Some("a"), Some("b"), Some("c")][row % 5]),
        )
        .into_array(),
    ]);
    let fields = vec![("k0".to_string(), DType::Utf8(Nullability::Nullable))];
    let names = vec!["k0".to_string()];
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for nulls_equal in [false, true] {
                let mut allowed =
                    RowSet::with_nulls_equal(&fields, &names, nulls_equal, context.memory())?;
                allowed.insert_batch(
                    record(vec![
                        VarBinViewArray::from_iter_nullable_str([None, Some("a"), Some("c")])
                            .into_array(),
                    ]),
                    None,
                    context,
                    3,
                    None,
                )?;
                for want_match in [false, true] {
                    let mut output =
                        RowSet::with_nulls_equal(&fields, &names, nulls_equal, context.memory())?;
                    let expected = if want_match && nulls_equal { 3 } else { 2 };
                    assert_eq!(
                        output.insert_batch(
                            source.clone(),
                            Some((&allowed, want_match)),
                            context,
                            2,
                            None
                        )?,
                        expected
                    );
                    assert_eq!(
                        output.insert_batch(
                            source.clone(),
                            Some((&allowed, want_match)),
                            context,
                            2,
                            None
                        )?,
                        0
                    );
                    let batch = Batch::new(source.clone(), &names, context)?;
                    for row in 0..source.len() {
                        let member = match row % 5 {
                            0 => nulls_equal,
                            2 | 4 => true,
                            _ => false,
                        };
                        let expected = (row % 5 != 0 || nulls_equal) && member == want_match;
                        assert_eq!(output.contains(&batch, row, context)?, expected);
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    drop(source);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn inline_cache_composite_dictionary_keeps_late_failure_order_and_releases_credits() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let values = VarBinViewArray::from_iter_str(["valid"]).into_array();
            let dictionary = DictArray::try_new(
                PrimitiveArray::from_iter((0..1025).map(|row| u8::from(row == 1024))).into_array(),
                values,
            )
            .unwrap()
            .into_array();
            let boolean = BoolArray::from_iter(std::iter::repeat_n(true, 1025)).into_array();
            let fields = vec![
                ("k0".into(), dictionary.dtype().clone()),
                ("k1".into(), boolean.dtype().clone()),
            ];
            let names = vec!["k0".into(), "k1".into()];
            let mut set = RowSet::new(&fields, &names, context.memory())?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let error = set
                .intern_batch(record(vec![dictionary, boolean]), context, 7)
                .err()
                .unwrap();
            assert!(
                error
                    .to_string()
                    .contains("dictionary code exceeds its value domain")
            );
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.key_inline_cache_batches, 0);
                assert_eq!(counts.key_exact_lookups, 1024);
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn inline_cache_late_cancellation_releases_cache_state_and_gathered_payloads() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let token = CancellationToken::default();
    let result = session.with_native_execution_context(&token, |context| {
        let source = VarBinViewArray::from_iter_str((0..4097).map(|row| ["a", "b", "c"][row % 3]))
            .into_array();
        let fields = vec![("k0".into(), source.dtype().clone())];
        let names = vec!["k0".into()];
        let mut set = RowSet::new(&fields, &names, context.memory())?;
        let mut completed = 0;
        #[cfg(debug_assertions)]
        let observation = super::super::native_relational_observation::Session::start();
        let error = set
            .insert_batch(
                record(vec![source]),
                None,
                context,
                1,
                Some(&mut |_array| {
                    completed += 1;
                    token.cancel();
                    Ok(())
                }),
            )
            .err()
            .unwrap();
        assert!(error.to_string().contains("cancel"));
        assert_eq!(completed, 1);
        #[cfg(debug_assertions)]
        assert!(observation.finish().key_inline_cache_hits > 0);
        Ok(())
    });
    assert!(result.is_err());
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
