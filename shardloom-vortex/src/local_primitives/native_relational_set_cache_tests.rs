//! First-occurrence references exercise admitted and declined key directories.

use super::*;
use crate::{
    local_primitives::native_relational_keys::Cell, resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{DictArray, PrimitiveArray, StructArray, VarBinArray, VarBinViewArray},
    dtype::{FieldNames, Nullability, PType},
    validity::Validity,
};

fn record(array: ArrayRef) -> ArrayRef {
    let rows = array.len();
    StructArray::try_new(
        FieldNames::from(["key"]),
        vec![array],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn verify_batches(arrays: &[ArrayRef], expect_cache: bool) {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let fields = vec![("key".into(), arrays[0].dtype().clone())];
    let names = vec!["key".into()];
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut set = RowSet::new(&fields, &names, context.memory())?;
            let mut first: Vec<Cell> = vec![];
            let mut total = 0;
            for source in arrays {
                let mut execution = context.native_session().create_execution_ctx();
                let reference = super::super::native_relational_keys::KeyColumn::values(
                    source,
                    &mut execution,
                    context.memory(),
                    context.cancellation(),
                )?;
                // Linear independent membership preserves first occurrence and
                // does not depend on the candidate hash or directory state.
                let expected = (0..source.len())
                    .map(|row| {
                        let value = reference.cell(row).unwrap();
                        first
                            .iter()
                            .position(|prior| *prior == value)
                            .unwrap_or_else(|| {
                                first.push(value);
                                first.len() - 1
                            })
                    })
                    .collect::<Vec<_>>();
                #[cfg(debug_assertions)]
                let observation = super::super::native_relational_observation::Session::start();
                let actual = set.intern_batch(record(source.clone()), context, 7)?;
                assert_eq!(actual.values, expected);
                assert_eq!(set.rows(), first.len());
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    assert_eq!(counts.key_cache_batches > 0, expect_cache);
                    assert_eq!(counts.group_ordinal_rows, source.len() as u64);
                    if expect_cache {
                        assert!(counts.key_cache_hits > 0);
                        assert!(counts.key_exact_lookups < source.len() as u64 / 2);
                    } else {
                        assert_eq!(counts.key_cache_hits, 0);
                        assert_eq!(counts.key_exact_lookups, source.len() as u64);
                    }
                }
                total += actual.values.len();
            }
            assert_eq!(total, arrays.iter().map(ArrayRef::len).sum::<usize>());
            let rows = (0..first.len()).map(Some).collect::<Vec<_>>();
            let output = set
                .gather(&rows, context)?
                .column("key", &fields[0].1, context)?;
            drop(set);
            let mut execution = context.native_session().create_execution_ctx();
            let output = super::super::native_relational_keys::KeyColumn::values(
                &output,
                &mut execution,
                context.memory(),
                context.cancellation(),
            )?;
            for (row, expected) in first.iter().enumerate() {
                assert_eq!(&output.cell(row)?, expected);
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dense_key_cache_preserves_negative_extreme_null_and_changed_batch_domains() {
    let dense = |offset: i64| {
        PrimitiveArray::from_option_iter(
            (0..4097_i64).map(|row| (row % 11 != 0).then_some(offset + (row * 7) % 29)),
        )
        .into_array()
    };
    verify_batches(&[dense(-43), dense(-17), dense(-43)], true);
    let extreme = PrimitiveArray::from_option_iter(
        (0..1024_u64).map(|row| (row % 13 != 0).then_some(u64::MAX - row % 17)),
    )
    .into_array();
    verify_batches(&[extreme.clone(), extreme], true);
    let minimum =
        PrimitiveArray::from_iter((0..1024_i64).map(|row| i64::MIN + row % 13)).into_array();
    verify_batches(&[minimum], true);
    verify_batches(
        &[PrimitiveArray::from_option_iter([None::<i8>; 128]).into_array()],
        true,
    );
}

#[test]
fn key_cache_declines_sparse_high_cardinality_and_tiny_domains() {
    verify_batches(
        &[PrimitiveArray::from_iter((0..1024).map(
            |row| {
                if row % 2 == 0 { i64::MIN } else { i64::MAX }
            },
        ))
        .into_array()],
        false,
    );
    verify_batches(
        &[PrimitiveArray::from_iter(0..4096_u32).into_array()],
        false,
    );
    verify_batches(
        &[PrimitiveArray::from_iter([1_i8, 2, 1]).into_array()],
        false,
    );
}

#[test]
fn key_cache_preserves_all_integer_widths_and_encoded_numeric_inputs() {
    macro_rules! check {
        ($($ty:ty),+) => { $(
            let values = PrimitiveArray::from_option_iter((0..256_u16).map(|row| {
                (row % 13 != 0).then_some(<$ty>::try_from(row % 7).unwrap())
            })).into_array();
            let dictionary = DictArray::try_new(
                PrimitiveArray::from_iter((0..1024_u16).map(|row| row % 256)).into_array(),
                values.clone(),
            ).unwrap().into_array();
            verify_batches(&[values, dictionary], true);
        )+ };
    }
    check!(u8, u16, u32, u64, i8, i16, i32, i64);
}

#[test]
fn membership_cache_does_not_intern_nulls_under_join_null_policy() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let fields = vec![(
                "key".into(),
                DType::Primitive(PType::I16, Nullability::Nullable),
            )];
            let names = vec!["key".into()];
            let mut set = RowSet::with_nulls_equal(&fields, &names, false, context.memory())?;
            let source = PrimitiveArray::from_option_iter(
                (0..1024_i16).map(|row| (row % 3 != 0).then_some(row % 7 - 3)),
            )
            .into_array();
            assert_eq!(
                set.insert_batch(record(source.clone()), None, context, 3, None)?,
                7
            );
            assert_eq!(set.insert_batch(record(source), None, context, 3, None)?, 0);
            assert_eq!(set.rows(), 7);
            let batch = Batch::new(
                record(PrimitiveArray::from_option_iter([None, Some(0_i16)]).into_array()),
                &names,
                context,
            )?;
            assert!(!set.contains(&batch, 0, context)?);
            assert!(set.contains(&batch, 1, context)?);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_key_cache_keeps_duplicate_values_null_codes_and_changing_dictionaries() {
    for binary in [false, true] {
        let domains = [
            [Some("b"), Some("a"), Some("b"), None, Some("東京\0")],
            [Some("東京\0"), None, Some("a"), Some("c"), Some("c")],
        ];
        let arrays = domains.map(|values| {
            let domain = if binary {
                VarBinArray::from(values.map(|v| v.map(str::as_bytes)).to_vec()).into_array()
            } else {
                VarBinViewArray::from_iter_nullable_str(values).into_array()
            };
            DictArray::try_new(
                PrimitiveArray::from_option_iter(
                    (0..1024_u16).map(|row| (row % 7 != 0).then_some(row % 5)),
                )
                .into_array(),
                domain,
            )
            .unwrap()
            .into_array()
        });
        verify_batches(&arrays, true);
    }
}

#[test]
fn cached_set_rejects_late_invalid_codes_and_releases_all_credits() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let fields = vec![("key".into(), DType::Utf8(Nullability::NonNullable))];
            let names = vec!["key".into()];
            let mut set = RowSet::new(&fields, &names, context.memory())?;
            let codes = (0..1024).map(|row| if row == 1023 { 2_u8 } else { 0 });
            let source = DictArray::try_new(
                PrimitiveArray::from_iter(codes).into_array(),
                VarBinViewArray::from_iter_str(["ok", "unused"]).into_array(),
            )
            .unwrap()
            .into_array();
            assert!(
                set.intern_batch(record(source), context, 8)
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("dictionary code exceeds its value domain")
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn key_directory_reservation_declines_low_memory_and_checks_cancellation() {
    use super::super::native_relational_keys::{KeyColumn, KeyLookupCache};
    let source = PrimitiveArray::from_iter((0..128_u8).map(|row| row % 4)).into_array();
    for (budget, admitted) in [(1023, false), (4096, true)] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let cancellation = CancellationToken::default();
        let mut execution = vortex::session::VortexSession::empty().create_execution_ctx();
        let key = KeyColumn::new(&source, &mut execution, &memory, &cancellation).unwrap();
        let cache = KeyLookupCache::new(&key, &memory, &cancellation).unwrap();
        assert_eq!(cache.is_some(), admitted);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            if admitted { 64 } else { 0 }
        );
        drop(cache);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        cancellation.cancel();
        assert!(KeyLookupCache::new(&key, &memory, &cancellation).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn zero_measure_grouping_keeps_first_occurrence_without_row_ordinals() {
    use super::super::native_relational_aggregate::{Aggregate, Spec};
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let fields = vec![(
                "key".into(),
                DType::Primitive(PType::I64, Nullability::NonNullable),
            )];
            let spec = Spec {
                fields: fields.clone(),
                groups: fields.clone(),
                group_names: vec!["key".into()],
                measures: vec![],
            };
            let mut aggregate = Aggregate::new(&spec, context.memory())?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            aggregate.consume(
                &record(
                    PrimitiveArray::from_iter((0..1024_i64).map(|row| 7 - row % 11)).into_array(),
                ),
                context,
                4,
            )?;
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.group_ordinal_rows, 0);
                assert_eq!(counts.key_cache_hits, 1013);
            }
            let mut observed: Vec<i64> = vec![];
            aggregate.finish(context, 4, &mut |array| {
                let key = crate::local_primitives::logical_field_from_native_array(&array, "key")?;
                let values = key
                    .execute::<PrimitiveArray>(&mut context.native_session().create_execution_ctx())
                    .map_err(crate::local_primitives::vortex_error)?;
                observed.extend(values.as_slice::<i64>());
                Ok(())
            })?;
            assert_eq!(observed, (0..11).map(|n| 7 - n).collect::<Vec<i64>>());
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)]
fn row_set_epoch_domain_map_reuses_pointer_and_byte_equal_domains_across_batches() {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let domain_a = VarBinViewArray::from_iter_nullable_str([
        Some("alpha"),
        None,
        Some("beta"),
        Some("alpha"),
        Some("東京\0"),
    ])
    .into_array();
    let domain_a_copy = VarBinViewArray::from_iter_nullable_str([
        Some("alpha"),
        None,
        Some("beta"),
        Some("alpha"),
        Some("東京\0"),
    ])
    .into_array();
    let domain_b =
        VarBinViewArray::from_iter_nullable_str([Some("gamma"), Some("alpha"), None]).into_array();
    let batches = [
        DictArray::try_new(
            PrimitiveArray::from_option_iter([Some(0_u8), None, Some(2), Some(3), Some(4)])
                .into_array(),
            domain_a.clone(),
        )
        .unwrap()
        .into_array(),
        // Pointer-identical dictionary domain across batch boundary.
        DictArray::try_new(
            PrimitiveArray::from_option_iter([Some(4_u8), Some(1), Some(0), Some(2), None])
                .into_array(),
            domain_a,
        )
        .unwrap()
        .into_array(),
        // Distinct domain owner with identical valid mask and bytes (<= 8,192 entries).
        DictArray::try_new(
            PrimitiveArray::from_iter([2_u8, 0, 3, 1, 4]).into_array(),
            domain_a_copy,
        )
        .unwrap()
        .into_array(),
        // Changed dictionary domain resets epoch slot map cleanly.
        DictArray::try_new(
            PrimitiveArray::from_option_iter([Some(0_u8), Some(1), Some(2), None]).into_array(),
            domain_b,
        )
        .unwrap()
        .into_array(),
    ];
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let fields = vec![("key".into(), batches[0].dtype().clone())];
            let names = vec!["key".into()];
            let mut set = super::RowSet::new(&fields, &names, context.memory())?;
            let mut first = Vec::new();
            let mut execution = context.native_session().create_execution_ctx();
            let expected_counts = [(0_u64, 5_u64), (5, 0), (5, 0), (1, 3)];
            for (batch_idx, source) in batches.iter().enumerate() {
                let reference = super::super::native_relational_keys::KeyColumn::values(
                    source,
                    &mut execution,
                    context.memory(),
                    context.cancellation(),
                )?;
                let expected = (0..source.len())
                    .map(|row| {
                        let value = reference.cell(row).unwrap();
                        first
                            .iter()
                            .position(|prior| *prior == value)
                            .unwrap_or_else(|| {
                                first.push(value);
                                first.len() - 1
                            })
                    })
                    .collect::<Vec<_>>();
                #[cfg(debug_assertions)]
                let observation = super::super::native_relational_observation::Session::start();
                let actual = set.intern_batch(record(source.clone()), context, 7)?;
                assert_eq!(actual.values, expected, "batch_idx={batch_idx}");
                assert_eq!(set.rows(), first.len(), "batch_idx={batch_idx}");
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    let (exp_hits, exp_lookups) = expected_counts[batch_idx];
                    assert!(counts.key_cache_batches > 0, "batch_idx={batch_idx}");
                    assert_eq!(counts.key_cache_hits, exp_hits, "batch_idx={batch_idx}");
                    assert_eq!(
                        counts.key_exact_lookups, exp_lookups,
                        "batch_idx={batch_idx}"
                    );
                }
            }
            let rows = (0..first.len()).map(Some).collect::<Vec<_>>();
            let output = set
                .gather(&rows, context)?
                .column("key", &fields[0].1, context)?;
            drop(set);
            let output = super::super::native_relational_keys::KeyColumn::values(
                &output,
                &mut execution,
                context.memory(),
                context.cancellation(),
            )?;
            for (row, expected) in first.iter().enumerate() {
                assert_eq!(&output.cell(row)?, expected);
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn epoch_domain_map_releases_input_buffer_owners_after_each_batch() {
    struct WitnessBytes {
        bytes: Vec<u8>,
        _witness: std::sync::Arc<()>,
    }

    impl AsRef<[u8]> for WitnessBytes {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let make_domain = |witness: &std::sync::Arc<()>| {
                let values = [
                    Some("category_alpha_long_witness"),
                    None,
                    Some("category_beta_long_witness"),
                    Some("category_alpha_long_witness"),
                    Some("東京_long_witness\0"),
                ];
                let mut raw_bytes = Vec::new();
                let mut offsets = Vec::with_capacity(values.len() + 1);
                offsets.push(0_u32);
                for entry in values {
                    if let Some(text) = entry {
                        raw_bytes.extend_from_slice(text.as_bytes());
                    }
                    offsets.push(u32::try_from(raw_bytes.len()).unwrap());
                }
                let buffer = vortex::buffer::ByteBuffer::from_bytes_aligned(
                    bytes::Bytes::from_owner(WitnessBytes {
                        bytes: raw_bytes,
                        _witness: std::sync::Arc::clone(witness),
                    }),
                    vortex::buffer::Alignment::of::<u8>(),
                );
                let mut execution = context.native_session().create_execution_ctx();
                VarBinArray::try_new(
                    PrimitiveArray::from_iter(offsets).into_array(),
                    buffer,
                    DType::Utf8(Nullability::Nullable),
                    Validity::from_iter([true, false, true, true, true]),
                    &mut execution,
                )
                .unwrap()
                .into_array()
            };
            let fields = vec![("key".into(), DType::Utf8(Nullability::Nullable))];
            let names = vec!["key".into()];
            let mut set = RowSet::new(&fields, &names, context.memory())?;

            let witness_0 = std::sync::Arc::new(());
            let weak_0 = std::sync::Arc::downgrade(&witness_0);
            let batch_0 = DictArray::try_new(
                PrimitiveArray::from_option_iter([Some(0_u8), None, Some(2), Some(3), Some(4)])
                    .into_array(),
                make_domain(&witness_0),
            )
            .unwrap()
            .into_array();
            drop(witness_0);
            let mapping_0 = set.intern_batch(record(batch_0), context, 7)?;
            assert_eq!(mapping_0.values, vec![0, 1, 2, 0, 3]);
            // While `set` (and `set.epoch_domain`) stays live across batches,
            // zero input buffer owners from batch 0 may remain retained.
            assert_eq!(weak_0.strong_count(), 0);
            assert!(set.epoch_domain.is_some());

            let witness_1 = std::sync::Arc::new(());
            let weak_1 = std::sync::Arc::downgrade(&witness_1);
            let batch_1 = DictArray::try_new(
                PrimitiveArray::from_option_iter([Some(4_u8), Some(1), Some(0), Some(2), None])
                    .into_array(),
                make_domain(&witness_1),
            )
            .unwrap()
            .into_array();
            drop(witness_1);
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let mapping_1 = set.intern_batch(record(batch_1), context, 7)?;
            assert_eq!(mapping_1.values, vec![3, 1, 0, 2, 1]);
            assert_eq!(weak_1.strong_count(), 0);
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert!(counts.key_cache_batches > 0);
                assert_eq!(counts.key_cache_hits, 5);
                assert_eq!(counts.key_exact_lookups, 0);
            }
            drop(set);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
