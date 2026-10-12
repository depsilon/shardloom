// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Dylan Justin Heinrich

use super::*;
use vortex_array::IntoArray;
use vortex_array::array_session;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinArray;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::StructFields;
use vortex_array::stats::PRUNING_STATS;

fn exact(stats: &StatsSet, stat: Stat, expected: impl Into<Scalar>) {
    assert_eq!(stats.get(stat).as_exact(), expected.into().into_value());
}

#[test]
fn running_integer_statistics_are_partition_independent_and_bounded() {
    let values = (0..4097)
        .map(|index| (index % 7 != 0).then_some(i64::from(index % 97) - 31))
        .collect::<Vec<_>>();
    let expected_sum: i64 = values.iter().flatten().sum();
    let expected_nulls = values.iter().filter(|value| value.is_none()).count() as u64;
    for chunk_size in [1, 17, 1024, values.len()] {
        let mut ctx = array_session().create_execution_ctx();
        let first = PrimitiveArray::from_option_iter(values.iter().copied()).into_array();
        let mut acc = StatsAccumulator::new(first.dtype(), PRUNING_STATS, 64);
        let capacity = acc.stats.capacity();
        for chunk in values.chunks(chunk_size) {
            acc.push_chunk(
                &PrimitiveArray::from_option_iter(chunk.iter().copied()).into_array(),
                &mut ctx,
            )
            .unwrap();
            assert_eq!(acc.stats.capacity(), capacity);
            assert!(acc.stats.iter().all(|stat| {
                stat.accumulated
                    .as_ref()
                    .is_none_or(|values| values.len() <= 1)
            }));
        }
        let stats = acc.as_stats_set();
        exact(&stats, Stat::Min, -31i64);
        exact(&stats, Stat::Max, 65i64);
        exact(&stats, Stat::Sum, expected_sum);
        exact(&stats, Stat::NullCount, expected_nulls);
        assert!(stats.get(Stat::NaNCount).is_absent());
    }
}

#[test]
fn overflow_never_recovers_into_a_partial_sum() {
    for chunks in [
        vec![vec![i64::MAX, i64::MAX], vec![-i64::MAX]],
        vec![vec![i64::MAX], vec![1], vec![-1]],
        vec![vec![-1], vec![i64::MIN, i64::MIN], vec![i64::MAX]],
    ] {
        let mut ctx = array_session().create_execution_ctx();
        let dtype = PrimitiveArray::from_iter([0i64]).dtype().clone();
        let mut acc = StatsAccumulator::new(&dtype, PRUNING_STATS, 64);
        for chunk in chunks {
            acc.push_chunk(&PrimitiveArray::from_iter(chunk).into_array(), &mut ctx)
                .unwrap();
        }
        assert!(acc.as_stats_set().get(Stat::Sum).is_absent());
        exact(&acc.as_stats_set(), Stat::NullCount, 0u64);
    }
}

#[test]
fn empty_null_and_nan_batches_do_not_erase_extrema() {
    let chunks = [
        vec![],
        vec![None, None],
        vec![Some(f64::NAN), None],
        vec![Some(7.0), Some(-2.0), None],
        vec![None, Some(f64::NAN)],
        vec![Some(3.0)],
    ];
    let mut ctx = array_session().create_execution_ctx();
    let mut acc = StatsAccumulator::new(
        &DType::Primitive(vortex_array::dtype::PType::F64, Nullability::Nullable),
        PRUNING_STATS,
        64,
    );
    assert!(acc.as_stats_set().is_empty());
    for chunk in chunks {
        acc.push_chunk(
            &PrimitiveArray::from_option_iter(chunk).into_array(),
            &mut ctx,
        )
        .unwrap();
    }
    let stats = acc.as_stats_set();
    exact(&stats, Stat::Min, -2.0f64);
    exact(&stats, Stat::Max, 7.0f64);
    exact(&stats, Stat::Sum, 8.0f64);
    exact(&stats, Stat::NullCount, 5u64);
    exact(&stats, Stat::NaNCount, 2u64);
}

#[test]
fn variable_length_statistics_are_compact_conservative_bounds() {
    for dtype in [
        DType::Utf8(Nullability::Nullable),
        DType::Binary(Nullability::Nullable),
    ] {
        let mut ctx = array_session().create_execution_ctx();
        let mut acc = StatsAccumulator::new(&dtype, &[Stat::Min, Stat::Max], 12);
        for values in [
            vec![Some("Value to be truncated"), Some("untruncated")],
            vec![None, None],
            vec![Some("Another truncated value"), Some("wait a minute")],
        ] {
            acc.push_chunk(
                &VarBinArray::from_iter(values, dtype.clone()).into_array(),
                &mut ctx,
            )
            .unwrap();
        }
        let stats = acc.as_stats_set();
        for (stat, expected) in [(Stat::Min, "Another trun"), (Stat::Max, "wait a minut")] {
            let precision = stats.get(stat);
            assert!(matches!(precision, Precision::Inexact(_)));
            let value = precision.into_inner().unwrap();
            let bytes = match value {
                ScalarValue::Utf8(value) => value.as_bytes().to_vec(),
                ScalarValue::Binary(value) => value.as_slice().to_vec(),
                _ => panic!("expected bounded string/binary statistic"),
            };
            assert!(bytes.len() <= 12);
            if stat == Stat::Min {
                assert_eq!(bytes, expected.as_bytes());
            } else {
                assert!(bytes.as_slice() > expected.as_bytes());
                assert!(bytes.as_slice() >= b"wait a minute".as_slice());
            }
        }
    }
}

#[test]
fn absent_finite_upper_bound_is_never_replaced_by_a_later_value() {
    let mut ctx = array_session().create_execution_ctx();
    let dtype = DType::Binary(Nullability::NonNullable);
    let mut acc = StatsAccumulator::new(&dtype, &[Stat::Min, Stat::Max], 4);
    for bytes in [vec![0xff; 100], b"short".to_vec(), b"a".to_vec()] {
        acc.push_chunk(
            &VarBinArray::from_iter([Some(bytes)], dtype.clone()).into_array(),
            &mut ctx,
        )
        .unwrap();
    }
    assert!(acc.as_stats_set().get(Stat::Max).is_absent());
    assert_eq!(
        acc.as_stats_set().get(Stat::Min).as_exact(),
        Scalar::binary(ByteBuffer::copy_from(b"a"), Nullability::NonNullable).into_value()
    );
}

#[test]
fn disabled_statistics_leave_nullable_constant_structs_unexecuted() {
    let session = array_session();
    let dtype = DType::Struct(StructFields::empty(), Nullability::Nullable);
    let array = ConstantArray::new(Scalar::null(dtype.clone()), 3).into_array();
    let accumulator = FileStatsAccumulator::new(&dtype, Arc::new([]), 64, &session);
    let (mut pointer, _eof) = SequenceId::root().split();
    let (_, output) = accumulator
        .process(Ok((pointer.advance(), array.clone())))
        .unwrap();
    assert!(ArrayRef::ptr_eq(&array, &output));
    assert!(accumulator.stats_sets().is_empty());
}

#[test]
fn nullable_root_statistics_never_publish_unmasked_child_bounds() {
    let session = array_session();
    let dtype = DType::Struct(
        StructFields::new(
            ["value"].into(),
            vec![DType::Primitive(
                vortex_array::dtype::PType::I64,
                Nullability::NonNullable,
            )],
        ),
        Nullability::Nullable,
    );
    let array = ConstantArray::new(Scalar::null(dtype.clone()), 5).into_array();
    let accumulator = FileStatsAccumulator::new(&dtype, PRUNING_STATS.into(), 64, &session);
    let (mut pointer, _eof) = SequenceId::root().split();
    let (_, output) = accumulator
        .process(Ok((pointer.advance(), array.clone())))
        .unwrap();
    assert!(ArrayRef::ptr_eq(&array, &output));
    let statistics = accumulator.stats_sets();
    assert_eq!(statistics.len(), 1);
    assert!(statistics[0].is_empty());
}
