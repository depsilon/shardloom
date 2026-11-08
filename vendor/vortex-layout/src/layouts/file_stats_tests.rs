// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Dylan Justin Heinrich
// ShardLoom regression tests for bounded, conservative file statistics.

use super::*;
use std::num::NonZeroUsize;
use vortex_array::IntoArray;

use vortex_array::ArrayContext;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinArray;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::StructFields;
use vortex_array::scalar::ScalarValue;
const PRUNING_STATS: &[Stat] = &[
    Stat::Min,
    Stat::Max,
    Stat::Sum,
    Stat::NullCount,
    Stat::NaNCount,
];
use vortex_buffer::ByteBuffer;

fn selected(dtype: &DType, stats: &[Stat]) -> StatsAccumulator {
    let stats = stats
        .iter()
        .filter_map(Stat::aggregate_fn)
        .collect::<Vec<_>>();
    StatsAccumulator::new(dtype, Some(&stats), &|_| true)
}

fn bounded(dtype: &DType, max_bytes: usize) -> StatsAccumulator {
    let max_bytes = NonZeroUsize::new(max_bytes).unwrap();
    StatsAccumulator::new(
        dtype,
        Some(&[
            BoundedMin.bind(BoundedMinOptions { max_bytes }),
            BoundedMax.bind(BoundedMaxOptions { max_bytes }),
        ]),
        &|_| true,
    )
}

fn stats_set(acc: &StatsAccumulator) -> StatsSet {
    acc.aggregate_stats(|_| true).unwrap().to_stats_set()
}

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
        let mut ctx = VortexSession::empty().create_execution_ctx();
        let first = PrimitiveArray::from_option_iter(values.iter().copied()).into_array();
        let mut acc = selected(first.dtype(), PRUNING_STATS);
        let capacity = acc.aggregates.capacity();
        for chunk in values.chunks(chunk_size) {
            acc.push_chunk(
                &PrimitiveArray::from_option_iter(chunk.iter().copied()).into_array(),
                &mut ctx,
            )
            .unwrap();
            assert_eq!(acc.aggregates.capacity(), capacity);
            assert!(acc.aggregates.len() <= PRUNING_STATS.len());
        }
        let stats = stats_set(&acc);
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
        let mut ctx = VortexSession::empty().create_execution_ctx();
        let dtype = PrimitiveArray::from_iter([0i64]).dtype().clone();
        let mut acc = selected(&dtype, PRUNING_STATS);
        for chunk in chunks {
            acc.push_chunk(&PrimitiveArray::from_iter(chunk).into_array(), &mut ctx)
                .unwrap();
        }
        assert!(stats_set(&acc).get(Stat::Sum).is_absent());
        exact(&stats_set(&acc), Stat::NullCount, 0u64);
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
    let mut ctx = VortexSession::empty().create_execution_ctx();
    let mut acc = selected(
        &DType::Primitive(vortex_array::dtype::PType::F64, Nullability::Nullable),
        PRUNING_STATS,
    );
    assert!(stats_set(&acc).get(Stat::Min).is_absent());
    assert!(stats_set(&acc).get(Stat::Max).is_absent());
    for chunk in chunks {
        acc.push_chunk(
            &PrimitiveArray::from_option_iter(chunk).into_array(),
            &mut ctx,
        )
        .unwrap();
    }
    let stats = stats_set(&acc);
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
        let mut ctx = VortexSession::empty().create_execution_ctx();
        let mut acc = bounded(&dtype, 12);
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
        let stats = stats_set(&acc);
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
    let mut ctx = VortexSession::empty().create_execution_ctx();
    let dtype = DType::Binary(Nullability::NonNullable);
    let mut acc = bounded(&dtype, 4);
    for bytes in [vec![0xff; 100], b"short".to_vec(), b"a".to_vec()] {
        acc.push_chunk(
            &VarBinArray::from_iter([Some(bytes)], dtype.clone()).into_array(),
            &mut ctx,
        )
        .unwrap();
    }
    assert!(stats_set(&acc).get(Stat::Max).is_absent());
    assert_eq!(
        stats_set(&acc).get(Stat::Min).into_inner(),
        Scalar::binary(ByteBuffer::copy_from(b"a"), Nullability::NonNullable).into_value()
    );
}

#[test]
fn disabled_statistics_leave_nullable_constant_structs_unexecuted() {
    let session = VortexSession::empty();
    let dtype = DType::Struct(StructFields::empty(), Nullability::Nullable);
    let array = ConstantArray::new(Scalar::null(dtype.clone()), 3).into_array();
    let accumulator = FileStatsAccumulator::try_new(
        &dtype,
        Some(Arc::new([])),
        false,
        true,
        &LayoutWriterContext::new(ArrayContext::empty()),
    )
    .unwrap();
    accumulator
        .process(&array, &mut session.create_execution_ctx())
        .unwrap();
    assert!(accumulator.legacy_aggregate_stats().unwrap().is_empty());
}

#[test]
fn nullable_root_statistics_never_publish_unmasked_child_bounds() {
    let session = VortexSession::empty();
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
    let accumulator = FileStatsAccumulator::try_new(
        &dtype,
        None,
        false,
        true,
        &LayoutWriterContext::new(ArrayContext::empty()),
    )
    .unwrap();
    accumulator
        .process(&array, &mut session.create_execution_ctx())
        .unwrap();
    let statistics = accumulator.legacy_aggregate_stats().unwrap();
    assert_eq!(statistics.len(), 1);
    assert!(statistics[0].is_empty());
}

#[test]
fn legacy_defaults_preserve_sum_and_absorbing_overflow() {
    let session = VortexSession::empty();
    for (chunks, expected) in [
        (vec![vec![1i64, 2], vec![3]], Some(6i64)),
        (vec![vec![i64::MAX], vec![1], vec![-1]], None),
    ] {
        let dtype = DType::Primitive(vortex_array::dtype::PType::I64, Nullability::NonNullable);
        let accumulator = FileStatsAccumulator::try_new(
            &dtype,
            None,
            false,
            true,
            &LayoutWriterContext::new(ArrayContext::empty()),
        )
        .unwrap();
        for chunk in chunks {
            accumulator
                .process(
                    &PrimitiveArray::from_iter(chunk).into_array(),
                    &mut session.create_execution_ctx(),
                )
                .unwrap();
        }
        assert_eq!(
            accumulator.legacy_aggregate_stats().unwrap()[0]
                .to_stats_set()
                .get(Stat::Sum)
                .as_exact(),
            expected.map(ScalarValue::from)
        );
    }
}
