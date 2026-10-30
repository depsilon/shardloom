//! Compare complete ordered state and output against the ordinary reducer.

use super::super::super::{Aggregate, keys, logical_field_from_native_array};
use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _,
    arrays::{BoolArray, ChunkedArray, ConstantArray, DictArray, PrimitiveArray, StructArray},
    dtype::FieldNames,
    scalar::Scalar,
    validity::Validity,
};

#[path = "native_relational_aggregate_typed_proof_tests.rs"]
mod proof_tests;

fn binding(column: &ArrayRef, functions: &[Function]) -> Spec {
    let measures: Vec<_> = functions
        .iter()
        .map(|&function| Measure {
            function,
            column: Some("n".into()),
            secondary_column: None,
            filter_column: None,
            quantile_level: None,
            selection: None,
            separator: None,
            dtype: match function {
                Function::Count => DType::Primitive(PType::U64, Nullability::NonNullable),
                Function::Sum | Function::Avg => {
                    DType::Primitive(PType::F64, Nullability::Nullable)
                }
                _ => column.dtype().as_nullable(),
            },
            input_ptype: match column.dtype() {
                DType::Primitive(ptype, _) => Some(*ptype),
                _ => None,
            },
            decimal_source: None,
            decimal_output: None,
            distinct_fields: vec![],
            distinct_names: vec!["group".into(), "value".into()],
        })
        .collect();
    let groups = vec![(
        "g".into(),
        DType::Primitive(PType::I64, Nullability::NonNullable),
    )];
    let mut fields = groups.clone();
    fields.extend(
        measures
            .iter()
            .enumerate()
            .map(|(index, measure)| (format!("a{index}"), measure.dtype.clone())),
    );
    Spec {
        fields,
        groups,
        group_names: vec!["g".into()],
        measures,
    }
}

fn batch(column: &ArrayRef, groups: usize) -> ArrayRef {
    let rows = column.len();
    StructArray::try_new(
        FieldNames::from(["g", "n", "keep"]),
        vec![
            PrimitiveArray::from_iter((0..rows).map(|row| i64::try_from(row % groups).unwrap()))
                .into_array(),
            column.clone(),
            BoolArray::from_iter((0..rows).map(|row| row % 3 != 1)).into_array(),
        ],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn assert_state(actual: &State, expected: &State) {
    assert_eq!(actual.count, expected.count);
    assert!(
        numeric_reuse::same_sum(actual.sum, expected.sum),
        "actual {:?}, expected {:?}",
        actual.sum,
        expected.sum
    );
    assert_eq!(actual.specials, expected.specials);
    assert_eq!(actual.extreme, expected.extreme);
}

fn assert_aggregate(actual: &Aggregate<'_>, expected: &Aggregate<'_>) {
    if let Some(table) = actual
        .compact
        .as_ref()
        .and_then(|table| table.typed.as_ref())
    {
        assert!(actual.states.values.is_empty());
        assert_eq!(
            table.groups * table.slots.len(),
            expected.states.values.len()
        );
        for group in 0..table.groups {
            for index in 0..table.slots.len() {
                assert_state(
                    &table.state(index, group),
                    &expected.states.values[group * table.slots.len() + index],
                );
            }
        }
    } else {
        assert!(actual.compact.is_none());
        assert_eq!(actual.states.values.len(), expected.states.values.len());
        for (actual, expected) in actual.states.values.iter().zip(&expected.states.values) {
            assert_state(actual, expected);
        }
    }
}

fn results(
    aggregate: &Aggregate<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Vec<Vec<Cell>>> {
    let mut result = Vec::new();
    aggregate.finish(context, 3, &mut |array| {
        let columns: Vec<_> = aggregate
            .spec
            .fields
            .iter()
            .map(|(name, _)| keys(&logical_field_from_native_array(&array, name)?, context))
            .collect::<Result<_>>()?;
        for row in 0..array.len() {
            result.push(
                columns
                    .iter()
                    .map(|column| column.raw_cell(row))
                    .collect::<Result<_>>()?,
            );
        }
        Ok(())
    })?;
    Ok(result)
}

fn compare(column: &ArrayRef, policy: FloatPolicy) {
    let session = ResidentVortexSession::new_with_float_policy(8 << 20, 1, policy).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut spec = binding(
                column,
                &[
                    Function::Sum,
                    Function::Avg,
                    Function::Min,
                    Function::Max,
                    Function::Count,
                    Function::Count,
                ],
            );
            spec.measures[5].column = None;
            let mut actual = Aggregate::new(&spec, &memory)?;
            let mut expected = Aggregate::new(&spec, &memory)?;
            expected.specialize = false;
            let halves = [
                column.slice(0..0).unwrap(),
                column.slice(0..column.len() / 2).unwrap(),
                column.slice(column.len() / 2..column.len()).unwrap(),
                column.clone(),
            ];
            for column in halves {
                let input = batch(&column, 3);
                let observed = actual.consume(&input, context, 7);
                let reference = expected.consume(&input, context, 7);
                assert_eq!(
                    observed.as_ref().err().map(ToString::to_string),
                    reference.as_ref().err().map(ToString::to_string)
                );
                assert_aggregate(&actual, &expected);
                if observed.is_err() {
                    return Ok(());
                }
                assert_eq!(results(&actual, context)?, results(&expected, context)?);
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn typed_grouped_state_matches_all_integer_widths_and_empty_nullable_domains() {
    macro_rules! check { ($($ty:ty),+) => { $(
        let column = PrimitiveArray::from_option_iter((0..1031).map(|row| match row % 7 {
            0 | 4 => None, 1 => Some(<$ty>::MIN), 2 => Some(<$ty>::MAX), _ => Some(1),
        })).into_array();
        for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
            compare(&column, policy);
            compare(&column.slice(3..1017).unwrap(), policy);
            compare(&column.slice(5..5).unwrap(), policy);
            compare(&PrimitiveArray::from_option_iter([None::<$ty>; 7]).into_array(), policy);
        }
    )+ }; }
    check!(i8, i16, i32, i64, u8, u16, u32, u64);
}

#[test]
fn typed_grouped_state_preserves_float_order_policy_bits_and_failures() {
    for values in [
        vec![
            Some(-0.0),
            Some(0.0),
            None,
            Some(-0.0),
            Some(0.0),
            None,
            Some(0.0),
        ],
        vec![
            Some(1e16),
            None,
            Some(-1e16),
            Some(1.0),
            Some(1e16),
            Some(-1e16),
            None,
        ],
        vec![Some(f64::MAX), None, None, Some(f64::MAX)],
        vec![
            Some(f64::from_bits(0x7ff8_0000_0000_4321)),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
            Some(-0.0),
            Some(0.0),
            Some(7.0),
        ],
        vec![None; 7],
    ] {
        for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
            compare(
                &PrimitiveArray::from_option_iter(values.iter().copied()).into_array(),
                policy,
            );
        }
    }
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        compare(
            &PrimitiveArray::from_option_iter([
                Some(f32::from_bits(0x7fc0_1234)),
                None,
                Some(f32::INFINITY),
                Some(-0.0),
                Some(0.0),
                Some(f32::NEG_INFINITY),
            ])
            .into_array(),
            policy,
        );
        compare(
            &PrimitiveArray::from_option_iter([
                Some(0.1f32),
                None,
                Some(-0.1),
                Some(5.5),
                Some(-0.0),
                Some(0.0),
            ])
            .into_array(),
            policy,
        );
    }
}

#[test]
fn typed_grouped_state_keeps_dictionary_slice_constant_and_wrapper_semantics() {
    let values =
        PrimitiveArray::from_option_iter([Some(i64::MAX), None, Some(-11), Some(0)]).into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_option_iter(
            (0..71).map(|row| (row % 11 != 0).then_some(u8::try_from(row % 4).unwrap())),
        )
        .into_array(),
        values.clone(),
    )
    .unwrap()
    .into_array();
    for column in [
        dictionary.clone(),
        dictionary.slice(3..65).unwrap(),
        ConstantArray::new(Scalar::from(-17i64), 43).into_array(),
        ConstantArray::new(Scalar::null(values.dtype().clone()), 43).into_array(),
        ChunkedArray::try_new(vec![values.clone(), values.clone()], values.dtype().clone())
            .unwrap()
            .into_array(),
        ChunkedArray::try_new(vec![values.clone()], values.dtype().clone())
            .unwrap()
            .into_array(),
    ] {
        compare(&column, FloatPolicy::Strict);
    }
}

#[test]
fn typed_grouped_state_restores_before_changed_width_signedness_or_numeric_domain() {
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        for functions in [
            &[Function::Sum, Function::Avg][..],
            &[Function::Min, Function::Max],
            &[Function::Sum, Function::Min, Function::Max, Function::Count],
        ] {
            let session = ResidentVortexSession::new_with_float_policy(8 << 20, 1, policy).unwrap();
            let memory = session.memory().clone();
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let first =
                        PrimitiveArray::from_option_iter([Some(3i16), None, Some(-7), Some(11)])
                            .into_array();
                    let spec = binding(&first, functions);
                    let mut actual = Aggregate::new(&spec, &memory)?;
                    let mut expected = Aggregate::new(&spec, &memory)?;
                    expected.specialize = false;
                    for (step, column) in [
                        first,
                        PrimitiveArray::from_iter([u64::MAX, 7, 1, 11]).into_array(),
                        PrimitiveArray::from_iter([0.5f64, 0.0, -7.0, f64::INFINITY]).into_array(),
                    ]
                    .iter()
                    .enumerate()
                    {
                        let input = batch(column, 2);
                        let observed = actual.consume(&input, context, 3);
                        let reference = expected.consume(&input, context, 3);
                        assert_eq!(
                            observed.as_ref().err().map(ToString::to_string),
                            reference.as_ref().err().map(ToString::to_string)
                        );
                        assert_aggregate(&actual, &expected);
                        assert_eq!(actual.compact.is_some(), step == 0);
                        if observed.is_err() {
                            break;
                        }
                        // An internal type change may make the original output
                        // dtype unrepresentable. Preserve that diagnostic too.
                        assert_eq!(
                            results(&actual, context).map_err(|error| error.to_string()),
                            results(&expected, context).map_err(|error| error.to_string())
                        );
                    }
                    Ok(())
                })
                .unwrap();
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

fn mixed_batch(start: usize, rows: usize, policy: FloatPolicy) -> ArrayRef {
    let positions = start..start + rows;
    let columns = vec![
        PrimitiveArray::from_iter(
            positions
                .clone()
                .map(|row| i64::try_from((row * 17) % 601).unwrap()),
        )
        .into_array(),
        PrimitiveArray::from_option_iter(
            positions.clone().map(|row| {
                (row % 7 != 0).then_some(if row % 2 == 0 { i64::MAX } else { i64::MIN })
            }),
        )
        .into_array(),
        PrimitiveArray::from_option_iter(
            positions
                .clone()
                .map(|row| (row % 11 != 0).then_some(if row % 3 == 0 { u64::MAX } else { 7 })),
        )
        .into_array(),
        PrimitiveArray::from_option_iter(positions.clone().map(|row| {
            if row % 13 == 0 {
                None
            } else {
                Some(match row % 5 {
                    0 if policy == FloatPolicy::Extended => f64::from_bits(0x7ff8_0000_0000_19ab),
                    1 if policy == FloatPolicy::Extended => f64::INFINITY,
                    2 => -0.0,
                    3 => 1e16,
                    _ => -1e16,
                })
            }
        }))
        .into_array(),
        PrimitiveArray::from_option_iter(
            positions
                .clone()
                .map(|row| (row % 17 != 0).then_some(if row % 2 == 0 { -0.0f32 } else { 7.25 })),
        )
        .into_array(),
        BoolArray::from_iter(
            positions
                .clone()
                .map(|row| (row % 19 != 0).then_some(row % 3 != 0)),
        )
        .into_array(),
        BoolArray::from_iter(positions.map(|row| row % 5 != 0)).into_array(),
    ];
    StructArray::try_new(
        FieldNames::from(["g", "s", "u", "f", "f32", "keep", "other"]),
        columns,
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn mixed_spec(input: &ArrayRef) -> Spec {
    let mut spec = binding(&logical_field_from_native_array(input, "s").unwrap(), &[]);
    for (column, function, filter) in [
        ("s", Function::Sum, Some("keep")),
        ("u", Function::Min, None),
        ("f", Function::Avg, None),
        ("s", Function::Count, None),
        ("s", Function::Min, None),
        ("f32", Function::Max, None),
        ("u", Function::Avg, Some("other")),
        ("f", Function::Sum, None),
        ("u", Function::Max, None),
        ("s", Function::Max, Some("other")),
        ("", Function::Count, Some("keep")),
        ("f32", Function::Sum, None),
        ("f", Function::Min, Some("other")),
        ("s", Function::Avg, Some("keep")),
        ("f", Function::Max, Some("other")),
        ("f32", Function::Min, None),
    ] {
        let source =
            logical_field_from_native_array(input, if column.is_empty() { "s" } else { column })
                .unwrap();
        let mut measure = binding(&source, &[function]).measures.remove(0);
        measure.column = (!column.is_empty()).then(|| column.into());
        measure.filter_column = filter.map(Into::into);
        spec.fields
            .push((format!("a{}", spec.measures.len()), measure.dtype.clone()));
        spec.measures.push(measure);
    }
    spec
}

#[test]
fn typed_grouped_state_keeps_mixed_columns_slots_filters_and_pages_aligned() {
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        let session = ResidentVortexSession::new_with_float_policy(16 << 20, 1, policy).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let spec = mixed_spec(&mixed_batch(0, 0, policy));
                let mut actual = Aggregate::new(&spec, &memory)?;
                let mut expected = Aggregate::new(&spec, &memory)?;
                expected.specialize = false;
                let mut start = 0;
                for rows in [0, 131, 1041, 17, 901, 0] {
                    let input = mixed_batch(start, rows, policy);
                    actual.consume(&input, context, 113)?;
                    expected.consume(&input, context, 113)?;
                    assert_aggregate(&actual, &expected);
                    assert_eq!(results(&actual, context)?, results(&expected, context)?);
                    if let Some(table) = actual
                        .compact
                        .as_ref()
                        .and_then(|table| table.typed.as_ref())
                    {
                        let absent = if policy == FloatPolicy::Strict {
                            Kind::ExtendedFloat
                        } else {
                            Kind::StrictFloat
                        } as usize;
                        for (kind, width) in table.widths.iter().enumerate() {
                            assert!(if kind == absent {
                                *width == 0
                            } else {
                                *width >= 2
                            });
                        }
                    } else {
                        assert_eq!(start + rows, 0);
                    }
                    start += rows;
                }
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn reserved(table: &Table) -> u64 {
    (table.slots.capacity() * size_of::<Slot>()) as u64
        + table.counts.reserved_bytes()
        + table.integers.reserved_bytes()
        + table.strict.reserved_bytes()
        + table.extended.reserved_bytes()
        + table.signed_extrema.reserved_bytes()
        + table.unsigned_extrema.reserved_bytes()
        + table.float_extrema.reserved_bytes()
}

#[test]
fn typed_grouped_state_sizes_growth_restoration_and_denials_are_credited() {
    assert_eq!(size_of::<Integer>(), 24);
    assert_eq!(size_of::<Floating>(), 16);
    assert_eq!(size_of::<Extended>(), 40);
    assert_eq!(size_of::<Extreme<i64>>(), 16);
    assert_eq!(size_of::<Extreme<u64>>(), 16);
    assert_eq!(size_of::<Extreme<f64>>(), 16);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let column = PrimitiveArray::from_iter([7i64; 8]).into_array();
            let input = batch(&column, 2);
            let spec = binding(
                &column,
                &[Function::Count, Function::Sum, Function::Min, Function::Max],
            );
            let baseline = memory.snapshot().reserved_bytes;
            let mut table = Table::new(&spec, &input, 1, context)?.unwrap();
            for groups in [17, 513, 1031, 4097] {
                table.reserve_groups(groups, context)?;
                assert_eq!(
                    memory.snapshot().reserved_bytes - baseline,
                    reserved(&table)
                );
                assert!(reserved(&table) < (groups * 4 * size_of::<State>()) as u64);
            }
            table.integers[0] = Integer {
                count: 3,
                sum: 1e18,
                correction: 1.0,
            };
            let expected = table.state(1, 0);
            let mut restored = ReservedVec::new(&memory)?;
            let deny = memory.reserve((8 << 20) - memory.snapshot().reserved_bytes - 64)?;
            let before = memory.snapshot().reserved_bytes;
            assert!(table.restore(&spec, &mut restored, context).is_err());
            assert!(restored.values.is_empty());
            assert_eq!(memory.snapshot().reserved_bytes, before);
            assert_state(&table.state(1, 0), &expected);
            drop(deny);
            table.restore(&spec, &mut restored, context)?;
            assert_state(&restored.values[1], &expected);
            drop(table);
            assert_state(&restored.values[1], &expected);
            drop(restored);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
            let deny = memory.reserve((8 << 20) - baseline - 64)?;
            assert!(Table::new(&spec, &input, 4097, context)?.is_none());
            drop(deny);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
