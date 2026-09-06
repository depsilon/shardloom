//! Feasibility experiment: native FoR/BitPacked slices feed the existing typed
//! scalar consumers. No production dispatch until paired measurements pass.

use super::*;
use vortex::array::{ArrayRef, IntoArray as _, arrays::PrimitiveArray};
use vortex::encodings::fastlanes::{BitPacked, BitPackedArrayExt as _, FoR, FoRArraySlotsExt as _};

#[derive(Default, Debug)]
struct Work {
    executions: usize,
    maximum_rows: usize,
    canonical_rows: usize,
    selected_rows: usize,
}

fn supported_leaf(array: &ArrayRef) -> bool {
    let packed = if let Some(frame) = array.as_opt::<FoR>() {
        frame.encoded().clone()
    } else {
        array.clone()
    };
    packed.as_opt::<BitPacked>().is_some_and(|packed| {
        packed.patches().is_none()
            && packed.packed().as_host_opt().is_some()
            && packed.validity().is_ok_and(|validity| match validity {
                vortex::array::validity::Validity::Array(validity) => {
                    validity.is::<vortex::array::arrays::Bool>() && validity.is_host()
                }
                _ => true,
            })
    })
}

/// Restrict the first experiment to one numeric column and at most one additive
/// measure. Splitting the existing SUM+AVG fused source-array reduction would
/// change its floating-point grouping; reject that shape before execution.
fn consume(
    states: &mut SimpleAggregateStates,
    array: &ArrayRef,
    columns: &[String],
    selection: Option<&[usize]>,
    block_rows: usize,
    ctx: &mut vortex::array::ExecutionCtx,
) -> Result<Option<Work>> {
    if columns.len() != 1
        || !supported_leaf(array)
        || !(256..=32_768).contains(&block_rows)
        || states.states.iter().any(|state| {
            !matches!(state.value_transform, AggregateValueTransform::Identity)
                || state.column_index.is_some_and(|index| index != 0)
                || (state.column_index.is_none()
                    && state.function != SimpleAggregateFunction::Count)
        })
        || states
            .states
            .iter()
            .filter(|state| {
                matches!(
                    state.function,
                    SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
                )
            })
            .count()
            > 1
    {
        return Ok(None);
    }
    if selection.is_some_and(|rows| rows.iter().any(|&row| row >= array.len())) {
        return Err(ShardLoomError::InvalidOperation(
            "bounded native numeric selection exceeds source rows; no fallback execution was attempted".into(),
        ));
    }
    let mut work = Work::default();
    let mut consume_range = |start: usize, selected: Option<&[usize]>| -> Result<()> {
        let end = start.saturating_add(block_rows).min(array.len());
        // The pinned FoR slice rewrites its known BitPacked child. BitPacked
        // without patches slices packed blocks and validity before execution.
        // This executes a bounded native leaf, never an unknown parent's child.
        let leaf = array.slice(start..end).map_err(vortex_error)?;
        let primitive = leaf.execute::<PrimitiveArray>(ctx).map_err(vortex_error)?;
        if primitive.len() != end - start || primitive.dtype() != array.dtype() {
            return Err(ShardLoomError::InvalidOperation(
                "bounded native numeric execution changed dtype or length; no fallback execution was attempted".into(),
            ));
        }
        work.executions += 1;
        work.maximum_rows = work.maximum_rows.max(primitive.len());
        work.canonical_rows += primitive.len();
        work.selected_rows += selected.map_or(primitive.len(), <[usize]>::len);
        if !states.update_direct_from_chunk(
            &primitive.into_array(),
            columns,
            selected,
            &mut NativeNumericAccessorWork::default(),
            ctx,
        )? {
            return Err(ShardLoomError::InvalidOperation(
                "bounded native numeric typed consumer was not admitted; no fallback execution was attempted".into(),
            ));
        }
        Ok(())
    };
    if let Some(selection) = selection {
        // Preserve arbitrary order and duplicate multiplicity. Runs are capped
        // even if millions of selections address the same block. Scratch is a
        // fixed stack array; no unbounded row-index copy or selection sorting.
        let mut local = [0_usize; 1024];
        let mut at = 0;
        while at < selection.len() {
            let start = selection[at] / block_rows * block_rows;
            let mut count = 0;
            while at < selection.len()
                && count < local.len()
                && selection[at] / block_rows * block_rows == start
            {
                local[count] = selection[at] - start;
                at += 1;
                count += 1;
            }
            consume_range(start, Some(&local[..count]))?;
        }
    } else {
        for start in (0..array.len()).step_by(block_rows) {
            consume_range(start, None)?;
        }
    }
    Ok(Some(work))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
    use vortex::{
        VortexSessionDefault as _,
        array::{VortexSessionExecute as _, scalar::Scalar, validity::Validity},
        encodings::fastlanes::BitPackedData,
    };

    fn states(functions: &[&str]) -> SimpleAggregateStates {
        let request = VortexSimpleAggregateRequest::new(
            functions
                .iter()
                .enumerate()
                .map(|(index, function)| {
                    VortexSimpleAggregateMeasure::new(
                        *function,
                        Some(ColumnRef::new("amount").unwrap()),
                        format!("m{index}"),
                    )
                })
                .collect(),
        );
        SimpleAggregateStates::new(&request, &["amount".into()]).unwrap()
    }

    fn packed(rows: usize, ctx: &mut vortex::array::ExecutionCtx) -> ArrayRef {
        let raw = PrimitiveArray::new(
            (0..rows)
                .map(|row| i64::try_from(row % 256).unwrap())
                .collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array();
        let packed = BitPackedData::encode(&raw, 8, ctx).unwrap().into_array();
        FoR::try_new(packed, Scalar::from(-(1_i64 << 60)))
            .unwrap()
            .into_array()
    }

    fn equal(actual: &SimpleAggregateStates, expected: &SimpleAggregateStates) {
        for (actual, expected) in actual.states.iter().zip(&expected.states) {
            assert_eq!(
                actual.result_json().unwrap(),
                expected.result_json().unwrap()
            );
            assert_eq!(actual.count, expected.count);
            assert_eq!(actual.sum.to_bits(), expected.sum.to_bits());
        }
    }

    #[test]
    fn bounded_numeric_preserves_selected_order_duplicates_and_carried_sum() {
        let session = vortex::session::VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let array = packed(8197, &mut ctx);
        let canonical = array
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .unwrap()
            .into_array();
        let columns = vec!["amount".to_owned()];
        let selection = [8196, 0, 4097, 4097, 2047, 0];
        for selection in [None, Some(selection.as_slice()), Some(&[])] {
            for additive in ["sum", "avg"] {
                let mut actual = states(&["count", "count_distinct", "min", "max", additive]);
                actual.states.last_mut().unwrap().sum = 7.0;
                actual.states.last_mut().unwrap().count = 2;
                let mut expected = actual.clone();
                let work = consume(&mut actual, &array, &columns, selection, 1024, &mut ctx)
                    .unwrap()
                    .unwrap();
                expected
                    .update_direct_from_chunk(
                        &canonical,
                        &columns,
                        selection,
                        &mut NativeNumericAccessorWork::default(),
                        &mut ctx,
                    )
                    .unwrap();
                equal(&actual, &expected);
                assert!(work.maximum_rows <= 1024);
            }
        }
    }

    #[test]
    fn bounded_numeric_mixed_sign_rounding_keeps_one_ordered_accumulator() {
        const BIG: i64 = 1_i64 << 60;
        let session = vortex::session::VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let columns = vec!["amount".to_owned()];
        for block_rows in [256, 1024] {
            let mut values = vec![0_i64; block_rows * 2 + 9];
            values[block_rows - 1] = -BIG;
            values[block_rows] = BIG;
            values[block_rows + 1] = 1;
            // A physical nonzero packed offset is retained after the native
            // slice, and the large opposite signs cross a consumer boundary.
            let mut encoded_values = vec![BIG; 19];
            encoded_values.extend(values.iter().map(|value| value + BIG));
            let raw = PrimitiveArray::new(encoded_values, Validity::NonNullable).into_array();
            let packed = BitPackedData::encode(&raw, 62, &mut ctx)
                .unwrap()
                .into_array();
            let array = FoR::try_new(packed, Scalar::from(-BIG))
                .unwrap()
                .into_array()
                .slice(19..19 + values.len())
                .unwrap();
            assert!(supported_leaf(&array));
            let regrouped = values.chunks(block_rows).fold(7.0, |sum, block| {
                sum + block
                    .iter()
                    .fold(0.0, |sum, value| sum + int64_stat_to_float64(*value))
            });
            let ordered = values
                .iter()
                .fold(7.0, |sum, value| sum + int64_stat_to_float64(*value));
            assert_eq!(ordered.to_bits(), 1.0_f64.to_bits());
            assert_ne!(
                ordered.to_bits(),
                regrouped.to_bits(),
                "fixture must detect per-window regrouping"
            );
            let selected = [block_rows + 1, block_rows - 1, block_rows, block_rows + 1];
            for selection in [None, Some(selected.as_slice()), Some(&[])] {
                let expected_rows = selection
                    .map_or_else(|| (0..values.len()).collect::<Vec<_>>(), <[usize]>::to_vec);
                let expected_sum = expected_rows
                    .iter()
                    .fold(7.0, |sum, &row| sum + int64_stat_to_float64(values[row]));
                for additive in ["sum", "avg"] {
                    let mut actual = states(&[additive]);
                    actual.states[0].sum = 7.0;
                    actual.states[0].count = 2;
                    let work = consume(
                        &mut actual,
                        &array,
                        &columns,
                        selection,
                        block_rows,
                        &mut ctx,
                    )
                    .unwrap()
                    .unwrap();
                    assert_eq!(actual.states[0].sum.to_bits(), expected_sum.to_bits());
                    assert_eq!(
                        actual.states[0].count,
                        2 + u64::try_from(expected_rows.len()).unwrap()
                    );
                    assert_eq!(work.selected_rows, expected_rows.len());
                    assert!(work.maximum_rows <= block_rows);
                }
            }
        }
    }

    #[test]
    fn bounded_numeric_count_star_keeps_null_rows_and_selected_multiplicity() {
        let session = vortex::session::VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let values = (0..2057)
            .map(|row| (row % 7 != 0).then(|| i16::try_from(row % 16).unwrap()))
            .collect::<Vec<_>>();
        let raw = PrimitiveArray::from_option_iter(values.iter().copied()).into_array();
        let array = BitPackedData::encode(&raw, 4, &mut ctx)
            .unwrap()
            .into_array();
        let request = VortexSimpleAggregateRequest::new(vec![
            VortexSimpleAggregateMeasure::new("count", None, "all_rows".to_owned()),
            VortexSimpleAggregateMeasure::new(
                "count",
                Some(ColumnRef::new("amount").unwrap()),
                "present_rows".to_owned(),
            ),
        ]);
        let columns = vec!["amount".to_owned()];
        let selected = [2056, 0, 7, 7, 1, 1024, 0, 1];
        for selection in [None, Some(selected.as_slice()), Some(&[])] {
            let expected_rows =
                selection.map_or_else(|| (0..values.len()).collect::<Vec<_>>(), <[usize]>::to_vec);
            let mut actual = SimpleAggregateStates::new(&request, &columns).unwrap();
            let mut reference = actual.clone();
            let work = consume(&mut actual, &array, &columns, selection, 256, &mut ctx)
                .unwrap()
                .unwrap();
            reference
                .update_direct_from_chunk(
                    &raw,
                    &columns,
                    selection,
                    &mut NativeNumericAccessorWork::default(),
                    &mut ctx,
                )
                .unwrap();
            equal(&actual, &reference);
            assert_eq!(
                actual.states[0].count,
                u64::try_from(expected_rows.len()).unwrap()
            );
            assert_eq!(
                actual.states[1].count,
                u64::try_from(
                    expected_rows
                        .iter()
                        .filter(|&&row| values[row].is_some())
                        .count()
                )
                .unwrap()
            );
            assert_eq!(work.selected_rows, expected_rows.len());
        }
    }

    #[test]
    fn bounded_numeric_fused_additive_misses_without_state_change() {
        let session = vortex::session::VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let array = packed(4097, &mut ctx);
        let mut state = states(&["sum", "avg"]);
        let original = state.clone();
        assert!(
            consume(&mut state, &array, &["amount".into()], None, 1024, &mut ctx)
                .unwrap()
                .is_none()
        );
        equal(&state, &original);
    }

    #[test]
    fn bounded_numeric_original_integer_widths_and_nullable_validity_are_exact() {
        let session = vortex::session::VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let columns = vec!["amount".to_owned()];
        let mut check = |raw: ArrayRef| {
            let array = BitPackedData::encode(&raw, 4, &mut ctx)
                .unwrap()
                .into_array();
            assert!(supported_leaf(&array));
            let mut actual = states(&["count", "count_distinct", "min", "max", "sum"]);
            let mut reference = actual.clone();
            let work = consume(&mut actual, &array, &columns, None, 256, &mut ctx)
                .unwrap()
                .unwrap();
            assert_eq!(work.canonical_rows, raw.len());
            assert!(work.maximum_rows <= 256);
            reference
                .update_direct_from_chunk(
                    &raw,
                    &columns,
                    None,
                    &mut NativeNumericAccessorWork::default(),
                    &mut ctx,
                )
                .unwrap();
            equal(&actual, &reference);
        };
        macro_rules! check_width {
            ($type:ty) => {
                check(
                    PrimitiveArray::new(
                        (0..2057)
                            .map(|row| <$type>::try_from(row % 16).unwrap())
                            .collect::<Vec<_>>(),
                        Validity::NonNullable,
                    )
                    .into_array(),
                );
                check(
                    PrimitiveArray::from_option_iter(
                        (0..2057).map(|row| {
                            (row % 7 != 0).then(|| <$type>::try_from(row % 16).unwrap())
                        }),
                    )
                    .into_array(),
                );
            };
        }
        check_width!(i8);
        check_width!(i16);
        check_width!(i32);
        check_width!(i64);
        check_width!(u8);
        check_width!(u16);
        check_width!(u32);
        check_width!(u64);
    }

    #[test]
    #[ignore = "manual release-mode bounded-native-consumer feasibility measurement"]
    fn bounded_numeric_measured_scalar_consumers() {
        let session = vortex::session::VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let rows = 8_000_003;
        let array = packed(rows, &mut ctx);
        let columns = vec!["amount".to_owned()];
        let mut trials = Vec::new();
        for block_rows in [1024, 8192, 32_768] {
            for trial in 0..7 {
                let mut times = [0_u128; 2];
                let mut reference = None;
                for bounded in if trial % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    let mut state = states(&["count", "min", "max", "sum"]);
                    let started = Instant::now();
                    if bounded {
                        let work =
                            consume(&mut state, &array, &columns, None, block_rows, &mut ctx)
                                .unwrap()
                                .unwrap();
                        assert_eq!(work.canonical_rows, rows);
                        assert_eq!(work.selected_rows, rows);
                        assert!(work.maximum_rows <= block_rows);
                    } else {
                        let canonical = array
                            .clone()
                            .execute::<PrimitiveArray>(&mut ctx)
                            .unwrap()
                            .into_array();
                        state
                            .update_direct_from_chunk(
                                &canonical,
                                &columns,
                                None,
                                &mut NativeNumericAccessorWork::default(),
                                &mut ctx,
                            )
                            .unwrap();
                    }
                    times[usize::from(bounded)] = started.elapsed().as_nanos();
                    if let Some(reference) = &reference {
                        equal(&state, reference);
                    } else {
                        reference = Some(state);
                    }
                }
                trials.push(serde_json::json!({"block_rows":block_rows,"trial":trial,"whole_native_nanos":times[0],"bounded_native_nanos":times[1]}));
            }
        }
        println!(
            "{}",
            serde_json::json!({"rows":rows,"trials":trials,"scope":"native_array_decode_plus_existing_typed_scalar_consumers;one_additive_measure;alternating_order;bounded_canonical_rows_not_provider_allocator_or_RSS_bound;not_file_or_process_time"})
        );
    }
}
