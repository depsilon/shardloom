use super::*;
use crate::owned_buffers::ReservedHostAllocator;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{VarBinViewArray, varbinview::BinaryView},
        dtype::{DType, Nullability},
        memory::HostAllocator as _,
        validity::Validity,
    },
    buffer::{Alignment, Buffer, ByteBuffer},
};

#[test]
fn source_dictionary_distinct_consumers_reuse_lazy_independent_promotions() {
    let expected = [
        "one-long-value",
        "two-long-value",
        "one-long-value",
        "three-long-value",
    ];
    let input = VarBinViewArray::from_iter_str(expected).into_array();
    let accessor = aggregate_direct_utf8_chunk_dictionary_accessor_profiled(
        "renamed",
        &input,
        &mut native_numeric_accessor::Utf8AccessorWork::default(),
        &mut vortex::array::legacy_session().create_execution_ctx(),
    )
    .unwrap()
    .unwrap();
    let AggregateDirectColumnAccessor::Utf8Dictionary { values, .. } = &accessor else {
        panic!("dictionary");
    };
    native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take);
    let mut first = SimpleAggregateState::new(
        SimpleAggregateFunction::CountDistinct,
        Some(0),
        "n".into(),
        None,
        AggregateValueTransform::Identity,
    );
    for row in 0..expected.len() {
        first
            .update_direct_row_from_accessors(std::slice::from_ref(&accessor), row, expected.len())
            .unwrap();
    }
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        3
    );
    assert_eq!(first.count, 4);
    assert_eq!(first.distinct_values.len(), 3);
    let mut second = AggregateDistinctSet::default();
    aggregate_direct_count_distinct_insert_utf8_dictionary_values(
        vec![true; values.len()],
        values,
        &mut second,
    )
    .unwrap();
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        0
    );
    for value in values {
        let first_owner = value.to_owned_arc();
        let cloned_value = value.clone();
        let second_owner = cloned_value.to_owned_arc();
        assert!(std::sync::Arc::ptr_eq(&first_owner, &second_owner));
        assert_ne!(first_owner.as_ptr(), value.as_ptr());
    }
    drop(accessor);
    drop(input);
    assert_eq!(first.distinct_values, second);
    for value in expected {
        assert!(second.contains(&AggregateDistinctValue::Utf8(std::sync::Arc::from(value))));
    }
}

#[test]
fn dense_utf8_extrema_share_one_promotion_and_keep_independent_owners() {
    let expected = [
        "https://example.test/middle-long-value",
        "https://example.test/aaa-long-value",
        "https://example.test/zzz-long-value",
    ];
    for (needs_utf8_min, needs_utf8_max) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let source = VarBinViewArray::from_iter_str(expected);
        let values = (0..expected.len())
            .map(|row| {
                Utf8DictionaryValue::source(
                    vortex::buffer::BufferString::try_from(source.bytes_at(row)).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let plan = TransformedDictionaryDenseGeneralPlan {
            needs_utf8_min,
            needs_utf8_max,
            ..Default::default()
        };
        let mut state = TransformedDictionaryDenseGeneralState::default();
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take);
        state
            .update_weighted_utf8_dictionary_value(plan, &values[0], 0)
            .unwrap();
        assert_eq!(
            native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
            0
        );
        assert_eq!(state.row_count, 0);
        state
            .update_weighted_utf8_dictionary_value(plan, &values[0], 2)
            .unwrap();
        assert_eq!(
            native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
            usize::from(needs_utf8_min || needs_utf8_max),
            "both extrema must share one independently owned payload"
        );
        if let (Some(min), Some(max)) = (&state.min_utf8, &state.max_utf8) {
            assert!(std::sync::Arc::ptr_eq(min, max));
            assert_ne!(min.as_ptr(), values[0].as_ptr());
        }
        for (index, weight, promotions) in [
            (0, 3, 0),
            (1, 4, usize::from(needs_utf8_min)),
            (2, 5, usize::from(needs_utf8_max)),
            (0, 1, 0),
        ] {
            state
                .update_weighted_utf8_dictionary_value(plan, &values[index], weight)
                .unwrap();
            assert_eq!(
                native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
                promotions
            );
        }
        drop(values);
        drop(source);
        assert_eq!(state.row_count, 15);
        assert_eq!(
            state.min_utf8.as_deref(),
            needs_utf8_min.then_some(expected[1])
        );
        assert_eq!(
            state.max_utf8.as_deref(),
            needs_utf8_max.then_some(expected[2])
        );
    }
}

fn charged_utf8_source(values: &[String]) -> (VarBinViewArray, LiveMemoryPool) {
    let template = VarBinViewArray::from_iter_str(values);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let allocator = ReservedHostAllocator::new(memory.clone());
    let copy = |bytes: &[u8], alignment| {
        let mut owned = allocator.allocate(bytes.len(), alignment).unwrap();
        owned.as_mut_slice().copy_from_slice(bytes);
        owned.freeze()
    };
    let views = copy(
        template.views_handle().as_host(),
        Alignment::of::<BinaryView>(),
    );
    let buffers = template
        .data_buffers()
        .iter()
        .map(|buffer| copy(buffer.as_host(), Alignment::of::<u8>()))
        .collect::<Vec<_>>();
    let source = VarBinViewArray::try_new(
        Buffer::from_byte_buffer(views),
        buffers.into(),
        DType::Utf8(Nullability::NonNullable),
        Validity::NonNullable,
        &mut vortex::array::legacy_session().create_execution_ctx(),
    )
    .unwrap();
    (source, memory)
}

#[test]
fn string_interner_preserves_mixed_ownership_and_releases_source_credit() {
    let expected = [
        "",
        "123456789012",
        "common-prefix-東京-a",
        "common-prefix-東京-b",
    ]
    .map(str::to_string);
    let (source, memory) = charged_utf8_source(&expected);
    let source_value = |row| {
        Utf8DictionaryValue::source(
            vortex::buffer::BufferString::try_from(source.bytes_at(row)).unwrap(),
        )
    };
    let mut interner = AggregateStringInterner::default();
    interner.reserve(expected.len(), "mixed interner").unwrap();
    native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take);
    for (index, text) in expected.iter().enumerate() {
        let value = source_value(index);
        let id = if index % 2 == 0 {
            interner.intern(text).unwrap()
        } else {
            interner.intern_dictionary(&value).unwrap()
        };
        assert_eq!(id, u64::try_from(index).unwrap());
        if !text.is_empty() {
            assert_ne!(interner.value(id).unwrap().as_ptr(), value.as_ptr());
        }
    }
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        2
    );
    for (index, text) in expected.iter().enumerate().rev() {
        // Fresh source values do not carry the first miss's lazy promotion cache.
        let fresh = source_value(index);
        let expected_id = u64::try_from(index).unwrap();
        assert_eq!(interner.intern_dictionary(&fresh).unwrap(), expected_id);
        assert_eq!(interner.intern(text).unwrap(), expected_id);
        assert_eq!(interner.id(text), Some(expected_id));
    }
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        0
    );
    let owned: std::sync::Arc<str> = std::sync::Arc::from("independent-owned-λ");
    let owned_id = interner
        .intern_dictionary(&Utf8DictionaryValue::from(std::sync::Arc::clone(&owned)))
        .unwrap();
    assert_eq!(owned_id, 4);
    assert!(std::sync::Arc::ptr_eq(
        &interner.value_arc(owned_id).unwrap(),
        &owned
    ));
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for (index, text) in expected.iter().enumerate() {
        assert_eq!(interner.value(u64::try_from(index).unwrap()).unwrap(), text);
    }
    interner.forget_id(1, "mixed interner").unwrap();
    assert_eq!(interner.id(&expected[1]), None);
    assert_eq!(interner.value(1).unwrap(), expected[1]);
    assert_eq!(interner.intern(&expected[1]).unwrap(), 5);
    assert_eq!(interner.id(&expected[1]), Some(5));
    assert_eq!(interner.len(), 6);
    assert_eq!(interner.value(1).unwrap(), interner.value(5).unwrap());
}

#[test]
fn string_interner_growth_reinsertion_and_failed_reservations_preserve_ids() {
    let expected = (0..2048)
        .map(|index| format!("shared-prefix-東京\0-{index:04}-suffix"))
        .collect::<Vec<_>>();
    let mut interner = AggregateStringInterner::default();
    let mut ids = vec![0; expected.len()];
    for sequence in 0..expected.len() {
        let index = (sequence * 109) % expected.len();
        if sequence % 17 == 0 {
            interner.reserve(17, "growing interner").unwrap();
        }
        let id = if sequence % 2 == 0 {
            interner.intern(&expected[index]).unwrap()
        } else {
            interner
                .intern_dictionary(&Utf8DictionaryValue::from(std::sync::Arc::<str>::from(
                    expected[index].as_str(),
                )))
                .unwrap()
        };
        assert_eq!(id, u64::try_from(sequence).unwrap());
        ids[index] = id;
    }
    for (index, text) in expected.iter().enumerate().rev() {
        assert_eq!(interner.intern(text).unwrap(), ids[index]);
        assert_eq!(interner.value(ids[index]).unwrap(), text);
    }
    let mut next_id = u64::try_from(expected.len()).unwrap();
    for index in (0..expected.len()).step_by(3) {
        interner.forget_id(ids[index], "growing interner").unwrap();
        assert_eq!(interner.id(&expected[index]), None);
        assert_eq!(interner.value(ids[index]).unwrap(), expected[index]);
        assert_eq!(interner.intern(&expected[index]).unwrap(), next_id);
        ids[index] = next_id;
        next_id += 1;
    }
    interner.reserve(0, "growing interner").unwrap();
    let error = interner
        .reserve(usize::MAX, "growing interner")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("growing interner state reservation failed")
    );
    assert!(
        error
            .to_string()
            .contains("no fallback execution was attempted")
    );
    for (index, text) in expected.iter().enumerate() {
        assert_eq!(interner.id(text), Some(ids[index]));
        assert_eq!(interner.value(ids[index]).unwrap(), text);
    }
    assert_eq!(interner.len(), usize::try_from(next_id).unwrap());
    assert!(interner.value(next_id).is_err());
    assert!(interner.forget_id(next_id, "growing interner").is_err());
}

#[allow(clippy::too_many_lines)] // Follow one complete chunk's state and source-owner lifetime.
fn check_dense_chunk_extrema_promotions(seed_better_extrema: bool) {
    for (needs_min, needs_max) in [(false, false), (true, false), (false, true), (true, true)] {
        for descending in [false, true] {
            let mut measures = vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "n".into()),
                crate::VortexSimpleAggregateMeasure::new(
                    "avg",
                    Some(ColumnRef::new("renamed").unwrap()),
                    "length".into(),
                )
                .with_value_transform("length"),
            ];
            for (enabled, function) in [(needs_min, "min"), (needs_max, "max")] {
                if enabled {
                    measures.push(crate::VortexSimpleAggregateMeasure::new(
                        function,
                        Some(ColumnRef::new("renamed").unwrap()),
                        function.into(),
                    ));
                }
            }
            let request = VortexSimpleAggregateRequest::grouped(Vec::new(), measures)
                .with_group_expressions(vec![crate::VortexAggregateExpression::new(
                    "domain".into(),
                    ColumnRef::new("renamed").unwrap(),
                    "url_domain",
                )])
                .with_order_by(vec![crate::VortexAggregateOrderExpr::new("n", true)]);
            let columns = ["renamed".into()];
            let mut states =
                GroupedAggregateStates::new(&request, Some(10), &columns, false, false).unwrap();
            let seeds = ["https://example.test/000", "https://example.test/999"];
            if seed_better_extrema {
                let seed = AggregateDirectColumnAccessor::Utf8Dictionary {
                    row_ids: vec![0, 1],
                    values: seeds
                        .iter()
                        .map(|value| Utf8DictionaryValue::from(std::sync::Arc::<str>::from(*value)))
                        .collect(),
                    value_nulls: None,
                    row_nulls: None,
                    source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
                };
                assert!(
                    states
                        .update_dense_general_direct_from_transformed_dictionary(&[seed], None)
                        .unwrap()
                );
            }
            let mut expected = (1..=96)
                .map(|index| format!("https://example.test/{index:03}-東京"))
                .collect::<Vec<_>>();
            if descending {
                expected.reverse();
            }
            expected.extend_from_within(..32);
            let (source, memory) = charged_utf8_source(&expected);
            let input = source.clone().into_array();
            let accessor = aggregate_direct_utf8_chunk_dictionary_accessor_profiled(
                "renamed",
                &input,
                &mut native_numeric_accessor::Utf8AccessorWork::default(),
                &mut vortex::array::legacy_session().create_execution_ctx(),
            )
            .unwrap()
            .unwrap();
            native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take);
            assert!(
                states
                    .update_dense_general_direct_from_transformed_dictionary(
                        std::slice::from_ref(&accessor),
                        None,
                    )
                    .unwrap()
            );
            assert!(states.transformed_dictionary_dense_general_chunk_partials);
            assert_eq!(
                native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
                if seed_better_extrema {
                    0
                } else {
                    usize::from(needs_min) + usize::from(needs_max)
                },
                "only globally adopted chunk extrema need independent strings"
            );
            drop(source);
            drop(input);
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(accessor);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            let id = states.string_interner.id("example.test").unwrap();
            let state = &states
                .transformed_dictionary_dense_general_groups
                .as_ref()
                .unwrap()[&id];
            assert_eq!(state.row_count, 128 + 2 * u64::from(seed_better_extrema));
            let length_sum = expected.iter().map(String::len).sum::<usize>()
                + if seed_better_extrema {
                    seeds.iter().map(|value| value.len()).sum()
                } else {
                    0
                };
            assert_eq!(
                state.length_sum,
                uint64_stat_to_float64(u64::try_from(length_sum).unwrap())
            );
            let min = if seed_better_extrema {
                seeds[0]
            } else {
                expected.iter().min().unwrap().as_str()
            };
            let max = if seed_better_extrema {
                seeds[1]
            } else {
                expected.iter().max().unwrap().as_str()
            };
            assert_eq!(state.min_utf8.as_deref(), needs_min.then_some(min));
            assert_eq!(state.max_utf8.as_deref(), needs_max.then_some(max));
        }
    }
}

#[test]
fn dense_chunk_extrema_promote_only_final_winners_and_release_source_credit() {
    check_dense_chunk_extrema_promotions(false);
}

#[test]
fn dense_chunk_extrema_rejected_globally_do_not_promote_source_values() {
    check_dense_chunk_extrema_promotions(true);
}

#[test]
fn dense_borrowed_chunk_statistics_preserve_weighted_order_and_shared_winners() {
    let texts = ["https://example.test/東京", "https://example.test/z-end"];
    let source = VarBinViewArray::from_iter_str(texts);
    let values = (0..texts.len())
        .map(|row| {
            Utf8DictionaryValue::source(
                vortex::buffer::BufferString::try_from(source.bytes_at(row)).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let plan = TransformedDictionaryDenseGeneralPlan {
        needs_length_additive: true,
        needs_length_min: true,
        needs_length_max: true,
        needs_utf8_min: true,
        needs_utf8_max: true,
        fused_transform: true,
    };
    native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take);
    let mut partial = TransformedDictionaryDenseGeneralState::<&Utf8DictionaryValue>::default();
    partial
        .update_weighted_utf8_dictionary_value_with(plan, &values[0], 0, |_| {
            panic!("a zero-weight value must not be retained")
        })
        .unwrap();
    let mut global = TransformedDictionaryDenseGeneralState::default();
    global.merge_chunk_partial(plan, &partial).unwrap();
    assert_eq!(global.row_count, 0);
    assert_eq!(global.length_sum, 0.0);
    assert!(global.min_utf8.is_none() && global.max_utf8.is_none());

    let weights = [(1_u64 << 53) + 1, 7, 3];
    let mut expected_sum = 0.0;
    for (index, weight) in weights.into_iter().enumerate() {
        let value = &values[index % values.len()];
        partial
            .update_weighted_utf8_dictionary_value_with(plan, value, weight, |value| value)
            .unwrap();
        expected_sum += uint64_stat_to_float64(u64::try_from(value.len()).unwrap())
            * uint64_stat_to_float64(weight);
    }
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        0
    );
    global.merge_chunk_partial(plan, &partial).unwrap();
    assert_eq!(global.row_count, weights.into_iter().sum::<u64>());
    assert_eq!(global.length_sum.to_bits(), expected_sum.to_bits());
    assert_eq!(global.min_length, Some(26));
    assert_eq!(global.max_length, Some(27));
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        2
    );
    global.merge_chunk_partial(plan, &partial).unwrap();
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        0
    );

    let fresh = Utf8DictionaryValue::source(
        vortex::buffer::BufferString::try_from(source.bytes_at(0)).unwrap(),
    );
    let mut single = TransformedDictionaryDenseGeneralState::<&Utf8DictionaryValue>::default();
    single
        .update_weighted_utf8_dictionary_value_with(plan, &fresh, 1, |value| value)
        .unwrap();
    let mut same_winner = TransformedDictionaryDenseGeneralState::default();
    same_winner.merge_chunk_partial(plan, &single).unwrap();
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        1
    );
    assert!(std::sync::Arc::ptr_eq(
        same_winner.min_utf8.as_ref().unwrap(),
        same_winner.max_utf8.as_ref().unwrap(),
    ));
}

#[test]
fn dense_borrowed_chunk_overflow_fails_before_independent_promotion() {
    let source = VarBinViewArray::from_iter_str(["https://example.test/long"]);
    let value = Utf8DictionaryValue::source(
        vortex::buffer::BufferString::try_from(source.bytes_at(0)).unwrap(),
    );
    let plan = TransformedDictionaryDenseGeneralPlan {
        needs_length_additive: true,
        needs_utf8_min: true,
        needs_utf8_max: true,
        ..Default::default()
    };
    let mut partial = TransformedDictionaryDenseGeneralState::<&Utf8DictionaryValue>::default();
    partial
        .update_weighted_utf8_dictionary_value_with(plan, &value, 1, |value| value)
        .unwrap();
    native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take);
    let mut overflow = TransformedDictionaryDenseGeneralState {
        row_count: u64::MAX,
        ..Default::default()
    };
    assert!(
        overflow
            .merge_chunk_partial(plan, &partial)
            .unwrap_err()
            .to_string()
            .contains("count overflowed u64 during chunk-partial merge")
    );
    partial.row_count = u64::MAX;
    assert!(
        partial
            .update_weighted_utf8_dictionary_value_with(plan, &value, 1, |_| {
                panic!("overflow must fail before retaining an extremum")
            })
            .unwrap_err()
            .to_string()
            .contains("count overflowed u64")
    );
    partial.row_count = 1;
    partial.length_sum = f64::MAX;
    overflow.row_count = 1;
    overflow.length_sum = f64::MAX;
    assert!(
        overflow
            .merge_chunk_partial(plan, &partial)
            .unwrap_err()
            .to_string()
            .contains("length sum became non-finite during chunk-partial merge")
    );
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        0
    );
}

#[test]
#[allow(clippy::too_many_lines)] // One cache lifetime: miss, hit, saturation, then source release.
fn source_backed_transformed_cache_promotes_only_retained_misses() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("text").unwrap()],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "n".into(),
        )],
    );
    let columns = ["text".into()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(10), &columns, false, false).unwrap();
    states.resource_envelope.group_state_soft_item_budget = 1;
    let text = format!("https://example.test/{}", "long-value-".repeat(32));
    let source = VarBinViewArray::from_iter_str([text.as_str()]);
    let value = Utf8DictionaryValue::source(
        vortex::buffer::BufferString::try_from(source.bytes_at(0)).unwrap(),
    );
    native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take);
    let domain = states
        .transformed_dictionary_group_key(AggregateValueTransform::UrlDomain, &value)
        .unwrap();
    let (retained, _) = states
        .transformed_dictionary_key_cache
        .url_domain
        .get_key_value(value.as_ref())
        .unwrap();
    assert_ne!(
        retained.as_ptr(),
        value.as_ptr(),
        "persistent cache keys must not retain provider slices"
    );
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        1
    );
    // A fresh equal source has no cached promotion, so this also detects
    // accidental ownership promotion before the borrowed membership check.
    let fresh_hit = Utf8DictionaryValue::source(
        vortex::buffer::BufferString::try_from(source.bytes_at(0)).unwrap(),
    );
    assert_eq!(
        domain,
        states
            .transformed_dictionary_group_key(AggregateValueTransform::UrlDomain, &fresh_hit)
            .unwrap()
    );
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        0,
        "a cache hit must borrow the source string"
    );
    let length = states
        .transformed_dictionary_group_key(AggregateValueTransform::Length, &value)
        .unwrap();
    assert_ne!(length, domain, "transform namespaces must remain distinct");
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        0,
        "independent transform keys can share the entry's already-owned payload"
    );
    assert_eq!(
        length,
        states
            .transformed_dictionary_group_key(AggregateValueTransform::Length, &fresh_hit)
            .unwrap()
    );
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        0
    );
    for index in 0..states.transformed_dictionary_key_cache_cap() {
        let fill =
            Utf8DictionaryValue::from(std::sync::Arc::<str>::from(format!("filler-{index}")));
        states
            .transformed_dictionary_group_key(AggregateValueTransform::Length, &fill)
            .unwrap();
    }
    assert_eq!(
        states.transformed_dictionary_key_cache.len(),
        states.transformed_dictionary_key_cache_cap()
    );
    let uncached_source =
        VarBinViewArray::from_iter_str(["https://different.test/long-uncached-value"]);
    let uncached = Utf8DictionaryValue::source(
        vortex::buffer::BufferString::try_from(uncached_source.bytes_at(0)).unwrap(),
    );
    states
        .transformed_dictionary_group_key(AggregateValueTransform::UrlDomain, &uncached)
        .unwrap();
    assert!(states.transformed_dictionary_key_cache_saturated);
    assert_eq!(
        native_utf8::SOURCE_PROMOTIONS.with(std::cell::Cell::take),
        0,
        "a saturated cache must not copy an unretained miss"
    );
    drop(value);
    drop(fresh_hit);
    drop(source);
    let independently_owned = Utf8DictionaryValue::from(std::sync::Arc::<str>::from(text));
    assert_eq!(
        domain,
        states
            .transformed_dictionary_group_key(
                AggregateValueTransform::UrlDomain,
                &independently_owned
            )
            .unwrap()
    );
    assert_eq!(
        length,
        states
            .transformed_dictionary_group_key(AggregateValueTransform::Length, &independently_owned)
            .unwrap()
    );
}

#[test]
fn borrowed_native_strings_match_provider_for_sliced_inline_and_multiple_buffers() {
    let first = "shared-prefix-東京-first";
    let second = "shared-prefix-東京-second";
    let expected = ["outside", "", "123456789012", first, second, first, "tail"];
    let buffers = vec![
        ByteBuffer::from(format!("pad{first}").into_bytes()),
        ByteBuffer::from(format!("padding{second}").into_bytes()),
    ];
    let views = Buffer::from(vec![
        BinaryView::new_inlined(expected[0].as_bytes()),
        BinaryView::new_inlined(expected[1].as_bytes()),
        BinaryView::new_inlined(expected[2].as_bytes()),
        BinaryView::make_view(first.as_bytes(), 0, 3),
        BinaryView::make_view(second.as_bytes(), 1, 7),
        BinaryView::make_view(first.as_bytes(), 0, 3),
        BinaryView::new_inlined(expected[6].as_bytes()),
    ]);
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let source = VarBinViewArray::try_new(
        views,
        buffers.into(),
        DType::Utf8(Nullability::NonNullable),
        Validity::NonNullable,
        &mut ctx,
    )
    .unwrap()
    .into_array();
    for range in [0..expected.len(), 1..6, 2..2] {
        let input = source.slice(range.clone()).unwrap();
        let values = input.clone().execute::<VarBinViewArray>(&mut ctx).unwrap();
        let provider_buffers = values
            .data_buffers()
            .iter()
            .map(|buffer| buffer.as_host().as_slice())
            .collect::<Vec<_>>();
        let mut dictionary = utf8_chunk_dictionary::Utf8ChunkDictionary::default();
        let mut expected_counts = std::collections::BTreeMap::new();
        for (row, value) in expected[range].iter().enumerate() {
            let bytes = native_utf8::borrowed_bytes(&values, row);
            assert_eq!(bytes, value.as_bytes());
            assert_eq!(bytes, values.views()[row].bytes(&provider_buffers));
            let owned = values.bytes_at(row);
            assert_eq!(bytes, owned.as_slice());
            // The provider uses a dangling sentinel for an empty owned buffer.
            if !bytes.is_empty() {
                assert_eq!(bytes.as_ptr(), owned.as_ptr());
            }
            dictionary.intern_source("text", &values, row).unwrap();
            *expected_counts.entry((*value).to_owned()).or_insert(0_u64) += 1;
        }
        let (dictionary_values, copied, retained) = dictionary.into_values();
        assert_eq!(copied, 0);
        assert_eq!(dictionary_values.len(), expected_counts.len());
        assert_eq!(
            retained,
            expected_counts
                .keys()
                .map(|value| value.len() as u64)
                .sum::<u64>()
        );
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut lease = memory
            .reserve(string_count_partial::partial_bytes(&input).unwrap())
            .unwrap();
        let partial = string_count_partial::count_string_chunk(
            &input,
            vortex::array::legacy_session().create_execution_ctx(),
            &aggregate_chunk_jobs::ChunkWorkerContext::Inline(
                shardloom_exec::compute_pool::CancellationToken::default(),
            ),
            &mut lease,
        )
        .unwrap();
        drop(provider_buffers);
        drop(values);
        drop(input);
        drop(dictionary_values);
        let mut actual = std::collections::BTreeMap::new();
        partial
            .for_each_count(|value, count| {
                assert!(actual.insert(value.to_owned(), count).is_none());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected_counts);
        for index in 0..actual.len() {
            let (bytes, _, count) = partial.entry(index).unwrap();
            assert_eq!(actual[std::str::from_utf8(bytes).unwrap()], count);
        }
        drop(partial);
        drop(lease);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn source_dictionary_credits_release_after_accessor_not_persistent_keys() {
    let expected = [
        "short",
        "https://example.test/z",
        "https://example.test/a",
        "short",
    ];
    let template = VarBinViewArray::from_iter_str(expected);
    let memory = LiveMemoryPool::new(4096).unwrap();
    let allocator = ReservedHostAllocator::new(memory.clone());
    let copy = |bytes: &[u8], alignment| {
        let mut owned = allocator.allocate(bytes.len(), alignment).unwrap();
        owned.as_mut_slice().copy_from_slice(bytes);
        owned.freeze()
    };
    let views = copy(
        template.views_handle().as_host(),
        Alignment::of::<BinaryView>(),
    );
    let buffers = template
        .data_buffers()
        .iter()
        .map(|buffer| copy(buffer.as_host(), Alignment::of::<u8>()))
        .collect::<Vec<_>>();
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let source = VarBinViewArray::try_new(
        Buffer::from_byte_buffer(views),
        buffers.into(),
        DType::Utf8(Nullability::NonNullable),
        Validity::NonNullable,
        &mut ctx,
    )
    .unwrap();
    drop(template);
    let retained = memory.snapshot().reserved_bytes;
    assert!(retained > 0);
    let input = source.clone().into_array();
    let mut work = native_numeric_accessor::Utf8AccessorWork::default();
    let accessor = aggregate_direct_utf8_chunk_dictionary_accessor_profiled(
        "renamed", &input, &mut work, &mut ctx,
    )
    .unwrap()
    .unwrap();
    assert_eq!(work.copied_bytes, 0);
    assert_eq!(
        work.source_backed_bytes,
        expected[..3].iter().map(|s| s.len() as u64).sum::<u64>()
    );
    let AggregateDirectColumnAccessor::Utf8Dictionary {
        values, row_ids, ..
    } = &accessor
    else {
        panic!("dictionary")
    };
    assert_eq!(row_ids, &[0, 1, 2, 0]);
    for (row, value) in values.iter().enumerate() {
        assert_eq!(value.as_bytes().as_ptr(), source.bytes_at(row).as_ptr());
    }
    let mut interner = AggregateStringInterner::default();
    let id = interner.intern_dictionary(&values[1]).unwrap();
    assert_eq!(interner.intern_dictionary(&values[1]).unwrap(), id);
    let key = interner.value_arc(id).unwrap();
    assert_ne!(key.as_bytes().as_ptr(), values[1].as_bytes().as_ptr());
    let mut distinct = AggregateDistinctSet::default();
    aggregate_direct_count_distinct_insert_utf8_dictionary_values(
        vec![true; 3],
        values,
        &mut distinct,
    )
    .unwrap();
    let escaped = aggregate_direct_distinct_value(&accessor, 2).unwrap();
    drop(source);
    drop(input);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    let full_budget_payload = 4096 - *Alignment::DEFAULT_ALIGNMENT;
    assert!(
        allocator
            .allocate(full_budget_payload, Alignment::of::<u8>())
            .is_err()
    );
    assert_eq!(
        aggregate_direct_stat_value(&accessor, 3).unwrap(),
        StatValue::Utf8("short".into())
    );
    drop(accessor);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(interner.value(id).unwrap(), expected[1]);
    assert_eq!(distinct.len(), 3);
    assert_eq!(
        escaped,
        AggregateDistinctValue::Utf8(std::sync::Arc::from(expected[2]))
    );
    let capacity = allocator
        .allocate(full_budget_payload, Alignment::of::<u8>())
        .unwrap();
    drop(capacity);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
