//! Dictionary-local reduction, retained with its source accessor until
//! ordered merge. Source indices identify keys without copying string payloads.

use super::{
    AggregateDirectColumnAccessor, AggregateValueTransform, GroupedAggregateStates,
    TRANSFORMED_DICTIONARY_DENSE_GENERAL_CHUNK_PARTIAL_SAMPLE_LIMIT,
    TransformedDictionaryDenseGeneralPlan, TransformedDictionaryDenseGeneralState,
    Utf8DictionaryValue, reserve_hash_map_capacity,
    transformed_dictionary_dense_general_should_use_chunk_partials,
    utf8_chunk_dictionary::exact_row_vec,
};
use hashbrown::HashMap;
use rustc_hash::FxBuildHasher;
use shardloom_core::{Result, ShardLoomError};
use std::hash::BuildHasher;

type BorrowedState<'a> = TransformedDictionaryDenseGeneralState<&'a Utf8DictionaryValue>;
struct IndexedValue<'a> {
    index: usize,
    value: &'a Utf8DictionaryValue,
}
impl AsRef<str> for IndexedValue<'_> {
    fn as_ref(&self) -> &str {
        self.value.as_ref()
    }
}
type PartialState<'a> = TransformedDictionaryDenseGeneralState<IndexedValue<'a>>;
type Entry<'a> = (&'a str, (usize, PartialState<'a>));

struct DictionaryPartial {
    source_index: usize,
    row_count: u64,
    length_sum: f64,
    min_length: Option<u64>,
    max_length: Option<u64>,
    min_utf8_index: Option<usize>,
    max_utf8_index: Option<usize>,
}

impl DictionaryPartial {
    fn borrowed<'a>(&self, values: &'a [Utf8DictionaryValue]) -> Result<BorrowedState<'a>> {
        let resolve = |index| {
            values
                .get(index)
                .ok_or_else(|| failed("extremum source index was out of bounds"))
        };
        Ok(BorrowedState {
            row_count: self.row_count,
            length_sum: self.length_sum,
            min_length: self.min_length,
            max_length: self.max_length,
            min_utf8: self.min_utf8_index.map(resolve).transpose()?,
            max_utf8: self.max_utf8_index.map(resolve).transpose()?,
        })
    }
}

pub(super) struct PreparedDictionaryPartials {
    transform: AggregateValueTransform,
    plan: TransformedDictionaryDenseGeneralPlan,
    rows: usize,
    value_count: usize,
    active_values: usize,
    partials: Vec<DictionaryPartial>,
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native dictionary partial {reason}; no fallback execution was attempted"
    ))
}

// Pinned hashbrown 0.17.1: at least twice the maximum entries, one control byte
// per bucket and conservative control/alignment padding. No table growth occurs
// after the single fallible reservation below.
fn table_bytes(entries: usize, entry_bytes: usize) -> Result<usize> {
    if entries == 0 {
        return Ok(0);
    }
    entries
        .checked_mul(2)
        .and_then(|n| n.max(16).checked_next_power_of_two())
        .and_then(|buckets| buckets.checked_mul(entry_bytes + 1))
        .and_then(|bytes| bytes.checked_add(64))
        .ok_or_else(|| failed("table capacity overflow"))
}

pub(super) fn bounded_capacity_bytes(rows: usize) -> Result<u64> {
    let table = table_bytes(rows, size_of::<Entry<'_>>())?;
    // The unchanged bounded sampler grows its set geometrically; account for
    // both old and new allocations while a growth is in progress.
    let sample = table_bytes(
        rows.min(TRANSFORMED_DICTIONARY_DENSE_GENERAL_CHUNK_PARTIAL_SAMPLE_LIMIT),
        size_of::<&str>(),
    )?
    .checked_mul(2)
    .ok_or_else(|| failed("sample capacity overflow"))?;
    rows.checked_mul(size_of::<u64>() + size_of::<DictionaryPartial>())
        .and_then(|bytes| bytes.checked_add(table))
        .and_then(|bytes| bytes.checked_add(sample))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("bounded metadata size overflow"))
}

pub(super) fn prepare(
    accessor: &AggregateDirectColumnAccessor,
    transform: AggregateValueTransform,
    plan: TransformedDictionaryDenseGeneralPlan,
    check_cancelled: &impl Fn() -> Result<()>,
) -> Result<Option<PreparedDictionaryPartials>> {
    check_cancelled()?;
    let AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids,
        values,
        value_nulls,
        row_nulls,
        ..
    } = accessor
    else {
        return Err(failed("preparation lost its dictionary accessor"));
    };
    if values.len() > row_ids.len()
        || value_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.iter().any(|v| *v))
        || row_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.iter().any(|v| *v))
    {
        return Err(failed("preparation violated its non-null recipe"));
    }
    let mut counts = exact_row_vec(values.len())?;
    counts.resize(values.len(), 0_u64);
    for (row, &id) in row_ids.iter().enumerate() {
        if row % 4096 == 0 {
            check_cancelled()?;
        }
        let index = usize::try_from(id).map_err(|_| failed("dictionary id exceeded usize"))?;
        let count = counts
            .get_mut(index)
            .ok_or_else(|| failed("dictionary id was out of bounds"))?;
        *count = count
            .checked_add(1)
            .ok_or_else(|| failed("dictionary count overflow"))?;
    }
    if !transformed_dictionary_dense_general_should_use_chunk_partials(transform, values, &counts) {
        return Ok(None);
    }
    let partials = reduce(
        transform,
        plan,
        values,
        &counts,
        FxBuildHasher,
        check_cancelled,
    )?;
    Ok(Some(PreparedDictionaryPartials {
        transform,
        plan,
        rows: row_ids.len(),
        value_count: values.len(),
        active_values: counts.iter().filter(|count| **count > 0).count(),
        partials,
    }))
}

fn reduce<S: BuildHasher>(
    transform: AggregateValueTransform,
    plan: TransformedDictionaryDenseGeneralPlan,
    values: &[Utf8DictionaryValue],
    counts: &[u64],
    hasher: S,
    check_cancelled: &impl Fn() -> Result<()>,
) -> Result<Vec<DictionaryPartial>> {
    if values.len() != counts.len() {
        return Err(failed("reduction recipe or count length changed"));
    }
    let active = counts.iter().filter(|count| **count > 0).count();
    let bound = table_bytes(active, size_of::<Entry<'_>>())?;
    let mut groups = HashMap::<&str, (usize, PartialState<'_>), S>::with_hasher(hasher);
    groups
        .try_reserve(active)
        .map_err(|_| failed("could not reserve group directory"))?;
    if groups.allocation_size() > bound {
        return Err(failed("group directory exceeded admitted capacity"));
    }
    for (index, (value, &count)) in values.iter().zip(counts).enumerate() {
        if index % 4096 == 0 {
            check_cancelled()?;
        }
        if count != 0 {
            groups
                .entry(transform.domain_str(value))
                .or_insert_with(|| (index, PartialState::default()))
                .1
                .update_weighted_utf8_dictionary_value_with(plan, value, count, |value| {
                    IndexedValue { index, value }
                })?;
        }
    }
    let mut partials = exact_row_vec(groups.len())?;
    for (index, (_, (source_index, state))) in groups.into_iter().enumerate() {
        if index % 4096 == 0 {
            check_cancelled()?;
        }
        partials.push(DictionaryPartial {
            source_index,
            row_count: state.row_count,
            length_sum: state.length_sum,
            min_length: state.min_length,
            max_length: state.max_length,
            min_utf8_index: state.min_utf8.map(|value| value.index),
            max_utf8_index: state.max_utf8.map(|value| value.index),
        });
    }
    Ok(partials)
}

impl PreparedDictionaryPartials {
    pub(super) fn active_values(&self) -> usize {
        self.active_values
    }

    pub(super) fn group_count(&self) -> usize {
        self.partials.len()
    }

    pub(super) fn merge(
        &self,
        states: &mut GroupedAggregateStates<'_>,
        plan: TransformedDictionaryDenseGeneralPlan,
        values: &[Utf8DictionaryValue],
        rows: usize,
    ) -> Result<()> {
        if plan != self.plan
            || rows != self.rows
            || values.len() != self.value_count
            || states.group_columns.first().map(|column| column.transform) != Some(self.transform)
        {
            return Err(failed("ordered consumer recipe or source shape changed"));
        }
        let groups = states
            .transformed_dictionary_dense_general_groups
            .as_mut()
            .ok_or_else(|| failed("ordered consumer lost its dense state"))?;
        reserve_hash_map_capacity(
            groups,
            self.partials.len(),
            "prepared dictionary partial merge",
        )?;
        for partial in &self.partials {
            let value = values
                .get(partial.source_index)
                .ok_or_else(|| failed("partial source index was out of bounds"))?;
            let key = states
                .string_interner
                .intern(self.transform.domain_str(value))?;
            groups
                .entry(key)
                .or_default()
                .merge_chunk_partial(plan, &partial.borrowed(values)?)?;
        }
        states.transformed_dictionary_dense_general_chunk_partials = true;
        states.transformed_dictionary_dense_general_chunk_partial_input_values = states
            .transformed_dictionary_dense_general_chunk_partial_input_values
            .saturating_add(u64::try_from(self.active_values).unwrap_or(u64::MAX));
        states.transformed_dictionary_dense_general_chunk_partial_groups = states
            .transformed_dictionary_dense_general_chunk_partial_groups
            .saturating_add(u64::try_from(self.partials.len()).unwrap_or(u64::MAX));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{AggregateUtf8DictionarySource, native_utf8, uint64_stat_to_float64};
    use super::*;
    use std::{
        cell::Cell,
        hash::{BuildHasherDefault, Hasher},
    };
    use vortex::array::arrays::{VarBinViewArray, varbinview::VarBinViewArrayExt as _};

    fn plan() -> TransformedDictionaryDenseGeneralPlan {
        TransformedDictionaryDenseGeneralPlan {
            needs_length_additive: true,
            needs_length_min: true,
            needs_length_max: true,
            fused_transform: true,
            ..Default::default()
        }
    }

    fn values(texts: &[String]) -> Vec<Utf8DictionaryValue> {
        let source = VarBinViewArray::from_iter_str(texts);
        (0..texts.len())
            .map(|i| Utf8DictionaryValue::source(source.utf8_buffer_at(i).unwrap()))
            .collect()
    }

    #[derive(Default)]
    struct Collision;
    impl Hasher for Collision {
        fn write(&mut self, _: &[u8]) {}
        fn finish(&self) -> u64 {
            0
        }
    }

    #[test]
    fn dictionary_partials_preserve_weighted_bits_and_full_keys_under_collisions() {
        let texts = [
            "https://é.test/東京",
            "https://other.test/x",
            "https://é.test/longer-path",
            "https://other.test/",
        ]
        .map(str::to_owned);
        let values = values(&texts);
        let counts = [(1_u64 << 53) + 1, 3, 7, 0];
        native_utf8::SOURCE_PROMOTIONS.with(Cell::take);
        let partials = reduce(
            AggregateValueTransform::UrlDomain,
            plan(),
            &values,
            &counts,
            BuildHasherDefault::<Collision>::default(),
            &|| Ok(()),
        )
        .unwrap();
        assert_eq!(partials.len(), 2);
        assert_eq!(native_utf8::SOURCE_PROMOTIONS.with(Cell::take), 0);
        let first = partials.iter().find(|p| p.source_index == 0).unwrap();
        let sum = uint64_stat_to_float64(u64::try_from(texts[0].len()).unwrap())
            * uint64_stat_to_float64(counts[0])
            + uint64_stat_to_float64(u64::try_from(texts[2].len()).unwrap())
                * uint64_stat_to_float64(counts[2]);
        assert_eq!(first.length_sum.to_bits(), sum.to_bits());
        assert_eq!(first.row_count, counts[0] + counts[2]);
        assert_eq!(
            first.min_length,
            Some(u64::try_from(texts[0].len().min(texts[2].len())).unwrap())
        );
        assert_eq!(
            first.max_length,
            Some(u64::try_from(texts[0].len().max(texts[2].len())).unwrap())
        );
        assert_eq!(
            partials
                .iter()
                .find(|p| p.source_index == 1)
                .unwrap()
                .row_count,
            3
        );
        assert!(
            reduce(
                AggregateValueTransform::UrlDomain,
                plan(),
                &values,
                &[u64::MAX, 0, 1, 0],
                FxBuildHasher,
                &|| Ok(())
            )
            .err()
            .unwrap()
            .to_string()
            .contains("count overflow")
        );
    }

    #[test]
    fn dictionary_partials_extremum_indices_preserve_shared_winners_and_owner_lifetime() {
        let texts = [
            "https://é.test/z",
            "https://é.test/a",
            "https://same.test/single",
            "https://same.test/ignored",
        ]
        .map(str::to_owned);
        let values = values(&texts);
        let plan = TransformedDictionaryDenseGeneralPlan {
            needs_utf8_min: true,
            needs_utf8_max: true,
            ..plan()
        };
        native_utf8::SOURCE_PROMOTIONS.with(Cell::take);
        let mut partials = reduce(
            AggregateValueTransform::UrlDomain,
            plan,
            &values,
            &[3, 2, 7, 0],
            BuildHasherDefault::<Collision>::default(),
            &|| Ok(()),
        )
        .unwrap();
        assert_eq!(partials.len(), 2);
        assert_eq!(native_utf8::SOURCE_PROMOTIONS.with(Cell::take), 0);
        let first = partials.iter().find(|p| p.source_index == 0).unwrap();
        assert_eq!(
            (first.min_utf8_index, first.max_utf8_index),
            (Some(1), Some(0))
        );
        let mut overflow = TransformedDictionaryDenseGeneralState {
            row_count: u64::MAX,
            ..Default::default()
        };
        assert!(
            overflow
                .merge_chunk_partial(plan, &first.borrowed(&values).unwrap())
                .is_err()
        );
        assert_eq!(native_utf8::SOURCE_PROMOTIONS.with(Cell::take), 0);
        let mut global = TransformedDictionaryDenseGeneralState::default();
        global
            .merge_chunk_partial(plan, &first.borrowed(&values).unwrap())
            .unwrap();
        assert_eq!(native_utf8::SOURCE_PROMOTIONS.with(Cell::take), 2);
        let same = partials.iter().find(|p| p.source_index == 2).unwrap();
        assert_eq!(
            (same.min_utf8_index, same.max_utf8_index),
            (Some(2), Some(2))
        );
        let mut same_global = TransformedDictionaryDenseGeneralState::default();
        same_global
            .merge_chunk_partial(plan, &same.borrowed(&values).unwrap())
            .unwrap();
        assert_eq!(native_utf8::SOURCE_PROMOTIONS.with(Cell::take), 1);
        assert!(std::sync::Arc::ptr_eq(
            same_global.min_utf8.as_ref().unwrap(),
            same_global.max_utf8.as_ref().unwrap()
        ));
        partials[0].min_utf8_index = Some(usize::MAX);
        assert!(
            partials[0]
                .borrowed(&values)
                .err()
                .unwrap()
                .to_string()
                .contains("out of bounds")
        );
        drop(values);
        assert_eq!(global.min_utf8.as_deref(), Some(texts[1].as_str()));
        assert_eq!(global.max_utf8.as_deref(), Some(texts[0].as_str()));
        assert_eq!(same_global.min_utf8.as_deref(), Some(texts[2].as_str()));
    }

    fn accessor(texts: &[String]) -> AggregateDirectColumnAccessor {
        AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids: (0..u32::try_from(texts.len()).unwrap()).collect(),
            values: values(texts),
            value_nulls: None,
            row_nulls: None,
            source: AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
        }
    }

    #[test]
    fn regex_dictionary_partials_keep_the_transform_in_the_recipe() {
        let texts = (0..256)
            .map(|row| {
                if row % 2 == 0 {
                    "http://example.test".to_owned()
                } else {
                    format!("http://example.test/{row:04}")
                }
            })
            .collect::<Vec<_>>();
        let input = accessor(&texts);
        let regex = prepare(
            &input,
            AggregateValueTransform::RegexDomain,
            plan(),
            &|| Ok(()),
        )
        .unwrap()
        .unwrap();
        let url = prepare(&input, AggregateValueTransform::UrlDomain, plan(), &|| {
            Ok(())
        })
        .unwrap()
        .unwrap();
        assert_eq!(regex.group_count(), 2);
        assert_eq!(url.group_count(), 1);
        let request = crate::VortexSimpleAggregateRequest::new(vec![
            crate::VortexSimpleAggregateMeasure::new("count", None, "count".into()),
        ])
        .with_group_expressions(vec![crate::VortexAggregateExpression::new(
            "domain".into(),
            shardloom_core::ColumnRef::new("url").unwrap(),
            "url_domain",
        )]);
        let columns = ["url".into()];
        let mut states =
            GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
        assert!(
            regex
                .merge(&mut states, plan(), &values(&texts), texts.len())
                .unwrap_err()
                .to_string()
                .contains("recipe or source shape changed")
        );
    }

    #[test]
    fn dictionary_partials_keep_sample_admission_and_reject_invalid_ids_and_nulls() {
        for rows in [0, 1, 63, 64, 512] {
            for unique in [false, true] {
                let texts = (0..rows)
                    .map(|i| format!("http://{}.test/{i}", if unique { i } else { i % 2 }))
                    .collect::<Vec<_>>();
                let prepared = prepare(
                    &accessor(&texts),
                    AggregateValueTransform::UrlDomain,
                    plan(),
                    &|| Ok(()),
                )
                .unwrap();
                assert_eq!(prepared.is_some(), rows >= 64 && !unique);
                if let Some(prepared) = prepared {
                    assert_eq!(prepared.active_values(), rows);
                    assert_eq!(prepared.group_count(), 2);
                }
            }
        }
        let mut invalid = accessor(&["http://one.test/a".into()]);
        if let AggregateDirectColumnAccessor::Utf8Dictionary { row_ids, .. } = &mut invalid {
            row_ids[0] = 1;
        }
        assert!(
            prepare(
                &invalid,
                AggregateValueTransform::UrlDomain,
                plan(),
                &|| Ok(())
            )
            .err()
            .unwrap()
            .to_string()
            .contains("out of bounds")
        );
        if let AggregateDirectColumnAccessor::Utf8Dictionary {
            row_ids, row_nulls, ..
        } = &mut invalid
        {
            row_ids[0] = 0;
            *row_nulls = Some(vec![true]);
        }
        assert!(
            prepare(
                &invalid,
                AggregateValueTransform::UrlDomain,
                plan(),
                &|| Ok(())
            )
            .is_err()
        );
        assert!(bounded_capacity_bytes(usize::MAX).is_err());
    }

    #[test]
    fn dictionary_partials_cancel_during_counts_and_reduction_without_promotion() {
        let texts = (0..8193)
            .map(|i| format!("http://{}.test/{i}", i % 2))
            .collect::<Vec<_>>();
        let input = accessor(&texts);
        let calls = Cell::new(0);
        let cancelled = || {
            calls.set(calls.get() + 1);
            if calls.get() == 3 {
                Err(failed("test cancellation"))
            } else {
                Ok(())
            }
        };
        assert!(
            prepare(
                &input,
                AggregateValueTransform::UrlDomain,
                plan(),
                &cancelled
            )
            .err()
            .unwrap()
            .to_string()
            .contains("cancellation")
        );
        assert_eq!(calls.get(), 3);
        calls.set(0);
        let values = values(&texts);
        native_utf8::SOURCE_PROMOTIONS.with(Cell::take);
        assert!(
            reduce(
                AggregateValueTransform::UrlDomain,
                plan(),
                &values,
                &vec![1; values.len()],
                FxBuildHasher,
                &cancelled
            )
            .is_err()
        );
        assert_eq!(calls.get(), 3);
        assert_eq!(native_utf8::SOURCE_PROMOTIONS.with(Cell::take), 0);
    }
}
