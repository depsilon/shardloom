//! Shared actual/schema admission for existing pair COUNT and UTF8 DISTINCT.
use super::{AggregateValueTransform, GroupedAggregateStates, NumericUtf8GroupRoles};
use vortex::array::dtype::{DType, Nullability};

#[derive(Clone, Copy)]
pub(super) enum Roles {
    Pair(NumericUtf8GroupRoles),
    Utf8Distinct { value: usize, group: usize },
}
impl Roles {
    pub(super) fn numeric_column(self) -> usize {
        match self {
            Self::Pair(r) => r.numeric_column,
            Self::Utf8Distinct { value, .. } => value,
        }
    }
    pub(super) fn text_column(self) -> usize {
        match self {
            Self::Pair(r) => r.utf8_column,
            Self::Utf8Distinct { group, .. } => group,
        }
    }
    pub(super) fn numeric_first(self) -> bool {
        match self {
            Self::Pair(r) => r.numeric_group < r.utf8_group,
            Self::Utf8Distinct { .. } => false,
        }
    }
    pub(super) fn pair(self) -> Option<NumericUtf8GroupRoles> {
        match self {
            Self::Pair(r) => Some(r),
            Self::Utf8Distinct { .. } => None,
        }
    }
    pub(super) fn utf8_distinct(self) -> bool {
        matches!(self, Self::Utf8Distinct { .. })
    }
}
pub(super) fn admit(
    states: &GroupedAggregateStates<'_>,
    dtype: &DType,
    columns: &[String],
) -> Option<Roles> {
    admit_utf8_distinct(states, dtype, columns).or_else(|| {
        super::compound_count_workers::pair_roles(states, dtype, columns).map(Roles::Pair)
    })
}

fn admit_utf8_distinct(
    states: &GroupedAggregateStates<'_>,
    dtype: &DType,
    columns: &[String],
) -> Option<Roles> {
    use super::{SimpleAggregateFunction, exact_distinct_pairs::workers};
    if states.request.group_by.len() != 1
        || !states.request.group_expressions.is_empty()
        || states.group_key_indices.len() != 1
        || states.group_columns.len() != 1
        || states.state_template.states.len() != 1
        || states.request.spill.is_some()
        || !states.request.having.is_empty()
        || !states.groups.is_empty()
        || !states.group_order.is_empty()
        || states.finalized_distinct_counts.is_some()
        || states
            .string_count_distinct_topk_heavy_hitter_sketch
            .is_some()
        || states.string_count_distinct_topk_exact_sets.is_some()
        || states
            .result_limit
            .is_none_or(|limit| limit == 0 || states.request.offset.checked_add(limit).is_none())
    {
        return None;
    }
    let group = states.group_columns.get(states.group_key_indices[0])?;
    let measure = &states.state_template.states[0];
    if !matches!(group.transform, AggregateValueTransform::Identity)
        || !group.extra_column_indices.is_empty()
        || measure.function != SimpleAggregateFunction::CountDistinct
        || !matches!(measure.value_transform, AggregateValueTransform::Identity)
        || measure.argument_offset.is_some()
        || !workers::order_admitted(&states.request.order_by, &measure.alias, &group.name)
    {
        return None;
    }
    let value = measure.column_index?;
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return None;
    };
    if !matches!(
        fields.field(columns.get(group.column_index)?.as_str()),
        Some(DType::Utf8(Nullability::NonNullable))
    ) || !matches!(fields.field(columns.get(value)?.as_str()), Some(DType::Primitive(ptype, Nullability::NonNullable)) if ptype.is_int())
    {
        return None;
    }
    Some(Roles::Utf8Distinct {
        value,
        group: group.column_index,
    })
}
