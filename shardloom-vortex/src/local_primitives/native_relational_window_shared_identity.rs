//! Exact observation identity for contiguous resident window outputs.

use super::{Aggregate, Bound, Frame, Function, Offset, RangeOffset, Spec};

pub(in super::super) fn can_share(primary: &Spec, companion: &Spec) -> bool {
    let family = match (primary.function, companion.function) {
        (
            Function::Aggregate(Aggregate::Sum | Aggregate::Avg),
            Function::Aggregate(Aggregate::Sum | Aggregate::Avg | Aggregate::Count),
        ) => true,
        (Function::Aggregate(left), Function::Aggregate(right)) => {
            single_moment(left) && single_moment(right)
        }
        _ => false,
    };
    family
        && primary.key.is_some()
        && primary.column.is_some()
        && primary.key == companion.key
        && primary.column == companion.column
        && primary.filter_key == companion.filter_key
        && [primary, companion].iter().all(|spec| {
            spec.secondary_key.is_none()
                && spec.quantile_level.is_none()
                && spec.separator.is_none()
                && spec.selection_order.is_empty()
        })
        // COUNT has no decimal output domain. Its input is the same bound key;
        // it borrows the arithmetic owner's observation count instead.
        && (companion.function == Function::Aggregate(Aggregate::Count)
            || primary.decimal == companion.decimal)
        && same_frame(&primary.frame, &companion.frame)
}

fn single_moment(function: Aggregate) -> bool {
    matches!(
        function,
        Aggregate::VarPop | Aggregate::VarSamp | Aggregate::StddevPop | Aggregate::StddevSamp
    )
}

fn same_frame(left: &Frame, right: &Frame) -> bool {
    left.unit == right.unit
        && left.exclusion == right.exclusion
        && left.order == right.order
        && same_bound(&left.start, &right.start)
        && same_bound(&left.end, &right.end)
}

fn same_bound(left: &Bound, right: &Bound) -> bool {
    match (left, right) {
        (Bound::UnboundedPreceding, Bound::UnboundedPreceding)
        | (Bound::CurrentRow, Bound::CurrentRow)
        | (Bound::UnboundedFollowing, Bound::UnboundedFollowing) => true,
        (Bound::Preceding(left), Bound::Preceding(right))
        | (Bound::Following(left), Bound::Following(right)) => same_offset(left, right),
        _ => false,
    }
}

fn same_offset(left: &Offset, right: &Offset) -> bool {
    match (left, right) {
        (Offset::Count(left), Offset::Count(right)) => left == right,
        (Offset::Range(left), Offset::Range(right)) => same_range_offset(left, right),
        _ => false,
    }
}

fn same_range_offset(left: &RangeOffset, right: &RangeOffset) -> bool {
    match (left, right) {
        (RangeOffset::Integer(left), RangeOffset::Integer(right)) => left == right,
        (RangeOffset::Float(left), RangeOffset::Float(right)) => left.to_bits() == right.to_bits(),
        (
            RangeOffset::Decimal {
                value: left,
                scale: left_scale,
            },
            RangeOffset::Decimal {
                value: right,
                scale: right_scale,
            },
        ) => left == right && left_scale == right_scale,
        (
            RangeOffset::Temporal {
                nanoseconds: left,
                tick_nanoseconds: left_tick,
            },
            RangeOffset::Temporal {
                nanoseconds: right,
                tick_nanoseconds: right_tick,
            },
        ) => left == right && left_tick == right_tick,
        (
            RangeOffset::Calendar {
                amount: left,
                kernel: left_kernel,
            },
            RangeOffset::Calendar {
                amount: right,
                kernel: right_kernel,
            },
        ) => left == right && left_kernel.same_semantics(right_kernel),
        _ => false,
    }
}
