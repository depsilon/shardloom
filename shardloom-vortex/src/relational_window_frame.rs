//! Declarative analytic frames; binding resolves offsets against native types.

use shardloom_core::temporal::{Ambiguity, TemporalUnit, civil::MonthEnd};
use shardloom_core::{ColumnRef, ScalarValue};

/// The coordinate system used by a window frame's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRelationalFrameUnit {
    Rows,
    Groups,
    Range,
}

/// Literal offsets are bound before row execution. Durations measure elapsed
/// time; calendar offsets use local date/time fields and explicit policies.
#[derive(Debug, Clone, PartialEq)]
pub enum VortexRelationalFrameOffset {
    Number(ScalarValue),
    DurationMicros(u64),
    Duration {
        value: u64,
        unit: TemporalUnit,
    },
    CalendarDays {
        days: u64,
        ambiguity: Ambiguity,
    },
    CalendarMonths {
        months: u64,
        month_end: MonthEnd,
        ambiguity: Ambiguity,
    },
}

/// Inclusive SQL frame endpoints. Execution uses checked half-open intervals.
#[derive(Debug, Clone, PartialEq)]
pub enum VortexRelationalFrameBound {
    UnboundedPreceding,
    Preceding(VortexRelationalFrameOffset),
    CurrentRow,
    Following(VortexRelationalFrameOffset),
    UnboundedFollowing,
}

/// Rows removed after computing the frame bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VortexRelationalFrameExclusion {
    #[default]
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

/// Default SQL frames include all preceding rows and the current ordering peers.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalWindowFrame {
    pub unit: VortexRelationalFrameUnit,
    pub start: VortexRelationalFrameBound,
    pub end: VortexRelationalFrameBound,
    pub exclusion: VortexRelationalFrameExclusion,
}

impl Default for VortexRelationalWindowFrame {
    fn default() -> Self {
        Self {
            unit: VortexRelationalFrameUnit::Range,
            start: VortexRelationalFrameBound::UnboundedPreceding,
            end: VortexRelationalFrameBound::CurrentRow,
            exclusion: VortexRelationalFrameExclusion::NoOthers,
        }
    }
}

/// Framed reductions and value selection. Computed arguments are ordinary native
/// projection columns, shared with grouped reductions and other expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexRelationalFrameFunction {
    /// Shared reducer declaration, including paired statistical inputs.
    Aggregate(Box<crate::query_primitive::VortexSimpleAggregateMeasure>),
    CountAll,
    Count(ColumnRef),
    CountDistinct(ColumnRef),
    Sum(ColumnRef),
    Avg(ColumnRef),
    Min(ColumnRef),
    Max(ColumnRef),
    FirstValue(ColumnRef),
    LastValue(ColumnRef),
    NthValue {
        column: ColumnRef,
        index: u64,
    },
}

impl VortexRelationalFrameFunction {
    /// Input demand for projection pruning; COUNT(*) has no measure column.
    #[must_use]
    pub fn column(&self) -> Option<&ColumnRef> {
        match self {
            Self::Aggregate(measure) => measure.column.as_ref(),
            Self::CountAll => None,
            Self::Count(column)
            | Self::CountDistinct(column)
            | Self::Sum(column)
            | Self::Avg(column)
            | Self::Min(column)
            | Self::Max(column)
            | Self::FirstValue(column)
            | Self::LastValue(column)
            | Self::NthValue { column, .. } => Some(column),
        }
    }

    pub fn input_columns(&self) -> impl Iterator<Item = &ColumnRef> {
        let (secondary, filter, order) = match self {
            Self::Aggregate(measure) => (
                measure.secondary_column.as_ref(),
                measure.filter_column.as_ref(),
                measure.order_by.as_slice(),
            ),
            _ => (None, None, &[][..]),
        };
        self.column()
            .into_iter()
            .chain(secondary)
            .chain(filter)
            .chain(order.iter().map(|key| &key.column))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        query_primitive::VortexSimpleAggregateMeasure,
        relational_query::{VortexRelationalNullOrder, VortexRelationalOrderKey},
    };

    #[test]
    fn aggregate_filter_window_dependencies_include_predicate_only_column() {
        let function = VortexRelationalFrameFunction::Aggregate(Box::new(
            VortexSimpleAggregateMeasure::new("count", None, "n".into())
                .with_filter_column(ColumnRef::new("keep").unwrap()),
        ));
        assert_eq!(
            function
                .input_columns()
                .map(ColumnRef::as_str)
                .collect::<Vec<_>>(),
            ["keep"]
        );
    }

    #[test]
    fn ordered_selector_window_dependencies_include_hidden_local_order_keys() {
        let function = VortexRelationalFrameFunction::Aggregate(Box::new(
            VortexSimpleAggregateMeasure::new(
                "arg_min",
                Some(ColumnRef::new("value").unwrap()),
                "out".into(),
            )
            .with_secondary_column(ColumnRef::new("argument_key").unwrap())
            .with_order_by(vec![VortexRelationalOrderKey {
                column: ColumnRef::new("hidden_order").unwrap(),
                descending: true,
                nulls: Some(VortexRelationalNullOrder::Last),
            }]),
        ));
        assert_eq!(
            function
                .input_columns()
                .map(ColumnRef::as_str)
                .collect::<Vec<_>>(),
            ["value", "argument_key", "hidden_order"],
        );
        assert_eq!(
            VortexRelationalFrameFunction::CountAll
                .input_columns()
                .count(),
            0
        );
    }
}
