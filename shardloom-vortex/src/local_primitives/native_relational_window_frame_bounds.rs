//! Exact RANGE endpoints, including calendar boundaries that can move backward.

use super::{Bound, Frame, Input, Offset, RangeOffset};
use crate::{
    local_primitives::{native_relational_batch::failed, native_relational_keys::Cell},
    relational_query::{
        VortexRelationalFrameExclusion as Exclusion, VortexRelationalFrameUnit as Unit,
        VortexRelationalNullOrder as NullOrder,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use std::{cmp::Ordering, ops::Range};
use vortex::array::scalar::DecimalValue;

#[derive(Default)]
pub(in super::super) struct Cursor {
    start: usize,
    end: usize,
}

pub(in super::super) struct Position {
    pub(in super::super) rows: usize,
    pub(in super::super) peer_start: usize,
    pub(in super::super) peer_end: usize,
    pub(in super::super) peer: usize,
    pub(in super::super) row: usize,
}

impl Cursor {
    pub(in super::super) fn advance(
        &mut self,
        frame: &Frame,
        at: &Position,
        input: &mut impl Input,
        peer_edge: &mut impl FnMut(usize) -> Result<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Range<usize>> {
        let start = endpoint(
            frame,
            &frame.start,
            false,
            self.start,
            at,
            input,
            peer_edge,
            context,
        )?;
        let end = endpoint(
            frame, &frame.end, true, self.end, at, input, peer_edge, context,
        )?;
        if !frame.can_rewind() && (start < self.start || end < self.end) {
            return Err(failed("window frame endpoints moved backwards"));
        }
        self.start = start;
        self.end = end;
        Ok(start..end.max(start))
    }
}

#[allow(clippy::too_many_arguments)] // Both endpoints share one positional input and peer index.
fn endpoint(
    frame: &Frame,
    bound: &Bound,
    end: bool,
    cursor: usize,
    at: &Position,
    input: &mut impl Input,
    peer_at: &mut impl FnMut(usize) -> Result<usize>,
    context: &NativeExecutionContext<'_>,
) -> Result<usize> {
    let peer_edge = || if end { at.peer_end } else { at.peer_start };
    Ok(match bound {
        Bound::UnboundedPreceding => 0,
        Bound::UnboundedFollowing => at.rows,
        Bound::CurrentRow => {
            if frame.unit == Unit::Rows {
                at.row + usize::from(end)
            } else {
                peer_edge()
            }
        }
        Bound::Preceding(Offset::Count(count)) | Bound::Following(Offset::Count(count)) => {
            let current = if frame.unit == Unit::Rows {
                at.row
            } else {
                at.peer
            };
            let position = if matches!(bound, Bound::Preceding(_)) {
                current.checked_sub(*count)
            } else {
                Some(current.saturating_add(*count))
            };
            match position {
                None => 0,
                Some(position) => {
                    let edge = position.saturating_add(usize::from(end));
                    if frame.unit == Unit::Rows {
                        edge.min(at.rows)
                    } else {
                        peer_at(edge)?
                    }
                }
            }
        }
        Bound::Preceding(Offset::Range(offset)) | Bound::Following(Offset::Range(offset)) => {
            let order = frame
                .order
                .as_ref()
                .ok_or_else(|| failed("RANGE frame has no ordering key"))?;
            if input.key_is_null(at.row, order.key, context)? {
                return Ok(peer_edge());
            }
            let current = input.raw_cell(at.row, order.key, context)?;
            let subtract = matches!(bound, Bound::Preceding(_)) ^ order.descending;
            let boundary = calendar_boundary(&current, offset, subtract)?;
            let mut comparison_at = |position: usize| -> Result<Ordering> {
                Ok(if input.key_is_null(position, order.key, context)? {
                    if order.nulls == Some(NullOrder::First) {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                } else {
                    let candidate = input.raw_cell(position, order.key, context)?;
                    // Compare candidate to the shifted current-row boundary.
                    let value = if let Some(boundary) = boundary {
                        integer(&candidate)?.cmp(&boundary)
                    } else {
                        compare_shifted(&current, &candidate, offset, subtract)?.reverse()
                    };
                    if order.descending {
                        value.reverse()
                    } else {
                        value
                    }
                })
            };
            let reached = |comparison: Ordering| {
                comparison == Ordering::Greater || (!end && comparison == Ordering::Equal)
            };
            if boundary.is_some() {
                // Local calendar shifts need not preserve instant order across
                // a fold. Each endpoint searches the complete sorted partition.
                let (mut lower, mut upper) = (0, at.rows);
                while lower < upper {
                    context.check_cancelled()?;
                    let middle = lower + (upper - lower) / 2;
                    if reached(comparison_at(middle)?) {
                        upper = middle;
                    } else {
                        lower = middle + 1;
                    }
                }
                return Ok(lower);
            }
            let mut cursor = cursor;
            while cursor < at.rows {
                if cursor.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                if reached(comparison_at(cursor)?) {
                    break;
                }
                cursor += 1;
            }
            cursor
        }
    })
}

pub(in super::super) fn validate_intervals(
    previous: &[Range<usize>; 3],
    next: &[Range<usize>; 3],
    rows: usize,
) -> Result<()> {
    validate_ranges(next, rows)?;
    if !monotone(previous, next) {
        return Err(failed(
            "window exclusion interval moved backwards or exceeded its partition",
        ));
    }
    Ok(())
}

pub(in super::super) fn monotone(previous: &[Range<usize>; 3], next: &[Range<usize>; 3]) -> bool {
    previous
        .iter()
        .zip(next)
        .all(|(prior, next)| prior.start <= next.start && prior.end <= next.end)
}

pub(in super::super) fn validate_ranges(next: &[Range<usize>; 3], rows: usize) -> Result<()> {
    let mut end = 0;
    for next in next {
        if next.start < end || next.start > next.end || next.end > rows {
            return Err(failed(
                "window exclusion intervals overlap or exceed their partition",
            ));
        }
        end = next.end;
    }
    Ok(())
}

/// Ordered, disjoint pieces of `left` absent from `right`, including empty bounds.
pub(in super::super) fn difference(left: &Range<usize>, right: &Range<usize>) -> [Range<usize>; 2] {
    [
        left.start..left.end.min(right.start).max(left.start),
        left.start.max(right.end).min(left.end)..left.end,
    ]
}

pub(in super::super) fn intervals(
    range: Range<usize>,
    at: &Position,
    exclusion: Exclusion,
) -> [Range<usize>; 3] {
    let clamp = |position: usize| position.clamp(range.start, range.end);
    match exclusion {
        Exclusion::NoOthers => [range.clone(), range.end..range.end, range.end..range.end],
        Exclusion::CurrentRow => [
            range.start..clamp(at.row),
            clamp(at.row + 1)..range.end,
            range.end..range.end,
        ],
        Exclusion::Group | Exclusion::Ties => [
            range.start..clamp(at.peer_start),
            clamp(at.row)..if exclusion == Exclusion::Ties {
                clamp(at.row + 1)
            } else {
                clamp(at.row)
            },
            clamp(at.peer_end)..range.end,
        ],
    }
}

fn integer(value: &Cell) -> Result<i128> {
    match value {
        Cell::NegativeInteger(value) | Cell::Timestamp(value) | Cell::Temporal(value) => {
            Ok(i128::from(*value))
        }
        Cell::NonnegativeInteger(value) => Ok(i128::from(*value)),
        Cell::Date(value) => Ok(i128::from(*value)),
        _ => Err(failed(
            "RANGE integer ordering domain changed after binding",
        )),
    }
}

fn calendar_boundary(current: &Cell, offset: &RangeOffset, subtract: bool) -> Result<Option<i128>> {
    let RangeOffset::Calendar { amount, kernel } = offset else {
        return Ok(None);
    };
    let value = i64::try_from(integer(current)?)
        .map_err(|_| failed("calendar RANGE key exceeds its temporal domain"))?;
    let amount = i128::from(*amount);
    kernel
        .apply(value, Some(if subtract { -amount } else { amount }))
        .map(|value| Some(i128::from(value)))
}

fn compare_shifted(
    current: &Cell,
    candidate: &Cell,
    offset: &RangeOffset,
    subtract: bool,
) -> Result<Ordering> {
    match offset {
        RangeOffset::Integer(offset) => {
            // Two 64-bit endpoints plus one unsigned 64-bit distance fit i128.
            let offset = if subtract {
                -i128::from(*offset)
            } else {
                i128::from(*offset)
            };
            Ok((integer(current)? - integer(candidate)? + offset).cmp(&0))
        }
        RangeOffset::Temporal {
            nanoseconds,
            tick_nanoseconds,
        } => {
            let difference = (integer(current)? - integer(candidate)?) * tick_nanoseconds;
            Ok((difference
                + if subtract {
                    -*nanoseconds
                } else {
                    *nanoseconds
                })
            .cmp(&0))
        }
        RangeOffset::Calendar { .. } => Err(failed(
            "calendar RANGE boundary must be resolved before searching",
        )),
        RangeOffset::Float(offset) => {
            let (Cell::Float(current), Cell::Float(candidate)) = (current, candidate) else {
                return Err(failed(
                    "RANGE floating ordering domain changed after binding",
                ));
            };
            let current = f64::from_bits(*current);
            // RANGE uses the F64 boundary arithmetic of its ordering domain.
            // An infinite boundary is a comparison sentinel, never an admitted
            // observation or result value. All operands were checked finite.
            let boundary = current + if subtract { -*offset } else { *offset };
            boundary
                .partial_cmp(&f64::from_bits(*candidate))
                .ok_or_else(|| failed("RANGE floating boundary is unordered"))
        }
        RangeOffset::Decimal { value, scale } => {
            let (Cell::Decimal(current, dtype), Cell::Decimal(candidate, other_dtype)) =
                (current, candidate)
            else {
                return Err(failed(
                    "RANGE decimal ordering domain changed after binding",
                ));
            };
            if dtype != other_dtype {
                return Err(failed("RANGE decimal ordering metadata changed"));
            }
            let source_scale =
                u8::try_from(dtype.scale()).map_err(|_| failed("invalid RANGE decimal scale"))?;
            let common = source_scale.max(*scale);
            let widen = |value: i128| DecimalValue::I256(DecimalValue::I128(value).as_i256());
            let value_factor = DecimalValue::I128(10i128.pow(u32::from(common - source_scale)));
            let offset_factor = DecimalValue::I128(10i128.pow(u32::from(common - *scale)));
            let difference = widen(*current)
                .checked_sub(&widen(*candidate))
                .and_then(|value| value.checked_mul(&value_factor))
                .ok_or_else(|| failed("wide RANGE decimal difference overflow"))?;
            let offset = widen(if subtract { -*value } else { *value })
                .checked_mul(&offset_factor)
                .ok_or_else(|| failed("wide RANGE decimal offset overflow"))?;
            let difference = difference
                .checked_add(&offset)
                .ok_or_else(|| failed("wide RANGE decimal boundary overflow"))?;
            difference
                .partial_cmp(&widen(0))
                .ok_or_else(|| failed("wide RANGE decimal comparison failed"))
        }
    }
}
