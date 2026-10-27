//! One exact selector comparator for resident queues and native stored summaries.

use super::{Aggregate, Function, Input, Spec};
use crate::{
    local_primitives::native_relational_batch::failed, relational_query::VortexRelationalNullOrder,
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use std::cmp::Ordering;

fn function(spec: &Spec) -> Result<Aggregate> {
    let Function::Aggregate(function) = spec.function else {
        return Err(failed("window selector requires an aggregate declaration"));
    };
    Ok(function)
}

pub(in crate::local_primitives) fn eligible(
    position: usize,
    spec: &Spec,
    input: &mut impl Input,
    context: &NativeExecutionContext<'_>,
) -> Result<bool> {
    if !super::admitted(position, spec, input, context)? {
        return Ok(false);
    }
    let function = function(spec)?;
    if function == Aggregate::StringAgg
        && input.key_is_null(
            position,
            spec.key
                .ok_or_else(|| failed("string window parent validity is absent"))?,
            context,
        )?
    {
        return Ok(false);
    }
    if matches!(function, Aggregate::Min | Aggregate::Max) {
        let key = spec
            .key
            .ok_or_else(|| failed("window extremum value key is absent"))?;
        return Ok(input.hash_key(position, key, context)?.is_some());
    }
    if function.requires_secondary() {
        let value = spec
            .key
            .ok_or_else(|| failed("window selector parent validity is absent"))?;
        let key = spec
            .secondary_key
            .ok_or_else(|| failed("window selector comparison key is absent"))?;
        if input.key_is_null(position, value, context)?
            || input.key_is_null(position, key, context)?
        {
            return Ok(false);
        }
        input.hash_key(position, key, context)?;
    }
    for key in &spec.selection_order {
        if input.hash_key(position, key.key, context)?.is_none() && key.nulls.is_none() {
            return Err(failed(
                "aggregate ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values",
            ));
        }
    }
    Ok(true)
}

/// Less means preferred. Ties use frame position, which already includes the
/// window's stable source-ordinal tie order; LAST reverses that final order too.
pub(in crate::local_primitives) fn compare(
    left: usize,
    right: usize,
    spec: &Spec,
    input: &mut impl Input,
    context: &NativeExecutionContext<'_>,
) -> Result<Ordering> {
    let function = function(spec)?;
    let primary = match function {
        Aggregate::Min | Aggregate::Max => spec.key,
        Aggregate::ArgMin | Aggregate::ArgMax => spec.secondary_key,
        _ => None,
    };
    if let Some(key) = primary {
        let order = input.compare_key(left, right, key, context)?;
        if order != Ordering::Equal {
            return Ok(if matches!(function, Aggregate::Max | Aggregate::ArgMax) {
                order.reverse()
            } else {
                order
            });
        }
    }
    for key in &spec.selection_order {
        let left_null = input.key_is_null(left, key.key, context)?;
        let right_null = input.key_is_null(right, key.key, context)?;
        if (left_null || right_null) && key.nulls.is_none() {
            return Err(failed(
                "aggregate ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values",
            ));
        }
        let values = if left_null || right_null {
            Ordering::Equal
        } else {
            input.compare_key(left, right, key.key, context)?
        };
        let order = key
            .nulls
            .unwrap_or(VortexRelationalNullOrder::Last)
            .compare(key.descending, left_null, right_null, || values);
        if order != Ordering::Equal {
            return Ok(if function == Aggregate::Last {
                order.reverse()
            } else {
                order
            });
        }
    }
    Ok(if function == Aggregate::Last {
        right.cmp(&left)
    } else {
        left.cmp(&right)
    })
}

pub(in crate::local_primitives) fn choose(
    left: Option<usize>,
    right: Option<usize>,
    spec: &Spec,
    input: &mut impl Input,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<usize>> {
    let (Some(left), Some(right)) = (left, right) else {
        return Ok(left.or(right));
    };
    Ok(Some(
        if compare(left, right, spec, input, context)?.is_gt() {
            right
        } else {
            left
        },
    ))
}
