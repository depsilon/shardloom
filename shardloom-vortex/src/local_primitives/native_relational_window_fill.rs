//! Two endpoint records over the existing resident or stored window input.

use super::super::{
    native_relational_batch::failed,
    native_relational_expression,
    native_relational_keys::Cell,
    native_relational_window_frame::{Input, Value},
};
use crate::{
    relational_query::VortexRelationalFillMethod as Method,
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;

#[cfg(test)]
type ProgressHook = Box<dyn FnMut(usize) -> bool>;
#[cfg(test)]
thread_local! {
    pub(crate) static FILL_PROGRESS: std::cell::RefCell<Option<ProgressHook>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn progress(position: usize) {
    FILL_PROGRESS.with(|slot| {
        let Some(mut hook) = slot.borrow_mut().take() else {
            return;
        };
        if !hook(position) {
            *slot.borrow_mut() = Some(hook);
        }
    });
}

pub(in crate::local_primitives) struct Spec {
    pub(in crate::local_primitives) method: Method,
    pub(in crate::local_primitives) time: usize,
    pub(in crate::local_primitives) value: usize,
    pub(in crate::local_primitives) descending: bool,
}

#[derive(Clone, Copy)]
struct Endpoint {
    position: usize,
    time: i64,
    value: f64,
}

pub(in crate::local_primitives) fn run(
    spec: &Spec,
    rows: usize,
    input: &mut impl Input,
    context: &NativeExecutionContext<'_>,
    mut emit: impl FnMut(usize, Value) -> Result<()>,
) -> Result<()> {
    let mut previous_time = None;
    let mut left = None;
    let mut pending = 0;
    for position in 0..rows {
        #[cfg(test)]
        progress(position);
        context.check_cancelled()?;
        let time = ticks(&input.raw_cell(position, spec.time, context)?)?;
        if previous_time.is_some_and(|previous| {
            if spec.descending {
                time >= previous
            } else {
                time <= previous
            }
        }) {
            return Err(failed(
                "temporal filling requires unique non-NULL times within each partition; aggregate duplicate times first",
            ));
        }
        previous_time = Some(time);
        if input.key_is_null(position, spec.value, context)? {
            continue;
        }
        let right = Endpoint {
            position,
            time,
            value: if spec.method == Method::Linear {
                number(&input.raw_cell(position, spec.value, context)?)?
            } else {
                0.0
            },
        };
        for missing in pending..position {
            context.check_cancelled()?;
            emit(
                missing,
                gap(spec, left, Some(right), missing, input, context)?,
            )?;
        }
        emit(
            position,
            if spec.method == Method::Linear {
                Value::Float(Some(right.value))
            } else {
                Value::Source(Some(position))
            },
        )?;
        pending = position + 1;
        left = Some(right);
    }
    for missing in pending..rows {
        context.check_cancelled()?;
        emit(missing, gap(spec, left, None, missing, input, context)?)?;
    }
    Ok(())
}

fn gap(
    spec: &Spec,
    left: Option<Endpoint>,
    right: Option<Endpoint>,
    position: usize,
    input: &mut impl Input,
    context: &NativeExecutionContext<'_>,
) -> Result<Value> {
    Ok(match spec.method {
        Method::Forward => Value::Source(left.map(|value| value.position)),
        Method::Backward => Value::Source(right.map(|value| value.position)),
        Method::Linear => Value::Float(match left.zip(right) {
            None => None,
            Some((left, right)) => Some(interpolate(
                left,
                right,
                ticks(&input.raw_cell(position, spec.time, context)?)?,
            )?),
        }),
    })
}

fn ticks(value: &Cell) -> Result<i64> {
    match value {
        Cell::Date(value) => Ok(i64::from(*value)),
        Cell::Timestamp(value) | Cell::Temporal(value) => Ok(*value),
        Cell::Null => Err(failed("temporal filling requires non-NULL ordering times")),
        _ => Err(failed("temporal filling lost its bound time domain")),
    }
}

fn number(value: &Cell) -> Result<f64> {
    let value = if let Cell::Decimal(coefficient, dtype) = value {
        if !(-9_007_199_254_740_992..=9_007_199_254_740_992).contains(coefficient) {
            return Err(failed(
                "linear interpolation requires decimal coefficients within +/-2^53; cast to Float64 explicitly to authorize rounding",
            ));
        }
        #[allow(clippy::cast_precision_loss)]
        // Coefficient conversion is exact; declared Float64 scaling is rounded.
        let coefficient = *coefficient as f64;
        coefficient / 10_f64.powi(i32::from(dtype.scale()))
    } else {
        native_relational_expression::float(value)?
    };
    if !value.is_finite() {
        return Err(failed(
            "linear interpolation requires finite numeric values",
        ));
    }
    Ok(value)
}

#[allow(clippy::cast_precision_loss)] // Temporal subtraction is exact before the explicitly Float64 weight.
fn interpolate(left: Endpoint, right: Endpoint, time: i64) -> Result<f64> {
    let distance = i128::from(right.time) - i128::from(left.time);
    let offset = i128::from(time) - i128::from(left.time);
    if distance == 0 || offset.signum() != distance.signum() || offset.abs() >= distance.abs() {
        return Err(failed(
            "linear interpolation point must lie strictly between its endpoints",
        ));
    }
    let weight = offset as f64 / distance as f64;
    // Opposite-sign finite endpoints may have an infinite difference. Same-sign
    // endpoints use the nearer endpoint so interpolation remains in their hull.
    let value = if left.value.signum() != right.value.signum() {
        left.value * (1.0 - weight) + right.value * weight
    } else if weight <= 0.5 {
        left.value + (right.value - left.value) * weight
    } else {
        right.value + (left.value - right.value) * (1.0 - weight)
    };
    if !value.is_finite() {
        return Err(failed("linear interpolation produced a nonfinite value"));
    }
    Ok(value)
}
