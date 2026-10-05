//! Numeric policies for one shared bounded rolling scheduler. Primitive floating
//! order is unchanged; decimal centered totals advance by exact add/remove.

use super::{Result, RollingWindowState, ShardLoomError, StatValue, VortexRollingWindowRequest};

pub(super) trait Observation: Copy {
    type Total: Default;
    type Output;
    const TRACK_CENTERED_TOTAL: bool;
    fn add(total: &mut Self::Total, value: Self) -> Result<()>;
    fn remove(total: &mut Self::Total, value: Self) -> Result<()>;
    fn current(
        state: &RollingWindowState<Self>,
        request: &VortexRollingWindowRequest,
    ) -> Result<Self::Output>;
    fn centered(
        state: &RollingWindowState<Self>,
        start: usize,
        end: usize,
        request: &VortexRollingWindowRequest,
    ) -> Result<Option<Self::Output>>;
}

impl Observation for f64 {
    type Total = f64;
    type Output = StatValue;
    const TRACK_CENTERED_TOTAL: bool = false;

    fn add(total: &mut f64, value: Self) -> Result<()> {
        *total += value;
        if !total.is_finite() {
            return Err(failed("produced a non-finite sum"));
        }
        Ok(())
    }

    fn remove(total: &mut f64, value: Self) -> Result<()> {
        *total -= value;
        Ok(())
    }

    #[allow(clippy::cast_precision_loss)] // Preserve the existing ordered floating policy.
    fn current(
        state: &RollingWindowState<Self>,
        request: &VortexRollingWindowRequest,
    ) -> Result<StatValue> {
        Ok(match request.aggregate.as_str() {
            "sum" => StatValue::Float64(state.sum),
            "mean" => {
                let count = state.current_count();
                if count == 0 {
                    return Err(failed("mean had zero rows in state"));
                }
                let mean = state.sum / count as f64;
                if !mean.is_finite() {
                    return Err(failed("mean produced a non-finite value"));
                }
                StatValue::Float64(mean)
            }
            "count" => StatValue::UInt64(super::usize_to_u64(state.current_count())?),
            "min" => StatValue::Float64(
                state
                    .values
                    .iter()
                    .flatten()
                    .copied()
                    .reduce(f64::min)
                    .ok_or_else(|| failed("min had zero rows in state"))?,
            ),
            "max" => StatValue::Float64(
                state
                    .values
                    .iter()
                    .flatten()
                    .copied()
                    .reduce(f64::max)
                    .ok_or_else(|| failed("max had zero rows in state"))?,
            ),
            _ => return Err(failed("aggregate was not admitted")),
        })
    }

    fn centered(
        state: &RollingWindowState<Self>,
        start: usize,
        end: usize,
        request: &VortexRollingWindowRequest,
    ) -> Result<Option<StatValue>> {
        super::rolling_aggregate_from_window(&state.values, start, end, request)
    }
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex rolling window {message}; no fallback execution was attempted"
    ))
}

#[cfg(unix)]
pub(super) use decimal::Decimal;

#[cfg(unix)]
mod decimal {
    use super::super::native_decimal_reduce;
    use super::{Observation, Result, RollingWindowState, VortexRollingWindowRequest, failed};
    use vortex::array::dtype::DecimalDType;

    #[derive(Clone, Copy)]
    pub(in crate::local_primitives) struct Decimal {
        pub(in crate::local_primitives) value: i128,
        pub(in crate::local_primitives) dtype: DecimalDType,
    }

    #[derive(Default)]
    pub(in crate::local_primitives) struct Total {
        total: native_decimal_reduce::Total,
        dtype: Option<DecimalDType>,
    }

    impl Observation for Decimal {
        type Total = Total;
        type Output = Self;
        const TRACK_CENTERED_TOTAL: bool = true;

        fn add(total: &mut Total, value: Self) -> Result<()> {
            if total.dtype.is_some_and(|dtype| dtype != value.dtype) {
                return Err(failed("decimal input domain changed"));
            }
            total.total.add(value.value, value.dtype)?;
            total.dtype = Some(value.dtype);
            Ok(())
        }

        fn remove(total: &mut Total, value: Self) -> Result<()> {
            if total.dtype != Some(value.dtype) {
                return Err(failed("decimal input domain changed"));
            }
            total.total.remove(value.value, value.dtype)
        }

        fn current(
            state: &RollingWindowState<Self>,
            request: &VortexRollingWindowRequest,
        ) -> Result<Self> {
            match request.aggregate.as_str() {
                "sum" | "mean" => {
                    if state.sum.total.count() != super::super::usize_to_u64(state.current_count())?
                    {
                        return Err(failed("decimal total and window counts differ"));
                    }
                    let source = state
                        .sum
                        .dtype
                        .ok_or_else(|| failed("decimal source dtype absent"))?;
                    let average = request.aggregate == "mean";
                    let value = state
                        .sum
                        .total
                        .finish(source, average)?
                        .ok_or_else(|| failed("decimal output had zero observations"))?;
                    Ok(Self {
                        value,
                        dtype: native_decimal_reduce::output_dtype(source, average)?,
                    })
                }
                "min" | "max" => state
                    .values
                    .iter()
                    .flatten()
                    .copied()
                    .reduce(|left, right| {
                        if (request.aggregate == "min" && right.value < left.value)
                            || (request.aggregate == "max" && right.value > left.value)
                        {
                            right
                        } else {
                            left
                        }
                    })
                    .ok_or_else(|| failed("decimal extremum had zero observations")),
                _ => Err(failed("decimal aggregate was not admitted")),
            }
        }

        fn centered(
            state: &RollingWindowState<Self>,
            start: usize,
            end: usize,
            request: &VortexRollingWindowRequest,
        ) -> Result<Option<Self>> {
            if start != 0 || end.checked_add(1) != Some(state.values.len()) {
                return Err(failed(
                    "decimal centered input must advance one observation at a time",
                ));
            }
            if !state.ready(request.min_periods) {
                return Ok(None);
            }
            Self::current(state, request).map(Some)
        }
    }
}
