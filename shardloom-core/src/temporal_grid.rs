//! Anchored grids in the existing metadata-bound temporal kernel.

use super::{
    Ambiguity, CivilDateTime, Kernel, Kind, NANOS_PER_DAY, Operation, Result, TemporalDType,
    TemporalGridStep as Step, Unit, civil, failed, narrow,
};

pub(super) fn validate(dtype: &TemporalDType, step: Step, origin: Option<i64>) -> Result<()> {
    match step {
        Step::Duration { value, unit } => {
            fixed_ticks(dtype, value, unit)?;
        }
        Step::CalendarDays { value, .. } | Step::CalendarMonths { value, .. } => {
            if value == 0 || i64::try_from(value).is_err() {
                return Err(failed(
                    "calendar grid spacing requires a positive signed-64-bit count",
                ));
            }
            if !matches!(
                dtype.kind(),
                Kind::Date32 | Kind::Date64 | Kind::Timestamp { .. }
            ) {
                return Err(failed("calendar grids require dates or timestamps"));
            }
        }
    }
    if let Some(origin) = origin {
        dtype.validate_value(origin)?;
    }
    Ok(())
}

fn fixed_ticks(dtype: &TemporalDType, value: u64, unit: Unit) -> Result<i128> {
    if value == 0 {
        return Err(failed("temporal grid spacing must be positive"));
    }
    let nanos = i128::from(value) * unit.nanoseconds();
    let ticks = match dtype.kind() {
        Kind::Date32 | Kind::Date64 => {
            if nanos % NANOS_PER_DAY != 0 {
                return Err(failed("date grid spacing requires whole elapsed days"));
            }
            if matches!(dtype.kind(), Kind::Date32) {
                return Ok(nanos / NANOS_PER_DAY);
            }
            Unit::Milliseconds.nanoseconds()
        }
        Kind::Time(unit) | Kind::Duration(unit) | Kind::Timestamp { unit, .. } => {
            unit.nanoseconds()
        }
    };
    if nanos % ticks != 0 {
        return Err(failed(
            "grid spacing exceeds the source precision; cast to a finer temporal unit explicitly",
        ));
    }
    Ok(nanos / ticks)
}

impl Kernel {
    pub(super) fn grid_origin(&self) -> Result<i64> {
        let Operation::Bucket { step, origin } = self.operation else {
            return Err(failed(
                "temporal grid methods require a bound bucket operation",
            ));
        };
        let origin = if let Some(origin) = origin {
            origin
        } else {
            match step {
                Step::Duration { .. } => 0,
                Step::CalendarDays { ambiguity, .. } | Step::CalendarMonths { ambiguity, .. } => {
                    self.civil_ticks(CivilDateTime::from_ticks(0, Unit::Seconds), ambiguity)?
                }
            }
        };
        self.source.validate_value(origin)?;
        if !matches!(step, Step::Duration { .. }) {
            self.civil(origin)?;
        }
        Ok(origin)
    }

    fn grid(&self) -> Result<(Step, i64)> {
        match self.operation {
            Operation::Bucket {
                step,
                origin: Some(origin),
            } => Ok((step, origin)),
            _ => Err(failed(
                "temporal grid methods require a bound bucket operation",
            )),
        }
    }

    /// Return the exact index of the half-open bucket containing `value`.
    ///
    /// # Errors
    /// Rejects invalid domains, calendar policies/ranges and non-advancing grids.
    pub fn bucket_index(&self, value: i64) -> Result<i128> {
        self.source.validate_value(value)?;
        let (step, origin) = self.grid()?;
        if let Step::Duration { value: width, unit } = step {
            return Ok(
                (i128::from(value) - i128::from(origin)).div_euclid(fixed_ticks(
                    &self.source,
                    width,
                    unit,
                )?),
            );
        }
        let value_civil = self.civil(value)?;
        let anchor = self.civil(origin)?;
        let (difference, width) = match step {
            Step::CalendarDays { value, .. } => (
                civil::days_from_civil(value_civil.year, value_civil.month, value_civil.day)
                    - civil::days_from_civil(anchor.year, anchor.month, anchor.day),
                value,
            ),
            Step::CalendarMonths { value, .. } => (
                (i128::from(value_civil.year) - i128::from(anchor.year)) * 12
                    + i128::from(value_civil.month)
                    - i128::from(anchor.month),
                value,
            ),
            Step::Duration { .. } => unreachable!("fixed grid handled above"),
        };
        let mut index = difference.div_euclid(i128::from(width));
        let mut lower = self.grid_point_wide(index)?;
        let mut upper = self.grid_point_wide(index + 1)?;
        advancing(lower, upper)?;
        let value = i128::from(value);
        // Civil-day/month arithmetic gets the nearby interval. Resolve its
        // instant endpoints explicitly: clock-of-day and gap/fold resolution
        // can put the value in a neighboring interval. Each move proves strict
        // progress; invalid or collapsed boundaries fail instead of spinning.
        while value < lower {
            index -= 1;
            upper = lower;
            lower = self.grid_point_wide(index)?;
            advancing(lower, upper)?;
        }
        while value >= upper {
            index += 1;
            lower = upper;
            upper = self.grid_point_wide(index + 1)?;
            advancing(lower, upper)?;
        }
        advancing(self.grid_point_wide(index - 1)?, lower)?;
        Ok(index)
    }

    /// Materialize one grid point in the exact original temporal domain.
    /// Calendar points always shift the original anchor, never a prior clamp.
    ///
    /// # Errors
    /// Rejects overflow, invalid calendar policies/ranges and collapsed neighbors.
    pub fn grid_point(&self, index: i128) -> Result<i64> {
        let (step, _) = self.grid()?;
        let point = self.grid_point_wide(index)?;
        if !matches!(step, Step::Duration { .. }) {
            let prior = index
                .checked_sub(1)
                .ok_or_else(|| failed("grid index overflow"))?;
            let next = index
                .checked_add(1)
                .ok_or_else(|| failed("grid index overflow"))?;
            advancing(self.grid_point_wide(prior)?, point)?;
            advancing(point, self.grid_point_wide(next)?)?;
        }
        let point = narrow(point)?;
        self.source.validate_value(point)?;
        Ok(point)
    }

    fn grid_point_wide(&self, index: i128) -> Result<i128> {
        let (step, origin) = self.grid()?;
        if let Step::Duration { value, unit } = step {
            return index
                .checked_mul(fixed_ticks(&self.source, value, unit)?)
                .and_then(|offset| i128::from(origin).checked_add(offset))
                .ok_or_else(|| failed("temporal grid arithmetic overflow"));
        }
        if index == 0 {
            return Ok(i128::from(origin));
        }
        let (width, ambiguity, months) = match step {
            Step::CalendarDays { value, ambiguity } => (value, ambiguity, None),
            Step::CalendarMonths {
                value,
                ambiguity,
                month_end,
            } => (value, ambiguity, Some(month_end)),
            Step::Duration { .. } => unreachable!("fixed grid handled above"),
        };
        let offset = narrow(
            index
                .checked_mul(i128::from(width))
                .ok_or_else(|| failed("calendar grid arithmetic overflow"))?,
        )?;
        let anchor = self.civil(origin)?;
        let point = match months {
            Some(policy) => anchor.add_months(offset, policy)?,
            None => anchor.add_days(offset)?,
        };
        self.grid_civil_ticks(point, ambiguity)
    }

    fn grid_civil_ticks(&self, point: CivilDateTime, ambiguity: Ambiguity) -> Result<i128> {
        let (nanos, tick) = match self.source.kind() {
            Kind::Date32 => return Ok(civil::days_from_civil(point.year, point.month, point.day)),
            Kind::Date64 => (point.epoch_nanoseconds()?, Unit::Milliseconds.nanoseconds()),
            Kind::Timestamp {
                unit,
                timezone: Some(_),
            } => (
                self.zone
                    .as_ref()
                    .ok_or_else(|| failed("calendar timezone rules absent"))?
                    .timestamp_nanoseconds(point, ambiguity)?,
                unit.nanoseconds(),
            ),
            Kind::Timestamp { unit, .. } => (point.epoch_nanoseconds()?, unit.nanoseconds()),
            _ => return Err(failed("calendar grids require dates or timestamps")),
        };
        if nanos % tick != 0 {
            return Err(failed("calendar grid boundary exceeds source precision"));
        }
        Ok(nanos / tick)
    }
}

fn advancing(left: i128, right: i128) -> Result<()> {
    if left < right {
        Ok(())
    } else {
        Err(failed(
            "calendar grid boundaries must advance strictly; the declared policy collapses or reverses adjacent points",
        ))
    }
}

#[cfg(test)]
#[path = "temporal_grid_tests.rs"]
mod tests;
