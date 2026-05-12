//! Small duration conversion helpers shared by CLI report surfaces.

use std::time::Duration;

pub(crate) fn duration_micros(duration: Duration) -> u64 {
    saturating_u128_to_u64(duration.as_micros())
}

pub(crate) fn micros_to_millis(micros: u64) -> u64 {
    micros / 1_000
}

pub(crate) fn saturating_u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
