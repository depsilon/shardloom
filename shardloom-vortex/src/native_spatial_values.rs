//! Representation checks shared by native, Arrow and explicit JSON intake.
//!
//! Ring topology is checked by planar predicates, not by payload preservation.

#[cfg(all(feature = "vortex-local-primitives", unix))]
use super::Kind;
use shardloom_core::{Result, ShardLoomError};

pub(crate) fn failed(reason: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native spatial: {reason}; no fallback execution was attempted"
    ))
}

/// Both NaNs encode an empty Point. All other present coordinates are finite.
pub(crate) fn point(x: f64, y: f64, allow_empty: bool) -> Result<bool> {
    if allow_empty && x.is_nan() && y.is_nan() {
        return Ok(true);
    }
    if !x.is_finite() || !y.is_finite() {
        return Err(failed(
            "coordinates must be finite; only a Point with two NaNs represents empty",
        ));
    }
    Ok(false)
}

pub(crate) fn sequence(
    count: usize,
    ring: bool,
    mut coordinate: impl FnMut(usize) -> Result<(f64, f64)>,
) -> Result<()> {
    if (ring && count < 4) || (!ring && count == 1) {
        return Err(failed(
            "LineString requires zero or at least two vertices; polygon rings require at least four",
        ));
    }
    let mut first = None;
    let mut last = None;
    for index in 0..count {
        let current = coordinate(index)?;
        point(current.0, current.1, false)?;
        if index == 0 {
            first = Some(current);
        }
        last = Some(current);
    }
    if ring && first != last {
        return Err(failed("polygon rings must be closed"));
    }
    Ok(())
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) fn validate_json(kind: Kind, value: &serde_json::Value) -> Result<()> {
    fn coordinate(value: &serde_json::Value) -> Result<(f64, f64)> {
        let x = value.get("x").and_then(serde_json::Value::as_f64);
        let y = value.get("y").and_then(serde_json::Value::as_f64);
        x.zip(y)
            .ok_or_else(|| failed("spatial coordinates require nonnull numeric x/y values"))
    }
    let vertices = |value: &serde_json::Value, ring| {
        let values = value
            .as_array()
            .ok_or_else(|| failed("spatial vertices require a list"))?;
        sequence(values.len(), ring, |index| coordinate(&values[index]))
    };
    match kind {
        Kind::Point => {
            let (x, y) = coordinate(value)?;
            point(x, y, true)?;
        }
        Kind::LineString => vertices(value, false)?,
        Kind::Polygon => {
            for ring in value
                .as_array()
                .ok_or_else(|| failed("polygon requires a list of rings"))?
            {
                vertices(ring, true)?;
            }
        }
    }
    Ok(())
}
