//! Exact binary64 orientation and bounded simple-polygon predicates.

use super::{Accumulator, NativeExecutionContext, Ordering, Result, check, failed};
use crate::local_primitives::native_spatial_input::{Coordinates, List};
type XY = (f64, f64);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Location {
    Outside,
    Inside,
    Boundary,
}

fn orientation(a: XY, b: XY, c: XY) -> Result<Ordering> {
    let mut determinant = Accumulator::new();
    // Expand before subtracting: intermediate differences/products may overflow
    // or underflow Float64 even though their exact sign is well-defined.
    for (left, right, negate) in [
        (a.0, b.1, false),
        (a.0, c.1, true),
        (b.0, c.1, false),
        (b.0, a.1, true),
        (c.0, a.1, false),
        (c.0, b.1, true),
    ] {
        determinant.product(left, right, negate, false)?;
    }
    Ok(determinant.finish().sign())
}
fn in_bounds(point: XY, a: XY, b: XY) -> bool {
    point.0 >= a.0.min(b.0)
        && point.0 <= a.0.max(b.0)
        && point.1 >= a.1.min(b.1)
        && point.1 <= a.1.max(b.1)
}
fn intersect(a: XY, b: XY, c: XY, d: XY) -> Result<bool> {
    if a.0.min(b.0) > c.0.max(d.0)
        || c.0.min(d.0) > a.0.max(b.0)
        || a.1.min(b.1) > c.1.max(d.1)
        || c.1.min(d.1) > a.1.max(b.1)
    {
        return Ok(false);
    }
    let ac = orientation(a, b, c)?;
    let ad = orientation(a, b, d)?;
    let ca = orientation(c, d, a)?;
    let cb = orientation(c, d, b)?;
    if (ac == Ordering::Equal && in_bounds(c, a, b))
        || (ad == Ordering::Equal && in_bounds(d, a, b))
        || (ca == Ordering::Equal && in_bounds(a, c, d))
        || (cb == Ordering::Equal && in_bounds(b, c, d))
    {
        return Ok(true);
    }
    Ok(ac != Ordering::Equal
        && ad != Ordering::Equal
        && ca != Ordering::Equal
        && cb != Ordering::Equal
        && ac != ad
        && ca != cb)
}

fn ring_location(
    vertices: &Coordinates,
    point: XY,
    context: &NativeExecutionContext<'_>,
) -> Result<Location> {
    let mut inside = false;
    for index in 1..vertices.len() {
        check(index, context)?;
        let (a, b) = (vertices.at(index - 1)?, vertices.at(index)?);
        if point.1 < a.1.min(b.1) || point.1 > a.1.max(b.1) {
            continue;
        }
        let side = orientation(a, b, point)?;
        if side == Ordering::Equal && in_bounds(point, a, b) {
            return Ok(Location::Boundary);
        }
        if (a.1 <= point.1 && point.1 < b.1 && side == Ordering::Greater)
            || (b.1 <= point.1 && point.1 < a.1 && side == Ordering::Less)
        {
            inside = !inside;
        }
    }
    Ok(if inside {
        Location::Inside
    } else {
        Location::Outside
    })
}

fn simple_ring(vertices: &Coordinates, context: &NativeExecutionContext<'_>) -> Result<()> {
    let edges = vertices
        .len()
        .checked_sub(1)
        .ok_or_else(|| failed("polygon ring is empty"))?;
    let mut area = Accumulator::new();
    for index in 0..edges {
        check(index, context)?;
        let (a, b) = (vertices.at(index)?, vertices.at(index + 1)?);
        if a == b {
            return Err(failed("polygon predicates do not admit zero-length edges"));
        }
        area.product(a.0, b.1, false, false)?;
        area.product(a.1, b.0, true, false)?;
        for other in index + 1..edges {
            check(other, context)?;
            let (c, d) = (vertices.at(other)?, vertices.at(other + 1)?);
            if other == index + 1 || (index == 0 && other == edges - 1) {
                // Adjacent edges may meet only at their shared endpoint. A
                // straight continuation is valid; backtracking is not.
                let (left, shared, right) = if other == index + 1 {
                    (a, b, d)
                } else {
                    (b, a, c)
                };
                if orientation(left, shared, right)? == Ordering::Equal
                    && (in_bounds(right, left, shared) || in_bounds(left, shared, right))
                {
                    return Err(failed(
                        "polygon predicates do not admit overlapping adjacent edges",
                    ));
                }
            } else if intersect(a, b, c, d)? {
                return Err(failed(
                    "polygon predicates require simple non-self-intersecting rings",
                ));
            }
        }
    }
    if area.finish().sign() == Ordering::Equal {
        return Err(failed("polygon predicates require nonzero-area rings"));
    }
    Ok(())
}

pub(super) fn validate_polygon(rings: &List, context: &NativeExecutionContext<'_>) -> Result<()> {
    for index in 0..rings.len() {
        check(index, context)?;
        let ring = rings.ring(index, context)?;
        simple_ring(&ring, context)?;
        for other in index + 1..rings.len() {
            check(other, context)?;
            let next = rings.ring(other, context)?;
            for a in 1..ring.len() {
                check(a, context)?;
                for b in 1..next.len() {
                    check(b, context)?;
                    if intersect(ring.at(a - 1)?, ring.at(a)?, next.at(b - 1)?, next.at(b)?)? {
                        return Err(failed(
                            "polygon predicates require disjoint ring boundaries",
                        ));
                    }
                }
            }
            let next_in_ring = ring_location(&ring, next.at(0)?, context)?;
            if index == 0 {
                if next_in_ring != Location::Inside {
                    return Err(failed(
                        "polygon hole must lie strictly inside the exterior ring",
                    ));
                }
            } else if next_in_ring != Location::Outside
                || ring_location(&next, ring.at(0)?, context)? != Location::Outside
            {
                return Err(failed("polygon holes must be disjoint and unnested"));
            }
        }
    }
    context.check_cancelled()
}

pub(super) fn polygon_location(
    rings: &List,
    point: XY,
    context: &NativeExecutionContext<'_>,
) -> Result<Location> {
    if rings.len() == 0 {
        return Ok(Location::Outside);
    }
    let exterior = rings.ring(0, context)?;
    let mut bounds = (
        f64::INFINITY,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    );
    for index in 0..exterior.len() {
        check(index, context)?;
        let (x, y) = exterior.at(index)?;
        bounds.0 = bounds.0.min(x);
        bounds.1 = bounds.1.min(y);
        bounds.2 = bounds.2.max(x);
        bounds.3 = bounds.3.max(y);
    }
    if !in_bounds(point, (bounds.0, bounds.1), (bounds.2, bounds.3)) {
        return Ok(Location::Outside);
    }
    let outer = ring_location(&exterior, point, context)?;
    if outer != Location::Inside {
        return Ok(outer);
    }
    for index in 1..rings.len() {
        check(index, context)?;
        match ring_location(&rings.ring(index, context)?, point, context)? {
            Location::Inside => return Ok(Location::Outside),
            Location::Boundary => return Ok(Location::Boundary),
            Location::Outside => {}
        }
    }
    Ok(Location::Inside)
}
