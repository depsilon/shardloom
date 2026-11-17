//! Bounded planar kernels over native coordinate views and scalar registers.

use super::vector::math::{Accumulator, Magnitude};
use super::{
    ArrayRef, Cell, DType, NativeExecutionContext, Nullability, Result, Value, keys, result_batch,
    vortex_error,
};
use crate::{
    local_primitives::native_spatial_input::{Column, Row, check},
    native_spatial::values::{failed, point},
};
use std::cmp::Ordering;
use vortex::array::{
    IntoArray,
    arrays::{ExtensionArray, StructArray},
    dtype::PType,
    memory::HostMemorySessionExt as _,
    validity::Validity,
};

#[path = "native_spatial_predicates.rs"]
mod predicates;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::local_primitives) enum Kernel {
    Point,
    EmptyPoint,
    X,
    Y,
    IsEmpty,
    NPoints,
    XMin,
    YMin,
    XMax,
    YMax,
    Distance,
    DWithin,
    Length,
    Contains,
    Covers,
}

impl Kernel {
    pub(super) fn evaluate(
        self,
        args: &[ArrayRef],
        rows: usize,
        dtype: &DType,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        context.check_cancelled()?;
        if matches!(self, Self::Point | Self::EmptyPoint) {
            return construct(args, rows, dtype, context);
        }
        let _scratch = context.memory().reserve(
            (4 * (std::mem::size_of::<Accumulator>() + std::mem::size_of::<Magnitude>()) + 1024)
                as u64,
        )?;
        let left = Column::new(&args[0], context)?;
        let right = if matches!(
            self,
            Self::Distance | Self::DWithin | Self::Contains | Self::Covers
        ) {
            Some(Column::new(&args[1], context)?)
        } else {
            None
        };
        let radius = (self == Self::DWithin)
            .then(|| keys(&args[2], context))
            .transpose()?;
        let allocator = crate::owned_buffers::with_credit(
            context.native_session().host_allocator(),
            crate::native_payload_schema::reserve_metadata(dtype, context)?,
        );
        result_batch::build_column_with_float_policy(
            dtype,
            rows,
            &allocator,
            context.float_policy(),
            |index| {
                check(index, context)?;
                let left = left.row(index, context)?;
                left.validate(context)?;
                let right = right
                    .as_ref()
                    .map(|right| right.row(index, context))
                    .transpose()?;
                if let Some(right) = &right {
                    right.validate(context)?;
                }
                if matches!(left, Row::Null) || matches!(right, Some(Row::Null)) {
                    return Ok(Value::Null);
                }
                Ok(match self {
                    Self::X
                    | Self::Y
                    | Self::IsEmpty
                    | Self::NPoints
                    | Self::XMin
                    | Self::YMin
                    | Self::XMax
                    | Self::YMax => self.inspect(&left, context)?,
                    Self::Distance => match left
                        .point()?
                        .zip(right.as_ref().expect("second point").point()?)
                    {
                        Some((a, b)) => Value::Float(distance(a, b)?),
                        None => Value::Null,
                    },
                    Self::DWithin => {
                        let Some(radius) =
                            number(&radius.as_ref().expect("radius").raw_cell(index)?)?
                        else {
                            return Ok(Value::Null);
                        };
                        if !radius.is_finite() || radius < 0.0 {
                            return Err(failed("ST_DWITHIN requires a finite nonnegative radius"));
                        }
                        let within = left
                            .point()?
                            .zip(right.as_ref().expect("second point").point()?)
                            .map(|(a, b)| {
                                let mut square = squared_distance(a, b)?;
                                square.product(radius, radius, true, false)?;
                                Ok(square.finish().sign() != Ordering::Greater)
                            })
                            .transpose()?
                            .unwrap_or(false);
                        Value::Bool(within)
                    }
                    Self::Length => length(&left, context)?,
                    Self::Contains | Self::Covers => containment(
                        &left,
                        right.as_ref().expect("point operand"),
                        self == Self::Covers,
                        context,
                    )?,
                    Self::Point | Self::EmptyPoint => {
                        unreachable!("constructor handled before views")
                    }
                })
            },
        )
    }

    fn inspect(self, row: &Row, context: &NativeExecutionContext<'_>) -> Result<Value<'static>> {
        Ok(match self {
            Self::X | Self::Y => row.point()?.map_or(Value::Null, |(x, y)| {
                Value::Float(if self == Self::X { x } else { y })
            }),
            Self::IsEmpty => Value::Bool(row.is_empty()?.expect("nonnull geometry")),
            Self::NPoints => {
                let mut count = 0_u64;
                row.visit(context, |_, _| {
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| failed("spatial vertex count overflow"))?;
                    Ok(())
                })?;
                Value::UInt(count)
            }
            Self::XMin | Self::YMin | Self::XMax | Self::YMax => {
                let mut bound: Option<f64> = None;
                row.visit(context, |x, y| {
                    let value = if matches!(self, Self::XMin | Self::XMax) {
                        x
                    } else {
                        y
                    };
                    bound = Some(bound.map_or(value, |prior| {
                        if matches!(self, Self::XMin | Self::YMin) {
                            prior.min(value)
                        } else {
                            prior.max(value)
                        }
                    }));
                    Ok(())
                })?;
                bound.map_or(Value::Null, Value::Float)
            }
            _ => unreachable!("coordinate inspection kernel"),
        })
    }
}

fn containment(
    polygon: &Row,
    point: &Row,
    include_boundary: bool,
    context: &NativeExecutionContext<'_>,
) -> Result<Value<'static>> {
    let Row::Polygon(rings) = polygon else {
        return Err(failed("spatial containment requires Polygon"));
    };
    // Validate topology before bounds or empty-point rejection.
    predicates::validate_polygon(rings, context)?;
    let location = if let Some(point) = point.point()? {
        predicates::polygon_location(rings, point, context)?
    } else {
        predicates::Location::Outside
    };
    Ok(Value::Bool(
        location == predicates::Location::Inside
            || (include_boundary && location == predicates::Location::Boundary),
    ))
}

fn length(row: &Row, context: &NativeExecutionContext<'_>) -> Result<Value<'static>> {
    let Row::Line(vertices) = row else {
        return Err(failed("ST_LENGTH requires LineString"));
    };
    let mut sum = Accumulator::new();
    let mut infinite = false;
    for index in 1..vertices.len() {
        check(index, context)?;
        let length = distance(vertices.at(index - 1)?, vertices.at(index)?)?;
        if length.is_infinite() {
            infinite = true;
        } else {
            sum.product(length, 1.0, false, false)?;
        }
    }
    Ok(Value::Float(if infinite {
        f64::INFINITY
    } else {
        sum.finish().rounded()
    }))
}

fn number(value: &Cell) -> Result<Option<f64>> {
    let result = match value {
        Cell::Null => return Ok(None),
        Cell::Float(bits) => f64::from_bits(*bits),
        Cell::NegativeInteger(value) => {
            #[allow(clippy::cast_precision_loss)]
            let float = *value as f64;
            #[allow(clippy::cast_possible_truncation)]
            if float as i128 != i128::from(*value) {
                return Err(failed(
                    "spatial integer coordinate is not exactly representable as Float64",
                ));
            }
            float
        }
        Cell::NonnegativeInteger(value) => {
            #[allow(clippy::cast_precision_loss)]
            let float = *value as f64;
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            if float as u128 != u128::from(*value) {
                return Err(failed(
                    "spatial integer coordinate is not exactly representable as Float64",
                ));
            }
            float
        }
        _ => {
            return Err(failed(
                "spatial coordinate or radius requires primitive numeric input",
            ));
        }
    };
    Ok(Some(result))
}

fn construct(
    args: &[ArrayRef],
    rows: usize,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let x = args.first().map(|arg| keys(arg, context)).transpose()?;
    let y = args.get(1).map(|arg| keys(arg, context)).transpose()?;
    let coordinate = |row| -> Result<Option<(f64, f64)>> {
        check(row, context)?;
        if let (Some(x), Some(y)) = (&x, &y) {
            // The pair mask precedes value conversion and the nonfinite rule.
            if x.is_null(row)? || y.is_null(row)? {
                return Ok(None);
            }
            let x = number(&x.raw_cell(row)?)?.expect("nonnull x");
            let y = number(&y.raw_cell(row)?)?.expect("nonnull y");
            point(x, y, true)?;
            Ok(Some((x, y)))
        } else {
            Ok(Some((f64::NAN, f64::NAN)))
        }
    };
    let allocator = crate::owned_buffers::with_credit(
        context.native_session().host_allocator(),
        crate::native_payload_schema::reserve_metadata(dtype, context)?,
    );
    let values = |axis| {
        result_batch::build_column_with_float_policy(
            &DType::Primitive(PType::F64, Nullability::NonNullable),
            rows,
            &allocator,
            shardloom_core::FloatPolicy::Extended,
            |row| {
                Ok(Value::Float(
                    coordinate(row)?.map_or(0.0, |(x, y)| if axis == 0 { x } else { y }),
                ))
            },
        )
    };
    let validity = if dtype.is_nullable() {
        Validity::Array(result_batch::build_column(
            &DType::Bool(Nullability::NonNullable),
            rows,
            &allocator,
            |row| Ok(Value::Bool(coordinate(row)?.is_some())),
        )?)
    } else {
        Validity::NonNullable
    };
    let storage = StructArray::try_new(
        ["x", "y"].into(),
        vec![values(0)?, values(1)?],
        rows,
        validity,
    )
    .map_err(vortex_error)?
    .into_array();
    let DType::Extension(extension) = dtype else {
        return Err(failed("Point constructor lost its logical dtype"));
    };
    ExtensionArray::try_new(extension.clone(), storage)
        .map(IntoArray::into_array)
        .map_err(vortex_error)
}

fn squared_distance(a: (f64, f64), b: (f64, f64)) -> Result<Accumulator> {
    let mut sum = Accumulator::new();
    for (a, b) in [(a.0, b.0), (a.1, b.1)] {
        sum.product(a, a, false, false)?;
        sum.product(a, b, true, true)?;
        sum.product(b, b, false, false)?;
    }
    Ok(sum)
}
fn distance(a: (f64, f64), b: (f64, f64)) -> Result<f64> {
    squared_distance(a, b)?.finish().sqrt()
}
