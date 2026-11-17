//! Shared spatial function admission for SQL, typed plans and `DataFrame` calls.

use super::{
    Binder, DType, Expression, Function, Input, Kind, Nullability, PType, Result, ScalarDefinition,
    failed,
};
use crate::{
    local_primitives::native_relational_expression::spatial::Kernel,
    native_spatial::{self, Kind as Geometry},
};
use vortex::array::dtype::StructFields;

pub(super) fn bind(
    binder: &mut Binder<'_>,
    definition: &ScalarDefinition,
    inputs: &[Input],
    fields: &[(String, DType)],
    depth: usize,
) -> Result<Expression> {
    binder.charge_fields(inputs.len())?;
    let mut args = Vec::with_capacity(inputs.len());
    for input in inputs {
        let expression = binder.expression(input, fields, depth + 1)?;
        if expression.dtype != DType::Null {
            super::super::validate_payload(&expression.dtype, binder.schema_scope())?;
        }
        args.push(expression);
    }
    let kernel = match definition.name {
        "st_point" => Kernel::Point,
        "st_emptypoint" => Kernel::EmptyPoint,
        "st_x" => Kernel::X,
        "st_y" => Kernel::Y,
        "st_isempty" => Kernel::IsEmpty,
        "st_npoints" => Kernel::NPoints,
        "st_xmin" => Kernel::XMin,
        "st_ymin" => Kernel::YMin,
        "st_xmax" => Kernel::XMax,
        "st_ymax" => Kernel::YMax,
        "st_distance" => Kernel::Distance,
        "st_dwithin" => Kernel::DWithin,
        "st_length" => Kernel::Length,
        "st_contains" => Kernel::Contains,
        "st_covers" => Kernel::Covers,
        _ => return Err(failed("unknown spatial function")),
    };
    let nullable = Nullability::from(
        args.iter()
            .any(|arg| arg.dtype.is_nullable() || arg.dtype == DType::Null),
    );
    let dtype = if matches!(kernel, Kernel::Point | Kernel::EmptyPoint) {
        for arg in &args {
            numeric(&arg.dtype)?;
        }
        let storage = DType::Struct(
            StructFields::new(
                ["x", "y"].into(),
                vec![DType::Primitive(PType::F64, Nullability::NonNullable); 2],
            ),
            nullable,
        );
        native_spatial::dtype(
            native_spatial::Metadata::parse(Geometry::Point, "{}")
                .map_err(super::super::vortex_error)?,
            storage,
        )
        .map_err(super::super::vortex_error)?
    } else {
        let first = metadata(&args[0].dtype)?;
        let required = match kernel {
            Kernel::X | Kernel::Y | Kernel::Distance | Kernel::DWithin => Some(Geometry::Point),
            Kernel::Length => Some(Geometry::LineString),
            Kernel::Contains | Kernel::Covers => Some(Geometry::Polygon),
            _ => None,
        };
        if required.is_some_and(|kind| first.kind() != kind) {
            return Err(failed("spatial function does not admit this geometry kind"));
        }
        if !matches!(
            kernel,
            Kernel::X | Kernel::Y | Kernel::IsEmpty | Kernel::NPoints
        ) && !first.is_planar()
        {
            return Err(failed(
                "spatial operation requires planar edges; geographic computation and reprojection are not admitted",
            ));
        }
        if matches!(
            kernel,
            Kernel::Distance | Kernel::DWithin | Kernel::Contains | Kernel::Covers
        ) {
            let second = metadata(&args[1].dtype)?;
            if second.kind() != Geometry::Point || !first.same_coordinates(second) {
                return Err(failed(
                    "spatial operation requires a Point and exactly matching CRS/edge metadata; no implicit reprojection",
                ));
            }
        }
        if kernel == Kernel::DWithin {
            numeric(&args[2].dtype)?;
        }
        match kernel {
            Kernel::IsEmpty | Kernel::DWithin | Kernel::Contains | Kernel::Covers => {
                DType::Bool(nullable)
            }
            Kernel::NPoints => DType::Primitive(PType::U64, nullable),
            Kernel::Length => DType::Primitive(PType::F64, nullable),
            _ => DType::Primitive(PType::F64, Nullability::Nullable),
        }
    };
    Ok(Expression::new(
        dtype,
        Kind::Function {
            function: Function::Spatial(kernel),
            args,
        },
    ))
}
fn metadata(dtype: &DType) -> Result<&native_spatial::Metadata> {
    native_spatial::metadata(dtype)
        .ok_or_else(|| failed("spatial functions require an admitted 2D GeoArrow geometry dtype"))
}
fn numeric(dtype: &DType) -> Result<()> {
    if matches!(dtype, DType::Null | DType::Primitive(..)) {
        Ok(())
    } else {
        Err(failed(
            "spatial numeric arguments require primitive numeric values",
        ))
    }
}
