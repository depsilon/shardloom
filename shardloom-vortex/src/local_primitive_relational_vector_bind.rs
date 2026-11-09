//! Type, shape and identity admission for native vector expressions.

use super::{
    Binder, DType, Expression, ExpressionKind, Function, Input, Kind, Nullability,
    PREPARATION_BYTES, PType, Result, ScalarDefinition, ScalarTypeRule, failed, literal_text,
};
use crate::{
    local_primitives::native_relational_expression::vector::{Kernel, Reduction, Shape},
    native_embedding, native_tensor, native_variable_tensor, native_vector,
};

pub(super) fn handles(rule: ScalarTypeRule) -> bool {
    matches!(
        rule,
        ScalarTypeRule::VectorDimension
            | ScalarTypeRule::TensorShape
            | ScalarTypeRule::VectorValues
            | ScalarTypeRule::VectorElement
            | ScalarTypeRule::VectorSlice
            | ScalarTypeRule::VectorReduction
            | ScalarTypeRule::VectorNormalize
    )
}

fn arguments(
    binder: &mut Binder<'_>,
    definition: &ScalarDefinition,
    inputs: &[Input],
    fields: &[(String, DType)],
    depth: usize,
) -> Result<(Vec<Expression>, bool)> {
    binder.charge_fields(inputs.len())?;
    let binary = matches!(
        definition.name,
        "vector_dot" | "vector_l2_distance" | "vector_cosine_similarity"
    );
    let mut args = Vec::with_capacity(inputs.len());
    for (index, input) in inputs.iter().enumerate() {
        let dense_position = definition.type_rule != ScalarTypeRule::TensorShape
            && (index == 0 || (binary && index == 1));
        let expression = if dense_position && let ExpressionKind::List { values } = &input.kind {
            if depth + 1 > crate::relational_query::NATIVE_RECURSION_LIMIT {
                return Err(failed("recursive scalar expression exceeds 24 levels"));
            }
            binder.charge(PREPARATION_BYTES)?;
            binder.list_expression(values, fields, depth + 1, true)?
        } else {
            binder.expression(input, fields, depth + 1)?
        };
        // Vector/tensor payload admission is deliberately separate from key
        // admission. These functions do not make embeddings, tensors or sparse
        // vectors orderable/groupable by the ordinary comparison kernels.
        if expression.dtype != DType::Null {
            super::super::validate_payload(&expression.dtype, binder.schema_scope())?;
        }
        args.push(expression);
    }
    Ok((args, binary))
}

pub(super) fn bind(
    binder: &mut Binder<'_>,
    definition: &ScalarDefinition,
    inputs: &[Input],
    fields: &[(String, DType)],
    depth: usize,
) -> Result<Expression> {
    let (mut args, binary) = arguments(binder, definition, inputs, fields, depth)?;
    let (dtype, kernel) = match definition.type_rule {
        ScalarTypeRule::VectorDimension => {
            let metadata = native_vector::metadata(&args[0].dtype).cloned();
            if metadata.is_none() {
                dense(&args[0].dtype)?;
            }
            (
                DType::Primitive(PType::U64, args[0].dtype.nullability()),
                Kernel::Dimension(metadata),
            )
        }
        ScalarTypeRule::TensorShape => {
            let physical = if let Some(layout) = args.get(1) {
                match literal_text(layout, "tensor layout")? {
                    Some("logical") => false,
                    Some("physical") => true,
                    _ => {
                        return Err(failed(
                            "tensor layout requires literal 'logical' or 'physical'",
                        ));
                    }
                }
            } else {
                false
            };
            let shape = shape(binder, &args[0].dtype, physical)?;
            let rank = u32::try_from(shape.rank())
                .map_err(|_| failed("tensor rank exceeds native fixed-list capacity"))?;
            args.truncate(1);
            (
                DType::FixedSizeList(
                    DType::Primitive(PType::U64, Nullability::NonNullable).into(),
                    rank,
                    args[0].dtype.nullability(),
                ),
                Kernel::Shape(shape),
            )
        }
        ScalarTypeRule::VectorValues => (dense(&args[0].dtype)?.clone(), Kernel::Values),
        ScalarTypeRule::VectorElement => {
            integer_control(&args[1].dtype)?;
            (
                component(dense(&args[0].dtype)?).as_nullable(),
                Kernel::Element,
            )
        }
        ScalarTypeRule::VectorSlice => {
            for control in &args[1..] {
                integer_control(&control.dtype)?;
            }
            let nullable = Nullability::from(args.iter().any(|arg| arg.dtype.is_nullable()));
            (
                DType::List(component(dense(&args[0].dtype)?).clone().into(), nullable),
                Kernel::Slice,
            )
        }
        ScalarTypeRule::VectorReduction => {
            dense(&args[0].dtype)?;
            let reduction = match definition.name {
                "vector_dot" => Reduction::Dot,
                "vector_norm" => Reduction::Norm,
                "vector_l2_distance" => Reduction::L2,
                "vector_cosine_similarity" => Reduction::Cosine,
                _ => return Err(failed("unknown native vector reduction")),
            };
            if binary {
                pair(binder, &args[0].dtype, &args[1].dtype)?;
            }
            (
                DType::Primitive(PType::F64, Nullability::Nullable),
                Kernel::Reduction(reduction),
            )
        }
        ScalarTypeRule::VectorNormalize => {
            let child = DType::Primitive(PType::F64, Nullability::NonNullable).into();
            let dtype = match dense(&args[0].dtype)? {
                DType::List(_, _) => DType::List(child, Nullability::Nullable),
                DType::FixedSizeList(_, size, _) => {
                    DType::FixedSizeList(child, *size, Nullability::Nullable)
                }
                _ => unreachable!("admitted dense storage"),
            };
            (dtype, Kernel::Normalize)
        }
        _ => return Err(failed("unknown native vector function type rule")),
    };
    Ok(Expression::new(
        dtype,
        Kind::Function {
            function: Function::Vector(Box::new(kernel)),
            args,
        },
    ))
}

fn dense(dtype: &DType) -> Result<&DType> {
    let storage = native_embedding::storage(dtype).unwrap_or(dtype);
    match storage {
        DType::List(child, _) | DType::FixedSizeList(child, _, _)
            if matches!(
                child.as_ref(),
                DType::Primitive(PType::F16 | PType::F32 | PType::F64, _)
            ) =>
        {
            Ok(storage)
        }
        _ => Err(failed(
            "vector function requires a dense Float16/Float32/Float64 list or embedding",
        )),
    }
}

fn component(storage: &DType) -> &DType {
    match storage {
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => child,
        _ => unreachable!("admitted dense storage"),
    }
}

fn integer_control(dtype: &DType) -> Result<()> {
    if *dtype == DType::Null || matches!(dtype, DType::Primitive(ptype, _) if ptype.is_int()) {
        Ok(())
    } else {
        Err(failed(
            "vector index and slice bounds require integer expressions",
        ))
    }
}

fn pair(binder: &mut Binder<'_>, left: &DType, right: &DType) -> Result<()> {
    let (ls, rs) = (dense(left)?, dense(right)?);
    if let (DType::FixedSizeList(_, l, _), DType::FixedSizeList(_, r, _)) = (ls, rs)
        && l != r
    {
        return Err(failed("vector dimensions differ"));
    }
    match (embedding_metadata(left), embedding_metadata(right)) {
        (None, None) => Ok(()),
        (Some(left), Some(right)) => {
            let left = metadata_json(binder, left.as_str())?;
            let right = metadata_json(binder, right.as_str())?;
            let anchor = identity(&left, "space_id");
            if anchor.is_none() || anchor != identity(&right, "space_id") {
                return Err(failed(
                    "embedding comparison requires equal known space_id; use VECTOR_VALUES for value-only arithmetic",
                ));
            }
            for field in [
                "model_id",
                "model_version",
                "representation_version",
                "preprocessing_id",
            ] {
                if identity(&left, field) != identity(&right, field) {
                    return Err(failed(&format!(
                        "embedding {field} differs; use VECTOR_VALUES for value-only arithmetic"
                    )));
                }
            }
            Ok(())
        }
        _ => Err(failed(
            "embedding/list comparison requires explicit VECTOR_VALUES",
        )),
    }
}

fn embedding_metadata(dtype: &DType) -> Option<&native_embedding::Metadata> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    extension.metadata_opt::<native_embedding::Embedding>()
}

fn identity<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(serde_json::Value::as_str)
}

fn metadata_json(binder: &mut Binder<'_>, raw: &str) -> Result<serde_json::Value> {
    binder.charge(
        raw.len()
            .checked_mul(128)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| failed("vector expression metadata size overflow"))?,
    )?;
    serde_json::from_str(raw).map_err(super::super::vortex_error)
}

fn shape(binder: &mut Binder<'_>, dtype: &DType, physical: bool) -> Result<Shape> {
    let DType::Extension(extension) = dtype else {
        return Err(failed("TENSOR_SHAPE requires a fixed or variable tensor"));
    };
    if let Some(metadata) = extension.metadata_opt::<native_tensor::FixedShapeTensor>() {
        let json = metadata_json(binder, metadata.as_str())?;
        let dimensions = json["shape"]
            .as_array()
            .ok_or_else(|| failed("fixed tensor metadata has no shape"))?;
        let order = order(&json, dimensions.len(), physical)?;
        let dimensions = order
            .into_iter()
            .map(|i| {
                dimensions[i]
                    .as_u64()
                    .ok_or_else(|| failed("invalid tensor dimension"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Shape::Fixed(dimensions))
    } else if let Some(metadata) =
        extension.metadata_opt::<native_variable_tensor::VariableShapeTensor>()
    {
        let rank = native_variable_tensor::rank(extension.storage_dtype())
            .map_err(super::super::vortex_error)?;
        metadata
            .validate_rank(rank as usize)
            .map_err(super::super::vortex_error)?;
        let json = metadata_json(binder, metadata.as_str())?;
        Ok(Shape::Variable {
            metadata: metadata.clone(),
            rank,
            order: order(&json, rank as usize, physical)?,
        })
    } else {
        Err(failed("TENSOR_SHAPE requires a fixed or variable tensor"))
    }
}

fn order(json: &serde_json::Value, rank: usize, physical: bool) -> Result<Vec<usize>> {
    if physical || json.get("permutation").is_none() {
        return Ok((0..rank).collect());
    }
    let values = json["permutation"]
        .as_array()
        .filter(|values| values.len() == rank)
        .ok_or_else(|| failed("tensor permutation does not match rank"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|v| usize::try_from(v).ok())
                .filter(|v| *v < rank)
                .ok_or_else(|| failed("tensor permutation index is out of range"))
        })
        .collect()
}
