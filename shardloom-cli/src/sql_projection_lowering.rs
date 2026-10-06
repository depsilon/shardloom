//! Ordered SQL projection lowering into the shared native expression IR.

use super::{
    ColumnRef, ExprId, Expression, ExpressionKind, LogicalDType, ParsedBinaryByteLengthProjection,
    ParsedBinaryHelperProjection, ParsedCastProjection, ParsedComplexProjection,
    ParsedComplexProjectionKind, ParsedConditionalProjection, ParsedDateArithmeticProjection,
    ParsedDateExtractProjection, ParsedGenericExpressionProjection, ParsedLiteralProjection,
    ParsedNullCoalesceProjection, ParsedNullIfProjection, ParsedNumericAbsProjection,
    ParsedNumericArithmeticProjection, ParsedNumericRoundingProjection, ParsedPredicateProjection,
    ParsedProjectionOutput, ParsedSqlLocalSource, ParsedStringFunctionProjection,
    ParsedStringLengthProjection, ParsedStringTransformProjection,
    ParsedTimestampArithmeticProjection, ParsedTimestampExtractProjection, ParsedWindowProjection,
    ScalarValue, ShardLoomError, unsupported_sql_error,
};

pub(super) trait ProjectionAlias {
    fn alias(&self) -> &str;
}

macro_rules! impl_projection_alias {
    ($($projection:ty),+ $(,)?) => {
        $(
            impl ProjectionAlias for $projection {
                fn alias(&self) -> &str {
                    &self.alias
                }
            }
        )+
    };
}

impl_projection_alias!(
    ParsedLiteralProjection,
    ParsedComplexProjection,
    ParsedCastProjection,
    ParsedNullCoalesceProjection,
    ParsedNullIfProjection,
    ParsedConditionalProjection,
    ParsedPredicateProjection,
    ParsedNumericArithmeticProjection,
    ParsedNumericAbsProjection,
    ParsedNumericRoundingProjection,
    ParsedGenericExpressionProjection,
    ParsedDateArithmeticProjection,
    ParsedTimestampArithmeticProjection,
    ParsedStringLengthProjection,
    ParsedStringTransformProjection,
    ParsedStringFunctionProjection,
    ParsedBinaryHelperProjection,
    ParsedBinaryByteLengthProjection,
    ParsedDateExtractProjection,
    ParsedTimestampExtractProjection,
    ParsedWindowProjection,
);

#[allow(clippy::too_many_lines)]
pub(super) fn append_ordered_projection_expression(
    expressions: &mut Vec<Expression>,
    parsed: &ParsedSqlLocalSource,
    output: &ParsedProjectionOutput,
    header: &[String],
    raw_expr_prefix: &str,
) -> Result<(), ShardLoomError> {
    match output {
        ParsedProjectionOutput::Raw(column) if column == "*" => {
            if header.is_empty() {
                return Err(unsupported_sql_error(
                    "SELECT * is not admitted for scoped join projections",
                ));
            }
            for column in header {
                expressions.push(Expression::column(
                    ExprId::new(format!("{raw_expr_prefix}.{column}"))?,
                    ColumnRef::new(column.clone())?,
                ));
            }
        }
        ParsedProjectionOutput::Raw(column) | ParsedProjectionOutput::Aggregate(column) => {
            expressions.push(Expression::column(
                ExprId::new(format!("{raw_expr_prefix}.{column}"))?,
                ColumnRef::new(column.clone())?,
            ));
        }
        ParsedProjectionOutput::Literal(alias) => expressions.push(literal_projection_expression(
            find_projection_by_alias(&parsed.literal_projections, alias, "literal projection")?,
        )?),
        ParsedProjectionOutput::Complex(alias) => expressions.push(complex_projection_expression(
            find_projection_by_alias(&parsed.complex_projections, alias, "complex projection")?,
        )?),
        ParsedProjectionOutput::Cast(alias) => expressions.push(cast_projection_expression(
            find_projection_by_alias(&parsed.cast_projections, alias, "cast projection")?,
        )?),
        ParsedProjectionOutput::NullCoalesce(alias) => expressions.push(
            null_coalesce_projection_expression(find_projection_by_alias(
                &parsed.null_coalesce_projections,
                alias,
                "null coalesce projection",
            )?)?,
        ),
        ParsedProjectionOutput::NullIf(alias) => expressions.push(nullif_projection_expression(
            find_projection_by_alias(&parsed.nullif_projections, alias, "nullif projection")?,
        )?),
        ParsedProjectionOutput::Conditional(alias) => expressions.push(
            conditional_projection_expression(find_projection_by_alias(
                &parsed.conditional_projections,
                alias,
                "conditional projection",
            )?)?,
        ),
        ParsedProjectionOutput::Predicate(alias) => {
            expressions.push(predicate_projection_expression(find_projection_by_alias(
                &parsed.predicate_projections,
                alias,
                "predicate projection",
            )?)?);
        }
        ParsedProjectionOutput::NumericArithmetic(alias) => expressions.push(
            numeric_arithmetic_projection_expression(find_projection_by_alias(
                &parsed.numeric_arithmetic_projections,
                alias,
                "numeric arithmetic projection",
            )?)?,
        ),
        ParsedProjectionOutput::NumericAbs(alias) => expressions.push(
            numeric_abs_projection_expression(find_projection_by_alias(
                &parsed.numeric_abs_projections,
                alias,
                "numeric abs projection",
            )?)?,
        ),
        ParsedProjectionOutput::NumericRounding(alias) => expressions.push(
            numeric_rounding_projection_expression(find_projection_by_alias(
                &parsed.numeric_rounding_projections,
                alias,
                "numeric rounding projection",
            )?)?,
        ),
        ParsedProjectionOutput::GenericExpression(alias) => expressions.push(
            generic_expression_projection_expression(find_projection_by_alias(
                &parsed.generic_expression_projections,
                alias,
                "generic expression projection",
            )?)?,
        ),
        ParsedProjectionOutput::DateArithmetic(alias) => expressions.push(
            date_arithmetic_projection_expression(find_projection_by_alias(
                &parsed.date_arithmetic_projections,
                alias,
                "date arithmetic projection",
            )?)?,
        ),
        ParsedProjectionOutput::TimestampArithmetic(alias) => expressions.push(
            timestamp_arithmetic_projection_expression(find_projection_by_alias(
                &parsed.timestamp_arithmetic_projections,
                alias,
                "timestamp arithmetic projection",
            )?)?,
        ),
        ParsedProjectionOutput::StringLength(alias) => expressions.push(
            string_length_projection_expression(find_projection_by_alias(
                &parsed.string_length_projections,
                alias,
                "string length projection",
            )?)?,
        ),
        ParsedProjectionOutput::StringTransform(alias) => expressions.push(
            string_transform_projection_expression(find_projection_by_alias(
                &parsed.string_transform_projections,
                alias,
                "string transform projection",
            )?)?,
        ),
        ParsedProjectionOutput::StringFunction(alias) => expressions.push(
            string_function_projection_expression(find_projection_by_alias(
                &parsed.string_function_projections,
                alias,
                "string function projection",
            )?)?,
        ),
        ParsedProjectionOutput::BinaryHelper(alias) => expressions.push(
            binary_helper_projection_expression(find_projection_by_alias(
                &parsed.binary_helper_projections,
                alias,
                "binary helper projection",
            )?)?,
        ),
        ParsedProjectionOutput::BinaryByteLength(alias) => expressions.push(
            binary_byte_length_projection_expression(find_projection_by_alias(
                &parsed.binary_byte_length_projections,
                alias,
                "binary byte length projection",
            )?)?,
        ),
        ParsedProjectionOutput::DateExtract(alias) => expressions.push(
            date_extract_projection_expression(find_projection_by_alias(
                &parsed.date_extract_projections,
                alias,
                "date extract projection",
            )?)?,
        ),
        ParsedProjectionOutput::TimestampExtract(alias) => expressions.push(
            timestamp_extract_projection_expression(find_projection_by_alias(
                &parsed.timestamp_extract_projections,
                alias,
                "timestamp extract projection",
            )?)?,
        ),
        ParsedProjectionOutput::Window(_) => {
            return Err(unsupported_sql_error(
                "window projections require the row-set window evaluator",
            ));
        }
    }
    Ok(())
}

pub(super) fn find_projection_by_alias<'a, T: ProjectionAlias>(
    projections: &'a [T],
    alias: &str,
    family: &str,
) -> Result<&'a T, ShardLoomError> {
    projections
        .iter()
        .find(|projection| projection.alias() == alias)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "projection order references missing {family} alias {alias:?}"
            ))
        })
}

fn generic_expression_projection_expression(
    projection: &ParsedGenericExpressionProjection,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(projection.expression.plain()?),
            alias: projection.alias.clone(),
        },
    ))
}

fn cast_projection_expression(
    projection: &ParsedCastProjection,
) -> Result<Expression, ShardLoomError> {
    let cast = projection.mode.build_expression(
        ExprId::new(format!("project.cast.{}", projection.alias))?,
        projection.expression.clone(),
        projection.target_dtype.clone(),
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(cast),
            alias: projection.alias.clone(),
        },
    ))
}

fn date_arithmetic_projection_expression(
    projection: &ParsedDateArithmeticProjection,
) -> Result<Expression, ShardLoomError> {
    let arithmetic = Expression::new(
        ExprId::new(format!("project.date_arithmetic.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![
                Expression::column(
                    ExprId::new(format!("project.{}", projection.column))?,
                    ColumnRef::new(projection.column.clone())?,
                ),
                Expression::literal(
                    ExprId::new(format!("project.date_arithmetic.days.{}", projection.alias))?,
                    ScalarValue::Int64(i64::from(projection.day_count)),
                ),
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(arithmetic),
            alias: projection.alias.clone(),
        },
    ))
}

fn timestamp_arithmetic_projection_expression(
    projection: &ParsedTimestampArithmeticProjection,
) -> Result<Expression, ShardLoomError> {
    let arithmetic = Expression::new(
        ExprId::new(format!("project.timestamp_arithmetic.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![
                Expression::column(
                    ExprId::new(format!("project.{}", projection.column))?,
                    ColumnRef::new(projection.column.clone())?,
                ),
                Expression::literal(
                    ExprId::new(format!(
                        "project.timestamp_arithmetic.seconds.{}",
                        projection.alias
                    ))?,
                    ScalarValue::Int64(projection.second_count),
                ),
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(arithmetic),
            alias: projection.alias.clone(),
        },
    ))
}

fn null_coalesce_projection_expression(
    projection: &ParsedNullCoalesceProjection,
) -> Result<Expression, ShardLoomError> {
    let coalesce = Expression::new(
        ExprId::new(format!("project.null_coalesce.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: "coalesce".to_string(),
            args: vec![
                Expression::column(
                    ExprId::new(format!("project.{}", projection.column))?,
                    ColumnRef::new(projection.column.clone())?,
                ),
                Expression::literal(
                    ExprId::new(format!(
                        "project.null_coalesce.literal.{}",
                        projection.alias
                    ))?,
                    projection.fallback.clone(),
                ),
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(coalesce),
            alias: projection.alias.clone(),
        },
    ))
}

fn nullif_projection_expression(
    projection: &ParsedNullIfProjection,
) -> Result<Expression, ShardLoomError> {
    let nullif = Expression::new(
        ExprId::new(format!("project.nullif.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: "nullif".to_string(),
            args: vec![
                Expression::column(
                    ExprId::new(format!("project.{}", projection.column))?,
                    ColumnRef::new(projection.column.clone())?,
                ),
                Expression::literal(
                    ExprId::new(format!("project.nullif.literal.{}", projection.alias))?,
                    projection.sentinel.clone(),
                ),
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(nullif),
            alias: projection.alias.clone(),
        },
    ))
}

fn conditional_projection_expression(
    projection: &ParsedConditionalProjection,
) -> Result<Expression, ShardLoomError> {
    let case_when = Expression::new(
        ExprId::new(format!("project.conditional.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: "case_when".to_string(),
            args: vec![
                projection.predicate.to_expression()?,
                projection.then_branch.to_expression(ExprId::new(format!(
                    "project.conditional.then.{}",
                    projection.alias
                ))?)?,
                projection.else_branch.to_expression(ExprId::new(format!(
                    "project.conditional.else.{}",
                    projection.alias
                ))?)?,
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(case_when),
            alias: projection.alias.clone(),
        },
    ))
}

fn predicate_projection_expression(
    projection: &ParsedPredicateProjection,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(projection.predicate.to_expression()?),
            alias: projection.alias.clone(),
        },
    ))
}

fn numeric_arithmetic_projection_expression(
    projection: &ParsedNumericArithmeticProjection,
) -> Result<Expression, ShardLoomError> {
    let binary = Expression::new(
        ExprId::new(format!("project.numeric_arithmetic.{}", projection.alias))?,
        ExpressionKind::Binary {
            left: Box::new(Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )),
            op: projection.op.binary_op(),
            right: Box::new(Expression::literal(
                ExprId::new(format!(
                    "project.numeric_arithmetic.literal.{}",
                    projection.alias
                ))?,
                projection.rhs.clone(),
            )),
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(binary),
            alias: projection.alias.clone(),
        },
    ))
}

fn numeric_abs_projection_expression(
    projection: &ParsedNumericAbsProjection,
) -> Result<Expression, ShardLoomError> {
    let abs = Expression::new(
        ExprId::new(format!("project.numeric_abs.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: "abs".to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(abs),
            alias: projection.alias.clone(),
        },
    ))
}

fn numeric_rounding_projection_expression(
    projection: &ParsedNumericRoundingProjection,
) -> Result<Expression, ShardLoomError> {
    let rounded = Expression::new(
        ExprId::new(format!("project.numeric_rounding.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(rounded),
            alias: projection.alias.clone(),
        },
    ))
}

fn string_transform_projection_expression(
    projection: &ParsedStringTransformProjection,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(projection.expression.clone()),
            alias: projection.alias.clone(),
        },
    ))
}

fn string_length_projection_expression(
    projection: &ParsedStringLengthProjection,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(projection.expression.clone()),
            alias: projection.alias.clone(),
        },
    ))
}

fn string_function_projection_expression(
    projection: &ParsedStringFunctionProjection,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(projection.expression.clone()),
            alias: projection.alias.clone(),
        },
    ))
}

fn binary_helper_projection_expression(
    projection: &ParsedBinaryHelperProjection,
) -> Result<Expression, ShardLoomError> {
    let decoded = Expression::new(
        ExprId::new(format!("project.binary_helper.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![projection.expression.clone()],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(decoded),
            alias: projection.alias.clone(),
        },
    ))
}

fn binary_byte_length_projection_expression(
    projection: &ParsedBinaryByteLengthProjection,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(projection.expression.clone()),
            alias: projection.alias.clone(),
        },
    ))
}

fn date_extract_projection_expression(
    projection: &ParsedDateExtractProjection,
) -> Result<Expression, ShardLoomError> {
    let extracted = Expression::new(
        ExprId::new(format!("project.date_extract.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(extracted),
            alias: projection.alias.clone(),
        },
    ))
}

fn timestamp_extract_projection_expression(
    projection: &ParsedTimestampExtractProjection,
) -> Result<Expression, ShardLoomError> {
    let extracted = Expression::new(
        ExprId::new(format!("project.timestamp_extract.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(extracted),
            alias: projection.alias.clone(),
        },
    ))
}

fn literal_projection_expression(
    projection: &ParsedLiteralProjection,
) -> Result<Expression, ShardLoomError> {
    let literal = Expression::literal(
        ExprId::new(format!("project.literal.{}", projection.alias))?,
        projection.value.clone(),
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(literal),
            alias: projection.alias.clone(),
        },
    ))
}

fn complex_projection_expression(
    projection: &ParsedComplexProjection,
) -> Result<Expression, ShardLoomError> {
    let expr = match &projection.kind {
        ParsedComplexProjectionKind::ArrayLiteral(values) => Expression::new(
            ExprId::new(format!("project.array.{}", projection.alias))?,
            ExpressionKind::List {
                values: values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        Ok(Expression::literal(
                            ExprId::new(format!(
                                "project.array.{}.element.{index}",
                                projection.alias
                            ))?,
                            value.clone(),
                        ))
                    })
                    .collect::<Result<Vec<_>, ShardLoomError>>()?,
            },
        )
        .with_dtype(LogicalDType::List),
        ParsedComplexProjectionKind::StructColumns(columns) => Expression::new(
            ExprId::new(format!("project.struct.{}", projection.alias))?,
            ExpressionKind::Struct {
                fields: columns
                    .iter()
                    .map(|column| {
                        Ok((
                            column.clone(),
                            Expression::column(
                                ExprId::new(format!(
                                    "project.struct.{}.field.{column}",
                                    projection.alias
                                ))?,
                                ColumnRef::new(column.clone())?,
                            ),
                        ))
                    })
                    .collect::<Result<Vec<_>, ShardLoomError>>()?,
            },
        )
        .with_dtype(LogicalDType::Struct),
    };
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(expr),
            alias: projection.alias.clone(),
        },
    ))
}
