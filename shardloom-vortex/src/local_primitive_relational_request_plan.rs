//! Primitive declarations lower into the same native operators as SQL and typed plans.

use super::{DType, Result, VortexQueryPrimitiveRequest as Request, failed};
use crate::{
    VortexQueryPrimitiveKind as Kind,
    query_primitive::VortexSortTiePolicy,
    relational_query::{
        VortexRelationalAggregate as Aggregate, VortexRelationalFilter as Filter,
        VortexRelationalLimit as Limit, VortexRelationalNullOrder as NullOrder,
        VortexRelationalOrderKey as OrderKey, VortexRelationalPlan as Plan,
        VortexRelationalProject as Project, VortexRelationalSort as Sort,
        VortexRelationalUnary as Unary, VortexRelationalWindow as Window,
        VortexRelationalWindowExpression as WindowExpression,
        VortexRelationalWindowFunction as WindowFunction,
    },
};
use shardloom_core::{
    BinaryOp, ColumnRef, ComparisonOp, ExprId, Expression, ExpressionKind as E, LogicalDType,
    ScalarValue,
};
use vortex::array::dtype::PType;

pub(super) fn lower(request: &Request, input: Plan, dtype: &DType) -> Result<Plan> {
    if request.diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.severity,
            shardloom_core::DiagnosticSeverity::Error | shardloom_core::DiagnosticSeverity::Fatal
        ) || diagnostic.fallback.attempted
    }) {
        return Err(failed("primitive declaration has an invalid payload"));
    }
    match request.kind {
        Kind::CountAll | Kind::CountWhere => Ok(Plan::Aggregate(Box::new(Aggregate {
            input,
            group_by: Vec::new(),
            grouping_sets: None,
            measures: vec![crate::VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "count_all".into(),
            )],
        }))),
        Kind::ProjectColumns | Kind::FilterPredicate | Kind::FilterAndProject => {
            Ok(range(input, 0, request.source_order_limit))
        }
        Kind::SimpleAggregate => aggregate(request, input, dtype),
        Kind::SortRows => sort(request, input, dtype),
        #[cfg(feature = "vortex-write")]
        Kind::ExpressionProjectRows if request.structured_projection.is_some() => {
            crate::local_primitives::prepared_dispatch::structured_projection_plan(request, input)
        }
        Kind::DistinctRows
        | Kind::DropDuplicateRows
        | Kind::DuplicateMaskRows
        | Kind::TailRows
        | Kind::SampleRows
        | Kind::ExpressionProjectRows
        | Kind::MeltRows
        | Kind::ExplodeRows
        | Kind::PivotRows
        | Kind::RollingWindowRows => {
            let mut request = request.clone();
            request.source_uri = None;
            request.predicate = None;
            Ok(Plan::Unary(Box::new(Unary { input, request })))
        }
        Kind::Unsupported => Err(failed(
            "native_pipeline_missing_operator: unsupported primitive has no native implementation",
        )),
    }
}

fn expr(kind: E) -> Expression {
    Expression::new(
        ExprId::new("primitive-plan").expect("static identifier"),
        kind,
    )
}

fn column(name: &str) -> Result<Expression> {
    Ok(expr(E::Column(ColumnRef::new(name)?)))
}

fn literal(value: ScalarValue) -> Expression {
    expr(E::Literal(value))
}

fn function(name: &str, args: Vec<Expression>) -> Expression {
    expr(E::FunctionCall {
        name: name.into(),
        args,
    })
}

fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    expr(E::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn compare(left: Expression, op: ComparisonOp, right: Expression) -> Expression {
    expr(E::Compare {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn cast(input: Expression, dtype: LogicalDType) -> Expression {
    expr(E::Cast {
        expr: Box::new(input),
        target_dtype: dtype,
    })
}

fn range(input: Plan, offset: usize, count: Option<usize>) -> Plan {
    if offset == 0 && count.is_none() {
        return input;
    }
    Plan::Limit(Box::new(Limit {
        input,
        offset,
        count: count.unwrap_or(usize::MAX - offset),
    }))
}

fn order(keys: &[crate::VortexAggregateOrderExpr]) -> Result<Vec<OrderKey>> {
    keys.iter()
        .map(|key| {
            Ok(OrderKey {
                column: ColumnRef::new(&key.column)?,
                descending: key.descending,
                nulls: Some(key.nulls.unwrap_or(if key.descending {
                    NullOrder::Last
                } else {
                    NullOrder::First
                })),
            })
        })
        .collect()
}

fn projected_names(request: &Request, dtype: &DType) -> Result<Vec<String>> {
    crate::local_primitives::projected_column_names(dtype, &request.projection, request.kind)
}

fn project(input: Plan, names: &[String]) -> Result<Plan> {
    Ok(Plan::Project(Box::new(Project {
        input,
        expressions: names
            .iter()
            .map(|name| Ok((name.clone(), column(name)?)))
            .collect::<Result<_>>()?,
    })))
}

fn unique_name(names: &mut std::collections::BTreeSet<String>, label: &str) -> String {
    let mut candidate = format!("__primitive_{label}");
    while !names.insert(candidate.clone()) {
        candidate.push('_');
    }
    candidate
}

#[allow(clippy::too_many_lines)] // One lowering preserves grouping, transforms, HAVING and output ranges.
fn aggregate(request: &Request, input: Plan, dtype: &DType) -> Result<Plan> {
    let spec = request
        .simple_aggregate
        .as_ref()
        .ok_or_else(|| failed("aggregate payload is absent"))?;
    let names = projected_names(request, dtype)?;
    let mut used = names
        .iter()
        .cloned()
        .chain(spec.output_columns())
        .collect::<std::collections::BTreeSet<_>>();
    let mut expressions = names
        .iter()
        .map(|name| Ok((name.clone(), column(name)?)))
        .collect::<Result<Vec<_>>>()?;
    let mut groups = spec.group_by.clone();
    let mut outputs = spec
        .group_by
        .iter()
        .map(|name| Ok((name.as_str().to_owned(), column(name.as_str())?)))
        .collect::<Result<Vec<_>>>()?;
    for group in &spec.group_expressions {
        let value = transform(
            dtype,
            &group.column,
            &group.function,
            group.argument_offset,
            &group.extra_columns,
        )?;
        let name = unique_name(&mut used, "group");
        expressions.push((name.clone(), value));
        groups.push(ColumnRef::new(&name)?);
        outputs.push((group.alias.clone(), column(&name)?));
    }
    let mut measures = spec.measures.clone();
    for measure in &mut measures {
        if measure.argument_offset.is_some() || measure.value_transform.is_some() {
            let source = measure
                .column
                .as_ref()
                .ok_or_else(|| failed("transformed measure requires a column"))?;
            let name = unique_name(&mut used, "measure");
            let mut value = transform(
                dtype,
                source,
                measure.value_transform.as_deref().unwrap_or("identity"),
                None,
                &[],
            )?;
            if let Some(offset) = measure.argument_offset {
                if !matches!(
                    measure.function.to_ascii_lowercase().as_str(),
                    "sum" | "avg"
                ) {
                    return Err(failed("primitive argument offsets require sum or avg"));
                }
                // Legacy measure offsets follow the input transform. Native
                // numeric aggregates promote their arguments to Float64.
                value = binary(
                    cast(value, LogicalDType::Float64),
                    BinaryOp::Add,
                    cast(literal(ScalarValue::Int64(offset)), LogicalDType::Float64),
                );
            }
            expressions.push((name.clone(), value));
            measure.column = Some(ColumnRef::new(name)?);
            measure.argument_offset = None;
            measure.value_transform = None;
        }
    }
    let input = if spec.group_expressions.is_empty() && expressions.len() == names.len() {
        input
    } else {
        Plan::Project(Box::new(Project { input, expressions }))
    };
    let mut plan = Plan::Aggregate(Box::new(Aggregate {
        input,
        group_by: groups,
        grouping_sets: None,
        measures,
    }));
    if !spec.group_expressions.is_empty() {
        outputs.extend(
            spec.measures
                .iter()
                .map(|measure| Ok((measure.alias.clone(), column(&measure.alias)?)))
                .collect::<Result<Vec<_>>>()?,
        );
        plan = Plan::Project(Box::new(Project {
            input: plan,
            expressions: outputs,
        }));
    }
    for having in &spec.having {
        let predicate = function(
            "primitive_aggregate_having",
            vec![
                column(&having.column)?,
                literal(ScalarValue::Utf8(having.op.as_str().into())),
                literal(ScalarValue::Utf8(having.value.clone())),
            ],
        );
        plan = Plan::Filter(Box::new(Filter {
            input: plan,
            predicate,
        }));
    }
    if !spec.order_by.is_empty() {
        plan = Plan::Sort(Box::new(Sort {
            input: plan,
            keys: order(&spec.order_by)?,
        }));
    }
    Ok(range(plan, spec.offset, request.source_order_limit))
}

fn transform(
    dtype: &DType,
    source: &ColumnRef,
    name: &str,
    offset: Option<i64>,
    extra: &[ColumnRef],
) -> Result<Expression> {
    use crate::local_primitives::AggregateValueTransform as T;
    let transform = T::from_parts(name, offset)?;
    let value = column(source.as_str())?;
    Ok(match transform {
        T::Identity => value,
        T::Length => {
            if !matches!(
                crate::local_primitives::completed_result::source_field(dtype, source.as_str())?,
                DType::Utf8(_) | DType::Null
            ) {
                return Err(failed("primitive length transform requires UTF8"));
            }
            cast(function("byte_length", vec![value]), LogicalDType::UInt64)
        }
        T::ConstantInt(value) => literal(ScalarValue::Int64(value)),
        T::AddOffset(offset) => numeric_offset(dtype, source, value, offset, false)?,
        T::SignedAddOffset(offset) => numeric_offset(dtype, source, value, offset, true)?,
        T::ExtractMinute => function("extract_minute", vec![value]),
        T::DateTruncMinute => function("date_trunc_minute", vec![value]),
        T::UrlDomain => function("url_domain", vec![value]),
        T::RegexDomain => function("regex_domain", vec![value]),
        T::CaseSearchAdvZeroRefererElseEmpty => {
            let [search, advertising] = extra else {
                return Err(failed("CASE grouping requires its two auxiliary columns"));
            };
            function(
                "case_search_adv_zero_referer_else_empty",
                vec![
                    value,
                    column(search.as_str())?,
                    column(advertising.as_str())?,
                ],
            )
        }
    })
}

fn numeric_offset(
    dtype: &DType,
    source: &ColumnRef,
    value: Expression,
    offset: i64,
    signed: bool,
) -> Result<Expression> {
    let source = crate::local_primitives::completed_result::source_field(dtype, source.as_str())?;
    if signed {
        if !matches!(source, DType::Primitive(ptype, _) if ptype.is_int()) {
            return Err(failed("signed offset requires integer input"));
        }
        // Every UInt64 fits precision 20; addition requires one extra digit.
        let decimal = LogicalDType::Extension("decimal128(20,0)".into());
        return Ok(cast(
            binary(
                cast(value, decimal),
                BinaryOp::Add,
                literal(ScalarValue::Decimal128 {
                    value: i128::from(offset),
                    precision: 19,
                    scale: 0,
                }),
            ),
            LogicalDType::Int64,
        ));
    }
    let DType::Primitive(ptype, _) = source else {
        return Err(failed("additive grouping requires a numeric column"));
    };
    if matches!(ptype, PType::U8 | PType::U16 | PType::U32 | PType::U64) {
        Ok(binary(
            cast(value, LogicalDType::UInt64),
            if offset < 0 {
                BinaryOp::Subtract
            } else {
                BinaryOp::Add
            },
            literal(ScalarValue::UInt64(offset.unsigned_abs())),
        ))
    } else if matches!(ptype, PType::F16 | PType::F32 | PType::F64) {
        Ok(binary(
            cast(value, LogicalDType::Float64),
            BinaryOp::Add,
            cast(literal(ScalarValue::Int64(offset)), LogicalDType::Float64),
        ))
    } else {
        Ok(binary(
            cast(value, LogicalDType::Int64),
            BinaryOp::Add,
            literal(ScalarValue::Int64(offset)),
        ))
    }
}

#[allow(clippy::too_many_lines)] // Keep all tie policies and hidden sort-key retention in one lowering.
fn sort(request: &Request, input: Plan, dtype: &DType) -> Result<Plan> {
    let spec = request
        .sort_rows
        .as_ref()
        .ok_or_else(|| failed("sort payload is absent"))?;
    let names = projected_names(request, dtype)?;
    let mut keys = order(&spec.order_by)?;
    if spec.tie_policy == VortexSortTiePolicy::First {
        return project(
            range(
                Plan::Sort(Box::new(Sort { input, keys })),
                spec.offset,
                request.source_order_limit,
            ),
            &names,
        );
    }
    let mut used = dtype
        .as_struct_fields_opt()
        .ok_or_else(|| failed("sort requires a struct source"))?
        .names()
        .iter()
        .map(ToString::to_string)
        .collect();
    let ordinal = unique_name(&mut used, "ordinal");
    let mut retained = names.clone();
    for key in &keys {
        if !retained.iter().any(|name| name == key.column.as_str()) {
            retained.push(key.column.as_str().to_owned());
        }
    }
    let mut numbering = Request::for_relational_input(
        Kind::ExpressionProjectRows,
        shardloom_plan::ProjectionRequest::Columns(
            retained.iter().map(ColumnRef::new).collect::<Result<_>>()?,
        ),
    );
    numbering.expression_projection = Some(crate::VortexExpressionProjectionRequest::new(vec![
        crate::VortexExpressionRewrite::RowNumber {
            target_column: ColumnRef::new(&ordinal)?,
            start: 0,
        },
    ]));
    let mut plan = Plan::Unary(Box::new(Unary {
        input,
        request: numbering,
    }));
    if spec.tie_policy == VortexSortTiePolicy::Last {
        keys.push(OrderKey {
            column: ColumnRef::new(&ordinal)?,
            descending: true,
            nulls: Some(NullOrder::Last),
        });
        return project(
            range(
                Plan::Sort(Box::new(Sort { input: plan, keys })),
                spec.offset,
                request.source_order_limit,
            ),
            &names,
        );
    }
    if let Some(limit) = request.source_order_limit {
        let rank = unique_name(&mut used, "rank");
        let mut columns = retained
            .iter()
            .map(ColumnRef::new)
            .collect::<Result<Vec<_>>>()?;
        columns.push(ColumnRef::new(&ordinal)?);
        plan = Plan::Window(Box::new(Window {
            input: plan,
            columns,
            expressions: vec![WindowExpression {
                output_column: rank.clone(),
                function: WindowFunction::Rank,
                partition_by: Vec::new(),
                order_by: keys.clone(),
                frame: None,
            }],
        }));
        let cutoff = spec
            .offset
            .checked_add(limit)
            .ok_or_else(|| failed("sort range overflow"))?;
        plan = Plan::Filter(Box::new(Filter {
            input: plan,
            predicate: compare(
                column(&rank)?,
                ComparisonOp::LtEq,
                literal(ScalarValue::UInt64(cutoff as u64)),
            ),
        }));
    }
    keys.push(OrderKey {
        column: ColumnRef::new(ordinal)?,
        descending: false,
        nulls: Some(NullOrder::Last),
    });
    project(
        range(
            Plan::Sort(Box::new(Sort { input: plan, keys })),
            spec.offset,
            None,
        ),
        &names,
    )
}
