//! Semantic lowering into existing file-provider requests. No SQL or query IDs.
//!
//! The ordinary binder validates the complete tree first. This pass recognizes
//! only complete equivalent pipelines; unknown expressions keep that bound tree.

use crate::{
    local_primitives::{
        native_relational_aggregate,
        native_relational_expression::{Expression, Kind as E, scalar::Function},
        prepared_relational::{Node, NodeKind, ScanSource},
    },
    query_primitive::{
        VortexAggregateExpression, VortexAggregateHavingExpr, VortexAggregateOrderExpr,
        VortexQueryPrimitiveKind as Kind, VortexQueryPrimitiveRequest as Request,
        VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest, VortexSortRowsRequest,
        VortexSortTiePolicy,
    },
};
use shardloom_core::{
    BinaryOp, ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue, UnaryOp,
};
use shardloom_plan::ProjectionRequest;
use std::collections::BTreeMap;
use vortex::array::dtype::{DType, PType};

pub(super) struct Lowered {
    pub(super) request: Request,
    /// Names in the provider output, in the original bound result's order.
    pub(super) output: Vec<String>,
}

#[derive(Clone, PartialEq, Eq)]
enum Value {
    Column(String),
    Constant(i64),
    Transform {
        column: String,
        function: &'static str,
        offset: Option<i64>,
        extra: Vec<String>,
    },
}

impl Value {
    fn source_column(&self) -> Option<&str> {
        match self {
            Self::Column(column) | Self::Transform { column, .. } => Some(column),
            Self::Constant(_) => None,
        }
    }

    fn column(&self) -> Option<&str> {
        match self {
            Self::Column(name) => Some(name),
            _ => None,
        }
    }

    fn group(&self, name: &str, anchor: &str) -> Option<VortexAggregateExpression> {
        let (column, function, offset, extra) = match self {
            Self::Column(column) => (column.as_str(), "identity", None, vec![]),
            Self::Constant(value) => (anchor, "constant_int", Some(*value), vec![]),
            Self::Transform {
                column,
                function,
                offset,
                extra,
            } => (column.as_str(), *function, *offset, extra.clone()),
        };
        Some(VortexAggregateExpression {
            alias: name.to_owned(),
            column: ColumnRef::new(column).ok()?,
            extra_columns: extra
                .iter()
                .map(ColumnRef::new)
                .collect::<Result<_, _>>()
                .ok()?,
            function: function.into(),
            argument_offset: offset,
        })
    }

    fn measure(
        &self,
        measure: &VortexSimpleAggregateMeasure,
    ) -> Option<VortexSimpleAggregateMeasure> {
        let mut measure = measure.clone();
        match self {
            Self::Column(column) => measure.column = Some(ColumnRef::new(column).ok()?),
            Self::Transform {
                column,
                function,
                offset,
                extra,
            } if extra.is_empty() => {
                measure.column = Some(ColumnRef::new(column).ok()?);
                if matches!(*function, "add_offset" | "add_signed_offset") {
                    if !matches!(
                        measure.function.to_ascii_lowercase().as_str(),
                        "sum" | "avg"
                    ) {
                        return None;
                    }
                    measure.argument_offset = *offset;
                } else {
                    measure.value_transform = Some((*function).into());
                }
            }
            Self::Constant(_) if measure.function.eq_ignore_ascii_case("count") => {
                measure.column = None;
            }
            _ => return None,
        }
        Some(measure)
    }
}

struct Input {
    uri: DatasetUri,
    columns: BTreeMap<String, Value>,
    predicate: Option<PredicateExpr>,
    // Do not discard an unused computed expression that could fail at runtime.
    evaluated: Vec<Value>,
}

/// The caller reserves declaration scratch before this metadata-only pass and
/// verifies every output dtype against the already-bound relational schema.
pub(super) fn lower(plan: &Node, source: &DType, names: &[String]) -> Option<Lowered> {
    let mut plan = plan;
    let mut output = names.to_vec();
    let mut limit = None;
    let mut order: Option<Vec<VortexAggregateOrderExpr>> = None;
    let mut having: Vec<VortexAggregateHavingExpr> = Vec::new();
    // Projections may select/rename output fields without moving data work.
    // Ranges and ordering have one position each; arbitrary compositions remain
    // on the native relational provider rather than being silently reordered.
    loop {
        match &plan.kind {
            NodeKind::Project { input, expressions }
                if expressions
                    .iter()
                    .all(|expr| matches!(expr.kind, E::Column(_))) =>
            {
                for column in &mut output {
                    let (_, expr) = plan
                        .fields
                        .iter()
                        .zip(expressions)
                        .find(|((name, _), _)| name == column)?;
                    let E::Column(source) = &expr.kind else {
                        return None;
                    };
                    column.clone_from(source);
                }
                if let Some(keys) = &mut order {
                    for key in keys {
                        let (_, expr) = plan
                            .fields
                            .iter()
                            .zip(expressions)
                            .find(|((name, _), _)| name == &key.column)?;
                        let E::Column(source) = &expr.kind else {
                            return None;
                        };
                        key.column.clone_from(source);
                    }
                }
                for predicate in &mut having {
                    let (_, expr) = plan
                        .fields
                        .iter()
                        .zip(expressions)
                        .find(|((name, _), _)| name == &predicate.column)?;
                    let E::Column(source) = &expr.kind else {
                        return None;
                    };
                    predicate.column.clone_from(source);
                }
                plan = input;
            }
            NodeKind::Limit {
                input,
                offset,
                count,
            } if limit.is_none() && order.is_none() && having.is_empty() => {
                if *count == 0 || offset.checked_add(*count).is_none() {
                    return None;
                }
                limit = Some((*count, *offset));
                plan = input;
            }
            NodeKind::Sort { input, spec, .. } if order.is_none() && having.is_empty() => {
                order = Some(
                    spec.keys
                        .iter()
                        .map(|key| VortexAggregateOrderExpr {
                            column: key.column.as_str().to_owned(),
                            descending: key.descending,
                            nulls: key.nulls,
                        })
                        .collect::<Vec<_>>(),
                );
                plan = input;
            }
            NodeKind::Filter { input, predicate } if alias_aggregate(input) => {
                if let Some(predicates) = having_predicates(predicate) {
                    having.extend(predicates);
                    plan = input;
                } else {
                    break;
                }
            }
            _ => break,
        }
    }
    if let NodeKind::Aggregate { input, spec } = &plan.kind {
        return lower_aggregate(input, spec, source, output, limit, order, having);
    }
    if !having.is_empty() {
        return None;
    }
    match order {
        Some(order) => lower_sort(plan, source, output, order, limit?),
        None => lower_projection(plan, source, output, limit),
    }
}

fn lower_projection(
    plan: &Node,
    source: &DType,
    mut output: Vec<String>,
    limit: Option<(usize, usize)>,
) -> Option<Lowered> {
    if limit.is_some_and(|(_, offset)| offset != 0) {
        return None;
    }
    let input = input(plan, source)?;
    if !input.evaluated.is_empty() {
        return None;
    }
    let mut columns = Vec::new();
    for column in &mut output {
        *column = input.columns.get(column)?.column()?.to_owned();
        let column = ColumnRef::new(column.as_str()).ok()?;
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    let projection = ProjectionRequest::Columns(columns);
    let mut request = match input.predicate {
        Some(predicate) => Request::filter_and_project(input.uri, predicate, projection),
        None => Request::project(input.uri, projection),
    };
    request.source_order_limit = limit.map(|(count, _)| count);
    Some(Lowered { request, output })
}

fn alias_aggregate(node: &Node) -> bool {
    match &node.kind {
        NodeKind::Aggregate { .. } => true,
        NodeKind::Project { input, expressions }
            if expressions
                .iter()
                .all(|expr| matches!(expr.kind, E::Column(_))) =>
        {
            alias_aggregate(input)
        }
        NodeKind::Filter { input, .. } => alias_aggregate(input),
        _ => false,
    }
}

fn constant_group_anchor<'a>(
    input: &'a Input,
    aggregate: &native_relational_aggregate::Spec,
    source: &'a DType,
) -> Option<&'a str> {
    // A constant needs row cardinality, not a new payload column. Reuse a
    // source field already required by a real key/measure where possible.
    aggregate
        .groups
        .iter()
        .map(|(name, _)| name)
        .chain(
            aggregate
                .measures
                .iter()
                .filter_map(|measure| measure.column.as_ref()),
        )
        .filter_map(|name| input.columns.get(name))
        .find_map(Value::source_column)
        .or_else(|| {
            source
                .as_struct_fields_opt()?
                .names()
                .iter()
                .next()
                .map(AsRef::as_ref)
        })
}

fn lower_aggregate(
    input_node: &Node,
    aggregate: &native_relational_aggregate::Spec,
    source: &DType,
    mut output: Vec<String>,
    limit: Option<(usize, usize)>,
    order: Option<Vec<VortexAggregateOrderExpr>>,
    having: Vec<VortexAggregateHavingExpr>,
) -> Option<Lowered> {
    if aggregate.measures.is_empty()
        || aggregate.measures.len() > 1024
        || aggregate.groups.len() > 1024
    {
        return None;
    }
    let input = input(input_node, source)?;
    let anchor = constant_group_anchor(&input, aggregate, source)?;
    let mut used = Vec::new();
    let mut groups = Vec::new();
    let mut expressions = Vec::new();
    for (key, _) in &aggregate.groups {
        let value = input.columns.get(key)?;
        used.push(value);
        if value.column() == Some(key.as_str()) {
            groups.push(ColumnRef::new(key).ok()?);
        } else {
            expressions.push(value.group(key.as_str(), anchor)?);
        }
    }
    // Mixed raw/computed keys must preserve the bound declaration's ordering.
    // The primitive SQL route uses these same identity expressions; silently
    // moving all raw keys first changes physical strategy selection.
    if !groups
        .iter()
        .map(ColumnRef::as_str)
        .chain(
            expressions
                .iter()
                .map(|expression| expression.alias.as_str()),
        )
        .eq(aggregate.groups.iter().map(|(name, _)| name.as_str()))
    {
        expressions = aggregate
            .groups
            .iter()
            .map(|(name, _)| input.columns.get(name)?.group(name, anchor))
            .collect::<Option<Vec<_>>>()?;
        groups.clear();
    }
    let mut measures = Vec::new();
    for (index, measure) in aggregate.measures.iter().enumerate() {
        let declaration = VortexSimpleAggregateMeasure::new(
            measure.function.as_str(),
            measure
                .column
                .as_ref()
                .map(ColumnRef::new)
                .transpose()
                .ok()?,
            aggregate
                .fields
                .get(aggregate.groups.len() + index)?
                .0
                .clone(),
        );
        if let Some(column) = &measure.column {
            let value = input.columns.get(column)?;
            used.push(value);
            measures.push(value.measure(&declaration)?);
        } else {
            measures.push(declaration);
        }
    }
    if input.evaluated.iter().any(|value| !used.contains(&value)) {
        return None;
    }
    // COUNT(*) has its own metadata/filtered-count provider. A range, HAVING,
    // or ordering retains the aggregate path so those operators remain real.
    if groups.is_empty()
        && expressions.is_empty()
        && measures.len() == 1
        && measures[0].function.eq_ignore_ascii_case("count")
        && measures[0].column.is_none()
        && limit.is_none()
        && order.is_none()
        && having.is_empty()
    {
        let mut request = Request::count_all(input.uri);
        if let Some(predicate) = input.predicate {
            request.kind = Kind::CountWhere;
            request.predicate = Some(predicate);
        }
        for name in &mut output {
            if name != &measures[0].alias {
                return None;
            }
            *name = "count".into();
        }
        return Some(Lowered { request, output });
    }
    let aggregate = VortexSimpleAggregateRequest {
        group_by: groups,
        group_expressions: expressions,
        measures,
        order_by: order.unwrap_or_default(),
        having,
        offset: limit.map_or(0, |(_, offset)| offset),
        spill: None,
    };
    let mut request = Request::simple_aggregate(input.uri, aggregate);
    request.predicate = input.predicate;
    request.source_order_limit = limit.map(|(count, _)| count);
    Some(Lowered { request, output })
}

fn lower_sort(
    plan: &Node,
    source: &DType,
    mut output: Vec<String>,
    order: Vec<VortexAggregateOrderExpr>,
    (count, offset): (usize, usize),
) -> Option<Lowered> {
    let input = input(plan, source)?;
    if !input.evaluated.is_empty() {
        return None;
    }
    let mut columns = Vec::new();
    for column in &mut output {
        *column = input.columns.get(column)?.column()?.to_owned();
        let column = ColumnRef::new(column.as_str()).ok()?;
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    let mut keys = Vec::new();
    for mut key in order {
        input
            .columns
            .get(&key.column)?
            .column()?
            .clone_into(&mut key.column);
        keys.push(key);
    }
    let mut request =
        Request::for_relational_input(Kind::SortRows, ProjectionRequest::Columns(columns));
    request.source_uri = Some(input.uri);
    request.predicate = input.predicate;
    request.source_order_limit = Some(count);
    request.sort_rows = Some(VortexSortRowsRequest {
        order_by: keys,
        offset,
        tie_policy: VortexSortTiePolicy::First,
        spill: None,
    });
    Some(Lowered { request, output })
}

fn input(plan: &Node, source: &DType) -> Option<Input> {
    match &plan.kind {
        NodeKind::Scan {
            source: ScanSource::File(0),
            source_uri,
            predicate,
            plan: scan,
            ..
        } if scan.source_order_limit.is_none() => Some(Input {
            uri: source_uri.clone(),
            columns: plan
                .fields
                .iter()
                .map(|(name, _)| (name.clone(), Value::Column(name.clone())))
                .collect(),
            predicate: predicate.clone(),
            evaluated: vec![],
        }),
        NodeKind::Project {
            input: child,
            expressions,
        } => {
            let mut input = input(child, source)?;
            let mut columns = BTreeMap::new();
            for ((name, _), expression) in plan.fields.iter().zip(expressions) {
                let value = value(expression, &input.columns, source)?;
                if matches!(value, Value::Transform { .. }) && !input.evaluated.contains(&value) {
                    input.evaluated.push(value.clone());
                }
                if columns.insert(name.clone(), value).is_some() {
                    return None;
                }
            }
            input.columns = columns;
            Some(input)
        }
        NodeKind::Filter {
            input: child,
            predicate,
        } => {
            let mut input = input(child, source)?;
            // A projection may fail even for a row that a later filter rejects.
            if !input.evaluated.is_empty() {
                return None;
            }
            let predicate = checked_predicate(predicate, &input.columns, source)?;
            input.predicate = Some(match input.predicate {
                Some(prior) => PredicateExpr::And(vec![prior, predicate]),
                None => predicate,
            });
            Some(input)
        }
        _ => None,
    }
}

fn value(expr: &Expression, columns: &BTreeMap<String, Value>, source: &DType) -> Option<Value> {
    match &expr.kind {
        E::Column(column) => columns.get(column).cloned(),
        E::Literal(_) => Some(Value::Constant(integer_literal(expr)?)),
        E::Binary(left, op @ (BinaryOp::Add | BinaryOp::Subtract), right) => {
            offset_value(left, *op, right, columns, source)
        }
        E::Function { function, args } => {
            let column = value(&args[0], columns, source)?.column()?.to_owned();
            let function = match function {
                Function::ByteLength => "length",
                Function::EpochMinute { truncate: false } => "extract_minute",
                Function::EpochMinute { truncate: true } => "date_trunc_minute",
                Function::Domain { regex: true } => "regex_domain",
                Function::Domain { regex: false } => "url_domain",
                _ => return None,
            };
            Some(Value::Transform {
                column,
                function,
                offset: None,
                extra: vec![],
            })
        }
        E::Conditional(condition, yes, no) => {
            // Reuse the admitted two-integer-zero conditional provider by
            // structure; operand names are arbitrary. Nullable inputs keep the
            // ordinary CASE evaluator because its branch/null rules differ.
            if !matches!(stat_literal(no), Some(StatValue::Utf8(text)) if text.is_empty()) {
                return None;
            }
            let E::Binary(left, BinaryOp::And, right) = &condition.kind else {
                return None;
            };
            let nonnull = |name: &str| {
                let dtype = source.as_struct_fields_opt()?.field(name)?;
                (!source.is_nullable() && !dtype.is_nullable()).then_some(dtype)
            };
            let zero_column = |expr: &Expression| {
                let E::Compare(left, ComparisonOp::Eq, right) = &expr.kind else {
                    return None;
                };
                if integer_literal(right)? != 0 {
                    return None;
                }
                let name = value(left, columns, source)?.column()?.to_owned();
                match nonnull(&name)? {
                    DType::Primitive(ptype, _)
                        if ptype.is_signed_int() || ptype.is_unsigned_int() =>
                    {
                        Some(name)
                    }
                    _ => None,
                }
            };
            let column = value(yes, columns, source)?.column()?.to_owned();
            if !matches!(nonnull(&column)?, DType::Utf8(_)) {
                return None;
            }
            Some(Value::Transform {
                column,
                function: "case_search_adv_zero_referer_else_empty",
                offset: None,
                extra: vec![zero_column(left)?, zero_column(right)?],
            })
        }
        _ => None,
    }
}

fn offset_value(
    left: &Expression,
    op: BinaryOp,
    right: &Expression,
    columns: &BTreeMap<String, Value>,
    source: &DType,
) -> Option<Value> {
    let column = value(left, columns, source)?.column()?.to_owned();
    let mut offset = integer_literal(right)?;
    if op == BinaryOp::Subtract {
        offset = offset.checked_neg()?;
    }
    let dtype = source.as_struct_fields_opt()?.field(&column)?;
    // Prove the entire source domain remains exact in both typed integer
    // evaluation and the existing aggregate's float argument transform.
    // Wide/overflow-prone arithmetic retains its ordinary evaluator.
    let (min, max) = match dtype {
        DType::Primitive(PType::I8, _) => (i64::from(i8::MIN), i64::from(i8::MAX)),
        DType::Primitive(PType::I16, _) => (i64::from(i16::MIN), i64::from(i16::MAX)),
        DType::Primitive(PType::I32, _) => (i64::from(i32::MIN), i64::from(i32::MAX)),
        DType::Primitive(PType::U8, _) => (0, i64::from(u8::MAX)),
        DType::Primitive(PType::U16, _) => (0, i64::from(u16::MAX)),
        DType::Primitive(PType::U32, _) => (0, i64::from(u32::MAX)),
        _ => return None,
    };
    let exact = 1_i64 << 53;
    if offset < -exact
        || offset > exact
        || min.checked_add(offset)? < -exact
        || max.checked_add(offset)? > exact
    {
        return None;
    }
    // An unsigned literal promotes unsigned input to U64; a signed literal
    // promotes these narrower inputs to I64.
    let unsigned = matches!(stat_literal(right), Some(StatValue::UInt64(_)))
        && matches!(dtype, DType::Primitive(ptype, _) if ptype.is_unsigned_int());
    if unsigned && min.checked_add(offset)? < 0 {
        return None;
    }
    Some(Value::Transform {
        column,
        function: if unsigned {
            "add_offset"
        } else {
            "add_signed_offset"
        },
        offset: Some(offset),
        extra: vec![],
    })
}

fn integer_literal(expr: &Expression) -> Option<i64> {
    match stat_literal(expr)? {
        StatValue::Int64(value) => Some(value),
        StatValue::UInt64(value) => i64::try_from(value).ok(),
        _ => None,
    }
}

fn stat_literal(expr: &Expression) -> Option<StatValue> {
    let E::Literal(scalar) = &expr.kind else {
        return None;
    };
    if scalar.is_null() {
        return Some(StatValue::Null);
    }
    let value = crate::local_primitives::vortex_scalar_to_stat_value(scalar)?;
    if matches!(value, StatValue::Float64(value) if !value.is_finite()) {
        return None;
    }
    Some(value)
}

fn column(expr: &Expression, columns: &BTreeMap<String, Value>) -> Option<ColumnRef> {
    let E::Column(name) = &expr.kind else {
        return None;
    };
    ColumnRef::new(columns.get(name.as_str())?.column()?).ok()
}

/// Reuse the provider's predicate lowering for a fully bound alias-only scan.
/// The binder owns declaration allocation and retains all operator barriers.
pub(in crate::local_primitives) fn scan_predicate(
    expr: &Expression,
    aliases: &BTreeMap<String, String>,
    source: &DType,
) -> Option<PredicateExpr> {
    let columns = aliases
        .iter()
        .map(|(name, source)| (name.clone(), Value::Column(source.clone())))
        .collect();
    checked_predicate(expr, &columns, source)
}

fn checked_predicate(
    expr: &Expression,
    columns: &BTreeMap<String, Value>,
    source: &DType,
) -> Option<PredicateExpr> {
    let predicate = predicate(expr, columns)?;
    predicate_preserves_typed_semantics(&predicate, source).then_some(predicate)
}

fn predicate_preserves_typed_semantics(predicate: &PredicateExpr, source: &DType) -> bool {
    // Typed numeric comparison is exact across integer domains and rejects
    // nonfinite floats. Native scan literals instead use the physical field's
    // dtype. Admit only lossless literals and infallible scalar domains.
    let field = |column: &ColumnRef| {
        let dtype = source.as_struct_fields_opt()?.field(column.as_str())?;
        (!source.is_nullable()
            && (matches!(dtype, DType::Bool(_) | DType::Utf8(_))
                || matches!(dtype, DType::Primitive(ptype, _) if ptype.is_signed_int() || ptype.is_unsigned_int())))
        .then_some(dtype)
    };
    let literal = |column: &ColumnRef, value: &StatValue| {
        let Some(dtype) = field(column) else {
            return false;
        };
        if !matches!(
            (&dtype, value),
            (DType::Bool(_), StatValue::Boolean(_))
                | (DType::Utf8(_), StatValue::Utf8(_))
                | (
                    DType::Primitive(_, _),
                    StatValue::Int64(_) | StatValue::UInt64(_)
                )
        ) {
            return false;
        }
        crate::local_primitives::stat_value_to_vortex_literal(
            value,
            &dtype,
            Some(column.as_str()),
            Kind::FilterAndProject,
        )
        .is_ok()
    };
    match predicate {
        PredicateExpr::AlwaysTrue | PredicateExpr::AlwaysFalse => true,
        PredicateExpr::And(children) => children
            .iter()
            .all(|child| predicate_preserves_typed_semantics(child, source)),
        PredicateExpr::Compare { column, value, .. } => literal(column, value),
        // Native IN with a NULL literal admits NULL rows. Typed (x = a OR
        // x = NULL) yields unknown for them, so it must retain its evaluator.
        PredicateExpr::InList { column, values, .. } => {
            values.iter().all(|value| literal(column, value))
        }
        PredicateExpr::IsNull { column } | PredicateExpr::IsNotNull { column } => {
            field(column).is_some()
        }
        PredicateExpr::StringContains { column, .. } => {
            matches!(field(column), Some(DType::Utf8(_)))
        }
    }
}

fn predicate(expr: &Expression, columns: &BTreeMap<String, Value>) -> Option<PredicateExpr> {
    Some(match &expr.kind {
        E::Binary(left, BinaryOp::And, right) => {
            PredicateExpr::And(vec![predicate(left, columns)?, predicate(right, columns)?])
        }
        E::Binary(left, BinaryOp::Or, right) => {
            let PredicateExpr::Compare {
                column: left_column,
                op: ComparisonOp::Eq,
                value: left,
            } = predicate(left, columns)?
            else {
                return None;
            };
            let PredicateExpr::Compare {
                column: right_column,
                op: ComparisonOp::Eq,
                value: right,
            } = predicate(right, columns)?
            else {
                return None;
            };
            if left_column != right_column {
                return None;
            }
            PredicateExpr::InList {
                column: left_column,
                values: vec![left, right],
                negated: false,
            }
        }
        E::Compare(left, op, right) => PredicateExpr::Compare {
            column: column(left, columns)?,
            op: *op,
            value: stat_literal(right)?,
        },
        E::Unary(UnaryOp::IsNull, expr) => PredicateExpr::IsNull {
            column: column(expr, columns)?,
        },
        E::Unary(UnaryOp::IsNotNull, expr) => PredicateExpr::IsNotNull {
            column: column(expr, columns)?,
        },
        E::Unary(UnaryOp::Not, expr) => {
            let PredicateExpr::StringContains {
                column,
                needle,
                negated,
            } = predicate(expr, columns)?
            else {
                return None;
            };
            PredicateExpr::StringContains {
                column,
                needle,
                negated: !negated,
            }
        }
        E::Function {
            function: Function::Contains,
            args,
        } => {
            let StatValue::Utf8(needle) = stat_literal(&args[1])? else {
                return None;
            };
            PredicateExpr::StringContains {
                column: column(&args[0], columns)?,
                needle,
                negated: false,
            }
        }
        _ => return None,
    })
}

fn having_predicates(expr: &Expression) -> Option<Vec<VortexAggregateHavingExpr>> {
    match &expr.kind {
        E::Binary(left, BinaryOp::And, right) => {
            let mut result = having_predicates(left)?;
            result.extend(having_predicates(right)?);
            Some(result)
        }
        E::Compare(left, op, right) => {
            let E::Column(column) = &left.kind else {
                return None;
            };
            let literal = match stat_literal(right)? {
                StatValue::Int64(value) => value.to_string(),
                StatValue::UInt64(value) => value.to_string(),
                // Keep floating syntax even for integral values. Display can
                // turn a rounded float into a different exact integer literal.
                StatValue::Float64(value) => serde_json::Number::from_f64(value)?.to_string(),
                _ => return None,
            };
            Some(vec![VortexAggregateHavingExpr::new(
                column.as_str(),
                *op,
                literal,
            )])
        }
        _ => None,
    }
}
