//! Bounded scalar declaration parsing into the shared expression IR.
//! Type admission and all value evaluation belong to the native binder/kernels.

use super::relation_sources::ParsedRelationQuery;
use super::{
    BinaryOp, CastMode, ColumnRef, ExprId, Expression, ExpressionKind, LogicalDType,
    ParsedPredicate, ScalarValue, ShardLoomError, expression_source_columns,
    find_keyword_outside_quotes_and_parentheses, find_top_level_numeric_operator,
    matching_closing_parenthesis, parse_binary_byte_length_call_expression,
    parse_cast_call_expression, parse_cast_target_dtype, parse_date_arithmetic_column_arg,
    parse_date_arithmetic_days, parse_null_coalesce_column_arg, parse_predicate,
    parse_projection_literal_value, parse_sql_literal, parse_string_length_call_expression,
    parse_string_scalar_expression, parse_timestamp_arithmetic_column_arg,
    parse_timestamp_arithmetic_seconds, parse_timestamp_extract_column_arg,
    parse_top_level_projection_literal_value, split_sql_csv,
    trim_enclosing_scalar_expression_parentheses, unsupported_sql_error, validate_sql_column_ref,
};

/// An inert scalar declaration. Relational bindings belong to the SQL frontend;
/// the shared expression IR contains only explicit, unresolved references.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ParsedScalarExpression {
    pub(super) expression: Expression,
    pub(super) bindings: Vec<(ExprId, RelationalBinding)>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum RelationalBinding {
    Scalar(Box<ParsedRelationQuery>),
    Predicate(Box<ParsedPredicate>),
}

impl From<Expression> for ParsedScalarExpression {
    fn from(expression: Expression) -> Self {
        Self {
            expression,
            bindings: Vec::new(),
        }
    }
}

impl std::ops::Deref for ParsedScalarExpression {
    type Target = Expression;
    fn deref(&self) -> &Expression {
        &self.expression
    }
}

impl ParsedScalarExpression {
    pub(super) fn plain(&self) -> Result<Expression, ShardLoomError> {
        if !self.bindings.is_empty() {
            return Err(unsupported_sql_error(
                "relational scalar declarations require native query lowering",
            ));
        }
        Ok(self.expression.clone())
    }
}

pub(super) fn predicate_has_bindings(predicate: &ParsedPredicate) -> bool {
    match predicate {
        ParsedPredicate::InSubquery { .. }
        | ParsedPredicate::RowValueInSubquery { .. }
        | ParsedPredicate::QuantifiedSubquery { .. }
        | ParsedPredicate::ExistsSubquery { .. } => true,
        ParsedPredicate::Logical { left, right, .. } => {
            predicate_has_bindings(left) || predicate_has_bindings(right)
        }
        ParsedPredicate::Not { inner } => predicate_has_bindings(inner),
        ParsedPredicate::GenericExpressionCompare { left, right, .. } => {
            !left.bindings.is_empty() || !right.bindings.is_empty()
        }
        _ => false,
    }
}

pub(super) fn parse(raw: &str, id: &str) -> Result<ParsedScalarExpression, ShardLoomError> {
    if raw.len() > 65_536 {
        return Err(unsupported_sql_error(
            "scalar expression exceeds 65536 bytes",
        ));
    }
    let mut parser = Parser {
        nodes: 0,
        bindings: Vec::new(),
    };
    let expression = parser.expression(raw, id, 0)?;
    Ok(ParsedScalarExpression {
        expression,
        bindings: parser.bindings,
    })
}

struct Parser {
    nodes: usize,
    bindings: Vec<(ExprId, RelationalBinding)>,
}

impl Parser {
    fn expression(
        &mut self,
        raw: &str,
        id: &str,
        depth: usize,
    ) -> Result<Expression, ShardLoomError> {
        self.nodes += 1;
        if depth > 24 || self.nodes > 4096 {
            return Err(unsupported_sql_error(
                "scalar expressions exceed 24 levels or 4096 nodes",
            ));
        }
        let raw = trim_enclosing_scalar_expression_parentheses(raw)?;
        if find_keyword_outside_quotes_and_parentheses(raw, "select")? == Some(0) {
            let query = ParsedRelationQuery::parse(raw)?;
            return self.relational(id, RelationalBinding::Scalar(Box::new(query)));
        }
        if let Ok(value) = parse_top_level_projection_literal_value(raw) {
            return Ok(Expression::literal(
                ExprId::new(format!("{id}.literal"))?,
                value,
            ));
        }
        if raw
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("case "))
        {
            return self.case(raw, id, depth);
        }
        for operators in [&['+', '-'][..], &['*', '/', '%'][..]] {
            if let Some((index, operator)) = find_top_level_numeric_operator(raw, operators)? {
                let op = match operator {
                    '+' => BinaryOp::Add,
                    '-' => BinaryOp::Subtract,
                    '*' => BinaryOp::Multiply,
                    '/' => BinaryOp::Divide,
                    '%' => BinaryOp::Remainder,
                    _ => unreachable!("operator list"),
                };
                return Ok(Expression::new(
                    ExprId::new(format!("{id}.binary"))?,
                    ExpressionKind::Binary {
                        left: Box::new(self.expression(
                            raw[..index].trim(),
                            &format!("{id}.left"),
                            depth + 1,
                        )?),
                        op,
                        right: Box::new(self.expression(
                            raw[index + 1..].trim(),
                            &format!("{id}.right"),
                            depth + 1,
                        )?),
                    },
                ));
            }
        }
        if let Some((mode, inner)) = parse_cast_call_expression(raw)? {
            let index = find_keyword_outside_quotes_and_parentheses(inner, "as")?
                .ok_or_else(|| unsupported_sql_error("CAST requires an expression and AS dtype"))?;
            let value = self.expression(
                inner[..index].trim(),
                &format!("{id}.cast.source"),
                depth + 1,
            )?;
            let dtype = parse_cast_target_dtype(inner[index + 2..].trim())?;
            let id = ExprId::new(format!("{id}.cast"))?;
            return Ok(match mode {
                CastMode::Strict => Expression::cast(id, value, dtype),
                CastMode::Try => Expression::try_cast(id, value, dtype),
            });
        }
        if let Some(value) = raw.strip_prefix('-') {
            return Ok(Expression::new(
                ExprId::new(format!("{id}.negate"))?,
                ExpressionKind::Unary {
                    op: shardloom_core::UnaryOp::Negate,
                    expr: Box::new(self.expression(
                        value.trim(),
                        &format!("{id}.value"),
                        depth + 1,
                    )?),
                },
            ));
        }
        if let Some(value) = raw.strip_prefix('+') {
            return self.expression(value.trim(), id, depth + 1);
        }
        if let Some(open) = raw.find('(') {
            return self.function(raw, open, id, depth);
        }
        validate_sql_column_ref(raw)?;
        Ok(Expression::column(
            ExprId::new(format!("{id}.{raw}"))?,
            ColumnRef::new(raw)?,
        ))
    }

    fn function(
        &mut self,
        raw: &str,
        open: usize,
        id: &str,
        depth: usize,
    ) -> Result<Expression, ShardLoomError> {
        let name = raw[..open].trim().to_ascii_lowercase();
        if !scalar_function(&name) {
            return Err(unsupported_sql_error(
                "function has no admitted scalar declaration",
            ));
        }
        let close = matching_closing_parenthesis(raw, open)?
            .ok_or_else(|| unsupported_sql_error("scalar function parentheses must be balanced"))?;
        if !raw[close + 1..].trim().is_empty() {
            return Err(unsupported_sql_error("scalar function has trailing input"));
        }
        let arguments = split_sql_csv(&raw[open + 1..close])?;
        if arguments.len() > 128 {
            return Err(unsupported_sql_error(
                "scalar function exceeds 128 arguments",
            ));
        }
        let args = arguments
                .iter()
                .enumerate()
                .map(|(index, argument)| {
                    if index == 1 && argument.to_ascii_lowercase().starts_with("interval ") {
                        let offset = match name.as_str() {
                            "date_add_days" | "date_sub_days" => i64::from(parse_date_arithmetic_days(argument)?),
                            "timestamp_add_seconds" | "timestamp_sub_seconds" => parse_timestamp_arithmetic_seconds(argument)?,
                            _ => return Err(unsupported_sql_error("INTERVAL is admitted only by date/day and timestamp/second helpers")),
                        };
                        return Ok(Expression::literal(ExprId::new(format!("{id}.{name}.{index}"))?, ScalarValue::Int64(offset)));
                    }
                    self.expression(argument, &format!("{id}.{name}.{index}"), depth + 1)
                })
                .collect::<Result<Vec<_>, _>>()?;
        Ok(Expression::new(
            ExprId::new(format!("{id}.{name}"))?,
            ExpressionKind::FunctionCall { name, args },
        ))
    }

    fn case(&mut self, raw: &str, id: &str, depth: usize) -> Result<Expression, ShardLoomError> {
        let marker = |word| {
            find_keyword_outside_quotes_and_parentheses(raw, word)?
                .ok_or_else(|| unsupported_sql_error("CASE requires WHEN, THEN, ELSE and END"))
        };
        let (when, then, otherwise, end) = (
            marker("when")?,
            marker("then")?,
            marker("else")?,
            marker("end")?,
        );
        if !(raw[..when].trim().eq_ignore_ascii_case("case")
            && when < then
            && then < otherwise
            && otherwise < end
            && raw[end + 3..].trim().is_empty())
        {
            return Err(unsupported_sql_error(
                "CASE requires a single ordered WHEN/THEN/ELSE/END expression",
            ));
        }
        let predicate = parse_predicate(raw[when + 4..then].trim())?;
        let condition = if predicate_has_bindings(&predicate) {
            self.relational(
                &format!("{id}.condition"),
                RelationalBinding::Predicate(Box::new(predicate)),
            )?
        } else {
            condition_expression(&predicate)?
        };
        let yes = self.expression(&raw[then + 4..otherwise], &format!("{id}.then"), depth + 1)?;
        let no = self.expression(&raw[otherwise + 4..end], &format!("{id}.else"), depth + 1)?;
        Ok(Expression::new(
            ExprId::new(format!("{id}.case"))?,
            ExpressionKind::FunctionCall {
                name: "case_when".into(),
                args: vec![condition, yes, no],
            },
        ))
    }

    fn relational(
        &mut self,
        id: &str,
        value: RelationalBinding,
    ) -> Result<Expression, ShardLoomError> {
        let binding = ExprId::new(format!("{id}.relational.{}", self.bindings.len()))?;
        self.bindings.push((binding.clone(), value));
        Ok(Expression::new(
            binding.clone(),
            ExpressionKind::RelationalValue { binding },
        ))
    }
}

fn condition_expression(predicate: &ParsedPredicate) -> Result<Expression, ShardLoomError> {
    let kind = match predicate {
        ParsedPredicate::All => ExpressionKind::Literal(ScalarValue::Boolean(true)),
        ParsedPredicate::Not { inner } => ExpressionKind::Unary {
            op: shardloom_core::UnaryOp::Not,
            expr: Box::new(condition_expression(inner)?),
        },
        ParsedPredicate::Logical { left, right, op } => ExpressionKind::Binary {
            left: Box::new(condition_expression(left)?),
            op: op.binary_op(),
            right: Box::new(condition_expression(right)?),
        },
        _ => return predicate.to_expression(),
    };
    Ok(Expression::new(ExprId::new("scalar.condition")?, kind))
}

fn scalar_function(name: &str) -> bool {
    matches!(
        name,
        "abs"
            | "numeric_abs"
            | "floor"
            | "numeric_floor"
            | "ceil"
            | "ceiling"
            | "numeric_ceil"
            | "round"
            | "numeric_round"
            | "lower"
            | "utf8_lower"
            | "upper"
            | "utf8_upper"
            | "trim"
            | "utf8_trim"
            | "length"
            | "utf8_length"
            | "concat"
            | "utf8_concat"
            | "substr"
            | "substring"
            | "utf8_substr"
            | "utf8_substring"
            | "left"
            | "utf8_left"
            | "right"
            | "utf8_right"
            | "replace"
            | "utf8_replace"
            | "byte_length"
            | "binary_byte_length"
            | "octet_length"
            | "unhex"
            | "binary_unhex"
            | "from_base64"
            | "binary_from_base64"
            | "date_year"
            | "year"
            | "date_month"
            | "month"
            | "date_day"
            | "day"
            | "timestamp_year"
            | "timestamp_month"
            | "timestamp_day"
            | "timestamp_hour"
            | "timestamp_minute"
            | "timestamp_second"
            | "date_add_days"
            | "date_sub_days"
            | "timestamp_add_seconds"
            | "timestamp_sub_seconds"
            | "date_diff_days"
            | "timestamp_diff_seconds"
            | "coalesce"
            | "nullif"
            | "json_extract"
            | "strptime"
            | "try_strptime"
    )
}

/// Existing simple forms retain their route metadata; composed forms use the
/// same IR parser and native binder, without retrying execution through a facade.
pub(super) fn composed(raw: &str) -> Result<bool, ShardLoomError> {
    let raw = trim_enclosing_scalar_expression_parentheses(raw)?;
    if find_keyword_outside_quotes_and_parentheses(raw, "select")? == Some(0) {
        return Ok(true);
    }
    if raw
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("case "))
    {
        return Ok(true);
    }
    if raw.starts_with(['+', '-']) && parse_sql_literal(raw).is_err() {
        return Ok(true);
    }
    if let Some((_, inner)) = parse_cast_call_expression(raw)? {
        let index = find_keyword_outside_quotes_and_parentheses(inner, "as")?
            .ok_or_else(|| unsupported_sql_error("CAST requires AS dtype"))?;
        let source = inner[..index].trim();
        let dtype = parse_cast_target_dtype(inner[index + 2..].trim())?;
        if validate_sql_column_ref(source).is_ok() && parse_sql_literal(source).is_err() {
            return Ok(false);
        }
        if dtype == LogicalDType::Binary
            && parse_string_scalar_expression(source, "cast.source")
                .is_ok_and(|expr| !expression_source_columns(&expr).is_empty())
        {
            return Ok(false);
        }
        return Ok(true);
    }
    let Some(open) = raw.find('(') else {
        return Ok(false);
    };
    let name = raw[..open].trim().to_ascii_lowercase();
    if !scalar_function(&name) {
        return Ok(false);
    }
    let close = matching_closing_parenthesis(raw, open)?
        .ok_or_else(|| unsupported_sql_error("scalar function parentheses must be balanced"))?;
    if !raw[close + 1..].trim().is_empty() {
        return Ok(false);
    }
    let args = split_sql_csv(&raw[open + 1..close])?;
    if let Some(composed) = scoped_function_composition(raw, &name, &args) {
        return Ok(composed);
    }
    Ok(!matches!(name.as_str(), "abs" | "floor" | "ceil" | "round")
        || args.len() != 1
        || args
            .iter()
            .any(|arg| arg.contains('(') || parse_top_level_projection_literal_value(arg).is_ok()))
}

fn scoped_function_composition(raw: &str, name: &str, args: &[String]) -> Option<bool> {
    // Keep the existing public route fields for declarations those routes
    // already represent. Every new shape still lowers to the same scalar IR.
    match (name, args) {
        ("coalesce" | "nullif", [source, value])
            if parse_null_coalesce_column_arg(source).is_ok()
                && parse_projection_literal_value(value)
                    .is_ok_and(|value| !matches!(value, ScalarValue::Null)) =>
        {
            return Some(false);
        }
        ("date_add_days" | "date_sub_days", [source, value])
            if parse_date_arithmetic_column_arg(source).is_ok()
                && (parse_date_arithmetic_days(value).is_ok()
                    || value.to_ascii_lowercase().starts_with("interval ")) =>
        {
            return Some(false);
        }
        ("timestamp_add_seconds" | "timestamp_sub_seconds", [source, value])
            if parse_timestamp_arithmetic_column_arg(source).is_ok()
                && (parse_timestamp_arithmetic_seconds(value).is_ok()
                    || value.to_ascii_lowercase().starts_with("interval ")) =>
        {
            return Some(false);
        }
        ("date_year" | "date_month" | "date_day", [source])
            if parse_date_arithmetic_column_arg(source).is_ok() =>
        {
            return Some(false);
        }
        (
            "timestamp_year" | "timestamp_month" | "timestamp_day" | "timestamp_hour"
            | "timestamp_minute" | "timestamp_second",
            [source],
        ) if parse_timestamp_extract_column_arg(source).is_ok() => {
            return Some(false);
        }
        ("byte_length" | "octet_length", _) => {
            return Some(
                !parse_binary_byte_length_call_expression(raw, "binary.source")
                    .is_ok_and(|value| value.is_some()),
            );
        }
        ("length", _)
            if parse_string_length_call_expression(raw, "string.source").is_ok_and(|value| {
                value.is_some_and(|value| !expression_source_columns(&value).is_empty())
            }) =>
        {
            return Some(false);
        }
        ("unhex" | "from_base64", [source]) => {
            return Some(
                !parse_string_scalar_expression(source, "binary.source")
                    .is_ok_and(|value| !expression_source_columns(&value).is_empty()),
            );
        }
        _ => {}
    }
    // Simple string compositions already lower through the shared IR.
    if matches!(
        name,
        "lower"
            | "upper"
            | "trim"
            | "length"
            | "concat"
            | "substr"
            | "substring"
            | "left"
            | "right"
            | "replace"
    ) && parse_string_scalar_expression(raw, "string.source")
        .is_ok_and(|expr| !expression_source_columns(&expr).is_empty())
    {
        return Some(false);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::super::parse_sql_local_source_statement;
    use super::*;
    use shardloom_core::{ExpressionEvaluationStatus, ExpressionInputRow, evaluate_expression};

    #[test]
    fn scalar_declarations_preserve_composed_typed_values() {
        for (raw, expected) in [
            (
                "CAST(CAST('12.300' AS decimal128(5,3)) AS decimal128(4,2))",
                ScalarValue::Decimal128 {
                    value: 1230,
                    precision: 4,
                    scale: 2,
                },
            ),
            ("CAST(X'c3a9' AS utf8)", ScalarValue::Utf8("é".into())),
            ("TRY_CAST(X'ff' AS utf8)", ScalarValue::Null),
            (
                "FLOOR(CAST('-1.23' AS decimal128(3,2)))",
                ScalarValue::Decimal128 {
                    value: -2,
                    precision: 2,
                    scale: 0,
                },
            ),
            (
                "DATE_ADD_DAYS(DATE '1970-01-01', -1)",
                ScalarValue::Date32(-1),
            ),
            (
                "COALESCE(TRY_CAST('bad' AS decimal128(4,2)), CAST('1.20' AS decimal128(4,2)))",
                ScalarValue::Decimal128 {
                    value: 120,
                    precision: 4,
                    scale: 2,
                },
            ),
        ] {
            let expression = parse(raw, "typed").unwrap();
            let result = evaluate_expression(&expression, &ExpressionInputRow::new());
            assert_eq!(
                result.status,
                ExpressionEvaluationStatus::Evaluated,
                "{raw}: {:?}",
                result.diagnostics
            );
            assert_eq!(result.value, Some(expected), "{raw}");
        }
    }

    #[test]
    fn scalar_declarations_preserve_signed_exponents_unary_and_lazy_case() {
        let mut row = ExpressionInputRow::new();
        row.insert("amount".into(), ScalarValue::Float64(1.0));
        for (raw, expected) in [
            ("amount + 1e-3", 1.001),
            ("amount - 1e+3", -999.0),
            ("-amount", -1.0),
        ] {
            let expression = parse(raw, "typed").unwrap();
            assert_eq!(
                evaluate_expression(&expression, &row).value,
                Some(ScalarValue::Float64(expected))
            );
        }
        let expression = parse("CASE WHEN amount > 0 THEN CAST('1.20' AS decimal128(4,2)) ELSE CAST('bad' AS decimal128(4,2)) END", "typed").unwrap();
        assert_eq!(expression_source_columns(&expression), vec!["amount"]);
        assert_eq!(
            evaluate_expression(&expression, &row).value,
            Some(ScalarValue::Decimal128 {
                value: 120,
                precision: 4,
                scale: 2
            })
        );
    }

    #[test]
    fn scalar_declarations_bound_recursion_and_keep_route_classification() {
        for raw in [
            format!("{}amount{}", "ABS(".repeat(26), ")".repeat(26)),
            "a".repeat(65_537),
            "unknown(amount)".to_owned(),
            "ABS(amount) trailing".to_owned(),
        ] {
            assert!(parse(&raw, "typed").is_err());
        }
        for raw in [
            "CAST(amount AS decimal128(10,2))",
            "CAST(CONCAT(label, 'x') AS binary)",
            "LOWER(label)",
        ] {
            assert!(!composed(raw).unwrap(), "{raw}");
        }
        for raw in [
            "TRY_CAST(CAST(amount AS utf8) AS decimal128(10,2))",
            "FLOOR(CAST(amount AS decimal128(10,2)))",
            "CAST('1.20' AS decimal128(4,2))",
            "-amount",
        ] {
            assert!(composed(raw).unwrap(), "{raw}");
        }
    }

    #[test]
    fn typed_compositions_survive_complete_source_statement_parsing() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CAST(CAST('12.300' AS decimal128(5,3)) AS decimal128(4,2)) AS exact,CAST(X'c3a9' AS utf8) AS text,CASE WHEN id > 0 THEN CAST('1.20' AS decimal128(4,2)) ELSE NULL END AS selected,COALESCE(TRY_CAST(raw AS decimal128(4,2)),CAST('1.20' AS decimal128(4,2))) AS filled,DATE_ADD_DAYS(DATE '1970-01-01',-1) AS day FROM 'input.vortex' LIMIT 5",
        ).unwrap();
        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.generic_expression_projections.len(), 5);

        assert_eq!(
            parsed.generic_expression_projections[0].source_columns,
            Vec::<String>::new()
        );
        assert_eq!(
            parsed.generic_expression_projections[2].source_columns,
            vec!["id"]
        );
        assert_eq!(
            parsed.generic_expression_projections[3].source_columns,
            vec!["raw"]
        );
        assert_eq!(
            parse_sql_literal("18446744073709551615").unwrap(),
            ScalarValue::UInt64(u64::MAX)
        );
    }
}
