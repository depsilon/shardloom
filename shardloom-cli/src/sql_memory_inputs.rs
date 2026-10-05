//! SQL input syntax only. Expressions and every relational operation use the
//! ordinary parser, binder and native executor after this bounded intake.

use super::{
    ScalarValue, ShardLoomError, find_keyword_outside_quotes_and_parentheses, keyword_boundary,
    matching_closing_parenthesis, parse_select_distinct_marker, parse_sql_literal, split_sql_csv,
    top_level_keyword_indexes, unsupported_sql_error,
};
use crate::native_memory_input::{MemoryInput, MemoryRow, MemoryValueType};

pub(super) fn normalize_statement(
    statement: String,
) -> Result<(String, Option<MemoryInput>), ShardLoomError> {
    if statement
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("values"))
        && keyword_boundary(&statement, 0, 6)
    {
        let end = first_clause(&statement, &["order by", "limit"])?;
        let input = parse_values(statement[6..end].trim())?;
        return Ok((
            format!("SELECT * FROM '__native_input__' {}", &statement[end..]),
            Some(input),
        ));
    }
    if !statement
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
        || !top_level_keyword_indexes(&statement, "from")?.is_empty()
    {
        return Ok((statement, None));
    }
    let end = first_clause(
        &statement,
        &["where", "group by", "having", "order by", "limit"],
    )?;
    let (distinct, projection) = parse_select_distinct_marker(statement[6..end].trim())?;
    let entries = split_sql_csv(projection)?;
    let mut projections = Vec::with_capacity(entries.len());
    for (index, entry) in entries.into_iter().enumerate() {
        if entry == "*" {
            return Err(unsupported_sql_error("SELECT * requires an input relation"));
        }
        projections.push(
            if find_keyword_outside_quotes_and_parentheses(&entry, "as")?.is_some() {
                entry
            } else {
                format!("{entry} AS column_{}", index + 1)
            },
        );
    }
    Ok((
        format!(
            "SELECT {}{} FROM '__native_unit__' {}",
            if distinct { "DISTINCT " } else { "" },
            projections.join(", "),
            &statement[end..]
        ),
        Some(MemoryInput::Unit),
    ))
}

fn first_clause(statement: &str, keywords: &[&str]) -> Result<usize, ShardLoomError> {
    let mut end = statement.len();
    for keyword in keywords {
        if let Some(index) = top_level_keyword_indexes(statement, keyword)?.first() {
            end = end.min(*index);
        }
    }
    Ok(end)
}

pub(super) fn parse_range(raw: &str) -> Result<Option<MemoryInput>, ShardLoomError> {
    let raw = raw.trim();
    for (name, inclusive) in [("range", false), ("generate_series", true)] {
        if !raw
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            || !keyword_boundary(raw, 0, name.len())
        {
            continue;
        }
        let args = raw[name.len()..].trim();
        if !args.starts_with('(') || matching_closing_parenthesis(args, 0)? != Some(args.len() - 1)
        {
            return Err(unsupported_sql_error(
                "range requires one parenthesized start, end and optional step",
            ));
        }
        let args = split_sql_csv(&args[1..args.len() - 1])?;
        if !(2..=3).contains(&args.len()) {
            return Err(unsupported_sql_error(
                "range requires start, end and optional step",
            ));
        }
        let integer = |raw: &str| {
            raw.parse::<i64>()
                .map_err(|_| unsupported_sql_error("range arguments must be int64 literals"))
        };
        let input = MemoryInput::Range {
            start: integer(&args[0])?,
            end: integer(&args[1])?,
            step: args.get(2).map_or(Ok(1), |step| integer(step))?,
            column: "value".into(),
            inclusive,
        };
        input.validate()?;
        return Ok(Some(input));
    }
    Ok(None)
}

fn parse_values(raw: &str) -> Result<MemoryInput, ShardLoomError> {
    let tuples = split_sql_csv(raw)?;
    if tuples.is_empty() || tuples.len() > 10_000 {
        return Err(unsupported_sql_error(
            "VALUES requires between 1 and 10,000 input rows",
        ));
    }
    let mut kinds = Vec::new();
    let mut rows = Vec::with_capacity(tuples.len());
    for tuple in tuples {
        if !tuple.starts_with('(')
            || matching_closing_parenthesis(&tuple, 0)? != Some(tuple.len() - 1)
        {
            return Err(unsupported_sql_error(
                "VALUES requires parenthesized literal rows",
            ));
        }
        let cells = split_sql_csv(&tuple[1..tuple.len() - 1])?;
        if cells.is_empty() || cells.len() > 64 || (!kinds.is_empty() && cells.len() != kinds.len())
        {
            return Err(unsupported_sql_error(
                "VALUES rows require equal widths between 1 and 64",
            ));
        }
        let values = cells
            .iter()
            .map(|cell| parse_sql_literal(cell))
            .collect::<Result<Vec<_>, _>>()?;
        let row = values
            .iter()
            .map(literal_cell)
            .collect::<Result<Vec<_>, _>>()?;
        if kinds.is_empty() {
            kinds.extend(row.iter().map(|(kind, _)| *kind));
        } else {
            for (kind, (next, _)) in kinds.iter_mut().zip(&row) {
                *kind = match (*kind, *next) {
                    (left, right) if left == right => left,
                    (None, right) => right,
                    (left, None) => left,
                    (Some(MemoryValueType::Int64), Some(MemoryValueType::Float64))
                    | (Some(MemoryValueType::Float64), Some(MemoryValueType::Int64)) => {
                        Some(MemoryValueType::Float64)
                    }
                    _ => {
                        return Err(unsupported_sql_error(
                            "VALUES columns require compatible literal types",
                        ));
                    }
                };
            }
        }
        rows.push(row);
    }
    for row in &rows {
        for (kind, (actual, value)) in kinds.iter().zip(row) {
            if *kind == Some(MemoryValueType::Float64)
                && *actual == Some(MemoryValueType::Int64)
                && value.as_ref().is_some_and(|value| {
                    value
                        .parse::<i64>()
                        .is_ok_and(|value| value.unsigned_abs() > (1_u64 << 53))
                })
            {
                return Err(unsupported_sql_error(
                    "VALUES integer-to-float promotion exceeds the exact integer range",
                ));
            }
        }
    }
    let schema = kinds
        .iter()
        .enumerate()
        .map(|(i, kind)| {
            (
                format!("column_{}", i + 1),
                kind.unwrap_or(MemoryValueType::Bool),
            )
        })
        .collect::<Vec<_>>();
    let rows = rows
        .into_iter()
        .map(|row| MemoryRow(row.into_iter().map(|(_, value)| value).collect()))
        .collect::<Vec<_>>();
    let input = MemoryInput::Rows { schema, rows };
    input.validate()?;
    Ok(input)
}

fn literal_cell(
    value: &ScalarValue,
) -> Result<(Option<MemoryValueType>, Option<String>), ShardLoomError> {
    match value {
        ScalarValue::Null => Ok((None, None)),
        ScalarValue::Int64(value) => Ok((Some(MemoryValueType::Int64), Some(value.to_string()))),
        ScalarValue::Float64(value) => {
            Ok((Some(MemoryValueType::Float64), Some(value.to_string())))
        }
        ScalarValue::Boolean(value) => Ok((Some(MemoryValueType::Bool), Some(value.to_string()))),
        ScalarValue::Utf8(value) => Ok((Some(MemoryValueType::Utf8), Some(value.clone()))),
        _ => Err(unsupported_sql_error(
            "VALUES input admits int64, finite float64, boolean, UTF8 and NULL; use typed native inputs for other types",
        )),
    }
}
