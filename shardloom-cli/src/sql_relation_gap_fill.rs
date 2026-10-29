//! Literal grid declarations; calculation and row expansion remain native.

use super::{
    Arc, ParsedRelationGapFill, ParsedRelationQuery, ShardLoomError, matching_closing_parenthesis,
    parse_sql_string_literal, split_sql_csv, unsupported_sql_error, validate_sql_identifier,
};
use shardloom_core::temporal::{Ambiguity, TemporalGridStep, civil::MonthEnd};

pub(super) fn parse(raw: &str) -> Result<Option<ParsedRelationGapFill>, ShardLoomError> {
    let Some(open) = raw.find('(') else {
        return Ok(None);
    };
    if !raw[..open].trim().eq_ignore_ascii_case("gap_fill") {
        return Ok(None);
    }
    let close = matching_closing_parenthesis(raw, open)?
        .ok_or_else(|| unsupported_sql_error("GAP_FILL parentheses must be balanced"))?;
    if close + 1 != raw.len() || raw[open + 1..close].trim_end().ends_with(',') {
        return Err(unsupported_sql_error(
            "unexpected text or trailing comma in GAP_FILL",
        ));
    }
    let args = split_sql_csv(&raw[open + 1..close])?;
    if !(4..=10).contains(&args.len()) {
        return Err(unsupported_sql_error(
            "GAP_FILL requires input, time, every, unit[, by[, start[, end[, origin[, month_end[, ambiguity]]]]]]",
        ));
    }
    let input = &args[0];
    if !input.starts_with('(') || matching_closing_parenthesis(input, 0)? != Some(input.len() - 1) {
        return Err(unsupported_sql_error(
            "GAP_FILL requires a parenthesized input query",
        ));
    }
    let time = parse_sql_string_literal(&args[1])?;
    validate_sql_identifier(&time)?;
    let every = args[2]
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            unsupported_sql_error(
                "GAP_FILL every requires a positive signed-64-bit integer literal",
            )
        })?;
    let unit = parse_sql_string_literal(&args[3])?;
    let keys = args
        .get(4)
        .map(|value| parse_sql_string_literal(value))
        .transpose()?
        .unwrap_or_default();
    let mut partition_by = Vec::new();
    if !keys.is_empty() {
        for key in keys.split(',').map(str::trim) {
            validate_sql_identifier(key)?;
            if key == time || partition_by.iter().any(|existing| existing == key) {
                return Err(unsupported_sql_error(
                    "GAP_FILL partition keys must be unique and distinct from time",
                ));
            }
            partition_by.push(key.to_owned());
        }
    }
    let text = |index: usize| -> Result<String, ShardLoomError> {
        args.get(index)
            .map(|value| parse_sql_string_literal(value))
            .transpose()
            .map(|value| value.unwrap_or_else(|| "reject".to_owned()))
    };
    let month_end = MonthEnd::parse(&text(8)?)
        .ok_or_else(|| unsupported_sql_error("GAP_FILL month_end must be reject or clamp"))?;
    let ambiguity = Ambiguity::parse(&text(9)?).ok_or_else(|| {
        unsupported_sql_error("GAP_FILL ambiguity must be reject, earlier or later")
    })?;
    let optional_tick = |index: usize| -> Result<Option<i64>, ShardLoomError> {
        match args.get(index) {
            None => Ok(None),
            Some(value) if value.eq_ignore_ascii_case("NULL") => Ok(None),
            Some(value) => value.parse().map(Some).map_err(|_| {
                unsupported_sql_error(
                    "GAP_FILL bounds and origin require signed-64-bit integer literals or NULL",
                )
            }),
        }
    };
    Ok(Some(ParsedRelationGapFill {
        input: Arc::new(ParsedRelationQuery::parse(&input[1..input.len() - 1])?),
        time,
        partition_by,
        step: TemporalGridStep::from_parts(every.cast_unsigned(), &unit, month_end, ambiguity)?,
        start: optional_tick(5)?,
        end: optional_tick(6)?,
        origin: optional_tick(7)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::super::parse_source;
    use super::*;

    #[test]
    fn gap_fill_keeps_typed_literal_controls_and_nested_sources() {
        let fill = parse("GAP_FILL((SELECT * FROM input), 'at', 2, 'calendar_months', 'fund, region', -3, 9, NULL, 'clamp', 'later')")
            .unwrap().unwrap();
        assert_eq!(fill.time, "at");
        assert_eq!(fill.partition_by, ["fund", "region"]);
        assert_eq!(
            (fill.start, fill.end, fill.origin),
            (Some(-3), Some(9), None)
        );
        assert_eq!(
            fill.step,
            TemporalGridStep::CalendarMonths {
                value: 2,
                month_end: MonthEnd::Clamp,
                ambiguity: Ambiguity::Later
            }
        );
        assert!(
            parse_source(
                "GAP_FILL((SELECT * FROM input), 'at', 1, 's') AS filled",
                false
            )
            .is_ok()
        );
        assert!(parse_source("GAP_FILL((SELECT * FROM input), 'at', 1, 's')", false).is_err());
    }

    #[test]
    fn gap_fill_rejects_expressions_and_invalid_literal_controls() {
        for arguments in [
            "input, 'at', 1, 's'",
            "(SELECT * FROM input), 'at', 0, 's'",
            "(SELECT * FROM input), 'at', 1+2, 's'",
            "(SELECT * FROM input), 'at', 1, 'week'",
            "(SELECT * FROM input), 'at', 1, 's', 'fund,fund'",
            "(SELECT * FROM input), 'at', 1, 's', 'at'",
            "(SELECT * FROM input), 'at', 1, 's', '', '0'",
            "(SELECT * FROM input), 'at', 1, 's', '', 0, 2, NULL, 'clamp'",
            "(SELECT * FROM input), 'at', 1, 's', '', 0, 2, NULL, 'reject', 'later'",
            "(SELECT * FROM input), 'at', 1, 's',",
        ] {
            assert!(
                parse(&format!("GAP_FILL({arguments})")).is_err(),
                "{arguments}"
            );
        }
    }
}
