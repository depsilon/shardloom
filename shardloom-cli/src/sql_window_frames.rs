//! Parse window values and inert frame declarations through existing SQL helpers.

use super::{
    CastMode, ParsedOrderBy, ScalarValue, ShardLoomError, WindowFunction,
    cast_scalar_literal_to_dtype, find_keyword_outside_quotes_and_parentheses as keyword,
    matching_closing_parenthesis, parse_cast_call_expression, parse_cast_target_dtype,
    parse_order_by, parse_sql_literal, parse_sql_string_literal, parse_window_function_args,
    parse_window_partition_by, scalar_expression, split_sql_csv, split_whitespace_outside_quotes,
    unsupported_sql_error,
};
use shardloom_vortex::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameExclusion as Exclusion,
    VortexRelationalFrameOffset as Offset, VortexRelationalFrameUnit as Unit,
    VortexRelationalWindowFrame as Frame,
};

pub(super) fn parse_value_function(raw: &str) -> Result<Option<WindowFunction>, ShardLoomError> {
    for name in ["first_value", "last_value", "nth_value"] {
        let Some(arguments) = parse_window_function_args(raw, name)? else {
            continue;
        };
        let args = split_sql_csv(arguments)?;
        if args.len() != if name == "nth_value" { 2 } else { 1 } {
            return Err(unsupported_sql_error(
                "FIRST_VALUE/LAST_VALUE require one expression; NTH_VALUE also requires a positive integer position",
            ));
        }
        let expression = scalar_expression::parse(&args[0], "window.argument")?;
        return Ok(Some(match name {
            "first_value" => WindowFunction::FirstValue(expression),
            "last_value" => WindowFunction::LastValue(expression),
            _ => {
                let index = args[1]
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .filter(|index| *index > 0)
                    .ok_or_else(|| {
                        unsupported_sql_error(
                            "NTH_VALUE position must be a positive UInt64 literal",
                        )
                    })?;
                WindowFunction::NthValue { expression, index }
            }
        }));
    }
    Ok(None)
}

pub(super) fn parse_spec(
    raw: &str,
) -> Result<(Vec<String>, ParsedOrderBy, Option<Frame>), ShardLoomError> {
    let raw = raw.trim();
    if !raw.starts_with('(') {
        return Err(unsupported_sql_error(
            "window specification must be OVER (...)",
        ));
    }
    let close = matching_closing_parenthesis(raw, 0)?.ok_or_else(|| {
        unsupported_sql_error("window specification parentheses must be balanced")
    })?;
    if !raw[close + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "window specification has trailing input",
        ));
    }
    let inner = raw[1..close].trim();
    let frame_start = frame_start(inner)?;
    let (prefix, frame) = if let Some(start) = frame_start {
        (&inner[..start], Some(parse_frame(&inner[start..])?))
    } else {
        (inner, None)
    };
    let (partition, order) = if let Some(order) = keyword(prefix, "order by")? {
        (
            &prefix[..order],
            parse_order_by(Some(prefix[order + 8..].trim()))?.expect("ORDER BY provided"),
        )
    } else {
        (prefix, ParsedOrderBy { keys: vec![] })
    };
    Ok((parse_window_partition_by(partition.trim())?, order, frame))
}

fn frame_start(raw: &str) -> Result<Option<usize>, ShardLoomError> {
    let mut first = None;
    for unit in ["rows", "groups", "range"] {
        let mut start = 0;
        while let Some(index) = keyword(&raw[start..], unit)? {
            let index = start + index;
            let rest = raw[index + unit.len()..].trim_start();
            // An ordering column may itself be named "rows". A frame unit must
            // be followed by an endpoint, not a direction, comma or clause end.
            let token = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            if matches!(
                token.as_str(),
                "between" | "unbounded" | "current" | "interval" | "cast"
            ) || token.starts_with("cast(")
                || token
                    .as_bytes()
                    .first()
                    .is_some_and(|byte| byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.'))
            {
                first = Some(first.map_or(index, |old: usize| old.min(index)));
                break;
            }
            start = index + unit.len();
        }
    }
    Ok(first)
}

fn parse_frame(raw: &str) -> Result<Frame, ShardLoomError> {
    let first_space = raw
        .find(char::is_whitespace)
        .ok_or_else(|| unsupported_sql_error("window frame requires bounds"))?;
    let unit = match raw[..first_space].to_ascii_lowercase().as_str() {
        "rows" => Unit::Rows,
        "groups" => Unit::Groups,
        "range" => Unit::Range,
        _ => {
            return Err(unsupported_sql_error(
                "window frame requires ROWS, GROUPS or RANGE",
            ));
        }
    };
    let bounds = raw[first_space..].trim();
    let (bounds, exclusion) = if let Some(exclude) = keyword(bounds, "exclude")? {
        let exclusion = match bounds[exclude + 7..]
            .split_whitespace()
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>()
            .join(" ")
            .as_str()
        {
            "no others" => Exclusion::NoOthers,
            "current row" => Exclusion::CurrentRow,
            "group" => Exclusion::Group,
            "ties" => Exclusion::Ties,
            _ => {
                return Err(unsupported_sql_error(
                    "window EXCLUDE requires CURRENT ROW, GROUP, TIES or NO OTHERS",
                ));
            }
        };
        (bounds[..exclude].trim(), exclusion)
    } else {
        (bounds, Exclusion::NoOthers)
    };
    let (start, end) = if keyword(bounds, "between")? == Some(0) {
        let bounds = bounds[7..].trim();
        let and = keyword(bounds, "and")?
            .ok_or_else(|| unsupported_sql_error("window BETWEEN bounds require AND"))?;
        (
            parse_bound(&bounds[..and])?,
            parse_bound(&bounds[and + 3..])?,
        )
    } else {
        (parse_bound(bounds)?, Bound::CurrentRow)
    };
    Ok(Frame {
        unit,
        start,
        end,
        exclusion,
    })
}

fn parse_bound(raw: &str) -> Result<Bound, ShardLoomError> {
    let raw = raw.trim();
    let normalized = raw
        .split_whitespace()
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>()
        .join(" ");
    match normalized.as_str() {
        "unbounded preceding" => return Ok(Bound::UnboundedPreceding),
        "unbounded following" => return Ok(Bound::UnboundedFollowing),
        "current row" => return Ok(Bound::CurrentRow),
        _ => {}
    }
    for direction in ["preceding", "following"] {
        if let Some(index) = keyword(raw, direction)?
            && raw[index + direction.len()..].trim().is_empty()
        {
            let offset = parse_offset(raw[..index].trim())?;
            return Ok(if direction == "preceding" {
                Bound::Preceding(offset)
            } else {
                Bound::Following(offset)
            });
        }
    }
    Err(unsupported_sql_error(
        "window bound requires UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW or a literal offset PRECEDING/FOLLOWING",
    ))
}

fn parse_offset(raw: &str) -> Result<Offset, ShardLoomError> {
    if keyword(raw, "interval")? == Some(0) {
        let tokens = split_whitespace_outside_quotes(raw)?;
        let [_, value, unit] = tokens.as_slice() else {
            return Err(unsupported_sql_error(
                "frame INTERVAL requires a nonnegative integer string and fixed unit",
            ));
        };
        let value = parse_sql_string_literal(value)?
            .parse::<u64>()
            .map_err(|_| {
                unsupported_sql_error("frame INTERVAL value must be a nonnegative UInt64 integer")
            })?;
        let multiplier = match unit.to_ascii_lowercase().as_str() {
            "day" | "days" => 86_400_000_000,
            "hour" | "hours" => 3_600_000_000,
            "minute" | "minutes" => 60_000_000,
            "second" | "seconds" => 1_000_000,
            "millisecond" | "milliseconds" => 1_000,
            "microsecond" | "microseconds" => 1,
            _ => {
                return Err(unsupported_sql_error(
                    "frame INTERVAL requires DAY, HOUR, MINUTE, SECOND, MILLISECOND or MICROSECOND; calendar units are not admitted",
                ));
            }
        };
        return Ok(Offset::DurationMicros(
            value.checked_mul(multiplier).ok_or_else(|| {
                unsupported_sql_error("frame duration exceeds UInt64 microseconds")
            })?,
        ));
    }
    let value = if let Some((mode, inner)) = parse_cast_call_expression(raw)? {
        if mode != CastMode::Strict {
            return Err(unsupported_sql_error(
                "frame offsets require a strict literal CAST",
            ));
        }
        let as_at = keyword(inner, "as")?
            .ok_or_else(|| unsupported_sql_error("frame CAST requires AS dtype"))?;
        let source = parse_sql_literal(inner[..as_at].trim())?;
        let dtype = parse_cast_target_dtype(inner[as_at + 2..].trim())?;
        cast_scalar_literal_to_dtype(source, &dtype)?
    } else {
        parse_sql_literal(raw)?
    };
    if !matches!(
        value,
        ScalarValue::Int64(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Float64(_)
            | ScalarValue::Decimal128 { .. }
    ) {
        return Err(unsupported_sql_error(
            "frame offset must be a numeric or fixed-duration literal",
        ));
    }
    Ok(Offset::Number(value))
}
