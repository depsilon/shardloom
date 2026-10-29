//! Inert ASOF syntax; matching executes only in the shared native operator.

use super::{
    ParsedAsOfJoin, ParsedJoin, ParsedJoinKeyPair, ParsedJoinOnPredicateFamily, ParsedJoinType,
    ParsedSourceClause, ShardLoomError, find_keyword_outside_quotes_and_parentheses,
    find_top_level_comparison_operator, matching_closing_parenthesis, parse_qualified_column_ref,
    parse_source, starts_with_keyword, trim_enclosing_predicate_parentheses, unsupported_sql_error,
};
use shardloom_vortex::relational_query::{
    VortexRelationalAsOfDirection as Direction, VortexRelationalAsOfOptions as Options,
};

pub(super) fn parse(raw: &str) -> Result<Option<ParsedSourceClause>, ShardLoomError> {
    let mut found = None;
    for keyword in ["asof left join", "asof join"] {
        if let Some(index) = find_keyword_outside_quotes_and_parentheses(raw, keyword)?
            && found.is_none_or(|(prior, _)| index < prior)
        {
            found = Some((index, keyword.len()));
        }
    }
    let Some((index, length)) = found else {
        return Ok(None);
    };
    let tail = raw[index + length..].trim();
    let condition = find_keyword_outside_quotes_and_parentheses(tail, "match_condition")?
        .ok_or_else(|| {
            unsupported_sql_error(
                "ASOF JOIN requires MATCH_CONDITION (<left key> >= <right key>) or NEAREST keys",
            )
        })?;
    let (source, left_alias) = parse_source(raw[..index].trim(), true)?;
    let (right_source, right_alias) = parse_source(tail[..condition].trim(), true)?;
    let left_alias = left_alias.expect("required alias checked");
    let right_alias = right_alias.expect("required alias checked");
    if left_alias == right_alias {
        return Err(unsupported_sql_error(
            "ASOF JOIN requires distinct source aliases",
        ));
    }
    let condition = tail[condition + "match_condition".len()..].trim();
    if !condition.starts_with('(') {
        return Err(unsupported_sql_error(
            "ASOF MATCH_CONDITION requires parentheses",
        ));
    }
    let end = matching_closing_parenthesis(condition, 0)?.ok_or_else(|| {
        unsupported_sql_error("ASOF MATCH_CONDITION parentheses must be balanced")
    })?;
    let (on, mut options) = matching(&condition[1..end])?;
    let mut remaining = condition[end + 1..].trim();
    if let Some(index) = unqualified_keyword(remaining, "tolerance")? {
        let number = remaining[index + "tolerance".len()..].trim();
        if number.is_empty() || !number.bytes().all(|value| value.is_ascii_digit()) {
            return Err(unsupported_sql_error(
                "ASOF TOLERANCE requires a nonnegative integer in ordering-key ticks",
            ));
        }
        options.tolerance = Some(
            number
                .parse()
                .map_err(|_| unsupported_sql_error("ASOF TOLERANCE exceeds uint64"))?,
        );
        remaining = remaining[..index].trim();
    }
    let key_pairs = if remaining.is_empty() {
        Vec::new()
    } else if starts_with_keyword(remaining, "on") {
        equality_keys(remaining["on".len()..].trim())?
    } else {
        return Err(unsupported_sql_error(
            "ASOF MATCH_CONDITION accepts optional ON equality keys followed by TOLERANCE",
        ));
    };
    for pair in key_pairs.iter().chain(std::iter::once(&on)) {
        if pair.left.alias != left_alias || pair.right.alias != right_alias {
            return Err(unsupported_sql_error(
                "ASOF keys must name the left alias first and the right alias second",
            ));
        }
    }
    Ok(Some(ParsedSourceClause {
        source,
        source_alias: Some(left_alias),
        join: Some(ParsedJoin {
            join_type: ParsedJoinType::LeftOuterEqui,
            right_source,
            right_alias,
            key_pairs,
            on_predicate: None,
            on_predicate_family: ParsedJoinOnPredicateFamily::EquiKeys,
            asof: Some(ParsedAsOfJoin { on, options }),
        }),
    }))
}

fn unqualified_keyword(raw: &str, keyword: &str) -> Result<Option<usize>, ShardLoomError> {
    let mut offset = 0;
    while let Some(found) = find_keyword_outside_quotes_and_parentheses(&raw[offset..], keyword)? {
        let index = offset + found;
        let end = index + keyword.len();
        // A qualified alias or column named after a keyword remains a key.
        if !raw[..index].trim_end().ends_with('.') && !raw[end..].trim_start().starts_with('.') {
            return Ok(Some(index));
        }
        offset = end;
    }
    Ok(None)
}

fn equality_keys(raw: &str) -> Result<Vec<ParsedJoinKeyPair>, ShardLoomError> {
    let mut pending = vec![raw];
    let mut keys = Vec::new();
    while let Some(raw) = pending.pop() {
        let raw = trim_enclosing_predicate_parentheses(raw)?;
        if let Some(index) = unqualified_keyword(raw, "and")? {
            pending.extend([raw[index + "and".len()..].trim(), raw[..index].trim()]);
            continue;
        }
        let (index, operator) = find_top_level_comparison_operator(raw)?.ok_or_else(|| {
            unsupported_sql_error("ASOF ON requires equality between qualified keys")
        })?;
        if operator != "=" {
            return Err(unsupported_sql_error(
                "ASOF ON admits equality partition keys only",
            ));
        }
        keys.push(pair(
            raw[..index].trim(),
            raw[index + operator.len()..].trim(),
        )?);
    }
    Ok(keys)
}

fn matching(raw: &str) -> Result<(ParsedJoinKeyPair, Options), ShardLoomError> {
    let raw = raw.trim();
    if let Some(open) = raw.find('(')
        && raw[..open].trim().eq_ignore_ascii_case("nearest")
    {
        let close = matching_closing_parenthesis(raw, open)?
            .ok_or_else(|| unsupported_sql_error("ASOF NEAREST parentheses must be balanced"))?;
        if !raw[close + 1..].trim().is_empty() {
            return Err(unsupported_sql_error("unexpected ASOF NEAREST suffix"));
        }
        // Arguments are qualified identifiers and an optional Boolean, not arbitrary expressions.
        let arguments: Vec<_> = raw[open + 1..close].split(',').map(str::trim).collect();
        let (left, right, exact) = match arguments.as_slice() {
            [left, right] => (*left, *right, true),
            [left, right, exact] if exact.eq_ignore_ascii_case("true") => (*left, *right, true),
            [left, right, exact] if exact.eq_ignore_ascii_case("false") => (*left, *right, false),
            _ => {
                return Err(unsupported_sql_error(
                    "ASOF NEAREST requires two qualified keys and optional allow-exact TRUE/FALSE",
                ));
            }
        };
        return Ok((
            pair(left, right)?,
            Options {
                direction: Direction::Nearest,
                allow_exact_matches: exact,
                tolerance: None,
            },
        ));
    }
    let (index, operator) = find_top_level_comparison_operator(raw)?.ok_or_else(|| {
        unsupported_sql_error("ASOF MATCH_CONDITION requires >=, >, <=, < or NEAREST")
    })?;
    let direction = match operator {
        ">=" | ">" => Direction::Backward,
        "<=" | "<" => Direction::Forward,
        _ => {
            return Err(unsupported_sql_error(
                "ASOF MATCH_CONDITION requires >=, >, <=, < or NEAREST",
            ));
        }
    };
    Ok((
        pair(raw[..index].trim(), raw[index + operator.len()..].trim())?,
        Options {
            direction,
            tolerance: None,
            allow_exact_matches: operator.ends_with('='),
        },
    ))
}

fn pair(left: &str, right: &str) -> Result<ParsedJoinKeyPair, ShardLoomError> {
    Ok(ParsedJoinKeyPair {
        left: parse_qualified_column_ref(left)?,
        right: parse_qualified_column_ref(right)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asof_syntax_binds_direction_exactness_partitions_and_tolerance_without_execution() {
        for (condition, direction, exact) in [
            ("l.t>=r.t", Direction::Backward, true),
            ("l.t > r.t", Direction::Backward, false),
            ("l.t<=r.t", Direction::Forward, true),
            ("l.t < r.t", Direction::Forward, false),
            ("NEAREST(l.t,r.t)", Direction::Nearest, true),
            ("nearest(l.t, r.t, FALSE)", Direction::Nearest, false),
        ] {
            let parsed = parse(&format!("'missing-left.vortex' AS l ASOF LEFT JOIN 'missing-right.vortex' AS r MATCH_CONDITION ({condition}) ON l.entity = r.entity AND l.currency = r.currency TOLERANCE 17")).unwrap().unwrap();
            let join = parsed.join.unwrap();
            assert_eq!(join.key_pairs.len(), 2);
            let asof = join.asof.unwrap();
            assert_eq!(
                asof.options,
                Options {
                    direction,
                    tolerance: Some(17),
                    allow_exact_matches: exact
                }
            );
            assert_eq!(asof.on.left.column, "t");
            assert_eq!(asof.on.right.column, "t");
        }
        assert!(
            parse("'l' AS l INNER JOIN 'r' AS r ON l.x = r.x")
                .unwrap()
                .is_none()
        );
        assert!(parse("'asof join.vortex'").unwrap().is_none());
        assert_eq!(
            parse("'l' AS l ASOF JOIN 'r' AS r MATCH_CONDITION(l.t>=r.t)")
                .unwrap()
                .unwrap()
                .join
                .unwrap()
                .key_pairs,
            [] as [ParsedJoinKeyPair; 0]
        );
    }

    #[test]
    fn asof_syntax_rejects_ambiguous_or_unhandled_matching() {
        for suffix in [
            "ON l.t = r.t",
            "MATCH_CONDITION(l.t = r.t)",
            "MATCH_CONDITION(r.t >= l.t)",
            "MATCH_CONDITION(l.t>=r.t) ON l.x > r.x",
            "MATCH_CONDITION(l.t>=r.t) TOLERANCE -1",
            "MATCH_CONDITION(l.t>=r.t) TOLERANCE 1.5",
            "MATCH_CONDITION(l.t>=r.t) TOLERANCE 18446744073709551616",
            "MATCH_CONDITION(NEAREST(l.t,r.t,0))",
            "MATCH_CONDITION(l.t>=r.t) EXTRA",
        ] {
            assert!(
                parse(&format!("'l' AS l ASOF JOIN 'r' AS r {suffix}")).is_err(),
                "{suffix}"
            );
        }
    }

    #[test]
    fn asof_tolerance_keyword_does_not_capture_qualified_partition_names() {
        for suffix in ["", " TOLERANCE 7"] {
            let parsed = parse(&format!("'l' AS tolerance ASOF JOIN 'r' AS r MATCH_CONDITION(tolerance.t>=r.t) ON (tolerance.tolerance=r.tolerance) AND tolerance.and=r.and{suffix}"))
                .unwrap().unwrap();
            let join = parsed.join.unwrap();
            assert_eq!(join.key_pairs.len(), 2);
            assert_eq!(join.key_pairs[0].left.column, "tolerance");
            assert_eq!(
                join.asof.unwrap().options.tolerance,
                if suffix.is_empty() { None } else { Some(7) }
            );
        }
    }
}
