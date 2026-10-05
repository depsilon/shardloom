//! Bounded nonrecursive CTE declarations lower to ordinary derived relations.
//! This is syntax lowering only; preparation, scans and execution remain shared.

use super::{NativeResult, matching_closing_parenthesis, unsupported_sql_error};
use std::collections::{BTreeMap, BTreeSet};

const MAX_BYTES: usize = 256 * 1024;

pub(super) fn expand(statement: String) -> NativeResult<String> {
    let tokens = words(&statement);
    if tokens
        .first()
        .is_none_or(|&(start, end)| !statement[start..end].eq_ignore_ascii_case("with"))
    {
        return Ok(statement);
    }
    let mut tail = statement[4..].trim_start();
    let mut definitions = Vec::new();
    let mut names = BTreeSet::new();
    loop {
        let end = tail
            .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
            .unwrap_or(tail.len());
        let name = &tail[..end];
        super::validate_sql_identifier(name)?;
        if name.eq_ignore_ascii_case("recursive") {
            return Err(unsupported_sql_error(
                "recursive CTEs require a recursive native plan and are not admitted",
            ));
        }
        if definitions.len() >= 64 || !names.insert(name.to_ascii_lowercase()) {
            return Err(unsupported_sql_error(
                "WITH requires at most 64 distinct CTE names",
            ));
        }
        tail = tail[end..].trim_start();
        if !tail
            .get(..2)
            .is_some_and(|word| word.eq_ignore_ascii_case("as"))
        {
            return Err(unsupported_sql_error(
                "CTE declarations require name AS (query); column lists and materialization hints are not admitted",
            ));
        }
        tail = tail[2..].trim_start();
        if !tail.starts_with('(') {
            return Err(unsupported_sql_error(
                "CTE AS requires a parenthesized query",
            ));
        }
        let close = matching_closing_parenthesis(tail, 0)?
            .ok_or_else(|| unsupported_sql_error("CTE query parentheses must be balanced"))?;
        definitions.push((name.to_owned(), &tail[1..close]));
        tail = tail[close + 1..].trim_start();
        if let Some(next) = tail.strip_prefix(',') {
            tail = next.trim_start();
        } else {
            break;
        }
    }
    let mut scope = BTreeMap::new();
    for (name, query) in definitions {
        let query = substitute(query, &scope, &names)?;
        super::relation_sources::validate_query_structure(&query)?;
        scope.insert(name.to_ascii_lowercase(), query);
    }
    let expanded = substitute(tail, &scope, &names)?;
    super::relation_sources::validate_query_structure(&expanded)?;
    Ok(expanded)
}

fn substitute(
    raw: &str,
    scope: &BTreeMap<String, String>,
    names: &BTreeSet<String>,
) -> NativeResult<String> {
    let tokens = words(raw);
    if tokens
        .iter()
        .any(|&(start, end)| raw[start..end].eq_ignore_ascii_case("with"))
    {
        return Err(unsupported_sql_error(
            "nested WITH scopes are not admitted; declare dependencies in the outer WITH list",
        ));
    }
    let mut result = String::new();
    let mut copied = 0;
    for pair in tokens.windows(2) {
        let [(start, end), (source, finish)] = pair else {
            unreachable!("two-token window")
        };
        if !(raw[*start..*end].eq_ignore_ascii_case("from")
            || raw[*start..*end].eq_ignore_ascii_case("join"))
            || !raw[*end..*source].chars().all(char::is_whitespace)
        {
            continue;
        }
        let name = &raw[*source..*finish];
        let canonical = name.to_ascii_lowercase();
        if !names.contains(&canonical) {
            continue;
        }
        let query = scope.get(&canonical).ok_or_else(|| {
            unsupported_sql_error("CTE self-reference and forward references are not admitted")
        })?;
        let remaining = raw[*finish..].trim_start();
        let explicit_alias = remaining
            .get(..2)
            .is_some_and(|word| word.eq_ignore_ascii_case("as"))
            && remaining
                .as_bytes()
                .get(2)
                .is_some_and(u8::is_ascii_whitespace);
        let additional =
            *source - copied + query.len() + 2 + if explicit_alias { 0 } else { 4 + name.len() };
        if result
            .len()
            .saturating_add(additional)
            .saturating_add(raw.len() - *finish)
            > MAX_BYTES
        {
            return Err(unsupported_sql_error(
                "expanded CTE declaration exceeds 256 KiB",
            ));
        }
        result.push_str(&raw[copied..*source]);
        result.push('(');
        result.push_str(query);
        result.push(')');
        if !explicit_alias {
            result.push_str(" AS ");
            result.push_str(name);
        }
        copied = *finish;
    }
    result.push_str(&raw[copied..]);
    Ok(result)
}

/// Word spans outside single-quoted literals; punctuation is preserved verbatim.
fn words(raw: &str) -> Vec<(usize, usize)> {
    let bytes = raw.as_bytes();
    let mut spans = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            index += 1;
            while index < bytes.len() {
                if bytes[index] == b'\'' {
                    index += 1;
                    if bytes.get(index) != Some(&b'\'') {
                        break;
                    }
                }
                index += 1;
            }
        } else if bytes[index].is_ascii_alphabetic() || bytes[index] == b'_' {
            let start = index;
            index += 1;
            while bytes
                .get(index)
                .is_some_and(|ch| ch.is_ascii_alphanumeric() || *ch == b'_')
            {
                index += 1;
            }
            spans.push((start, index));
        } else {
            index += 1;
        }
    }
    spans
}
