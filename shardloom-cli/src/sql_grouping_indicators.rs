//! Rewrite grouping indicators into inert aggregate output references.

use super::super::{
    aggregate_expression_end, aggregate_function_prefix_at, contains_function_call_outside_quotes,
    find_keyword_outside_quotes_and_parentheses, matching_closing_parenthesis, split_sql_csv,
    sql_identifier_boundary_before, starts_with_keyword, unsupported_sql_error,
};
use super::{Indicator, Result};

pub(in super::super) struct Rewrite<'a> {
    statement: &'a str,
    pub(in super::super) indicators: Vec<Indicator>,
}

impl<'a> Rewrite<'a> {
    pub(in super::super) fn new(statement: &'a str) -> Self {
        Self {
            statement,
            indicators: vec![],
        }
    }

    pub(in super::super) fn select(&mut self, raw: &str) -> Result<String> {
        let mut entries = Vec::new();
        for entry in split_sql_csv(raw)? {
            let default_alias =
                if find_keyword_outside_quotes_and_parentheses(&entry, "as")?.is_none() {
                    indicator_call(&entry, 0)?
                        .filter(|(_, _, close)| *close + 1 == entry.len())
                        .map(|(name, _, _)| name.to_ascii_lowercase())
                } else {
                    None
                };
            let mut value = self.expression(&entry)?;
            if let Some(alias) = default_alias {
                value.push_str(" AS ");
                value.push_str(&alias);
            }
            entries.push(value);
        }
        Ok(entries.join(","))
    }

    pub(in super::super) fn expression(&mut self, raw: &str) -> Result<String> {
        if !["grouping", "grouping_id"]
            .iter()
            .any(|name| contains_function_call_outside_quotes(raw, name))
        {
            return Ok(raw.to_owned());
        }
        let mut output = String::with_capacity(raw.len());
        let mut index = 0;
        let mut cursor = 0;
        let bytes = raw.as_bytes();
        let mut quote = None;
        while index < raw.len() {
            if let Some(delimiter) = quote {
                if bytes[index] == delimiter {
                    if bytes.get(index + 1) == Some(&delimiter) {
                        index += 2;
                        continue;
                    }
                    quote = None;
                }
                index += 1;
                continue;
            }
            if matches!(bytes[index], b'\'' | b'"') {
                quote = Some(bytes[index]);
                index += 1;
                continue;
            }
            if bytes[index] == b'(' && starts_with_keyword(raw[index + 1..].trim_start(), "select")
            {
                index = matching_closing_parenthesis(raw, index)?.ok_or_else(|| {
                    unsupported_sql_error("GROUPING subquery parentheses are not closed")
                })? + 1;
                continue;
            }
            if let Some(function) = aggregate_function_prefix_at(raw, index) {
                let end = aggregate_expression_end(raw, index + function.len())?;
                if !starts_with_keyword(raw[end + 1..].trim_start(), "over") {
                    // An ordinary aggregate cannot observe its own result mask.
                    // Parse the argument scope separately so subqueries retain theirs.
                    let mut nested = Self::new(self.statement);
                    nested.expression(&raw[index + function.len() + 1..end])?;
                    if !nested.indicators.is_empty() {
                        return Err(unsupported_sql_error(
                            "GROUPING cannot appear inside an aggregate argument or FILTER",
                        ));
                    }
                    index = end + 1;
                    continue;
                }
            }
            if let Some((_, open, close)) = indicator_call(raw, index)? {
                let columns = split_sql_csv(raw[open + 1..close].trim())?;
                if columns.is_empty() || columns.len() > 64 {
                    return Err(unsupported_sql_error(
                        "GROUPING requires between one and 64 grouping expressions",
                    ));
                }
                let alias = self.alias(columns)?;
                output.push_str(&raw[cursor..index]);
                output.push_str(&alias);
                index = close + 1;
                cursor = index;
                continue;
            }
            index += 1;
        }
        output.push_str(&raw[cursor..]);
        Ok(output)
    }

    fn alias(&mut self, columns: Vec<String>) -> Result<String> {
        if let Some(existing) = self
            .indicators
            .iter()
            .find(|value| value.columns == columns)
        {
            return Ok(existing.alias.clone());
        }
        let mut suffix = self.indicators.len();
        loop {
            let name = format!("__shardloom_grouping_{suffix}");
            suffix = suffix
                .checked_add(1)
                .ok_or_else(|| unsupported_sql_error("GROUPING helper name overflow"))?;
            if !self.statement.contains(&name)
                && !self.indicators.iter().any(|value| value.alias == name)
            {
                self.indicators.push(Indicator {
                    columns,
                    alias: name.clone(),
                });
                return Ok(name);
            }
        }
    }
}

fn indicator_call(raw: &str, index: usize) -> Result<Option<(&str, usize, usize)>> {
    if !sql_identifier_boundary_before(raw, index)
        || raw.as_bytes().get(index.wrapping_sub(1)) == Some(&b'.')
    {
        return Ok(None);
    }
    let Some(rest) = raw.get(index..) else {
        return Ok(None);
    };
    for name in ["grouping_id", "grouping"] {
        if !rest
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
        {
            continue;
        }
        let trailing = &rest[name.len()..];
        let open = index + name.len() + trailing.len() - trailing.trim_start().len();
        if raw.as_bytes().get(open) != Some(&b'(') {
            continue;
        }
        let close = matching_closing_parenthesis(raw, open)?
            .ok_or_else(|| unsupported_sql_error("GROUPING parentheses are not closed"))?;
        return Ok(Some((&rest[..name.len()], open, close)));
    }
    Ok(None)
}
