//! Parameter inventory is lexical; values bind typed scalar syntax nodes.
//! Include unused WITH declarations in the parameter contract, while ignoring
//! quoted text. Nothing here rewrites a statement or evaluates a value.

use super::{BTreeMap, BTreeSet, NativeResult, unsupported_sql_error, validate_sql_identifier};

pub(super) fn validate<T>(raw: &str, values: &BTreeMap<String, T>) -> NativeResult<()> {
    let expected = names(raw)?;
    for name in values.keys() {
        validate_sql_identifier(name)?;
    }
    let supplied = values.keys().cloned().collect::<BTreeSet<_>>();
    if let Some(name) = expected.difference(&supplied).next() {
        return Err(unsupported_sql_error(&format!(
            "missing scalar parameter :{name}"
        )));
    }
    if let Some(name) = supplied.difference(&expected).next() {
        return Err(unsupported_sql_error(&format!(
            "extra scalar parameter :{name}"
        )));
    }
    Ok(())
}

pub(super) fn names(raw: &str) -> NativeResult<BTreeSet<String>> {
    let mut chars = raw.chars().peekable();
    let mut names = BTreeSet::new();
    while let Some(ch) = chars.next() {
        if ch == '\'' || ch == '"' {
            loop {
                let Some(next) = chars.next() else {
                    return Err(unsupported_sql_error("SQL quoted value is not closed"));
                };
                if next == ch {
                    if chars.peek() == Some(&ch) {
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
        } else if ch == ':' {
            let mut name = String::new();
            while chars
                .peek()
                .is_some_and(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            {
                name.push(chars.next().expect("peeked parameter character"));
            }
            validate_sql_identifier(&name)?;
            names.insert(name);
        } else if ch == '?' || ch == '$' {
            return Err(unsupported_sql_error(
                "positional parameters are not admitted; use named scalar parameters :name",
            ));
        }
    }
    Ok(names)
}
