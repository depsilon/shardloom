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
    shardloom_plan::sql_binder::extract_named_parameters(raw)
}
