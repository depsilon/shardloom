//! Inert, recursively owned query sources. A derived relation is never a path.

use super::*;
use std::sync::Arc;

#[path = "sql_relation_unary.rs"]
mod unary;

pub(super) fn validate_query_structure(raw: &str) -> Result<(), ShardLoomError> {
    if raw.len() > 256 * 1024 {
        return Err(unsupported_sql_error("SQL exceeds 256 KiB"));
    }
    let mut chars = raw.chars().peekable();
    let mut quoted = false;
    let mut depth = 0usize;
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if quoted && chars.peek() == Some(&'\'') {
                chars.next();
            } else {
                quoted = !quoted;
            }
        } else if !quoted {
            match ch {
                '(' => {
                    depth += 1;
                    if depth > 24 {
                        return Err(unsupported_sql_error("SQL exceeds 24 nesting levels"));
                    }
                }
                ')' => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or_else(|| unsupported_sql_error("SQL parentheses must be balanced"))?;
                }
                _ => {}
            }
        }
    }
    if quoted {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    if depth != 0 {
        return Err(unsupported_sql_error("SQL parentheses must be balanced"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum ParsedRelationSource {
    Local(ParsedRelationLeaf),
    Derived(Arc<ParsedRelationQuery>),
    Unary(Box<ParsedRelationUnary>),
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ParsedRelationUnary {
    pub(super) input: Arc<ParsedRelationQuery>,
    pub(super) request: shardloom_vortex::VortexQueryPrimitiveRequest,
}

/// Keep a declared table identifier distinct from an exact quoted file path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ParsedRelationLeaf {
    pub(crate) path: PathBuf,
    pub(crate) declared_identifier: bool,
    pub(crate) memory_input: Option<crate::native_memory_input::MemoryInput>,
}

impl ParsedRelationLeaf {
    pub(super) fn parse(raw: &str) -> Result<Self, ShardLoomError> {
        Ok(Self {
            path: parse_source_path(raw)?,
            declared_identifier: validate_sql_identifier(raw).is_ok(),
            memory_input: None,
        })
    }

    pub(super) fn memory(
        input: crate::native_memory_input::MemoryInput,
    ) -> Result<Self, ShardLoomError> {
        use sha2::{Digest as _, Sha256};
        input.validate()?;
        let declaration = serde_json::to_vec(&input)
            .map_err(|error| unsupported_sql_error(&error.to_string()))?;
        let mut uri = String::from("memory://sql-input/");
        for byte in Sha256::digest(&declaration) {
            write!(&mut uri, "{byte:02x}").expect("String writes cannot fail");
        }
        Ok(Self {
            path: PathBuf::from(uri),
            declared_identifier: false,
            memory_input: Some(input),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum ParsedRelationQuery {
    Select(Box<ParsedSqlLocalSource>),
    Set(Box<ParsedSqlLocalSourceUnion>),
}

#[cfg(test)]
impl ParsedRelationSource {
    /// Inspect a file leaf in parser tests without executing the declaration.
    pub(super) fn local_path(&self) -> Result<&Path, ShardLoomError> {
        match self {
            Self::Local(leaf) if leaf.memory_input.is_none() => Ok(&leaf.path),
            Self::Local(_) | Self::Derived(_) | Self::Unary(_) => Err(unsupported_sql_error(
                "expected a file leaf in the parsed relation",
            )),
        }
    }
}

impl std::fmt::Display for ParsedRelationSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(leaf) => leaf.path.display().fmt(formatter),
            Self::Derived(query) => write!(formatter, "derived({})", query.statement()),
            Self::Unary(operation) => write!(
                formatter,
                "unary({:?}; {})",
                operation.request.kind,
                operation.input.statement()
            ),
        }
    }
}

impl ParsedRelationQuery {
    pub(super) fn parse(raw: &str) -> Result<Self, ShardLoomError> {
        let mut statement = normalize_sql_statement(raw)?;
        let synthetic = top_level_keyword_indexes(&statement, "limit")?.is_empty();
        if synthetic {
            write!(&mut statement, " LIMIT {}", usize::MAX).expect("String writes cannot fail");
        }
        if top_level_sql_union_operators(&statement)?.is_empty() {
            let mut parsed = parse_sql_local_source_statement(&statement)?;
            parsed.limit_is_synthetic = synthetic;
            Ok(Self::Select(Box::new(parsed)))
        } else {
            Ok(Self::Set(Box::new(parse_sql_local_source_union_statement(
                &statement,
            )?)))
        }
    }

    fn statement(&self) -> &str {
        match self {
            Self::Select(select) => &select.normalized_statement,
            Self::Set(set) => &set.normalized_statement,
        }
    }
}

pub(super) fn parse_source_clause(raw: &str) -> Result<ParsedSourceClause, ShardLoomError> {
    let Some((join_index, join_keyword_len, join_type)) = find_join_keyword(raw)? else {
        let (source, source_alias) = parse_source(raw, false)?;
        return Ok(ParsedSourceClause {
            source,
            source_alias,
            join: None,
        });
    };
    let left_raw = raw[..join_index].trim();
    let join_tail = raw[join_index + join_keyword_len..].trim();
    let on_index = find_keyword_outside_quotes_and_parentheses(join_tail, "on")?;
    let (right_raw, join_on) = if join_type.requires_equi_on() {
        let on_index = on_index.ok_or_else(|| {
            unsupported_sql_error("JOIN requires an ON predicate after its right source")
        })?;
        (
            join_tail[..on_index].trim(),
            parse_join_on(join_tail[on_index + "on".len()..].trim())?,
        )
    } else {
        if on_index.is_some() {
            return Err(unsupported_sql_error(
                "CROSS JOIN does not admit an ON clause; use WHERE for filters",
            ));
        }
        (
            join_tail,
            ParsedJoinOn {
                key_pairs: Vec::new(),
                predicate: None,
                predicate_family: ParsedJoinOnPredicateFamily::NotApplicable,
            },
        )
    };
    if join_type != ParsedJoinType::InnerEqui
        && join_on
            .predicate
            .as_ref()
            .is_some_and(ParsedPredicate::contains_logical_or)
    {
        return Err(unsupported_sql_error(
            "logical OR JOIN ON predicates are admitted only for INNER JOIN in this runtime slice; outer/semi/anti OR join semantics remain deterministic blockers",
        ));
    }
    let (source, left_alias) = parse_source(left_raw, true)?;
    let (right_source, right_alias) = parse_source(right_raw, true)?;
    let left_alias = left_alias.expect("required source alias was checked");
    let right_alias = right_alias.expect("required source alias was checked");
    if left_alias == right_alias {
        return Err(unsupported_sql_error(
            "JOIN requires distinct left and right aliases",
        ));
    }
    if join_on
        .key_pairs
        .iter()
        .any(|pair| pair.left.alias != left_alias || pair.right.alias != right_alias)
    {
        return Err(unsupported_sql_error(
            "JOIN ON predicates must be ordered as <left_alias>.<column> = <right_alias>.<column>",
        ));
    }
    Ok(ParsedSourceClause {
        source,
        source_alias: Some(left_alias),
        join: Some(ParsedJoin {
            join_type,
            right_source,
            right_alias,
            key_pairs: join_on.key_pairs,
            on_predicate: join_on.predicate,
            on_predicate_family: join_on.predicate_family,
        }),
    })
}

pub(super) fn parse_source(
    raw: &str,
    require_alias: bool,
) -> Result<(ParsedRelationSource, Option<String>), ShardLoomError> {
    let raw = raw.trim();
    let as_index = find_keyword_outside_quotes_and_parentheses(raw, "as")?;
    let (relation, alias) = if let Some(index) = as_index {
        let alias = raw[index + "as".len()..].trim();
        validate_sql_identifier(alias)?;
        if alias.eq_ignore_ascii_case("outer") {
            return Err(unsupported_sql_error(
                "source alias 'outer' is reserved for correlation",
            ));
        }
        (raw[..index].trim(), Some(alias.to_owned()))
    } else {
        (raw, None)
    };
    if (require_alias || relation.starts_with('(')) && alias.is_none() {
        return Err(unsupported_sql_error(
            "JOIN and derived sources require <source> AS <alias> syntax",
        ));
    }
    let source = if relation.starts_with('(') {
        let close = matching_closing_parenthesis(relation, 0)?
            .ok_or_else(|| unsupported_sql_error("derived source parentheses must be balanced"))?;
        if close + 1 != relation.len() {
            return Err(unsupported_sql_error(
                "unexpected text after a derived source",
            ));
        }
        ParsedRelationSource::Derived(Arc::new(ParsedRelationQuery::parse(&relation[1..close])?))
    } else if let Some(input) = super::memory_inputs::parse_range(relation)? {
        ParsedRelationSource::Local(ParsedRelationLeaf::memory(input)?)
    } else if let Some(unary) = unary::parse(relation)? {
        if alias.is_none() {
            return Err(unsupported_sql_error(
                "unary table expressions require AS <alias>",
            ));
        }
        ParsedRelationSource::Unary(Box::new(unary))
    } else {
        ParsedRelationSource::Local(ParsedRelationLeaf::parse(relation)?)
    };
    Ok((source, alias))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_sources_are_parsed_without_opening_or_inventing_files() {
        let parsed = ParsedRelationQuery::parse(
            "SELECT q.value FROM (SELECT value FROM 'missing-left.vortex' LIMIT 2) AS q \
             LEFT JOIN (SELECT value FROM 'missing-right.vortex' UNION ALL \
             SELECT value FROM 'missing-third.vortex') AS r ON q.value = r.value",
        )
        .unwrap();
        let ParsedRelationQuery::Select(parsed) = parsed else {
            panic!("SELECT expected")
        };
        assert!(parsed.limit_is_synthetic);
        let ParsedRelationSource::Derived(left) = &parsed.source else {
            panic!("derived left expected")
        };
        let ParsedRelationQuery::Select(left) = left.as_ref() else {
            panic!("SELECT expected")
        };
        assert_eq!(
            left.source.local_path().unwrap(),
            Path::new("missing-left.vortex")
        );
        assert_eq!(left.limit, 2);
        assert!(!left.limit_is_synthetic);
        assert!(
            parsed
                .source
                .local_path()
                .unwrap_err()
                .to_string()
                .contains("expected a file leaf")
        );
        let right = &parsed.join.as_ref().unwrap().right_source;
        assert!(
            matches!(right, ParsedRelationSource::Derived(query) if matches!(query.as_ref(), ParsedRelationQuery::Set(_)))
        );
    }

    #[test]
    fn nested_join_keywords_do_not_change_the_outer_source_clause() {
        let parsed = ParsedRelationQuery::parse(
            "SELECT value FROM (SELECT l.value AS value FROM 'missing join on (.vortex' AS l \
             JOIN 'right.vortex' AS r ON l.value = r.value) AS q",
        )
        .unwrap();
        let ParsedRelationQuery::Select(parsed) = parsed else {
            panic!("SELECT expected")
        };
        assert!(parsed.join.is_none());
        assert_eq!(parsed.source_alias.as_deref(), Some("q"));
    }

    #[test]
    fn malformed_derived_scopes_fail_before_source_access() {
        for sql in [
            "SELECT * FROM (SELECT value FROM 'missing.vortex')",
            "SELECT * FROM (SELECT value FROM 'missing.vortex') extra AS q",
            "SELECT * FROM (SELECT value FROM 'missing.vortex') AS q JOIN 'b.vortex' AS q ON q.value = q.value",
            "SELECT * FROM (SELECT value FROM 'missing.vortex') AS outer",
            "SELECT * FROM (SELECT value FROM 'missing.vortex' AS q",
        ] {
            assert!(ParsedRelationQuery::parse(sql).is_err(), "{sql}");
        }
        let mut sql = "SELECT value FROM 'missing.vortex'".to_owned();
        for _ in 0..25 {
            sql = format!("SELECT value FROM ({sql}) AS q");
        }
        assert!(
            ParsedRelationQuery::parse(&sql)
                .unwrap_err()
                .to_string()
                .contains("24 nesting levels")
        );
    }
}
