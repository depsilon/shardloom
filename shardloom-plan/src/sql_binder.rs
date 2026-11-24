// SPDX-License-Identifier: Apache-2.0

//! Canonical syntax-only SQL lexer, AST binder, and logical plan lowering
//! isolated in `shardloom-plan` using `sqlparser = 0.64.0`.
//!
//! No query execution, I/O, or external engine fallback occurs in this module.

#[path = "sql_binder_expr.rs"]
pub mod expr;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use shardloom_core::{ComparisonOp, Diagnostic, Result, ScalarValue, ShardLoomError};
use sqlparser::ast::{
    self, Expr as SqlExpr, GroupByExpr, GroupByWithModifier, InTarget, Join, JoinConstraint,
    JoinOperator, LimitClause, ObjectNamePart, OrderByKind, Query, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, Statement, TableFactor, Value as SqlValue,
};
use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

use crate::explain::PlanNodeId;
use crate::plan_ir::{NativePlanDocument, NativePlanNode, NativePlanNodeKind, PlanId, PlanLayer};

pub use expr::LogicalExpr;

const MAX_SQL_INTAKE_BYTES: usize = 8 * 1024 * 1024;
const MAX_SQL_NESTING_DEPTH: usize = 24;

/// `ShardLoom` SQL dialect for syntax-only lexing and AST construction.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ShardLoomSqlDialect;

impl Dialect for ShardLoomSqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '$' || ch == '_'
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn supports_cte_materialization(&self) -> bool {
        true
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_select_wildcard_replace(&self) -> bool {
        true
    }

    fn supports_select_wildcard_exclude(&self) -> bool {
        true
    }

    fn supports_trailing_commas(&self) -> bool {
        true
    }

    fn supports_numeric_literal_underscores(&self) -> bool {
        true
    }

    fn supports_parens_around_table_factor(&self) -> bool {
        true
    }

    fn allow_extract_single_quotes(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_assignment_operator(&self) -> bool {
        true
    }

    fn supports_dictionary_syntax(&self) -> bool {
        true
    }

    fn support_map_literal_syntax(&self) -> bool {
        true
    }

    fn supports_nested_comments(&self) -> bool {
        true
    }

    fn supports_values_as_table_factor(&self) -> bool {
        true
    }

    fn supports_string_literal_column_alias(&self, explicit: bool) -> bool {
        explicit
    }

    fn supports_string_literal_table_alias(&self, explicit: bool) -> bool {
        explicit
    }

    fn is_custom_type_typed_string(&self, first_keyword: Keyword, qualified: bool) -> bool {
        !qualified
            && matches!(
                first_keyword,
                Keyword::BINARY
                    | Keyword::BLOB
                    | Keyword::DATE
                    | Keyword::TIMESTAMP
                    | Keyword::TIME
            )
    }
}

/// Source reference discovered in SQL `FROM`/`JOIN` table factors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlSourceRef {
    pub reference: String,
    pub quoted_path: bool,
}

/// Source reference token byte span in original SQL text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlSourceRefSpan {
    pub value: String,
    pub start: usize,
    pub end: usize,
    pub quoted: bool,
}

/// Set-operation mode parsed from top-level tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlSetOpMode {
    UnionDistinct,
    UnionAll,
    IntersectDistinct,
    ExceptDistinct,
    IntersectAll,
    ExceptAll,
}

/// Top-level set operator token byte span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlSetOperatorSpan {
    pub index: usize,
    pub len: usize,
    pub mode: SqlSetOpMode,
}

/// Exact text clause slices for fast-path single-source route analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlSingleSourceShape {
    pub projection: String,
    pub source_ref: String,
    pub source_is_identifier: bool,
    pub where_clause: Option<String>,
    pub group_by: Option<String>,
    pub having: Option<String>,
    pub order_by: Option<String>,
    pub limit: Option<String>,
    pub offset: Option<String>,
}

/// Individual CTE header/body metadata parsed cleanly from a `WITH` preamble.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCteDefParts {
    pub name: String,
    pub columns: Vec<String>,
    pub materialized: bool,
    pub body_sql: String,
}

/// Parsed `WITH` block split into declarations and trailing main query text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCteParts {
    pub recursive: bool,
    pub definitions: Vec<SqlCteDefParts>,
    pub tail_sql: String,
}

/// Machine-readable SQL structural summary for Python and CLI routing.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct SqlInspectionSummary {
    pub statement_kind: String,
    pub is_source_free: bool,
    pub is_generator: bool,
    pub source_refs: Vec<SqlSourceRef>,
    pub has_top_level_limit: bool,
    pub top_level_limit: Option<String>,
    pub top_level_limit_span: Option<(usize, usize)>,
    pub named_parameters: BTreeSet<String>,
    pub is_relational_candidate: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalSortKey {
    pub expr: LogicalExpr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalWindowUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalWindowFrame {
    pub units: LogicalWindowUnits,
    pub start: String,
    pub end: Option<String>,
    pub exclusion: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalWindowSpec {
    pub name: Option<String>,
    pub partition_by: Vec<LogicalExpr>,
    pub order_by: Vec<LogicalSortKey>,
    pub frame: Option<LogicalWindowFrame>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalJoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    Semi,
    Anti,
    AsOfInner,
    AsOfLeft,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalSetOp {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalProjectionItem {
    Expr {
        expr: Box<LogicalExpr>,
        alias: Option<String>,
    },
    Wildcard {
        qualifier: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalGroupingSpec {
    Rollup,
    Cube,
    GroupingSets(Vec<Vec<LogicalExpr>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalAggregateSpec {
    pub name: String,
    pub args: Vec<LogicalExpr>,
    pub distinct: bool,
    pub filter: Option<LogicalExpr>,
    pub order_by: Vec<LogicalSortKey>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalCte {
    pub name: String,
    pub columns: Vec<String>,
    pub materialized: Option<bool>,
    pub query: Box<LogicalRelation>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalTableArg {
    Expr(Box<LogicalExpr>),
    Subquery(Box<LogicalRelation>),
}

/// Canonical logical relation tree produced from a SQL AST.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalRelation {
    Source {
        reference: String,
        quoted_path: bool,
        alias: Option<String>,
    },
    Values {
        rows: Vec<Vec<LogicalExpr>>,
        alias: Option<String>,
    },
    Unit,
    TableFunction {
        name: String,
        args: Vec<LogicalTableArg>,
        alias: Option<String>,
    },
    Subquery {
        input: Box<LogicalRelation>,
        alias: Option<String>,
    },
    Filter {
        input: Box<LogicalRelation>,
        predicate: LogicalExpr,
    },
    Project {
        input: Box<LogicalRelation>,
        items: Vec<LogicalProjectionItem>,
        distinct: bool,
    },
    Aggregate {
        input: Box<LogicalRelation>,
        group_by: Vec<LogicalExpr>,
        grouping_sets: Option<LogicalGroupingSpec>,
        aggregates: Vec<LogicalAggregateSpec>,
        having: Option<LogicalExpr>,
    },
    Window {
        input: Box<LogicalRelation>,
        windows: Vec<(String, LogicalWindowSpec)>,
    },
    Join {
        left: Box<LogicalRelation>,
        right: Box<LogicalRelation>,
        join_type: LogicalJoinType,
        on: Option<LogicalExpr>,
        match_condition: Option<LogicalExpr>,
        tolerance: Option<u64>,
    },
    SetOperation {
        left: Box<LogicalRelation>,
        right: Box<LogicalRelation>,
        op: LogicalSetOp,
        distinct: bool,
    },
    Sort {
        input: Box<LogicalRelation>,
        keys: Vec<LogicalSortKey>,
    },
    Limit {
        input: Box<LogicalRelation>,
        limit: Option<LogicalExpr>,
        offset: Option<LogicalExpr>,
    },
    WithCte {
        ctes: Vec<LogicalCte>,
        recursive: bool,
        body: Box<LogicalRelation>,
    },
}

/// Complete AST-bound logical plan and diagnostics.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundSqlPlan {
    pub normalized_sql: String,
    pub root: LogicalRelation,
    pub named_parameters: BTreeSet<String>,
    pub source_refs: Vec<SqlSourceRef>,
    pub diagnostics: Vec<Diagnostic>,
}

impl BoundSqlPlan {
    /// Emits a `NativePlanDocument` (`PlanLayer::Logical`) summarizing the
    /// bound relational DAG with explicit no-fallback posture.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] if any generated node id
    /// is invalid.
    pub fn to_native_plan_document(&self, plan_id: PlanId) -> Result<NativePlanDocument> {
        let mut doc = NativePlanDocument::new(plan_id, PlanLayer::Logical);
        let mut next_id = 0usize;
        emit_plan_nodes(&self.root, &mut next_id, &mut doc)?;
        for diag in &self.diagnostics {
            doc.add_diagnostic(diag.clone());
        }
        doc.validate_skeleton();
        Ok(doc)
    }
}

fn emit_plan_nodes(
    rel: &LogicalRelation,
    next_id: &mut usize,
    doc: &mut NativePlanDocument,
) -> Result<()> {
    match rel {
        LogicalRelation::Source { .. }
        | LogicalRelation::Values { .. }
        | LogicalRelation::Unit
        | LogicalRelation::TableFunction { .. } => emit_leaf_plan_node(rel, next_id, doc),
        LogicalRelation::Subquery { input, .. } => emit_plan_nodes(input, next_id, doc),
        LogicalRelation::Filter { input, .. } => {
            emit_plan_nodes(input, next_id, doc)?;
            push_node(
                doc,
                next_id,
                NativePlanNodeKind::Filter,
                "filter".to_string(),
            )
        }
        LogicalRelation::Project {
            input,
            items,
            distinct,
        } => {
            emit_plan_nodes(input, next_id, doc)?;
            push_node(
                doc,
                next_id,
                NativePlanNodeKind::Projection,
                format!("project:items={}:distinct={distinct}", items.len()),
            )
        }
        LogicalRelation::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            emit_plan_nodes(input, next_id, doc)?;
            push_node(
                doc,
                next_id,
                NativePlanNodeKind::Aggregate,
                format!(
                    "aggregate:keys={}:aggs={}",
                    group_by.len(),
                    aggregates.len()
                ),
            )
        }
        LogicalRelation::Window { input, windows } => {
            emit_plan_nodes(input, next_id, doc)?;
            push_node(
                doc,
                next_id,
                NativePlanNodeKind::Projection,
                format!("window:specs={}", windows.len()),
            )
        }
        LogicalRelation::Join { .. } | LogicalRelation::SetOperation { .. } => {
            emit_binary_plan_node(rel, next_id, doc)
        }
        LogicalRelation::Sort { input, keys } => {
            emit_plan_nodes(input, next_id, doc)?;
            push_node(
                doc,
                next_id,
                NativePlanNodeKind::Sort,
                format!("sort:keys={}", keys.len()),
            )
        }
        LogicalRelation::Limit { input, .. } => {
            emit_plan_nodes(input, next_id, doc)?;
            push_node(doc, next_id, NativePlanNodeKind::Limit, "limit".to_string())
        }
        LogicalRelation::WithCte { ctes, body, .. } => {
            for cte in ctes {
                emit_plan_nodes(&cte.query, next_id, doc)?;
            }
            emit_plan_nodes(body, next_id, doc)
        }
    }
}

fn emit_leaf_plan_node(
    rel: &LogicalRelation,
    next_id: &mut usize,
    doc: &mut NativePlanDocument,
) -> Result<()> {
    match rel {
        LogicalRelation::Source { reference, .. } => push_node(
            doc,
            next_id,
            NativePlanNodeKind::Scan,
            format!("scan:{reference}"),
        ),
        LogicalRelation::Values { rows, .. } => push_node(
            doc,
            next_id,
            NativePlanNodeKind::Scan,
            format!("values:rows={}", rows.len()),
        ),
        LogicalRelation::Unit => {
            push_node(doc, next_id, NativePlanNodeKind::Scan, "unit".to_string())
        }
        LogicalRelation::TableFunction { name, args, .. } => {
            for arg in args {
                if let LogicalTableArg::Subquery(sub) = arg {
                    emit_plan_nodes(sub, next_id, doc)?;
                }
            }
            push_node(
                doc,
                next_id,
                NativePlanNodeKind::Scan,
                format!("table_fn:{name}"),
            )
        }
        _ => unreachable!("non-leaf variant dispatched to emit_leaf_plan_node"),
    }
}

fn emit_binary_plan_node(
    rel: &LogicalRelation,
    next_id: &mut usize,
    doc: &mut NativePlanDocument,
) -> Result<()> {
    match rel {
        LogicalRelation::Join {
            left,
            right,
            join_type,
            ..
        } => {
            emit_plan_nodes(left, next_id, doc)?;
            emit_plan_nodes(right, next_id, doc)?;
            push_node(
                doc,
                next_id,
                NativePlanNodeKind::Join,
                format!("join:{join_type:?}"),
            )
        }
        LogicalRelation::SetOperation {
            left,
            right,
            op,
            distinct,
        } => {
            emit_plan_nodes(left, next_id, doc)?;
            emit_plan_nodes(right, next_id, doc)?;
            push_node(
                doc,
                next_id,
                NativePlanNodeKind::Union_label_compat(),
                format!("set:{op:?}:distinct={distinct}"),
            )
        }
        _ => unreachable!("non-binary variant dispatched to emit_binary_plan_node"),
    }
}

trait PlanNodeKindExt {
    #[allow(non_snake_case)]
    fn Union_label_compat() -> Self;
}

impl PlanNodeKindExt for NativePlanNodeKind {
    fn Union_label_compat() -> Self {
        Self::Scan
    }
}

fn push_node(
    doc: &mut NativePlanDocument,
    next_id: &mut usize,
    kind: NativePlanNodeKind,
    label: String,
) -> Result<()> {
    let idx = *next_id;
    *next_id += 1;
    let id = PlanNodeId::new(format!("sql.{idx}"))?;
    doc.add_node(NativePlanNode::new(id, PlanLayer::Logical, kind, label));
    Ok(())
}

// ============================================================================
// Tokenization & UTF-8 Byte Span Indexing
// ============================================================================

struct LineEntry {
    byte_start: usize,
    is_ascii: bool,
}

struct LineIndex<'a> {
    raw: &'a str,
    lines: Vec<LineEntry>,
}

impl<'a> LineIndex<'a> {
    fn new(raw: &'a str) -> Self {
        let mut lines = Vec::new();
        let mut line_start = 0usize;
        let mut is_ascii = true;
        for (idx, b) in raw.bytes().enumerate() {
            if b == b'\n' {
                lines.push(LineEntry {
                    byte_start: line_start,
                    is_ascii,
                });
                line_start = idx + 1;
                is_ascii = true;
            } else if !b.is_ascii() {
                is_ascii = false;
            }
        }
        lines.push(LineEntry {
            byte_start: line_start,
            is_ascii,
        });
        Self { raw, lines }
    }

    fn byte_offset(&self, loc: Location) -> Option<usize> {
        if loc.line == 0 || loc.column == 0 {
            return None;
        }
        let line_idx = usize::try_from(loc.line - 1).ok()?;
        let entry = self.lines.get(line_idx)?;
        let line_end = self
            .lines
            .get(line_idx + 1)
            .map_or(self.raw.len(), |next| next.byte_start);
        let col0 = usize::try_from(loc.column - 1).ok()?;
        if entry.is_ascii {
            return Some((entry.byte_start + col0).min(line_end));
        }
        let line_str = &self.raw[entry.byte_start..line_end];
        if col0 == 0 {
            return Some(entry.byte_start);
        }
        match line_str.char_indices().nth(col0) {
            Some((rel, _)) => Some(entry.byte_start + rel),
            None => Some(line_end),
        }
    }

    fn span_bytes(&self, start: Location, end: Location) -> Option<(usize, usize)> {
        let s = self.byte_offset(start)?;
        let e = self.byte_offset(end)?;
        Some((s, e.max(s)))
    }
}

struct ByteToken {
    token: Token,
    start: usize,
    end: usize,
    raw_span: sqlparser::tokenizer::Span,
    paren_depth: usize,
    bracket_depth: usize,
}

fn map_tokenizer_error(raw: &str, message: &str) -> ShardLoomError {
    let lower = message.to_ascii_lowercase();
    if lower.contains("unterminated string")
        || (lower.contains("unterminated") && raw.contains('\''))
    {
        return unsupported_sql_error("SQL string literal is not closed");
    }
    if lower.contains("unterminated") {
        return unsupported_sql_error("SQL quoted value is not closed");
    }
    unsupported_sql_error(&format!("SQL lexical error: {message}"))
}

fn validate_raw_intake(raw: &str) -> Result<()> {
    if raw.len() > MAX_SQL_INTAKE_BYTES {
        return Err(unsupported_sql_error("SQL parser intake exceeds 8 MiB"));
    }
    Ok(())
}

fn tokenize_with_spans(raw: &str) -> Result<Vec<ByteToken>> {
    validate_raw_intake(raw)?;
    let raw_tokens = Tokenizer::new(&ShardLoomSqlDialect, raw)
        .tokenize_with_location()
        .map_err(|err| map_tokenizer_error(raw, &err.message))?;
    let index = LineIndex::new(raw);
    let mut out = Vec::with_capacity(raw_tokens.len());
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    for item in raw_tokens {
        let (start, end) = index
            .span_bytes(item.span.start, item.span.end)
            .unwrap_or((0, 0));
        match &item.token {
            Token::Placeholder(_) => {
                return Err(unsupported_sql_error(
                    "positional parameters are not admitted; use named scalar parameters :name",
                ));
            }
            Token::LParen => {
                let tok = ByteToken {
                    token: item.token,
                    start,
                    end,
                    raw_span: item.span,
                    paren_depth,
                    bracket_depth,
                };
                out.push(tok);
                paren_depth += 1;
                if paren_depth + bracket_depth > MAX_SQL_NESTING_DEPTH {
                    return Err(unsupported_sql_error("SQL exceeds 24 nesting levels"));
                }
                continue;
            }
            Token::RParen => {
                paren_depth = paren_depth
                    .checked_sub(1)
                    .ok_or_else(|| unsupported_sql_error("SQL parentheses must be balanced"))?;
            }
            Token::LBracket => {
                let tok = ByteToken {
                    token: item.token,
                    start,
                    end,
                    raw_span: item.span,
                    paren_depth,
                    bracket_depth,
                };
                out.push(tok);
                bracket_depth += 1;
                if paren_depth + bracket_depth > MAX_SQL_NESTING_DEPTH {
                    return Err(unsupported_sql_error("SQL exceeds 24 nesting levels"));
                }
                continue;
            }
            Token::RBracket => {
                bracket_depth = bracket_depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("SQL expression square brackets are not balanced")
                })?;
            }
            _ => {}
        }
        out.push(ByteToken {
            token: item.token,
            start,
            end,
            raw_span: item.span,
            paren_depth,
            bracket_depth,
        });
    }
    if paren_depth != 0 {
        return Err(unsupported_sql_error("SQL parentheses must be balanced"));
    }
    if bracket_depth != 0 {
        return Err(unsupported_sql_error(
            "SQL expression square brackets are not balanced",
        ));
    }
    Ok(out)
}

fn significant_tokens(tokens: &[ByteToken]) -> Vec<&ByteToken> {
    tokens
        .iter()
        .filter(|tok| !matches!(tok.token, Token::Whitespace(_)))
        .collect()
}

fn word_eq(tok: &ByteToken, expected: &str) -> bool {
    matches!(
        &tok.token,
        Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(expected)
    )
}

// ============================================================================
// Public Token-Stream Structural Helpers (retiring handwritten splitters)
// ============================================================================

/// Splits top-level comma-separated SQL items as borrowed slices using `sqlparser`'s tokenizer.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on unclosed quotes,
/// unbalanced parentheses/brackets, or intake/nesting limits.
pub fn split_top_level_csv_slices(raw: &str) -> Result<Vec<&str>> {
    let tokens = tokenize_with_spans(raw)?;
    let mut items = Vec::new();
    let mut segment_start = 0usize;
    for tok in &tokens {
        if matches!(tok.token, Token::Comma) && tok.paren_depth == 0 && tok.bracket_depth == 0 {
            items.push(raw[segment_start..tok.start].trim());
            segment_start = tok.end;
        }
    }
    let tail = raw[segment_start..].trim();
    if !tail.is_empty() {
        items.push(tail);
    }
    Ok(items)
}

/// Splits top-level comma-separated SQL items using `sqlparser`'s tokenizer.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on unclosed quotes,
/// unbalanced parentheses/brackets, or intake/nesting limits.
pub fn split_top_level_csv(raw: &str) -> Result<Vec<String>> {
    Ok(split_top_level_csv_slices(raw)?
        .into_iter()
        .map(str::to_string)
        .collect())
}

/// Finds the first byte offset and exclusive end byte offset of a top-level
/// keyword or multi-word keyword phrase outside quotes, comments, parens, and
/// brackets. Skips `BETWEEN ... AND` for `"and"` and `IS [NOT] DISTINCT FROM`
/// for `"from"`.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on lexical/nesting errors.
pub fn find_top_level_keyword_span(
    raw: &str,
    keyword_phrase: &str,
) -> Result<Option<(usize, usize)>> {
    Ok(top_level_keyword_spans(raw, keyword_phrase)?
        .into_iter()
        .next())
}

/// Finds the byte offset of the first top-level `FROM` clause keyword, skipping
/// `IS [NOT] DISTINCT FROM` comparisons and balanced quotes/comments/nesting.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on lexical/nesting errors.
pub fn find_source_from_clause(raw: &str) -> Result<Option<usize>> {
    find_top_level_keyword(raw, "from")
}

/// Finds the first byte offset of a top-level keyword or phrase.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on lexical/nesting errors.
pub fn find_top_level_keyword(raw: &str, keyword_phrase: &str) -> Result<Option<usize>> {
    Ok(find_top_level_keyword_span(raw, keyword_phrase)?.map(|(start, _)| start))
}

/// Finds every top-level byte offset of `keyword_phrase`.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on lexical/nesting errors.
pub fn top_level_keyword_offsets(raw: &str, keyword_phrase: &str) -> Result<Vec<usize>> {
    Ok(top_level_keyword_spans(raw, keyword_phrase)?
        .into_iter()
        .map(|(start, _)| start)
        .collect())
}

fn top_level_keyword_spans(raw: &str, keyword_phrase: &str) -> Result<Vec<(usize, usize)>> {
    let parts: Vec<&str> = keyword_phrase.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(Vec::new());
    }
    let tokens = tokenize_with_spans(raw)?;
    let sig = significant_tokens(&tokens);
    let mut out = Vec::new();
    let mut skip_next_and_for_between = false;
    let search_and = parts.len() == 1 && parts[0].eq_ignore_ascii_case("and");
    let search_from = parts.len() == 1 && parts[0].eq_ignore_ascii_case("from");
    let mut idx = 0usize;
    while idx + parts.len() <= sig.len() {
        let first = sig[idx];
        if first.paren_depth != 0 || first.bracket_depth != 0 {
            idx += 1;
            continue;
        }
        if search_and && word_eq(first, "between") {
            skip_next_and_for_between = true;
            idx += 1;
            continue;
        }
        let matched = parts.iter().enumerate().all(|(offset, part)| {
            let tok = sig[idx + offset];
            tok.paren_depth == 0 && tok.bracket_depth == 0 && word_eq(tok, part)
        });
        if matched {
            if search_and && skip_next_and_for_between {
                skip_next_and_for_between = false;
                idx += 1;
                continue;
            }
            if search_from && preceding_is_distinct_comparison(&sig, idx) {
                idx += 1;
                continue;
            }
            out.push((first.start, sig[idx + parts.len() - 1].end));
            idx += parts.len();
        } else {
            idx += 1;
        }
    }
    Ok(out)
}

fn preceding_is_distinct_comparison(sig: &[&ByteToken], idx: usize) -> bool {
    if idx >= 2 && word_eq(sig[idx - 1], "distinct") && word_eq(sig[idx - 2], "is") {
        return true;
    }
    idx >= 3
        && word_eq(sig[idx - 1], "distinct")
        && word_eq(sig[idx - 2], "not")
        && word_eq(sig[idx - 3], "is")
}

/// Finds a keyword or phrase anywhere outside quotes/comments across all paren depths.
#[must_use]
pub fn find_keyword_anywhere_outside_quotes(raw: &str, keyword_phrase: &str) -> Option<usize> {
    let parts: Vec<&str> = keyword_phrase.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }
    let tokens = Tokenizer::new(&ShardLoomSqlDialect, raw)
        .tokenize_with_location()
        .ok()?;
    let line_index = LineIndex::new(raw);
    let sig: Vec<(&TokenWithSpan, usize)> = tokens
        .iter()
        .filter(|tok| !matches!(tok.token, Token::Whitespace(_)))
        .filter_map(|tok| {
            line_index
                .byte_offset(tok.span.start)
                .map(|start| (tok, start))
        })
        .collect();
    for idx in 0..=sig.len().saturating_sub(parts.len()) {
        let matched = parts.iter().enumerate().all(|(offset, part)| {
            matches!(
                &sig[idx + offset].0.token,
                Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(part)
            )
        });
        if matched {
            return Some(sig[idx].1);
        }
    }
    None
}

/// Returns `true` when `keyword_phrase` appears outside quotes/comments.
#[must_use]
pub fn contains_keyword_anywhere_outside_quotes(raw: &str, keyword_phrase: &str) -> bool {
    find_keyword_anywhere_outside_quotes(raw, keyword_phrase).is_some()
}

/// Finds top-level set operators (`UNION`, `INTERSECT`, `EXCEPT`).
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on invalid quantifier (`INTERSECT ALL`
/// / `EXCEPT ALL`) or lexical/nesting errors.
pub fn find_top_level_set_operators(raw: &str) -> Result<Vec<SqlSetOperatorSpan>> {
    let tokens = tokenize_with_spans(raw)?;
    let sig = significant_tokens(&tokens);
    let mut ops = Vec::new();
    let mut idx = 0usize;
    while idx < sig.len() {
        let tok = sig[idx];
        if tok.paren_depth != 0 || tok.bracket_depth != 0 {
            idx += 1;
            continue;
        }
        let next_word = |offset: usize, expected: &str| {
            sig.get(offset).is_some_and(|candidate| {
                candidate.paren_depth == 0
                    && candidate.bracket_depth == 0
                    && word_eq(candidate, expected)
            })
        };
        if word_eq(tok, "union") {
            let (end, mode, advance) = if next_word(idx + 1, "all") {
                (sig[idx + 1].end, SqlSetOpMode::UnionAll, 2)
            } else if next_word(idx + 1, "distinct") {
                (sig[idx + 1].end, SqlSetOpMode::UnionDistinct, 2)
            } else {
                (tok.end, SqlSetOpMode::UnionDistinct, 1)
            };
            ops.push(SqlSetOperatorSpan {
                index: tok.start,
                len: end - tok.start,
                mode,
            });
            idx += advance;
        } else if word_eq(tok, "intersect") {
            if next_word(idx + 1, "all") {
                return Err(unsupported_sql_error(
                    "INTERSECT ALL and EXCEPT ALL are not admitted in this scoped local-source runtime; use distinct set semantics only",
                ));
            }
            let (end, advance) = if next_word(idx + 1, "distinct") {
                (sig[idx + 1].end, 2)
            } else {
                (tok.end, 1)
            };
            ops.push(SqlSetOperatorSpan {
                index: tok.start,
                len: end - tok.start,
                mode: SqlSetOpMode::IntersectDistinct,
            });
            idx += advance;
        } else if word_eq(tok, "except") {
            if next_word(idx + 1, "all") {
                return Err(unsupported_sql_error(
                    "INTERSECT ALL and EXCEPT ALL are not admitted in this scoped local-source runtime; use distinct set semantics only",
                ));
            }
            let (end, advance) = if next_word(idx + 1, "distinct") {
                (sig[idx + 1].end, 2)
            } else {
                (tok.end, 1)
            };
            ops.push(SqlSetOperatorSpan {
                index: tok.start,
                len: end - tok.start,
                mode: SqlSetOpMode::ExceptDistinct,
            });
            idx += advance;
        } else {
            idx += 1;
        }
    }
    Ok(ops)
}

/// Finds the **rightmost** top-level binary arithmetic operator in `operators`,
/// skipping unary leading `+`/`-` signs and scientific exponents.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on lexical/nesting errors.
pub fn find_top_level_binary_operator(
    raw: &str,
    operators: &[char],
) -> Result<Option<(usize, char)>> {
    let tokens = tokenize_with_spans(raw)?;
    let sig = significant_tokens(&tokens);
    let mut candidate = None;
    for (idx, tok) in sig.iter().enumerate() {
        if tok.paren_depth != 0 || tok.bracket_depth != 0 {
            continue;
        }
        let ch = match &tok.token {
            Token::Plus => '+',
            Token::Minus => '-',
            Token::Mul => '*',
            Token::Div => '/',
            Token::Mod => '%',
            _ => continue,
        };
        if !operators.contains(&ch) {
            continue;
        }
        if matches!(ch, '+' | '-') {
            let sign_prev = idx
                .checked_sub(1)
                .and_then(|prev| sig.get(prev))
                .is_none_or(|prev| {
                    matches!(
                        prev.token,
                        Token::LParen
                            | Token::Comma
                            | Token::Plus
                            | Token::Minus
                            | Token::Mul
                            | Token::Div
                            | Token::Mod
                    )
                });
            let has_after = sig.get(idx + 1).is_some();
            if sign_prev && has_after {
                continue;
            }
        }
        candidate = Some((tok.start, ch));
    }
    Ok(candidate)
}

/// Finds a unique top-level comparison operator (`=`, `!=`, `<>`, `>=`, `<=`, `>`, `<`).
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] if more than one top-level
/// comparison operator appears or on lexical/nesting errors.
pub fn find_top_level_comparison_operator(
    raw: &str,
) -> Result<Option<(usize, &'static str, ComparisonOp)>> {
    let tokens = tokenize_with_spans(raw)?;
    let mut candidate = None;
    for tok in &tokens {
        if tok.paren_depth != 0 || tok.bracket_depth != 0 {
            continue;
        }
        let found = match &tok.token {
            Token::Eq => ("=", ComparisonOp::Eq),
            Token::Neq => {
                let symbol = if raw.get(tok.start..tok.end) == Some("<>") {
                    "<>"
                } else {
                    "!="
                };
                (symbol, ComparisonOp::NotEq)
            }
            Token::GtEq => (">=", ComparisonOp::GtEq),
            Token::LtEq => ("<=", ComparisonOp::LtEq),
            Token::Gt => (">", ComparisonOp::Gt),
            Token::Lt => ("<", ComparisonOp::Lt),
            _ => continue,
        };
        if candidate.is_some() {
            return Err(unsupported_sql_error(
                "generic numeric expression predicates admit exactly one comparison operator",
            ));
        }
        candidate = Some((tok.start, found.0, found.1));
    }
    Ok(candidate)
}

/// Extracts `:name` scalar parameters from SQL text using `sqlparser`'s tokenizer.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on positional placeholders
/// (`?`/`$`), invalid parameter names, or unclosed quotes.
pub fn extract_named_parameters(raw: &str) -> Result<BTreeSet<String>> {
    validate_raw_intake(raw)?;
    let raw_tokens = Tokenizer::new(&ShardLoomSqlDialect, raw)
        .tokenize_with_location()
        .map_err(|_| unsupported_sql_error("SQL quoted value is not closed"))?;
    let line_index = LineIndex::new(raw);
    let mut names = BTreeSet::new();
    let mut idx = 0usize;
    while idx < raw_tokens.len() {
        let tok = &raw_tokens[idx];
        match &tok.token {
            Token::Placeholder(_) => {
                return Err(unsupported_sql_error(
                    "positional parameters are not admitted; use named scalar parameters :name",
                ));
            }
            Token::Colon => {
                let Some(next) = raw_tokens.get(idx + 1) else {
                    return Err(unsupported_sql_error(
                        "scalar parameter name must not be empty",
                    ));
                };
                let adjacent = tok.span.end == next.span.start;
                if !adjacent {
                    return Err(unsupported_sql_error(
                        "scalar parameter name must immediately follow `:`",
                    ));
                }
                let raw_name = line_index
                    .span_bytes(next.span.start, next.span.end)
                    .map_or("", |(s, e)| &raw[s..e]);
                if raw_name.is_empty()
                    || !raw_name
                        .chars()
                        .next()
                        .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
                    || !raw_name
                        .chars()
                        .all(|c| c == '_' || c.is_ascii_alphanumeric())
                {
                    return Err(unsupported_sql_error(&format!(
                        "invalid scalar parameter identifier `:{raw_name}`"
                    )));
                }
                names.insert(raw_name.to_string());
                idx += 2;
                continue;
            }
            _ => {}
        }
        idx += 1;
    }
    Ok(names)
}

#[derive(Clone, Copy, Default)]
struct SourceScopeState {
    in_select: bool,
    in_from_clause: bool,
    expect_source: bool,
}

/// Extracts source reference byte spans (`FROM` and `JOIN` targets), skipping
/// string literals, comments, and `IS [NOT] DISTINCT FROM`.
#[must_use]
pub fn extract_source_ref_spans(raw: &str) -> Vec<SqlSourceRefSpan> {
    let Ok(raw_tokens) = Tokenizer::new(&ShardLoomSqlDialect, raw).tokenize_with_location() else {
        return Vec::new();
    };
    let line_index = LineIndex::new(raw);
    let mut spans = Vec::new();
    let mut scopes = vec![SourceScopeState::default()];
    let sig: Vec<&TokenWithSpan> = raw_tokens
        .iter()
        .filter(|tok| !matches!(tok.token, Token::Whitespace(_)))
        .collect();
    for (idx, tok) in sig.iter().enumerate() {
        let Some((start, end)) = line_index.span_bytes(tok.span.start, tok.span.end) else {
            continue;
        };
        step_source_ref_token(tok, idx, &sig, start, end, &mut scopes, &mut spans);
    }
    spans
}

fn step_source_ref_token(
    tok: &TokenWithSpan,
    idx: usize,
    sig: &[&TokenWithSpan],
    start: usize,
    end: usize,
    scopes: &mut Vec<SourceScopeState>,
    spans: &mut Vec<SqlSourceRefSpan>,
) {
    if matches!(tok.token, Token::LParen) {
        if let Some(scope) = scopes.last_mut() {
            scope.expect_source = false;
        }
        scopes.push(SourceScopeState::default());
        return;
    }
    if matches!(tok.token, Token::RParen) {
        if scopes.len() > 1 {
            scopes.pop();
        } else if let Some(scope) = scopes.last_mut() {
            scope.expect_source = false;
        }
        return;
    }
    let Some(scope) = scopes.last_mut() else {
        return;
    };
    match &tok.token {
        Token::SingleQuotedString(value) => {
            if scope.expect_source {
                spans.push(SqlSourceRefSpan {
                    value: value.clone(),
                    start,
                    end,
                    quoted: true,
                });
            }
            scope.expect_source = false;
        }
        Token::Word(word) if word.quote_style == Some('"') => {
            if scope.expect_source {
                spans.push(SqlSourceRefSpan {
                    value: word.value.clone(),
                    start,
                    end,
                    quoted: true,
                });
            }
            scope.expect_source = false;
        }
        Token::Word(word) => step_source_ref_word(scope, &word.value, idx, sig, start, end, spans),
        _ => {
            scope.expect_source = false;
        }
    }
}

fn step_source_ref_word(
    scope: &mut SourceScopeState,
    word_value: &str,
    idx: usize,
    sig: &[&TokenWithSpan],
    start: usize,
    end: usize,
    spans: &mut Vec<SqlSourceRefSpan>,
) {
    let upper = word_value.to_ascii_uppercase();
    if scope.expect_source {
        spans.push(SqlSourceRefSpan {
            value: word_value.to_string(),
            start,
            end,
            quoted: false,
        });
        scope.expect_source = false;
    } else if upper == "SELECT" {
        scope.in_select = true;
        scope.in_from_clause = false;
    } else if scope.in_select && upper == "FROM" {
        let is_null_safe_from = matches!(
            idx.checked_sub(1).and_then(|i| sig.get(i)),
            Some(prev) if matches!(&prev.token, Token::Word(w) if w.value.eq_ignore_ascii_case("DISTINCT"))
        ) && matches!(
            idx.checked_sub(2).and_then(|i| sig.get(i)),
            Some(prev) if matches!(&prev.token, Token::Word(w) if w.value.eq_ignore_ascii_case("IS") || w.value.eq_ignore_ascii_case("NOT"))
        );
        if !is_null_safe_from {
            scope.in_from_clause = true;
            scope.expect_source = true;
        }
    } else if scope.in_select && scope.in_from_clause && upper == "JOIN" {
        scope.expect_source = true;
    } else if matches!(
        upper.as_str(),
        "WHERE" | "GROUP" | "HAVING" | "ORDER" | "LIMIT" | "OFFSET"
    ) {
        scope.in_select = false;
        scope.in_from_clause = false;
        scope.expect_source = false;
    }
}

/// Splits top-level `item AS alias` using the SQL tokenizer.
#[must_use]
pub fn split_sql_alias(item: &str) -> (&str, Option<&str>) {
    let trimmed = item.trim();
    let Ok(tokens) = tokenize_with_spans(trimmed) else {
        return (trimmed, None);
    };
    let sig = significant_tokens(&tokens);
    for (idx, tok) in sig.iter().enumerate() {
        if tok.paren_depth == 0
            && tok.bracket_depth == 0
            && word_eq(tok, "as")
            && let Some(next) = sig.get(idx + 1)
        {
            let expr = trimmed[..tok.start].trim();
            let alias = trimmed[next.start..].trim();
            if !alias.is_empty() {
                return (expr, Some(alias));
            }
        }
    }
    (trimmed, None)
}

/// Splits top-level `AND` conjunctions outside quotes, parens, and `BETWEEN ... AND`.
#[must_use]
pub fn split_top_level_conjunctions(expr: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let Ok(spans) = top_level_keyword_spans(expr.trim(), "and") else {
        let t = expr.trim();
        if !t.is_empty() {
            parts.push(t);
        }
        return parts;
    };
    let trimmed = expr.trim();
    let mut start = 0usize;
    for (kw_start, kw_end) in spans {
        let piece = trimmed[start..kw_start].trim();
        if !piece.is_empty() {
            parts.push(piece);
        }
        start = kw_end;
    }
    let tail = trimmed[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

/// Returns `true` when an AST or lexical structure contains relational operators
/// (joins, set ops, window specs, CTEs, or nested `SELECT` subqueries).
#[must_use]
pub fn is_relational_sql_candidate(raw: &str) -> bool {
    if let Ok(stmt) = parse_single_ast_statement(raw)
        && let Statement::Query(query) = stmt
    {
        return query_is_relational(&query);
    }
    let Ok(tokens) = tokenize_with_spans(raw) else {
        return false;
    };
    let sig = significant_tokens(&tokens);
    let mut selects = 0usize;
    for tok in sig {
        if matches!(&tok.token, Token::Word(w) if w.quote_style.is_none()) {
            if word_eq(tok, "select") {
                selects += 1;
            }
            if ["join", "union", "intersect", "except", "over", "with"]
                .iter()
                .any(|kw| word_eq(tok, kw))
            {
                return true;
            }
        }
    }
    selects > 1
}

fn query_is_relational(query: &Query) -> bool {
    if query.with.is_some() {
        return true;
    }
    set_expr_is_relational(&query.body)
}

fn set_expr_is_relational(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => {
            if select.from.len() != 1 || !select.from[0].joins.is_empty() {
                return true;
            }
            if !matches!(
                select.from[0].relation,
                TableFactor::Table { args: None, .. }
            ) {
                return true;
            }
            select.projection.iter().any(|item| match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    expr_is_relational(e)
                }
                _ => false,
            }) || select.selection.as_ref().is_some_and(expr_is_relational)
                || select.having.as_ref().is_some_and(expr_is_relational)
        }
        SetExpr::Query(inner) => query_is_relational(inner),
        _ => true,
    }
}

fn function_arg_is_relational(arg: &ast::FunctionArg) -> bool {
    match arg {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
        | ast::FunctionArg::Named {
            arg: ast::FunctionArgExpr::Expr(e),
            ..
        }
        | ast::FunctionArg::ExprNamed {
            arg: ast::FunctionArgExpr::Expr(e),
            ..
        } => expr_is_relational(e),
        _ => false,
    }
}

fn expr_is_relational(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Function(f) => {
            f.over.is_some()
                || f.filter.as_deref().is_some_and(expr_is_relational)
                || match &f.args {
                    ast::FunctionArguments::Subquery(_) => true,
                    ast::FunctionArguments::List(list) => {
                        list.args.iter().any(function_arg_is_relational)
                    }
                    ast::FunctionArguments::None => false,
                }
        }
        SqlExpr::Subquery(_) | SqlExpr::Exists { .. } => true,
        SqlExpr::In { expr, target, .. } => {
            expr_is_relational(expr)
                || match target {
                    InTarget::Subquery(_) => true,
                    InTarget::List(items) => items.iter().any(expr_is_relational),
                    _ => false,
                }
        }
        SqlExpr::BinaryOp { left, right, .. }
        | SqlExpr::AnyOp { left, right, .. }
        | SqlExpr::AllOp { left, right, .. }
        | SqlExpr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | SqlExpr::ILike {
            expr: left,
            pattern: right,
            ..
        }
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right) => {
            expr_is_relational(left) || expr_is_relational(right)
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => expr_is_relational(expr) || expr_is_relational(low) || expr_is_relational(high),
        SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Nested(expr)
        | SqlExpr::Cast { expr, .. }
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::IsTrue(expr)
        | SqlExpr::IsNotTrue(expr)
        | SqlExpr::IsFalse(expr)
        | SqlExpr::IsNotFalse(expr) => expr_is_relational(expr),
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(expr_is_relational)
                || conditions
                    .iter()
                    .any(|w| expr_is_relational(&w.condition) || expr_is_relational(&w.result))
                || else_result.as_deref().is_some_and(expr_is_relational)
        }
        _ => false,
    }
}

/// AST-backed extraction of single-source `SELECT` clause slices for
/// deterministic fast-path primitive routing in `public_workflow_route.rs`.
#[must_use]
pub fn extract_single_source_shape(raw: &str) -> Option<SqlSingleSourceShape> {
    let Statement::Query(query) = parse_single_ast_statement(raw).ok()? else {
        return None;
    };
    if query.with.is_some() || query.fetch.is_some() || !query.locks.is_empty() {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.from.len() != 1
        || !select.from[0].joins.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || !select.lateral_views.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return None;
    }
    let TableFactor::Table {
        name,
        args: None,
        alias: None,
        ..
    } = &select.from[0].relation
    else {
        return None;
    };
    if name.0.len() != 1 {
        return None;
    }
    let ObjectNamePart::Identifier(source_ident) = &name.0[0] else {
        return None;
    };
    let source_ref = source_ident.value.clone();
    let source_is_identifier = source_ident.quote_style.is_none();

    let projection = slice_projection_before_from(raw)?;
    let where_clause = select.selection.as_ref().map(ToString::to_string);
    let group_by = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers)
            if !exprs.is_empty() || !modifiers.is_empty() =>
        {
            let mut parts: Vec<String> = exprs.iter().map(ToString::to_string).collect();
            for m in modifiers {
                parts.push(m.to_string());
            }
            Some(parts.join(", "))
        }
        GroupByExpr::All(modifiers) => {
            let mut parts = vec!["ALL".to_string()];
            for m in modifiers {
                parts.push(m.to_string());
            }
            Some(parts.join(" "))
        }
        GroupByExpr::Expressions(..) => None,
    };
    let having = select.having.as_ref().map(ToString::to_string);
    let order_by = query.order_by.as_ref().map(|o| match &o.kind {
        OrderByKind::Expressions(exprs) => exprs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", "),
        OrderByKind::All(_) => "ALL".to_string(),
    });
    let (limit, offset) = match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => (
            limit
                .as_ref()
                .map(parse_positive_limit_text)
                .transpose()
                .ok()?,
            offset
                .as_ref()
                .map(|o| parse_positive_limit_text(&o.value))
                .transpose()
                .ok()?,
        ),
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (
            Some(parse_positive_limit_text(limit).ok()?),
            Some(parse_positive_limit_text(offset).ok()?),
        ),
        None => (None, None),
    };

    Some(SqlSingleSourceShape {
        projection,
        source_ref,
        source_is_identifier,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset,
    })
}

fn slice_projection_before_from(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_end_matches(';').trim();
    let tokens = tokenize_with_spans(trimmed).ok()?;
    let sig = significant_tokens(&tokens);
    let select_tok = sig.first().filter(|tok| word_eq(tok, "select"))?;
    let from_spans = top_level_keyword_spans(trimmed, "from").ok()?;
    let (from_start, _) = from_spans.into_iter().next()?;
    let projection = trimmed[select_tok.end..from_start].trim();
    if projection.is_empty() {
        return None;
    }
    Some(projection.to_string())
}

fn parse_positive_limit_text(expr: &SqlExpr) -> Result<String> {
    match expr {
        SqlExpr::Value(v) => match &v.value {
            SqlValue::Number(n, false)
                if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) =>
            {
                let parsed = n.parse::<u64>().map_err(|_| {
                    unsupported_sql_error(
                        "LIMIT/OFFSET requires a non-negative u64 integer literal",
                    )
                })?;
                Ok(parsed.to_string())
            }
            _ => Err(unsupported_sql_error(
                "LIMIT/OFFSET requires a non-negative integer literal",
            )),
        },
        _ => Err(unsupported_sql_error(
            "LIMIT/OFFSET requires a non-negative integer literal",
        )),
    }
}

/// Parses a `WITH [RECURSIVE] ... <body_query>` preamble into its declarations
/// and trailing query text using `sqlparser`'s tokenizer and AST spans.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when `WITH` syntax, duplicate
/// CTE/column names, or parentheses are invalid.
pub fn extract_cte_definitions(raw: &str) -> Result<SqlCteParts> {
    let tokens = tokenize_with_spans(raw)?;
    let sig = significant_tokens(&tokens);
    if sig.is_empty() || !word_eq(sig[0], "with") {
        return Err(unsupported_sql_error("CTE statement must begin with WITH"));
    }
    let mut pos = 1usize;
    let recursive = sig.get(pos).is_some_and(|t| word_eq(t, "recursive"));
    if recursive {
        pos += 1;
    }
    let mut definitions = Vec::new();
    let mut seen_names = BTreeSet::new();
    loop {
        definitions.push(parse_one_cte_definition(
            raw,
            &sig,
            &mut pos,
            &mut seen_names,
        )?);
        if sig
            .get(pos)
            .is_some_and(|t| matches!(t.token, Token::Comma))
        {
            pos += 1;
        } else {
            break;
        }
    }

    let tail_start = sig.get(pos).map_or(raw.len(), |tok| tok.start);
    Ok(SqlCteParts {
        recursive,
        definitions,
        tail_sql: raw[tail_start..].trim().to_string(),
    })
}

fn parse_one_cte_definition(
    raw: &str,
    sig: &[&ByteToken],
    pos: &mut usize,
    seen_names: &mut BTreeSet<String>,
) -> Result<SqlCteDefParts> {
    let Some(name_tok) = sig.get(*pos) else {
        return Err(unsupported_sql_error("CTE declaration requires a name"));
    };
    let name = match &name_tok.token {
        Token::Word(w) if w.quote_style.is_none() => w.value.clone(),
        _ => return Err(unsupported_sql_error("CTE name must be an identifier")),
    };
    validate_simple_ident(&name)?;
    if !seen_names.insert(name.to_ascii_lowercase()) {
        return Err(unsupported_sql_error(
            "WITH names must be distinct ignoring case",
        ));
    }
    *pos += 1;

    let columns = if sig
        .get(*pos)
        .is_some_and(|t| matches!(t.token, Token::LParen))
    {
        parse_cte_column_list(raw, sig, pos)?
    } else {
        Vec::new()
    };

    if !sig.get(*pos).is_some_and(|t| word_eq(t, "as")) {
        return Err(unsupported_sql_error("CTE requires AS (<query>)"));
    }
    *pos += 1;

    let materialized = if sig.get(*pos).is_some_and(|t| word_eq(t, "not"))
        && sig
            .get(*pos + 1)
            .is_some_and(|t| word_eq(t, "materialized"))
    {
        *pos += 2;
        false
    } else if sig.get(*pos).is_some_and(|t| word_eq(t, "materialized")) {
        *pos += 1;
        true
    } else {
        false
    };

    let Some(open_tok) = sig.get(*pos).filter(|t| matches!(t.token, Token::LParen)) else {
        return Err(unsupported_sql_error(
            "CTE AS requires a parenthesized query",
        ));
    };
    let depth_at_open = open_tok.paren_depth;
    *pos += 1;
    while let Some(tok) = sig.get(*pos) {
        if matches!(tok.token, Token::RParen) && tok.paren_depth == depth_at_open {
            break;
        }
        *pos += 1;
    }
    let Some(close_tok) = sig.get(*pos) else {
        return Err(unsupported_sql_error("CTE parentheses must be balanced"));
    };
    *pos += 1;
    Ok(SqlCteDefParts {
        name: name.to_ascii_lowercase(),
        columns,
        materialized,
        body_sql: raw[open_tok.end..close_tok.start].trim().to_string(),
    })
}

fn parse_cte_column_list(raw: &str, sig: &[&ByteToken], pos: &mut usize) -> Result<Vec<String>> {
    let depth_at_open = sig[*pos].paren_depth;
    let open_start = sig[*pos].end;
    *pos += 1;
    while let Some(tok) = sig.get(*pos) {
        if matches!(tok.token, Token::RParen) && tok.paren_depth == depth_at_open {
            break;
        }
        *pos += 1;
    }
    let Some(close_tok) = sig.get(*pos) else {
        return Err(unsupported_sql_error("CTE parentheses must be balanced"));
    };
    let mut seen_cols = BTreeSet::new();
    let mut columns = Vec::new();
    for col in split_top_level_csv(&raw[open_start..close_tok.start])? {
        validate_simple_ident(&col)?;
        if !seen_cols.insert(col.to_ascii_lowercase()) {
            return Err(unsupported_sql_error("CTE column names must be distinct"));
        }
        columns.push(col);
    }
    if columns.is_empty() {
        return Err(unsupported_sql_error(
            "CTE column-name list must not be empty",
        ));
    }
    *pos += 1;
    Ok(columns)
}

fn validate_simple_ident(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
        && name
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric());
    if !valid {
        return Err(unsupported_sql_error(&format!(
            "SQL identifier `{name}` must contain ASCII letters, digits, or underscores only"
        )));
    }
    Ok(())
}

// ============================================================================
// Canonical AST Parsing + Binding (ASOF LEFT JOIN / TOLERANCE token adapter)
// ============================================================================

struct AsofModifiers {
    left_join_indexes: Vec<usize>,
    tolerances: Vec<Option<u64>>,
    over_exclusions: Vec<Option<String>>,
    named_window_exclusions: BTreeMap<String, String>,
}

fn parse_single_ast_statement_with_asof(raw: &str) -> Result<(Statement, AsofModifiers)> {
    let tokens = tokenize_with_spans(raw)?;
    let (adapted, modifiers) = adapt_shardloom_tokens(tokens)?;
    let raw_for_parser: Vec<TokenWithSpan> = adapted
        .into_iter()
        .map(|bt| TokenWithSpan {
            token: bt.token,
            span: bt.raw_span,
        })
        .collect();
    let mut parser = Parser::new(&ShardLoomSqlDialect)
        .with_recursion_limit(MAX_SQL_NESTING_DEPTH)
        .with_tokens_with_locations(raw_for_parser);
    let mut statements = parser
        .parse_statements()
        .map_err(|err| unsupported_sql_error(&format!("SQL syntax error: {err}")))?;
    if statements.is_empty() {
        return Err(unsupported_sql_error("SQL statement must not be empty"));
    }
    if statements.len() > 1 {
        return Err(unsupported_sql_error(
            "SQL local-source runtime admits a single statement only",
        ));
    }
    Ok((statements.remove(0), modifiers))
}

fn parse_single_ast_statement(raw: &str) -> Result<Statement> {
    parse_single_ast_statement_with_asof(raw).map(|(stmt, _)| stmt)
}

fn adapt_shardloom_tokens(tokens: Vec<ByteToken>) -> Result<(Vec<ByteToken>, AsofModifiers)> {
    let sig_indices: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter_map(|(i, t)| (!matches!(t.token, Token::Whitespace(_))).then_some(i))
        .collect();

    let mut drop_mask = vec![false; tokens.len()];
    let mut left_join_indexes = Vec::new();
    let mut tolerances = Vec::new();
    let mut asof_ordinal = 0usize;

    let mut s = 0usize;
    while s < sig_indices.len() {
        let tok_idx = sig_indices[s];
        if word_eq(&tokens[tok_idx], "asof") {
            let current_ordinal = asof_ordinal;
            asof_ordinal += 1;
            let base_depth = tokens[tok_idx].paren_depth;
            if let (Some(&left_idx), Some(&join_idx)) =
                (sig_indices.get(s + 1), sig_indices.get(s + 2))
                && word_eq(&tokens[left_idx], "left")
                && word_eq(&tokens[join_idx], "join")
            {
                drop_mask[left_idx] = true;
                left_join_indexes.push(current_ordinal);
            }
            let mut found_tolerance = None;
            let mut scan = s + 1;
            while scan < sig_indices.len() {
                let ti = sig_indices[scan];
                if tokens[ti].paren_depth < base_depth {
                    break;
                }
                if tokens[ti].paren_depth == base_depth
                    && tokens[ti].bracket_depth == 0
                    && matches!(
                        &tokens[ti].token,
                        Token::Word(w) if w.quote_style.is_none()
                            && matches!(
                                w.value.to_ascii_uppercase().as_str(),
                                "WHERE" | "GROUP" | "HAVING" | "ORDER" | "LIMIT" | "OFFSET"
                                    | "UNION" | "INTERSECT" | "EXCEPT" | "JOIN" | "ASOF"
                            )
                    )
                {
                    let is_match_on = word_eq(&tokens[ti], "on");
                    if !is_match_on && scan > s + 2 {
                        break;
                    }
                }
                if tokens[ti].paren_depth == base_depth && word_eq(&tokens[ti], "tolerance") {
                    let prev_period = scan
                        .checked_sub(1)
                        .and_then(|p| sig_indices.get(p))
                        .is_some_and(|&pi| matches!(tokens[pi].token, Token::Period));
                    let next_period = sig_indices
                        .get(scan + 1)
                        .is_some_and(|&ni| matches!(tokens[ni].token, Token::Period));
                    if !prev_period && !next_period {
                        let Some(&val_idx) = sig_indices.get(scan + 1) else {
                            return Err(unsupported_sql_error(
                                "ASOF TOLERANCE requires a nonnegative integer",
                            ));
                        };
                        let parsed = match &tokens[val_idx].token {
                            Token::Number(n, false) => n.parse::<u64>().map_err(|_| {
                                unsupported_sql_error("ASOF TOLERANCE exceeds uint64")
                            })?,
                            _ => {
                                return Err(unsupported_sql_error(
                                    "ASOF TOLERANCE requires a nonnegative integer",
                                ));
                            }
                        };
                        drop_mask[ti] = true;
                        drop_mask[val_idx] = true;
                        found_tolerance = Some(parsed);
                        break;
                    }
                }
                scan += 1;
            }
            tolerances.push(found_tolerance);
        }
        s += 1;
    }

    let (over_exclusions, named_window_exclusions) =
        extract_window_exclusions(&tokens, &sig_indices, &mut drop_mask);

    let kept = tokens
        .into_iter()
        .enumerate()
        .filter_map(|(i, t)| (!drop_mask[i]).then_some(t))
        .collect();
    Ok((
        kept,
        AsofModifiers {
            left_join_indexes,
            tolerances,
            over_exclusions,
            named_window_exclusions,
        },
    ))
}

fn match_exclude_clause(
    tokens: &[ByteToken],
    sig: &[usize],
    pos: usize,
) -> Option<(usize, String)> {
    let idx = *sig.get(pos)?;
    if !word_eq(&tokens[idx], "exclude") {
        return None;
    }
    let next1 = *sig.get(pos + 1)?;
    if word_eq(&tokens[next1], "group") {
        return Some((2, "GROUP".to_string()));
    }
    if word_eq(&tokens[next1], "ties") {
        return Some((2, "TIES".to_string()));
    }
    let next2 = *sig.get(pos + 2)?;
    if word_eq(&tokens[next1], "current") && word_eq(&tokens[next2], "row") {
        return Some((3, "CURRENT ROW".to_string()));
    }
    if word_eq(&tokens[next1], "no") && word_eq(&tokens[next2], "others") {
        return Some((3, "NO OTHERS".to_string()));
    }
    None
}

fn extract_trailing_exclude(
    tokens: &[ByteToken],
    sig: &[usize],
    open_sig: usize,
    drop_mask: &mut [bool],
) -> Option<String> {
    let mut depth = 0usize;
    let mut k = open_sig;
    while k < sig.len() {
        match tokens[sig[k]].token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    for start in [k.saturating_sub(3), k.saturating_sub(2)] {
                        if start > open_sig
                            && let Some((count, label)) = match_exclude_clause(tokens, sig, start)
                            && start + count == k
                        {
                            for offset in 0..count {
                                drop_mask[sig[start + offset]] = true;
                            }
                            return Some(label);
                        }
                    }
                    return None;
                }
            }
            _ => {}
        }
        k += 1;
    }
    None
}

fn extract_window_exclusions(
    tokens: &[ByteToken],
    sig: &[usize],
    drop_mask: &mut [bool],
) -> (Vec<Option<String>>, BTreeMap<String, String>) {
    let mut over_exclusions = Vec::new();
    let mut named = BTreeMap::new();
    for s in 0..sig.len() {
        let ti = sig[s];
        if word_eq(&tokens[ti], "over") {
            if let Some(&next) = sig.get(s + 1)
                && matches!(tokens[next].token, Token::LParen)
            {
                over_exclusions.push(extract_trailing_exclude(tokens, sig, s + 1, drop_mask));
            } else {
                over_exclusions.push(None);
            }
        } else if word_eq(&tokens[ti], "window")
            && let (Some(&name_idx), Some(&as_idx), Some(&lparen_idx)) =
                (sig.get(s + 1), sig.get(s + 2), sig.get(s + 3))
            && word_eq(&tokens[as_idx], "as")
            && matches!(tokens[lparen_idx].token, Token::LParen)
            && let Token::Word(w) = &tokens[name_idx].token
            && let Some(label) = extract_trailing_exclude(tokens, sig, s + 3, drop_mask)
        {
            named.insert(w.value.to_ascii_lowercase(), label);
        }
    }
    (over_exclusions, named)
}

/// Parses and binds a SQL statement into a canonical [`BoundSqlPlan`].
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] or [`ShardLoomError::NotImplemented`]
/// with explicit `RFC 0012` diagnostics when SQL is malformed, positional-parameterized,
/// DML/DDL, or uses unsupported syntax. Never delegates to external engines.
pub fn bind_sql(raw: &str) -> Result<BoundSqlPlan> {
    let trimmed = raw.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err(unsupported_sql_error("SQL statement must not be empty"));
    }
    let named_parameters = extract_named_parameters(trimmed)?;
    let (statement, modifiers) = parse_single_ast_statement_with_asof(trimmed)?;
    let mut diagnostics = Vec::new();
    let Statement::Query(query) = statement else {
        let msg = format!(
            "ShardLoom admits read-only SQL queries only; statement `{statement}` is not admitted"
        );
        diagnostics.push(expr::unsupported_diag("sql.non_query_statement", &msg));
        return Err(unsupported_sql_error(&msg));
    };
    let mut root = bind_ast_query_with_window_exclusions(
        &query,
        &modifiers.named_window_exclusions,
        &mut diagnostics,
    )?;
    apply_asof_modifiers(&mut root, &modifiers, &mut 0);
    if modifiers.over_exclusions.iter().any(Option::is_some) {
        apply_window_exclusions_rel(&mut root, &modifiers, &mut 0);
    }
    let mut source_refs = Vec::new();
    collect_bound_sources(&root, &mut source_refs);
    Ok(BoundSqlPlan {
        normalized_sql: trimmed.to_string(),
        root,
        named_parameters,
        source_refs,
        diagnostics,
    })
}

fn apply_asof_modifiers(rel: &mut LogicalRelation, mods: &AsofModifiers, cursor: &mut usize) {
    match rel {
        LogicalRelation::Join {
            left,
            right,
            join_type,
            tolerance,
            ..
        } => {
            apply_asof_modifiers(left, mods, cursor);
            apply_asof_modifiers(right, mods, cursor);
            if matches!(
                join_type,
                LogicalJoinType::AsOfInner | LogicalJoinType::AsOfLeft
            ) {
                let idx = *cursor;
                *cursor += 1;
                if mods.left_join_indexes.contains(&idx) {
                    *join_type = LogicalJoinType::AsOfLeft;
                }
                if let Some(tol) = mods.tolerances.get(idx).copied().flatten() {
                    *tolerance = Some(tol);
                }
            }
        }
        LogicalRelation::Subquery { input, .. }
        | LogicalRelation::Filter { input, .. }
        | LogicalRelation::Project { input, .. }
        | LogicalRelation::Aggregate { input, .. }
        | LogicalRelation::Window { input, .. }
        | LogicalRelation::Sort { input, .. }
        | LogicalRelation::Limit { input, .. } => apply_asof_modifiers(input, mods, cursor),
        LogicalRelation::SetOperation { left, right, .. } => {
            apply_asof_modifiers(left, mods, cursor);
            apply_asof_modifiers(right, mods, cursor);
        }
        LogicalRelation::WithCte { ctes, body, .. } => {
            for cte in ctes {
                apply_asof_modifiers(&mut cte.query, mods, cursor);
            }
            apply_asof_modifiers(body, mods, cursor);
        }
        LogicalRelation::TableFunction { args, .. } => {
            for arg in args {
                if let LogicalTableArg::Subquery(sub) = arg {
                    apply_asof_modifiers(sub, mods, cursor);
                }
            }
        }
        LogicalRelation::Source { .. } | LogicalRelation::Values { .. } | LogicalRelation::Unit => {
        }
    }
}

fn apply_window_exclusions_fn(
    args: &mut [LogicalExpr],
    filter: Option<&mut LogicalExpr>,
    order_by: &mut [LogicalSortKey],
    over: Option<&mut LogicalWindowSpec>,
    mods: &AsofModifiers,
    cursor: &mut usize,
) {
    for arg in args {
        apply_window_exclusions_expr(arg, mods, cursor);
    }
    if let Some(f) = filter {
        apply_window_exclusions_expr(f, mods, cursor);
    }
    for key in order_by {
        apply_window_exclusions_expr(&mut key.expr, mods, cursor);
    }
    let Some(spec) = over else {
        return;
    };
    for p in &mut spec.partition_by {
        apply_window_exclusions_expr(p, mods, cursor);
    }
    for key in &mut spec.order_by {
        apply_window_exclusions_expr(&mut key.expr, mods, cursor);
    }
    let idx = *cursor;
    *cursor += 1;
    let excl = mods
        .over_exclusions
        .get(idx)
        .and_then(Clone::clone)
        .or_else(|| {
            spec.name.as_ref().and_then(|n| {
                mods.named_window_exclusions
                    .get(&n.to_ascii_lowercase())
                    .cloned()
            })
        });
    if let (Some(frame), Some(label)) = (&mut spec.frame, excl)
        && frame.exclusion.is_none()
    {
        frame.exclusion = Some(label);
    }
}

fn apply_window_exclusions_expr(expr: &mut LogicalExpr, mods: &AsofModifiers, cursor: &mut usize) {
    match expr {
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over,
            ..
        } => apply_window_exclusions_fn(
            args,
            filter.as_deref_mut(),
            order_by,
            over.as_mut(),
            mods,
            cursor,
        ),
        LogicalExpr::Unary { expr: inner, .. } | LogicalExpr::Cast { expr: inner, .. } => {
            apply_window_exclusions_expr(inner, mods, cursor);
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            apply_window_exclusions_expr(left, mods, cursor);
            apply_window_exclusions_expr(right, mods, cursor);
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            apply_window_exclusions_expr(expr, mods, cursor);
            apply_window_exclusions_expr(low, mods, cursor);
            apply_window_exclusions_expr(high, mods, cursor);
        }
        LogicalExpr::InList { expr, list, .. } => {
            apply_window_exclusions_expr(expr, mods, cursor);
            for item in list {
                apply_window_exclusions_expr(item, mods, cursor);
            }
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                apply_window_exclusions_expr(op, mods, cursor);
            }
            for (w, t) in when_then {
                apply_window_exclusions_expr(w, mods, cursor);
                apply_window_exclusions_expr(t, mods, cursor);
            }
            if let Some(e) = else_expr {
                apply_window_exclusions_expr(e, mods, cursor);
            }
        }
        LogicalExpr::InSubquery { expr, subquery, .. } => {
            apply_window_exclusions_expr(expr, mods, cursor);
            apply_window_exclusions_rel(subquery, mods, cursor);
        }
        LogicalExpr::Exists { subquery, .. } | LogicalExpr::ScalarSubquery(subquery) => {
            apply_window_exclusions_rel(subquery, mods, cursor);
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            for item in items {
                apply_window_exclusions_expr(item, mods, cursor);
            }
        }
        _ => {}
    }
}

fn apply_window_exclusions_rel(
    rel: &mut LogicalRelation,
    mods: &AsofModifiers,
    cursor: &mut usize,
) {
    match rel {
        LogicalRelation::WithCte { ctes, body, .. } => {
            for cte in ctes {
                apply_window_exclusions_rel(&mut cte.query, mods, cursor);
            }
            apply_window_exclusions_rel(body, mods, cursor);
        }
        LogicalRelation::Project { input, items, .. } => {
            for item in items.iter_mut() {
                if let LogicalProjectionItem::Expr { expr, .. } = item {
                    apply_window_exclusions_expr(expr, mods, cursor);
                }
            }
            apply_window_exclusions_rel(input, mods, cursor);
            if let LogicalRelation::Window { windows, .. } = input.as_mut() {
                windows.clear();
                for item in items.iter() {
                    if let LogicalProjectionItem::Expr { expr, .. } = item {
                        collect_windows(expr, windows);
                    }
                }
            }
        }
        LogicalRelation::Window { input, windows } => {
            apply_window_exclusions_rel(input, mods, cursor);
            let _ = windows;
        }
        LogicalRelation::Filter { input, predicate } => {
            apply_window_exclusions_rel(input, mods, cursor);
            apply_window_exclusions_expr(predicate, mods, cursor);
        }
        LogicalRelation::Aggregate {
            input,
            group_by,
            having,
            ..
        } => {
            apply_window_exclusions_rel(input, mods, cursor);
            for expr in group_by {
                apply_window_exclusions_expr(expr, mods, cursor);
            }
            if let Some(expr) = having {
                apply_window_exclusions_expr(expr, mods, cursor);
            }
        }
        LogicalRelation::Join {
            left, right, on, ..
        } => {
            apply_window_exclusions_rel(left, mods, cursor);
            apply_window_exclusions_rel(right, mods, cursor);
            if let Some(expr) = on {
                apply_window_exclusions_expr(expr, mods, cursor);
            }
        }
        LogicalRelation::SetOperation { left, right, .. } => {
            apply_window_exclusions_rel(left, mods, cursor);
            apply_window_exclusions_rel(right, mods, cursor);
        }
        LogicalRelation::Sort { input, keys } => {
            apply_window_exclusions_rel(input, mods, cursor);
            let mut order_windows = Vec::new();
            for k in keys {
                apply_window_exclusions_expr(&mut k.expr, mods, cursor);
                collect_windows(&k.expr, &mut order_windows);
            }
            attach_order_windows(input, order_windows);
        }
        LogicalRelation::Subquery { input, .. } | LogicalRelation::Limit { input, .. } => {
            apply_window_exclusions_rel(input, mods, cursor);
        }
        LogicalRelation::TableFunction { args, .. } => {
            for arg in args {
                match arg {
                    LogicalTableArg::Subquery(sub) => {
                        apply_window_exclusions_rel(sub, mods, cursor);
                    }
                    LogicalTableArg::Expr(e) => {
                        apply_window_exclusions_expr(e, mods, cursor);
                    }
                }
            }
        }
        LogicalRelation::Source { .. } | LogicalRelation::Values { .. } | LogicalRelation::Unit => {
        }
    }
}

fn collect_bound_sources(rel: &LogicalRelation, out: &mut Vec<SqlSourceRef>) {
    match rel {
        LogicalRelation::Source {
            reference,
            quoted_path,
            ..
        } => {
            if !out.iter().any(|existing| {
                existing.reference == *reference && existing.quoted_path == *quoted_path
            }) {
                out.push(SqlSourceRef {
                    reference: reference.clone(),
                    quoted_path: *quoted_path,
                });
            }
        }
        LogicalRelation::TableFunction { args, .. } => {
            for arg in args {
                match arg {
                    LogicalTableArg::Subquery(sub) => collect_bound_sources(sub, out),
                    LogicalTableArg::Expr(expr) => collect_expr_sources(expr, out),
                }
            }
        }
        LogicalRelation::Subquery { input, .. } | LogicalRelation::Limit { input, .. } => {
            collect_bound_sources(input, out);
        }
        LogicalRelation::Filter { input, predicate } => {
            collect_bound_sources(input, out);
            collect_expr_sources(predicate, out);
        }
        LogicalRelation::Project { input, items, .. } => {
            collect_bound_sources(input, out);
            for item in items {
                if let LogicalProjectionItem::Expr { expr, .. } = item {
                    collect_expr_sources(expr, out);
                }
            }
        }
        LogicalRelation::Aggregate { .. } | LogicalRelation::Window { .. } => {
            collect_agg_or_window_sources(rel, out);
        }
        LogicalRelation::Sort { input, keys } => {
            collect_bound_sources(input, out);
            for k in keys {
                collect_expr_sources(&k.expr, out);
            }
        }
        LogicalRelation::Join {
            left,
            right,
            on,
            match_condition,
            ..
        } => {
            collect_bound_sources(left, out);
            collect_bound_sources(right, out);
            if let Some(expr) = on {
                collect_expr_sources(expr, out);
            }
            if let Some(expr) = match_condition {
                collect_expr_sources(expr, out);
            }
        }
        LogicalRelation::SetOperation { left, right, .. } => {
            collect_bound_sources(left, out);
            collect_bound_sources(right, out);
        }
        LogicalRelation::WithCte { ctes, body, .. } => {
            for cte in ctes {
                collect_bound_sources(&cte.query, out);
            }
            collect_bound_sources(body, out);
        }
        LogicalRelation::Values { rows, .. } => {
            for row in rows {
                for cell in row {
                    collect_expr_sources(cell, out);
                }
            }
        }
        LogicalRelation::Unit => {}
    }
}

fn collect_agg_or_window_sources(rel: &LogicalRelation, out: &mut Vec<SqlSourceRef>) {
    match rel {
        LogicalRelation::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            ..
        } => {
            collect_bound_sources(input, out);
            for key in group_by {
                collect_expr_sources(key, out);
            }
            for agg in aggregates {
                for arg in &agg.args {
                    collect_expr_sources(arg, out);
                }
                if let Some(f) = &agg.filter {
                    collect_expr_sources(f, out);
                }
            }
            if let Some(h) = having {
                collect_expr_sources(h, out);
            }
        }
        LogicalRelation::Window { input, windows } => {
            collect_bound_sources(input, out);
            for (_, spec) in windows {
                for key in &spec.partition_by {
                    collect_expr_sources(key, out);
                }
                for s in &spec.order_by {
                    collect_expr_sources(&s.expr, out);
                }
            }
        }
        _ => {}
    }
}

fn collect_expr_sources(expr: &LogicalExpr, out: &mut Vec<SqlSourceRef>) {
    match expr {
        LogicalExpr::InSubquery { expr, subquery, .. } => {
            collect_expr_sources(expr, out);
            collect_bound_sources(subquery, out);
        }
        LogicalExpr::Exists { subquery, .. } | LogicalExpr::ScalarSubquery(subquery) => {
            collect_bound_sources(subquery, out);
        }
        LogicalExpr::Unary { expr, .. } | LogicalExpr::Cast { expr, .. } => {
            collect_expr_sources(expr, out);
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. } => {
            collect_expr_sources(left, out);
            collect_expr_sources(right, out);
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_sources(expr, out);
            collect_expr_sources(low, out);
            collect_expr_sources(high, out);
        }
        LogicalExpr::InList { expr, list, .. } => {
            collect_expr_sources(expr, out);
            for item in list {
                collect_expr_sources(item, out);
            }
        }
        LogicalExpr::Like { expr, pattern, .. } => {
            collect_expr_sources(expr, out);
            collect_expr_sources(pattern, out);
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_expr_sources(op, out);
            }
            for (c, t) in when_then {
                collect_expr_sources(c, out);
                collect_expr_sources(t, out);
            }
            if let Some(el) = else_expr {
                collect_expr_sources(el, out);
            }
        }
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            for arg in args {
                collect_expr_sources(arg, out);
            }
            if let Some(f) = filter {
                collect_expr_sources(f, out);
            }
            for s in order_by {
                collect_expr_sources(&s.expr, out);
            }
            if let Some(spec) = over {
                for key in &spec.partition_by {
                    collect_expr_sources(key, out);
                }
                for s in &spec.order_by {
                    collect_expr_sources(&s.expr, out);
                }
            }
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            for item in items {
                collect_expr_sources(item, out);
            }
        }
        LogicalExpr::Literal(_)
        | LogicalExpr::Column { .. }
        | LogicalExpr::Parameter(_)
        | LogicalExpr::Interval { .. }
        | LogicalExpr::Wildcard { .. } => {}
    }
}

pub(crate) fn bind_ast_query(
    query: &Query,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalRelation> {
    let empty = BTreeMap::new();
    bind_ast_query_with_window_exclusions(query, &empty, diagnostics)
}

fn bind_ast_query_with_window_exclusions(
    query: &Query,
    named_window_exclusions: &BTreeMap<String, String>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalRelation> {
    if !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(unsupported_sql_error(
            "unsupported query-level modifier clause (FOR/LOCKS/SETTINGS/FORMAT/PIPE)",
        ));
    }
    let (mut plan, named_windows) =
        bind_set_expr(&query.body, named_window_exclusions, diagnostics)?;
    if let Some(order_by) = &query.order_by {
        plan = bind_query_order_by(plan, order_by, &named_windows, diagnostics)?;
    }
    if let Some((limit, offset)) = bind_limit_and_fetch(query, diagnostics)? {
        plan = LogicalRelation::Limit {
            input: Box::new(plan),
            limit,
            offset,
        };
    }
    if let Some(with) = &query.with {
        let mut ctes = Vec::with_capacity(with.cte_tables.len());
        for cte in &with.cte_tables {
            if cte.from.is_some() || cte.alias.at.is_some() {
                return Err(unsupported_sql_error("unsupported CTE FROM/AT modifier"));
            }
            let columns = cte
                .alias
                .columns
                .iter()
                .map(|c| c.name.value.clone())
                .collect();
            let materialized = cte.materialized.map(|m| match m {
                ast::CteAsMaterialized::Materialized => true,
                ast::CteAsMaterialized::NotMaterialized => false,
            });
            ctes.push(LogicalCte {
                name: cte.alias.name.value.clone(),
                columns,
                materialized,
                query: Box::new(bind_ast_query_with_window_exclusions(
                    &cte.query,
                    named_window_exclusions,
                    diagnostics,
                )?),
            });
        }
        plan = LogicalRelation::WithCte {
            ctes,
            recursive: with.recursive,
            body: Box::new(plan),
        };
    }
    Ok(plan)
}

fn bind_query_order_by(
    mut plan: LogicalRelation,
    order_by: &ast::OrderBy,
    named_windows: &BTreeMap<String, LogicalWindowSpec>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalRelation> {
    if order_by.interpolate.is_some() {
        return Err(unsupported_sql_error(
            "ORDER BY INTERPOLATE is not admitted",
        ));
    }
    let mut keys = match &order_by.kind {
        OrderByKind::Expressions(exprs) => exprs
            .iter()
            .map(|k| expr::bind_sort_key(k, diagnostics))
            .collect::<Result<Vec<_>>>()?,
        OrderByKind::All(_) => {
            return Err(unsupported_sql_error(
                "ORDER BY ALL is not admitted; list explicit ordering columns",
            ));
        }
    };
    let mut order_windows = Vec::new();
    for key in &mut keys {
        resolve_expr_named_windows(&mut key.expr, named_windows)?;
        collect_windows(&key.expr, &mut order_windows);
    }
    attach_order_windows(&mut plan, order_windows);
    if !keys.is_empty() {
        plan = LogicalRelation::Sort {
            input: Box::new(plan),
            keys,
        };
    }
    Ok(plan)
}

fn attach_order_windows(
    rel: &mut LogicalRelation,
    order_windows: Vec<(String, LogicalWindowSpec)>,
) {
    if order_windows.is_empty() {
        return;
    }
    if let LogicalRelation::Project { input, .. } = rel {
        if let LogicalRelation::Window { windows, .. } = input.as_mut() {
            windows.extend(order_windows);
        } else {
            let inner = std::mem::replace(input.as_mut(), LogicalRelation::Unit);
            **input = LogicalRelation::Window {
                input: Box::new(inner),
                windows: order_windows,
            };
        }
    } else {
        let inner = std::mem::replace(rel, LogicalRelation::Unit);
        *rel = LogicalRelation::Window {
            input: Box::new(inner),
            windows: order_windows,
        };
    }
}

fn bind_limit_and_fetch(
    query: &Query,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<Option<(Option<LogicalExpr>, Option<LogicalExpr>)>> {
    let (mut limit_expr, offset_expr) = match &query.limit_clause {
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return Err(unsupported_sql_error("LIMIT BY is not admitted"));
            }
            (
                limit
                    .as_ref()
                    .map(|e| expr::bind_ast_expr(e, diagnostics))
                    .transpose()?,
                offset
                    .as_ref()
                    .map(|o| expr::bind_ast_expr(&o.value, diagnostics))
                    .transpose()?,
            )
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (
            Some(expr::bind_ast_expr(limit, diagnostics)?),
            Some(expr::bind_ast_expr(offset, diagnostics)?),
        ),
        None => (None, None),
    };
    if let Some(fetch) = &query.fetch {
        if fetch.with_ties || fetch.percent {
            let msg = "FETCH WITH TIES and FETCH PERCENT are not admitted; use FETCH FIRST <n> ROWS ONLY or LIMIT";
            diagnostics.push(expr::unsupported_diag("sql.fetch", msg));
            return Err(unsupported_sql_error(msg));
        }
        if limit_expr.is_some() {
            return Err(unsupported_sql_error(
                "cannot combine LIMIT and FETCH in the same query block",
            ));
        }
        let fetch_limit = match &fetch.quantity {
            Some(qty) => expr::bind_ast_expr(qty, diagnostics)?,
            None => LogicalExpr::Literal(ScalarValue::Int64(1)),
        };
        limit_expr = Some(fetch_limit);
    }
    Ok((limit_expr.is_some() || offset_expr.is_some()).then_some((limit_expr, offset_expr)))
}

fn bind_set_expr(
    body: &SetExpr,
    named_window_exclusions: &BTreeMap<String, String>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<(LogicalRelation, BTreeMap<String, LogicalWindowSpec>)> {
    match body {
        SetExpr::Select(select) => bind_select(select, named_window_exclusions, diagnostics),
        SetExpr::Query(inner) => Ok((
            bind_ast_query_with_window_exclusions(inner, named_window_exclusions, diagnostics)?,
            BTreeMap::new(),
        )),
        SetExpr::Values(values) => {
            let rows = values
                .rows
                .iter()
                .map(|row| {
                    row.content
                        .iter()
                        .map(|e| expr::bind_ast_expr(e, diagnostics))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((
                LogicalRelation::Values { rows, alias: None },
                BTreeMap::new(),
            ))
        }
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } => {
            let logical_op = match op {
                SetOperator::Union => LogicalSetOp::Union,
                SetOperator::Intersect => LogicalSetOp::Intersect,
                SetOperator::Except => LogicalSetOp::Except,
                SetOperator::Minus => {
                    return Err(unsupported_sql_error("MINUS is not admitted; use EXCEPT"));
                }
            };
            let distinct = match set_quantifier {
                SetQuantifier::All => false,
                SetQuantifier::Distinct | SetQuantifier::None => true,
                SetQuantifier::ByName
                | SetQuantifier::AllByName
                | SetQuantifier::DistinctByName => {
                    let msg = "set operations with BY NAME alignment are not admitted; align projected columns explicitly";
                    diagnostics.push(expr::unsupported_diag("sql.set_operation", msg));
                    return Err(unsupported_sql_error(msg));
                }
            };
            Ok((
                LogicalRelation::SetOperation {
                    left: Box::new(bind_set_expr(left, named_window_exclusions, diagnostics)?.0),
                    right: Box::new(bind_set_expr(right, named_window_exclusions, diagnostics)?.0),
                    op: logical_op,
                    distinct,
                },
                BTreeMap::new(),
            ))
        }
        other => {
            let msg = format!("unsupported SQL query body `{other}`");
            diagnostics.push(expr::unsupported_diag("sql.query_body", &msg));
            Err(unsupported_sql_error(&msg))
        }
    }
}

fn validate_select_modifiers(select: &Select, diagnostics: &mut Vec<Diagnostic>) -> Result<()> {
    if select.qualify.is_some() {
        let msg = "SQL QUALIFY predicates are not admitted; filter window outputs with an enclosing subquery or CTE WHERE clause";
        diagnostics.push(expr::unsupported_diag("sql.qualify", msg));
        return Err(unsupported_sql_error(msg));
    }
    if matches!(select.distinct, Some(ast::Distinct::On(_))) {
        return Err(unsupported_sql_error("SELECT DISTINCT ON is not admitted"));
    }
    if select.top.is_some()
        || select.into.is_some()
        || select.prewhere.is_some()
        || !select.lateral_views.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.connect_by.is_empty()
    {
        return Err(unsupported_sql_error(
            "unsupported SELECT modifier clause (TOP/INTO/PREWHERE/LATERAL/CLUSTER/DISTRIBUTE/SORT BY/CONNECT BY)",
        ));
    }
    Ok(())
}

pub(crate) fn validate_wildcard_options(
    opts: &ast::WildcardAdditionalOptions,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<()> {
    if opts.opt_ilike.is_some()
        || opts.opt_exclude.is_some()
        || opts.opt_except.is_some()
        || opts.opt_replace.is_some()
        || opts.opt_rename.is_some()
        || opts.opt_alias.is_some()
    {
        let msg = "SQL wildcard modifiers (EXCLUDE, EXCEPT, REPLACE, RENAME, ILIKE, AS) are not admitted; project explicit columns instead";
        diagnostics.push(expr::unsupported_diag("sql.wildcard", msg));
        return Err(unsupported_sql_error(msg));
    }
    Ok(())
}

fn bind_select_item(
    item: &SelectItem,
    named_windows: &BTreeMap<String, LogicalWindowSpec>,
    windows: &mut Vec<(String, LogicalWindowSpec)>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalProjectionItem> {
    match item {
        SelectItem::UnnamedExpr(e) => {
            let mut bound = expr::bind_ast_expr(e, diagnostics)?;
            resolve_expr_named_windows(&mut bound, named_windows)?;
            collect_windows(&bound, windows);
            Ok(LogicalProjectionItem::Expr {
                expr: Box::new(bound),
                alias: None,
            })
        }
        SelectItem::ExprWithAlias { expr: e, alias } => {
            let mut bound = expr::bind_ast_expr(e, diagnostics)?;
            resolve_expr_named_windows(&mut bound, named_windows)?;
            collect_windows(&bound, windows);
            Ok(LogicalProjectionItem::Expr {
                expr: Box::new(bound),
                alias: Some(alias.value.clone()),
            })
        }
        SelectItem::Wildcard(opts) => {
            validate_wildcard_options(opts, diagnostics)?;
            Ok(LogicalProjectionItem::Wildcard { qualifier: None })
        }
        SelectItem::QualifiedWildcard(kind, opts) => {
            validate_wildcard_options(opts, diagnostics)?;
            let qualifier = match kind {
                ast::SelectItemQualifiedWildcardKind::ObjectName(name) => name.to_string(),
                ast::SelectItemQualifiedWildcardKind::Expr(expr) => {
                    return Err(unsupported_sql_error(&format!(
                        "expression-qualified wildcard `{expr}.*` is not admitted"
                    )));
                }
            };
            Ok(LogicalProjectionItem::Wildcard {
                qualifier: Some(qualifier),
            })
        }
        other @ SelectItem::ExprWithAliases { .. } => Err(unsupported_sql_error(&format!(
            "unsupported SELECT item `{other}`"
        ))),
    }
}

fn bind_named_windows(
    defs: &[ast::NamedWindowDefinition],
    named_window_exclusions: &BTreeMap<String, String>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<BTreeMap<String, LogicalWindowSpec>> {
    let mut named_windows = BTreeMap::new();
    for ast::NamedWindowDefinition(ident, def) in defs {
        let mut resolved = match def {
            ast::NamedWindowExpr::WindowSpec(spec) => {
                expr::bind_window_spec_ast(spec, diagnostics)?
            }
            ast::NamedWindowExpr::NamedWindow(parent) => LogicalWindowSpec {
                name: Some(parent.value.clone()),
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
            },
        };
        for expr in &mut resolved.partition_by {
            resolve_expr_named_windows(expr, &named_windows)?;
        }
        for key in &mut resolved.order_by {
            resolve_expr_named_windows(&mut key.expr, &named_windows)?;
        }
        resolve_spec_named_window(&mut resolved, &named_windows)?;
        let key = ident.value.to_ascii_lowercase();
        if named_windows.contains_key(&key) {
            return Err(unsupported_sql_error(&format!(
                "duplicate named window definition `{}`",
                ident.value
            )));
        }
        if let (Some(frame), Some(label)) = (&mut resolved.frame, named_window_exclusions.get(&key))
            && frame.exclusion.is_none()
        {
            frame.exclusion = Some(label.clone());
        }
        resolved.name = None;
        named_windows.insert(key, resolved);
    }
    Ok(named_windows)
}

fn bind_select(
    select: &Select,
    named_window_exclusions: &BTreeMap<String, String>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<(LogicalRelation, BTreeMap<String, LogicalWindowSpec>)> {
    validate_select_modifiers(select, diagnostics)?;
    let mut input = if select.from.is_empty() {
        LogicalRelation::Unit
    } else {
        bind_from_list(&select.from, diagnostics)?
    };
    if let Some(selection) = &select.selection {
        input = LogicalRelation::Filter {
            input: Box::new(input),
            predicate: expr::bind_ast_expr(selection, diagnostics)?,
        };
    }

    let named_windows =
        bind_named_windows(&select.named_window, named_window_exclusions, diagnostics)?;
    if let LogicalRelation::Filter { predicate, .. } = &mut input {
        resolve_expr_named_windows(predicate, &named_windows)?;
    }

    let mut items = Vec::with_capacity(select.projection.len());
    let mut windows = Vec::new();
    for item in &select.projection {
        items.push(bind_select_item(
            item,
            &named_windows,
            &mut windows,
            diagnostics,
        )?);
    }

    let (mut group_by, grouping_sets) = bind_group_by(&select.group_by, diagnostics)?;
    for gb in &mut group_by {
        resolve_expr_named_windows(gb, &named_windows)?;
    }
    let mut having = select
        .having
        .as_ref()
        .map(|h| expr::bind_ast_expr(h, diagnostics))
        .transpose()?;
    if let Some(ref mut h) = having {
        resolve_expr_named_windows(h, &named_windows)?;
    }
    let mut aggregates = Vec::new();
    for item in &items {
        if let LogicalProjectionItem::Expr { expr: e, alias } = item {
            collect_aggregates(e, alias.clone(), &mut aggregates);
        }
    }
    if !group_by.is_empty() || grouping_sets.is_some() || having.is_some() || !aggregates.is_empty()
    {
        input = LogicalRelation::Aggregate {
            input: Box::new(input),
            group_by,
            grouping_sets,
            aggregates,
            having,
        };
    }

    if !windows.is_empty() {
        input = LogicalRelation::Window {
            input: Box::new(input),
            windows,
        };
    }

    let distinct = matches!(select.distinct, Some(ast::Distinct::Distinct));
    Ok((
        LogicalRelation::Project {
            input: Box::new(input),
            items,
            distinct,
        },
        named_windows,
    ))
}

fn bind_from_list(
    from: &[ast::TableWithJoins],
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalRelation> {
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| unsupported_sql_error("missing FROM relation"))?;
    let mut current = bind_table_with_joins(first, diagnostics)?;
    for next in iter {
        let right = bind_table_with_joins(next, diagnostics)?;
        current = LogicalRelation::Join {
            left: Box::new(current),
            right: Box::new(right),
            join_type: LogicalJoinType::Cross,
            on: None,
            match_condition: None,
            tolerance: None,
        };
    }
    Ok(current)
}

fn bind_table_with_joins(
    twj: &ast::TableWithJoins,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalRelation> {
    let mut left = bind_table_factor(&twj.relation, diagnostics)?;
    for join in &twj.joins {
        left = bind_join(left, join, diagnostics)?;
    }
    Ok(left)
}

fn bind_join(
    left: LogicalRelation,
    join: &Join,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalRelation> {
    let right = bind_table_factor(&join.relation, diagnostics)?;
    let (join_type, constraint, match_condition) = match &join.join_operator {
        JoinOperator::Join(c) | JoinOperator::Inner(c) => (LogicalJoinType::Inner, c, None),
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (LogicalJoinType::LeftOuter, c, None),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => {
            (LogicalJoinType::RightOuter, c, None)
        }
        JoinOperator::FullOuter(c) => (LogicalJoinType::FullOuter, c, None),
        JoinOperator::CrossJoin(c) => (LogicalJoinType::Cross, c, None),
        JoinOperator::Semi(c) | JoinOperator::LeftSemi(c) => (LogicalJoinType::Semi, c, None),
        JoinOperator::Anti(c) | JoinOperator::LeftAnti(c) => (LogicalJoinType::Anti, c, None),
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => (
            LogicalJoinType::AsOfInner,
            constraint,
            Some(expr::bind_ast_expr(match_condition, diagnostics)?),
        ),
        other => {
            return Err(unsupported_sql_error(&format!(
                "unsupported JOIN operator `{other:?}`"
            )));
        }
    };
    let on = match constraint {
        JoinConstraint::On(e) => Some(expr::bind_ast_expr(e, diagnostics)?),
        JoinConstraint::None => None,
        other => {
            return Err(unsupported_sql_error(&format!(
                "unsupported JOIN constraint `{other:?}`"
            )));
        }
    };
    Ok(LogicalRelation::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        on,
        match_condition,
        tolerance: None,
    })
}

fn bind_simple_table_alias(alias: Option<&ast::TableAlias>) -> Result<Option<String>> {
    let Some(a) = alias else {
        return Ok(None);
    };
    if !a.columns.is_empty() || a.at.is_some() {
        return Err(unsupported_sql_error(
            "table alias column lists `AS t(c1, ...)` and `AT` modifiers are not admitted; alias columns in SELECT",
        ));
    }
    Ok(Some(a.name.value.clone()))
}

fn bind_table_factor(
    factor: &TableFactor,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalRelation> {
    match factor {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_directives,
            inheritance,
        } => {
            if !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_directives.is_empty()
                || inheritance.is_some()
            {
                return Err(unsupported_sql_error(
                    "unsupported FROM table modifier (TABLESAMPLE/VERSION/WITH ORDINALITY/PARTITION/HINTS/ONLY)",
                ));
            }
            let alias_name = bind_simple_table_alias(alias.as_ref())?;
            if let Some(tf_args) = args {
                if tf_args.settings.is_some() {
                    return Err(unsupported_sql_error(
                        "table function SETTINGS clause is not admitted",
                    ));
                }
                let mut bound_args = Vec::with_capacity(tf_args.args.len());
                for arg in &tf_args.args {
                    bound_args.push(bind_table_arg(arg, diagnostics)?);
                }
                Ok(LogicalRelation::TableFunction {
                    name: name.to_string(),
                    args: bound_args,
                    alias: alias_name,
                })
            } else {
                let quoted_path = name.0.first().is_some_and(|part| match part {
                    ObjectNamePart::Identifier(ident) => ident.quote_style == Some('\''),
                    ObjectNamePart::Function(_) => false,
                });
                let reference = if quoted_path && name.0.len() == 1 {
                    name.0[0]
                        .as_ident()
                        .map_or_else(|| name.to_string(), |i| i.value.clone())
                } else {
                    name.to_string()
                };
                Ok(LogicalRelation::Source {
                    reference,
                    quoted_path,
                    alias: alias_name,
                })
            }
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if *lateral || sample.is_some() {
                return Err(unsupported_sql_error(
                    "LATERAL derived subqueries and TABLESAMPLE are not admitted",
                ));
            }
            let alias_name = bind_simple_table_alias(alias.as_ref())?;
            Ok(LogicalRelation::Subquery {
                input: Box::new(bind_ast_query(subquery, diagnostics)?),
                alias: alias_name,
            })
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let alias_name = bind_simple_table_alias(alias.as_ref())?;
            let inner = bind_table_with_joins(table_with_joins, diagnostics)?;
            if let Some(a) = alias_name {
                Ok(LogicalRelation::Subquery {
                    input: Box::new(inner),
                    alias: Some(a),
                })
            } else {
                Ok(inner)
            }
        }
        other => Err(unsupported_sql_error(&format!(
            "unsupported FROM table factor `{other}`"
        ))),
    }
}

fn bind_table_arg(
    arg: &ast::FunctionArg,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalTableArg> {
    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(SqlExpr::Subquery(subquery))) = arg
    {
        return Ok(LogicalTableArg::Subquery(Box::new(bind_ast_query(
            subquery,
            diagnostics,
        )?)));
    }
    Ok(LogicalTableArg::Expr(Box::new(expr::bind_function_arg(
        arg,
        diagnostics,
    )?)))
}

fn bind_group_by(
    group_by: &GroupByExpr,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<(Vec<LogicalExpr>, Option<LogicalGroupingSpec>)> {
    match group_by {
        GroupByExpr::All(_) => Err(unsupported_sql_error(
            "GROUP BY ALL is not admitted; list explicit grouping columns",
        )),
        GroupByExpr::Expressions(exprs, modifiers) => {
            let mut keys = Vec::new();
            let mut spec = None;
            for expr_item in exprs {
                match expr_item {
                    SqlExpr::Rollup(sets) => {
                        spec = Some(LogicalGroupingSpec::Rollup);
                        for set in sets {
                            for e in set {
                                keys.push(expr::bind_ast_expr(e, diagnostics)?);
                            }
                        }
                    }
                    SqlExpr::Cube(sets) => {
                        spec = Some(LogicalGroupingSpec::Cube);
                        for set in sets {
                            for e in set {
                                keys.push(expr::bind_ast_expr(e, diagnostics)?);
                            }
                        }
                    }
                    SqlExpr::GroupingSets(sets) => {
                        let bound_sets = sets
                            .iter()
                            .map(|set| {
                                set.iter()
                                    .map(|e| expr::bind_ast_expr(e, diagnostics))
                                    .collect::<Result<Vec<_>>>()
                            })
                            .collect::<Result<Vec<_>>>()?;
                        spec = Some(LogicalGroupingSpec::GroupingSets(bound_sets));
                    }
                    other => keys.push(expr::bind_ast_expr(other, diagnostics)?),
                }
            }
            for modifier in modifiers {
                match modifier {
                    GroupByWithModifier::Rollup => spec = Some(LogicalGroupingSpec::Rollup),
                    GroupByWithModifier::Cube => spec = Some(LogicalGroupingSpec::Cube),
                    other => {
                        return Err(unsupported_sql_error(&format!(
                            "unsupported GROUP BY modifier `{other:?}`"
                        )));
                    }
                }
            }
            Ok((keys, spec))
        }
    }
}

fn resolve_spec_named_window(
    spec: &mut LogicalWindowSpec,
    named: &BTreeMap<String, LogicalWindowSpec>,
) -> Result<()> {
    let Some(win_name) = spec.name.take() else {
        return Ok(());
    };
    let base = named
        .get(&win_name.to_ascii_lowercase())
        .ok_or_else(|| unsupported_sql_error(&format!("unknown named window `{win_name}`")))?;
    if !base.partition_by.is_empty() && !spec.partition_by.is_empty() {
        return Err(unsupported_sql_error(&format!(
            "named window reference `{win_name}` cannot override PARTITION BY"
        )));
    }
    if !base.order_by.is_empty() && !spec.order_by.is_empty() {
        return Err(unsupported_sql_error(&format!(
            "named window reference `{win_name}` cannot override ORDER BY"
        )));
    }
    if base.frame.is_some() && spec.frame.is_some() {
        return Err(unsupported_sql_error(&format!(
            "named window reference `{win_name}` cannot override window frame"
        )));
    }
    if spec.partition_by.is_empty() {
        spec.partition_by.clone_from(&base.partition_by);
    }
    if spec.order_by.is_empty() {
        spec.order_by.clone_from(&base.order_by);
    }
    if spec.frame.is_none() {
        spec.frame.clone_from(&base.frame);
    }
    Ok(())
}

fn resolve_expr_named_windows(
    expr: &mut LogicalExpr,
    named: &BTreeMap<String, LogicalWindowSpec>,
) -> Result<()> {
    match expr {
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            for arg in args {
                resolve_expr_named_windows(arg, named)?;
            }
            if let Some(f) = filter {
                resolve_expr_named_windows(f, named)?;
            }
            for k in order_by {
                resolve_expr_named_windows(&mut k.expr, named)?;
            }
            if let Some(spec) = over {
                for p in &mut spec.partition_by {
                    resolve_expr_named_windows(p, named)?;
                }
                for k in &mut spec.order_by {
                    resolve_expr_named_windows(&mut k.expr, named)?;
                }
                resolve_spec_named_window(spec, named)?;
            }
        }
        LogicalExpr::Unary { expr: inner, .. } | LogicalExpr::Cast { expr: inner, .. } => {
            resolve_expr_named_windows(inner, named)?;
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            resolve_expr_named_windows(left, named)?;
            resolve_expr_named_windows(right, named)?;
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            resolve_expr_named_windows(expr, named)?;
            resolve_expr_named_windows(low, named)?;
            resolve_expr_named_windows(high, named)?;
        }
        LogicalExpr::InList { expr, list, .. } => {
            resolve_expr_named_windows(expr, named)?;
            for item in list {
                resolve_expr_named_windows(item, named)?;
            }
        }
        LogicalExpr::InSubquery { expr, .. } => {
            resolve_expr_named_windows(expr, named)?;
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                resolve_expr_named_windows(op, named)?;
            }
            for (w, t) in when_then {
                resolve_expr_named_windows(w, named)?;
                resolve_expr_named_windows(t, named)?;
            }
            if let Some(e) = else_expr {
                resolve_expr_named_windows(e, named)?;
            }
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            for item in items {
                resolve_expr_named_windows(item, named)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn collect_windows(expr: &LogicalExpr, out: &mut Vec<(String, LogicalWindowSpec)>) {
    match expr {
        LogicalExpr::Function {
            name,
            args,
            filter,
            order_by,
            over: Some(spec),
            ..
        } => {
            out.push((name.clone(), spec.clone()));
            for arg in args {
                collect_windows(arg, out);
            }
            if let Some(f) = filter {
                collect_windows(f, out);
            }
            for k in order_by {
                collect_windows(&k.expr, out);
            }
            for p in &spec.partition_by {
                collect_windows(p, out);
            }
            for k in &spec.order_by {
                collect_windows(&k.expr, out);
            }
        }
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            ..
        } => {
            for arg in args {
                collect_windows(arg, out);
            }
            if let Some(f) = filter {
                collect_windows(f, out);
            }
            for k in order_by {
                collect_windows(&k.expr, out);
            }
        }
        LogicalExpr::Unary { expr: inner, .. } | LogicalExpr::Cast { expr: inner, .. } => {
            collect_windows(inner, out);
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            collect_windows(left, out);
            collect_windows(right, out);
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            collect_windows(expr, out);
            collect_windows(low, out);
            collect_windows(high, out);
        }
        LogicalExpr::InList { expr, list, .. } => {
            collect_windows(expr, out);
            for item in list {
                collect_windows(item, out);
            }
        }
        LogicalExpr::InSubquery { expr, .. } => {
            collect_windows(expr, out);
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_windows(op, out);
            }
            for (w, t) in when_then {
                collect_windows(w, out);
                collect_windows(t, out);
            }
            if let Some(e) = else_expr {
                collect_windows(e, out);
            }
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            for item in items {
                collect_windows(item, out);
            }
        }
        _ => {}
    }
}

fn is_sql_agg_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "var_samp"
            | "var_pop"
            | "variance"
            | "median"
            | "percentile_cont"
            | "percentile_disc"
            | "approx_quantile"
            | "approx_count_distinct"
            | "group_concat"
            | "string_agg"
            | "any_value"
            | "count_distinct"
    )
}

fn collect_aggregates(
    expr: &LogicalExpr,
    alias: Option<String>,
    out: &mut Vec<LogicalAggregateSpec>,
) {
    match expr {
        LogicalExpr::Function {
            name,
            args,
            distinct,
            filter,
            order_by,
            over: None,
        } if is_sql_agg_name(name) => {
            out.push(LogicalAggregateSpec {
                name: name.to_ascii_lowercase(),
                args: args.clone(),
                distinct: *distinct,
                filter: filter.as_deref().cloned(),
                order_by: order_by.clone(),
                alias,
            });
        }
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            for arg in args {
                collect_aggregates(arg, None, out);
            }
            if let Some(f) = filter {
                collect_aggregates(f, None, out);
            }
            for k in order_by {
                collect_aggregates(&k.expr, None, out);
            }
            if let Some(spec) = over {
                for p in &spec.partition_by {
                    collect_aggregates(p, None, out);
                }
                for k in &spec.order_by {
                    collect_aggregates(&k.expr, None, out);
                }
            }
        }
        LogicalExpr::Unary { expr: inner, .. } | LogicalExpr::Cast { expr: inner, .. } => {
            collect_aggregates(inner, None, out);
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            collect_aggregates(left, None, out);
            collect_aggregates(right, None, out);
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            collect_aggregates(expr, None, out);
            collect_aggregates(low, None, out);
            collect_aggregates(high, None, out);
        }
        LogicalExpr::InList { expr, list, .. } => {
            collect_aggregates(expr, None, out);
            for item in list {
                collect_aggregates(item, None, out);
            }
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_aggregates(op, None, out);
            }
            for (w, t) in when_then {
                collect_aggregates(w, None, out);
                collect_aggregates(t, None, out);
            }
            if let Some(e) = else_expr {
                collect_aggregates(e, None, out);
            }
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            for item in items {
                collect_aggregates(item, None, out);
            }
        }
        _ => {}
    }
}

// ============================================================================
// Inspection Summary & JSON Serializer (used by CLI and shardloom-python)
// ============================================================================

/// Inspects a SQL query via `sqlparser`'s tokenizer and AST parser without executing
/// anything.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] on lexical or syntax errors.
pub fn inspect_sql(raw: &str) -> Result<SqlInspectionSummary> {
    let trimmed = raw.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err(unsupported_sql_error("SQL statement must not be empty"));
    }
    let tokens = tokenize_with_spans(trimmed)?;
    let sig = significant_tokens(&tokens);
    let named_parameters = extract_named_parameters(trimmed)?;

    let (
        statement_kind,
        is_source_free,
        is_generator,
        source_refs,
        top_level_limit,
        limit_span,
        is_rel,
    ) = if let Ok(stmt) = parse_single_ast_statement(trimmed) {
        inspect_parsed_statement(trimmed, &stmt, &sig)?
    } else {
        inspect_tokens_only(trimmed, &sig)?
    };

    Ok(SqlInspectionSummary {
        statement_kind,
        is_source_free,
        is_generator,
        source_refs,
        has_top_level_limit: top_level_limit.is_some() || limit_span.is_some(),
        top_level_limit,
        top_level_limit_span: limit_span,
        named_parameters,
        is_relational_candidate: is_rel,
    })
}

#[allow(clippy::type_complexity)]
fn inspect_parsed_statement(
    trimmed: &str,
    stmt: &Statement,
    sig: &[&ByteToken],
) -> Result<(
    String,
    bool,
    bool,
    Vec<SqlSourceRef>,
    Option<String>,
    Option<(usize, usize)>,
    bool,
)> {
    let limit_span = find_top_level_limit_value_span(trimmed, sig)?;
    let top_level_limit = limit_span.map(|(s, e)| trimmed[s..e].trim().to_string());
    let Statement::Query(query) = stmt else {
        return Ok((
            "other".to_string(),
            false,
            false,
            Vec::new(),
            top_level_limit,
            limit_span,
            false,
        ));
    };
    let is_rel = query_is_relational(query);
    match query.body.as_ref() {
        SetExpr::Values(_) => Ok((
            "values".to_string(),
            true,
            false,
            Vec::new(),
            top_level_limit,
            limit_span,
            true,
        )),
        SetExpr::Select(select) => {
            if select.from.is_empty() {
                return Ok((
                    "select".to_string(),
                    true,
                    false,
                    Vec::new(),
                    top_level_limit,
                    limit_span,
                    is_rel,
                ));
            }
            let is_generator = select.from.len() == 1
                && select.from[0].joins.is_empty()
                && matches!(
                    &select.from[0].relation,
                    TableFactor::Table { name, args: Some(_), .. }
                        if matches!(name.to_string().to_ascii_lowercase().as_str(), "range" | "generate_series")
                );
            let refs = extract_source_ref_spans(trimmed)
                .into_iter()
                .map(|span| SqlSourceRef {
                    reference: span.value,
                    quoted_path: span.quoted,
                })
                .collect();
            Ok((
                "select".to_string(),
                is_generator,
                is_generator,
                refs,
                top_level_limit,
                limit_span,
                is_rel,
            ))
        }
        _ => {
            let refs = extract_source_ref_spans(trimmed)
                .into_iter()
                .map(|span| SqlSourceRef {
                    reference: span.value,
                    quoted_path: span.quoted,
                })
                .collect();
            Ok((
                "query".to_string(),
                false,
                false,
                refs,
                top_level_limit,
                limit_span,
                true,
            ))
        }
    }
}

#[allow(clippy::type_complexity)]
fn inspect_tokens_only(
    trimmed: &str,
    sig: &[&ByteToken],
) -> Result<(
    String,
    bool,
    bool,
    Vec<SqlSourceRef>,
    Option<String>,
    Option<(usize, usize)>,
    bool,
)> {
    let limit_span = find_top_level_limit_value_span(trimmed, sig)?;
    let top_level_limit = limit_span.map(|(s, e)| trimmed[s..e].trim().to_string());
    let first_word = sig.first().and_then(|t| match &t.token {
        Token::Word(w) => Some(w.value.to_ascii_lowercase()),
        _ => None,
    });
    let statement_kind = first_word.clone().unwrap_or_else(|| "unknown".to_string());
    let is_values = first_word.as_deref() == Some("values");
    let has_from = !top_level_keyword_offsets(trimmed, "from")?.is_empty();
    let refs: Vec<SqlSourceRef> = extract_source_ref_spans(trimmed)
        .into_iter()
        .map(|span| SqlSourceRef {
            reference: span.value,
            quoted_path: span.quoted,
        })
        .collect();
    let is_source_free = is_values || (first_word.as_deref() == Some("select") && !has_from);
    Ok((
        statement_kind,
        is_source_free,
        false,
        refs,
        top_level_limit,
        limit_span,
        is_relational_sql_candidate(trimmed),
    ))
}

fn find_top_level_limit_value_span(
    trimmed: &str,
    sig: &[&ByteToken],
) -> Result<Option<(usize, usize)>> {
    let limit_offsets = top_level_keyword_spans(trimmed, "limit")?;
    let Some((limit_start, limit_end)) = limit_offsets.last().copied() else {
        return Ok(None);
    };
    let after = sig
        .iter()
        .position(|t| t.start == limit_start && t.end == limit_end);
    let Some(pos) = after else {
        return Ok(None);
    };
    let Some(val_tok) = sig.get(pos + 1) else {
        return Ok(Some((limit_start, limit_end)));
    };
    if val_tok.paren_depth == 0
        && val_tok.bracket_depth == 0
        && matches!(&val_tok.token, Token::Number(_, false))
    {
        Ok(Some((limit_start, val_tok.end)))
    } else {
        Ok(Some((limit_start, limit_end)))
    }
}

/// Serializes [`SqlInspectionSummary`] into deterministic JSON without adding
/// `serde_json` to `shardloom-plan`.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] if `inspect_sql` fails.
pub fn inspect_sql_json(raw: &str) -> Result<String> {
    let summary = inspect_sql(raw)?;
    let mut out = String::with_capacity(256);
    out.push('{');
    write_json_kv_str(&mut out, "statement_kind", &summary.statement_kind, true);
    let _ = write!(out, ",\"is_source_free\":{}", summary.is_source_free);
    let _ = write!(out, ",\"is_generator\":{}", summary.is_generator);
    let _ = write!(
        out,
        ",\"has_top_level_limit\":{}",
        summary.has_top_level_limit
    );
    let _ = write!(
        out,
        ",\"is_relational_candidate\":{}",
        summary.is_relational_candidate
    );
    out.push_str(",\"top_level_limit\":");
    match &summary.top_level_limit {
        Some(lim) => push_json_string(&mut out, lim),
        None => out.push_str("null"),
    }
    out.push_str(",\"top_level_limit_span\":");
    match summary.top_level_limit_span {
        Some((s, e)) => {
            let _ = write!(out, "[{s},{e}]");
        }
        None => out.push_str("null"),
    }
    out.push_str(",\"source_refs\":[");
    for (idx, r) in summary.source_refs.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push('{');
        write_json_kv_str(&mut out, "reference", &r.reference, true);
        let _ = write!(out, ",\"quoted_path\":{}", r.quoted_path);
        out.push('}');
    }
    out.push_str("],\"named_parameters\":[");
    for (idx, param) in summary.named_parameters.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        push_json_string(&mut out, param);
    }
    out.push_str("]}");
    Ok(out)
}

fn write_json_kv_str(out: &mut String, key: &str, value: &str, first: bool) {
    if !first {
        out.push(',');
    }
    push_json_string(out, key);
    out.push(':');
    push_json_string(out, value);
}

fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

pub(crate) fn unsupported_sql_error(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "{reason}; no fallback execution was attempted and external_engine_invoked=false"
    ))
}

#[cfg(test)]
#[path = "sql_binder_tests.rs"]
mod tests;
