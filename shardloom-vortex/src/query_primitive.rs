use std::fmt::Write as _;

use shardloom_core::{
    ColumnRef, ComparisonOp, DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity,
    PredicateExpr, Result, ScalarValue, StatValue,
};
use shardloom_plan::ProjectionRequest;

/// Query primitive kind for minimal `Vortex` planning in `ShardLoom`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveKind {
    CountAll,
    CountWhere,
    ProjectColumns,
    FilterPredicate,
    FilterAndProject,
    DistinctRows,
    DropDuplicateRows,
    DuplicateMaskRows,
    TailRows,
    SampleRows,
    ExpressionProjectRows,
    MeltRows,
    ExplodeRows,
    PivotRows,
    RollingWindowRows,
    SimpleAggregate,
    SortRows,
    Unsupported,
}
impl VortexQueryPrimitiveKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CountAll => "count_all",
            Self::CountWhere => "count_where",
            Self::ProjectColumns => "project_columns",
            Self::FilterPredicate => "filter_predicate",
            Self::FilterAndProject => "filter_and_project",
            Self::DistinctRows => "distinct_rows",
            Self::DropDuplicateRows => "drop_duplicate_rows",
            Self::DuplicateMaskRows => "duplicate_mask_rows",
            Self::TailRows => "tail_rows",
            Self::SampleRows => "sample_rows",
            Self::ExpressionProjectRows => "expression_project_rows",
            Self::MeltRows => "melt_rows",
            Self::ExplodeRows => "explode_rows",
            Self::PivotRows => "pivot_rows",
            Self::RollingWindowRows => "rolling_window_rows",
            Self::SimpleAggregate => "simple_aggregate",
            Self::SortRows => "sort_rows",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_data_read(&self) -> bool {
        !matches!(self, Self::CountAll | Self::CountWhere | Self::Unsupported)
    }
    #[must_use]
    pub const fn requires_decode(&self) -> bool {
        matches!(
            self,
            Self::DistinctRows
                | Self::DropDuplicateRows
                | Self::TailRows
                | Self::SampleRows
                | Self::ExpressionProjectRows
                | Self::MeltRows
                | Self::ExplodeRows
                | Self::PivotRows
                | Self::RollingWindowRows
                | Self::SortRows
                | Self::DuplicateMaskRows
        )
    }
    #[must_use]
    pub const fn requires_materialization(&self) -> bool {
        matches!(
            self,
            Self::DistinctRows
                | Self::DropDuplicateRows
                | Self::TailRows
                | Self::SampleRows
                | Self::ExpressionProjectRows
                | Self::MeltRows
                | Self::ExplodeRows
                | Self::PivotRows
                | Self::RollingWindowRows
                | Self::SortRows
                | Self::DuplicateMaskRows
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VortexExpressionRewrite {
    MaskScalar {
        target_column: ColumnRef,
        predicate: PredicateExpr,
        replacement: StatValue,
    },
    ReplaceScalar {
        target_column: ColumnRef,
        to_replace: StatValue,
        replacement: StatValue,
    },
    StringReplaceScalar {
        target_column: ColumnRef,
        needle: String,
        replacement: String,
    },
    NumericScalarArithmetic {
        target_column: ColumnRef,
        operator: String,
        operand: StatValue,
    },
}
impl VortexExpressionRewrite {
    #[must_use]
    pub fn target_column(&self) -> &ColumnRef {
        match self {
            Self::MaskScalar { target_column, .. }
            | Self::ReplaceScalar { target_column, .. }
            | Self::StringReplaceScalar { target_column, .. }
            | Self::NumericScalarArithmetic { target_column, .. } => target_column,
        }
    }

    #[must_use]
    pub fn family(&self) -> &'static str {
        match self {
            Self::MaskScalar { .. } => "mask_scalar",
            Self::ReplaceScalar { .. } => "replace_scalar",
            Self::StringReplaceScalar { .. } => "string_replace_scalar",
            Self::NumericScalarArithmetic { .. } => "numeric_scalar_arithmetic",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexExpressionProjectionRequest {
    pub rewrites: Vec<VortexExpressionRewrite>,
}
impl VortexExpressionProjectionRequest {
    #[must_use]
    pub fn new(rewrites: Vec<VortexExpressionRewrite>) -> Self {
        Self { rewrites }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rewrites.is_empty()
    }

    #[must_use]
    pub fn changed_columns(&self) -> Vec<String> {
        self.rewrites
            .iter()
            .map(|rewrite| rewrite.target_column().as_str().to_string())
            .collect()
    }

    #[must_use]
    pub fn family_summary(&self) -> String {
        self.rewrites
            .iter()
            .map(VortexExpressionRewrite::family)
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VortexStructuredProjectionExpr {
    SourceColumn(ColumnRef),
    ArrayLiteral(Vec<ScalarValue>),
    StructColumns(Vec<ColumnRef>),
}
impl VortexStructuredProjectionExpr {
    #[must_use]
    pub fn family(&self) -> &'static str {
        match self {
            Self::SourceColumn(_) => "source_column",
            Self::ArrayLiteral(_) => "array_literal",
            Self::StructColumns(_) => "struct_columns",
        }
    }

    pub fn source_columns(&self, out: &mut Vec<ColumnRef>) {
        match self {
            Self::SourceColumn(column) => push_unique_column(out, column.clone()),
            Self::ArrayLiteral(_) => {}
            Self::StructColumns(columns) => {
                for column in columns {
                    push_unique_column(out, column.clone());
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStructuredProjectionColumn {
    pub output_column: String,
    pub expr: VortexStructuredProjectionExpr,
}
impl VortexStructuredProjectionColumn {
    #[must_use]
    pub fn new(output_column: String, expr: VortexStructuredProjectionExpr) -> Self {
        Self {
            output_column,
            expr,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStructuredProjectionRequest {
    pub columns: Vec<VortexStructuredProjectionColumn>,
}
impl VortexStructuredProjectionRequest {
    #[must_use]
    pub fn new(columns: Vec<VortexStructuredProjectionColumn>) -> Self {
        Self { columns }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    #[must_use]
    pub fn output_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|column| column.output_column.clone())
            .collect()
    }

    #[must_use]
    pub fn source_columns(&self) -> Vec<ColumnRef> {
        let mut out = Vec::new();
        for column in &self.columns {
            column.expr.source_columns(&mut out);
        }
        out
    }

    #[must_use]
    pub fn family_summary(&self) -> String {
        self.columns
            .iter()
            .map(|column| format!("{}:{}", column.output_column, column.expr.family()))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn push_unique_column(out: &mut Vec<ColumnRef>, column: ColumnRef) {
    if !out.iter().any(|existing| existing == &column) {
        out.push(column);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexDuplicateKeepPolicy {
    First,
    Last,
    AllDuplicates,
}
impl VortexDuplicateKeepPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Last => "last",
            Self::AllDuplicates => "false",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexMeltProjectionRequest {
    pub id_columns: Vec<ColumnRef>,
    pub value_columns: Vec<ColumnRef>,
    pub variable_column: String,
    pub value_column: String,
}
impl VortexMeltProjectionRequest {
    #[must_use]
    pub fn new(
        id_columns: Vec<ColumnRef>,
        value_columns: Vec<ColumnRef>,
        variable_column: String,
        value_column: String,
    ) -> Self {
        Self {
            id_columns,
            value_columns,
            variable_column,
            value_column,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.value_columns.is_empty()
    }

    #[must_use]
    pub fn projected_columns(&self) -> Vec<ColumnRef> {
        self.id_columns
            .iter()
            .chain(self.value_columns.iter())
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn output_columns(&self) -> Vec<String> {
        self.id_columns
            .iter()
            .map(|column| column.as_str().to_string())
            .chain([self.variable_column.clone(), self.value_column.clone()])
            .collect()
    }

    #[must_use]
    pub fn summary(&self) -> String {
        let id_columns = self
            .id_columns
            .iter()
            .map(ColumnRef::as_str)
            .collect::<Vec<_>>()
            .join(",");
        let value_columns = self
            .value_columns
            .iter()
            .map(ColumnRef::as_str)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "id_columns={id_columns};value_columns={value_columns};variable_column={};value_column={}",
            self.variable_column, self.value_column
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexExplodeProjectionRequest {
    pub column: ColumnRef,
}
impl VortexExplodeProjectionRequest {
    #[must_use]
    pub const fn new(column: ColumnRef) -> Self {
        Self { column }
    }

    #[must_use]
    pub fn output_columns(&self, projected_columns: &[String]) -> Vec<String> {
        if projected_columns.is_empty() {
            return vec![self.column.as_str().to_string()];
        }
        projected_columns.to_vec()
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!("column={}", self.column.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexPivotProjectionRequest {
    pub index_column: ColumnRef,
    pub pivot_column: ColumnRef,
    pub value_column: ColumnRef,
    pub aggregate: String,
}
impl VortexPivotProjectionRequest {
    #[must_use]
    pub fn new(
        index_column: ColumnRef,
        pivot_column: ColumnRef,
        value_column: ColumnRef,
        aggregate: impl Into<String>,
    ) -> Self {
        Self {
            index_column,
            pivot_column,
            value_column,
            aggregate: aggregate.into(),
        }
    }

    #[must_use]
    pub fn projected_columns(&self) -> Vec<ColumnRef> {
        vec![
            self.index_column.clone(),
            self.pivot_column.clone(),
            self.value_column.clone(),
        ]
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "index_column={};pivot_column={};value_column={};aggregate={}",
            self.index_column.as_str(),
            self.pivot_column.as_str(),
            self.value_column.as_str(),
            self.aggregate
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRollingWindowRequest {
    pub source_column: ColumnRef,
    pub output_column: String,
    pub window_size: usize,
    pub min_periods: usize,
    pub aggregate: String,
}
impl VortexRollingWindowRequest {
    #[must_use]
    pub fn new(
        source_column: ColumnRef,
        output_column: String,
        window_size: usize,
        min_periods: usize,
        aggregate: String,
    ) -> Self {
        Self {
            source_column,
            output_column,
            window_size,
            min_periods,
            aggregate,
        }
    }

    #[must_use]
    pub fn projected_columns(&self) -> Vec<ColumnRef> {
        vec![self.source_column.clone()]
    }

    #[must_use]
    pub fn output_columns(&self) -> Vec<String> {
        vec![self.output_column.clone()]
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "source_column={};output_column={};window_size={};min_periods={};aggregate={}",
            self.source_column.as_str(),
            self.output_column,
            self.window_size,
            self.min_periods,
            self.aggregate
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexSimpleAggregateMeasure {
    pub function: String,
    pub column: Option<ColumnRef>,
    pub alias: String,
    pub argument_offset: Option<i64>,
    pub value_transform: Option<String>,
}
impl VortexSimpleAggregateMeasure {
    #[must_use]
    pub fn new(function: impl Into<String>, column: Option<ColumnRef>, alias: String) -> Self {
        Self {
            function: function.into(),
            column,
            alias,
            argument_offset: None,
            value_transform: None,
        }
    }

    #[must_use]
    pub fn with_argument_offset(mut self, argument_offset: i64) -> Self {
        self.argument_offset = Some(argument_offset);
        self
    }

    #[must_use]
    pub fn with_value_transform(mut self, value_transform: impl Into<String>) -> Self {
        self.value_transform = Some(value_transform.into());
        self
    }

    #[must_use]
    pub fn summary(&self) -> String {
        let offset = self
            .argument_offset
            .map_or_else(String::new, |value| format!(" offset {value:+}"));
        let transform = self
            .value_transform
            .as_ref()
            .map_or_else(String::new, |value| format!(" transform {value}"));
        format!(
            "{}({}{}{}) as {}",
            self.function,
            self.column.as_ref().map_or("*", ColumnRef::as_str),
            transform,
            offset,
            self.alias
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexAggregateOrderExpr {
    pub column: String,
    pub descending: bool,
}
impl VortexAggregateOrderExpr {
    #[must_use]
    pub fn new(column: impl Into<String>, descending: bool) -> Self {
        Self {
            column: column.into(),
            descending,
        }
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{} {}",
            self.column,
            if self.descending { "desc" } else { "asc" }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexAggregateExpression {
    pub alias: String,
    pub column: ColumnRef,
    pub extra_columns: Vec<ColumnRef>,
    pub function: String,
    pub argument_offset: Option<i64>,
}
impl VortexAggregateExpression {
    #[must_use]
    pub fn new(alias: String, column: ColumnRef, function: impl Into<String>) -> Self {
        Self {
            alias,
            column,
            extra_columns: Vec::new(),
            function: function.into(),
            argument_offset: None,
        }
    }

    #[must_use]
    pub fn with_argument_offset(mut self, argument_offset: i64) -> Self {
        self.argument_offset = Some(argument_offset);
        self
    }

    #[must_use]
    pub fn with_extra_columns(mut self, extra_columns: Vec<ColumnRef>) -> Self {
        self.extra_columns = extra_columns;
        self
    }

    #[must_use]
    pub fn summary(&self) -> String {
        let offset = self
            .argument_offset
            .map_or_else(String::new, |value| format!(" offset {value:+}"));
        format!(
            "{}({}{}) as {}",
            self.function,
            self.column.as_str(),
            offset,
            self.alias
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexAggregateHavingExpr {
    pub column: String,
    pub op: ComparisonOp,
    pub value: String,
}
impl VortexAggregateHavingExpr {
    #[must_use]
    pub fn new(column: impl Into<String>, op: ComparisonOp, value: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            op,
            value: value.into(),
        }
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!("{} {:?} {}", self.column, self.op, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexSortRowsRequest {
    pub order_by: Vec<VortexAggregateOrderExpr>,
    pub offset: usize,
}
impl VortexSortRowsRequest {
    #[must_use]
    pub fn new(order_by: Vec<VortexAggregateOrderExpr>) -> Self {
        Self {
            order_by,
            offset: 0,
        }
    }

    #[must_use]
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order_by.is_empty()
    }

    #[must_use]
    pub fn order_columns(&self) -> Vec<ColumnRef> {
        self.order_by
            .iter()
            .filter_map(|order| ColumnRef::new(&order.column).ok())
            .collect()
    }

    #[must_use]
    pub fn summary(&self) -> String {
        let mut parts = vec![format!(
            "order_by={}",
            self.order_by
                .iter()
                .map(VortexAggregateOrderExpr::summary)
                .collect::<Vec<_>>()
                .join(",")
        )];
        if self.offset > 0 {
            parts.push(format!("offset={}", self.offset));
        }
        parts.join(";")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexSimpleAggregateRequest {
    pub group_by: Vec<ColumnRef>,
    pub group_expressions: Vec<VortexAggregateExpression>,
    pub measures: Vec<VortexSimpleAggregateMeasure>,
    pub order_by: Vec<VortexAggregateOrderExpr>,
    pub having: Vec<VortexAggregateHavingExpr>,
    pub offset: usize,
}
impl VortexSimpleAggregateRequest {
    #[must_use]
    pub fn new(measures: Vec<VortexSimpleAggregateMeasure>) -> Self {
        Self {
            group_by: Vec::new(),
            group_expressions: Vec::new(),
            measures,
            order_by: Vec::new(),
            having: Vec::new(),
            offset: 0,
        }
    }

    #[must_use]
    pub fn grouped(group_by: Vec<ColumnRef>, measures: Vec<VortexSimpleAggregateMeasure>) -> Self {
        Self {
            group_by,
            group_expressions: Vec::new(),
            measures,
            order_by: Vec::new(),
            having: Vec::new(),
            offset: 0,
        }
    }

    #[must_use]
    pub fn with_order_by(mut self, order_by: Vec<VortexAggregateOrderExpr>) -> Self {
        self.order_by = order_by;
        self
    }

    #[must_use]
    pub fn with_group_expressions(
        mut self,
        group_expressions: Vec<VortexAggregateExpression>,
    ) -> Self {
        self.group_expressions = group_expressions;
        self
    }

    #[must_use]
    pub fn with_having(mut self, having: Vec<VortexAggregateHavingExpr>) -> Self {
        self.having = having;
        self
    }

    #[must_use]
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.measures.is_empty()
    }

    #[must_use]
    pub fn projected_columns(&self) -> Vec<ColumnRef> {
        let mut out = Vec::new();
        for column in &self.group_by {
            if !out
                .iter()
                .any(|existing: &ColumnRef| existing.as_str() == column.as_str())
            {
                out.push(column.clone());
            }
        }
        for expression in &self.group_expressions {
            if !out
                .iter()
                .any(|existing: &ColumnRef| existing.as_str() == expression.column.as_str())
            {
                out.push(expression.column.clone());
            }
            for column in &expression.extra_columns {
                if !out
                    .iter()
                    .any(|existing: &ColumnRef| existing.as_str() == column.as_str())
                {
                    out.push(column.clone());
                }
            }
        }
        for measure in &self.measures {
            let Some(column) = &measure.column else {
                continue;
            };
            if !out
                .iter()
                .any(|existing: &ColumnRef| existing.as_str() == column.as_str())
            {
                out.push(column.clone());
            }
        }
        out
    }

    #[must_use]
    pub fn output_columns(&self) -> Vec<String> {
        self.group_by
            .iter()
            .map(|column| column.as_str().to_string())
            .chain(
                self.group_expressions
                    .iter()
                    .map(|expression| expression.alias.clone()),
            )
            .chain(self.measures.iter().map(|measure| measure.alias.clone()))
            .collect()
    }

    #[must_use]
    pub fn summary(&self) -> String {
        let measures = self
            .measures
            .iter()
            .map(VortexSimpleAggregateMeasure::summary)
            .collect::<Vec<_>>()
            .join(",");
        let mut parts = Vec::new();
        if !self.group_by.is_empty() {
            let group_by = self
                .group_by
                .iter()
                .map(ColumnRef::as_str)
                .collect::<Vec<_>>()
                .join(",");
            parts.push(format!("group_by={group_by}"));
        }
        if !self.group_expressions.is_empty() {
            parts.push(format!(
                "group_expressions={}",
                self.group_expressions
                    .iter()
                    .map(VortexAggregateExpression::summary)
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        parts.push(format!("measures={measures}"));
        if !self.order_by.is_empty() {
            parts.push(format!(
                "order_by={}",
                self.order_by
                    .iter()
                    .map(VortexAggregateOrderExpr::summary)
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if self.offset > 0 {
            parts.push(format!("offset={}", self.offset));
        }
        if !self.having.is_empty() {
            parts.push(format!(
                "having={}",
                self.having
                    .iter()
                    .map(VortexAggregateHavingExpr::summary)
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        parts.join(";")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveMode {
    MetadataOnly,
    EncodedReadRequired,
    Deferred,
    Unsupported,
}
impl VortexQueryPrimitiveMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::EncodedReadRequired => "encoded_read_required",
            Self::Deferred => "deferred",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn reads_data(&self) -> bool {
        matches!(self, Self::EncodedReadRequired)
    }
    #[must_use]
    pub const fn decodes_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn materializes_data(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveStatus {
    Planned,
    MetadataAnswered,
    NeedsEncodedRead,
    NeedsEncodedPredicate,
    NeedsProjection,
    MissingMetadata,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    Unsupported,
}
impl VortexQueryPrimitiveStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataAnswered => "metadata_answered",
            Self::NeedsEncodedRead => "needs_encoded_read",
            Self::NeedsEncodedPredicate => "needs_encoded_predicate",
            Self::NeedsProjection => "needs_projection",
            Self::MissingMetadata => "missing_metadata",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
    #[must_use]
    pub const fn has_result(&self) -> bool {
        matches!(self, Self::MetadataAnswered)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexQueryPrimitiveRequest {
    pub kind: VortexQueryPrimitiveKind,
    pub source_uri: Option<DatasetUri>,
    pub projection: ProjectionRequest,
    pub predicate: Option<PredicateExpr>,
    pub source_order_limit: Option<usize>,
    pub sample_seed: Option<u64>,
    pub sample_fraction: Option<f64>,
    pub sample_with_replacement: bool,
    pub sample_weight_column: Option<ColumnRef>,
    pub duplicate_keep: VortexDuplicateKeepPolicy,
    pub deduplicate_key_projection: Option<ProjectionRequest>,
    pub expression_projection: Option<VortexExpressionProjectionRequest>,
    pub melt_projection: Option<VortexMeltProjectionRequest>,
    pub explode_projection: Option<VortexExplodeProjectionRequest>,
    pub pivot_projection: Option<VortexPivotProjectionRequest>,
    pub rolling_window: Option<VortexRollingWindowRequest>,
    pub simple_aggregate: Option<VortexSimpleAggregateRequest>,
    pub sort_rows: Option<VortexSortRowsRequest>,
    pub structured_projection: Option<VortexStructuredProjectionRequest>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexQueryPrimitiveRequest {
    #[must_use]
    pub fn count_all(uri: DatasetUri) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::CountAll,
            source_uri: Some(uri),
            projection: ProjectionRequest::all(),
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn count_where(uri: DatasetUri, predicate: PredicateExpr) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::CountWhere,
            source_uri: Some(uri),
            projection: ProjectionRequest::all(),
            predicate: Some(predicate),
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn project(uri: DatasetUri, projection: ProjectionRequest) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::ProjectColumns,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn filter(uri: DatasetUri, predicate: PredicateExpr) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::FilterPredicate,
            source_uri: Some(uri),
            projection: ProjectionRequest::all(),
            predicate: Some(predicate),
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn filter_and_project(
        uri: DatasetUri,
        predicate: PredicateExpr,
        projection: ProjectionRequest,
    ) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::FilterAndProject,
            source_uri: Some(uri),
            projection,
            predicate: Some(predicate),
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn distinct_rows(
        uri: DatasetUri,
        projection: ProjectionRequest,
        predicate: Option<PredicateExpr>,
    ) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::DistinctRows,
            source_uri: Some(uri),
            projection,
            predicate,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn drop_duplicate_rows(
        uri: DatasetUri,
        output_projection: ProjectionRequest,
        key_projection: ProjectionRequest,
    ) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::DropDuplicateRows,
            source_uri: Some(uri),
            projection: output_projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: Some(key_projection),
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn duplicate_mask_rows(uri: DatasetUri, projection: ProjectionRequest) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::DuplicateMaskRows,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn tail_rows(uri: DatasetUri, projection: ProjectionRequest, limit: usize) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::TailRows,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: Some(limit),
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn sample_rows(
        uri: DatasetUri,
        projection: ProjectionRequest,
        predicate: Option<PredicateExpr>,
        limit: usize,
        seed: u64,
    ) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::SampleRows,
            source_uri: Some(uri),
            projection,
            predicate,
            source_order_limit: Some(limit),
            sample_seed: Some(seed),
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn sample_fraction_rows(
        uri: DatasetUri,
        projection: ProjectionRequest,
        predicate: Option<PredicateExpr>,
        fraction: f64,
        seed: u64,
    ) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::SampleRows,
            source_uri: Some(uri),
            projection,
            predicate,
            source_order_limit: None,
            sample_seed: Some(seed),
            sample_fraction: Some(fraction),
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn expression_project_rows(
        uri: DatasetUri,
        projection: ProjectionRequest,
        expression_projection: VortexExpressionProjectionRequest,
    ) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::ExpressionProjectRows,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: Some(expression_projection),
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn melt_rows(uri: DatasetUri, melt_projection: VortexMeltProjectionRequest) -> Self {
        let projection = ProjectionRequest::columns(melt_projection.projected_columns());
        Self {
            kind: VortexQueryPrimitiveKind::MeltRows,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: Some(melt_projection),
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn explode_rows(
        uri: DatasetUri,
        projection: ProjectionRequest,
        explode_projection: VortexExplodeProjectionRequest,
    ) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::ExplodeRows,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: Some(explode_projection),
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn pivot_rows(uri: DatasetUri, pivot_projection: VortexPivotProjectionRequest) -> Self {
        let projection = ProjectionRequest::columns(pivot_projection.projected_columns());
        Self {
            kind: VortexQueryPrimitiveKind::PivotRows,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: Some(pivot_projection),
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn rolling_window_rows(
        uri: DatasetUri,
        rolling_window: VortexRollingWindowRequest,
    ) -> Self {
        let projection = ProjectionRequest::columns(rolling_window.projected_columns());
        Self {
            kind: VortexQueryPrimitiveKind::RollingWindowRows,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: Some(rolling_window),
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn simple_aggregate(
        uri: DatasetUri,
        simple_aggregate: VortexSimpleAggregateRequest,
    ) -> Self {
        let projection = ProjectionRequest::columns(simple_aggregate.projected_columns());
        Self {
            kind: VortexQueryPrimitiveKind::SimpleAggregate,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: Some(simple_aggregate),
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn sort_rows(
        uri: DatasetUri,
        projection: ProjectionRequest,
        predicate: Option<PredicateExpr>,
        sort_rows: VortexSortRowsRequest,
        limit: usize,
    ) -> Self {
        Self {
            kind: VortexQueryPrimitiveKind::SortRows,
            source_uri: Some(uri),
            projection,
            predicate,
            source_order_limit: Some(limit),
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: Some(sort_rows),
            structured_projection: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn structured_project_rows(
        uri: DatasetUri,
        structured_projection: VortexStructuredProjectionRequest,
    ) -> Self {
        let projection = ProjectionRequest::columns(structured_projection.source_columns());
        Self {
            kind: VortexQueryPrimitiveKind::ExpressionProjectRows,
            source_uri: Some(uri),
            projection,
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: Some(structured_projection),
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn with_source_order_limit(mut self, limit: usize) -> Self {
        self.source_order_limit = Some(limit);
        self.sample_fraction = None;
        self
    }
    #[must_use]
    pub fn with_sample_seed(mut self, seed: u64) -> Self {
        self.sample_seed = Some(seed);
        self
    }
    #[must_use]
    pub fn with_sample_fraction(mut self, fraction: f64) -> Self {
        self.sample_fraction = Some(fraction);
        self.source_order_limit = None;
        self
    }
    #[must_use]
    pub const fn with_sample_replacement(mut self, replacement: bool) -> Self {
        self.sample_with_replacement = replacement;
        self
    }
    #[must_use]
    pub fn with_sample_weight_column(mut self, column: ColumnRef) -> Self {
        self.sample_weight_column = Some(column);
        self
    }
    #[must_use]
    pub const fn with_duplicate_keep(mut self, keep: VortexDuplicateKeepPolicy) -> Self {
        self.duplicate_keep = keep;
        self
    }
    #[must_use]
    pub fn with_deduplicate_key_projection(mut self, projection: ProjectionRequest) -> Self {
        self.deduplicate_key_projection = Some(projection);
        self
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut request = Self {
            kind: VortexQueryPrimitiveKind::Unsupported,
            source_uri: None,
            projection: ProjectionRequest::all(),
            predicate: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_with_replacement: false,
            sample_weight_column: None,
            duplicate_keep: VortexDuplicateKeepPolicy::First,
            deduplicate_key_projection: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            structured_projection: None,
            diagnostics: vec![],
        };
        request.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature.into(),
            "Requested query primitive is not supported for native `Vortex` execution.",
            Some(reason.into()),
        ));
        request
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "kind={} uri={} projection={} predicate={} source_order_limit={} sample_seed={} sample_fraction={} sample_with_replacement={} sample_weight_column={} duplicate_keep={} deduplicate_key_projection={} expression_projection={} structured_projection={} melt_projection={} explode_projection={} pivot_projection={} rolling_window={} simple_aggregate={} sort_rows={} diagnostics={}",
            self.kind.as_str(),
            self.source_uri
                .as_ref()
                .map_or("<none>", DatasetUri::as_str),
            self.projection.summary(),
            self.predicate
                .as_ref()
                .map_or_else(|| "none".to_string(), PredicateExpr::summary),
            self.source_order_limit
                .map_or_else(|| "none".to_string(), |limit| limit.to_string()),
            self.sample_seed
                .map_or_else(|| "none".to_string(), |seed| seed.to_string()),
            self.sample_fraction
                .map_or_else(|| "none".to_string(), format_fraction),
            self.sample_with_replacement,
            self.sample_weight_column
                .as_ref()
                .map_or("none", ColumnRef::as_str),
            self.duplicate_keep.as_str(),
            self.deduplicate_key_projection
                .as_ref()
                .map_or_else(|| "none".to_string(), ProjectionRequest::summary),
            self.expression_projection.as_ref().map_or_else(
                || "none".to_string(),
                VortexExpressionProjectionRequest::family_summary,
            ),
            self.structured_projection.as_ref().map_or_else(
                || "none".to_string(),
                VortexStructuredProjectionRequest::family_summary,
            ),
            self.melt_projection
                .as_ref()
                .map_or_else(|| "none".to_string(), VortexMeltProjectionRequest::summary),
            self.explode_projection.as_ref().map_or_else(
                || "none".to_string(),
                VortexExplodeProjectionRequest::summary
            ),
            self.pivot_projection
                .as_ref()
                .map_or_else(|| "none".to_string(), VortexPivotProjectionRequest::summary),
            self.rolling_window
                .as_ref()
                .map_or_else(|| "none".to_string(), VortexRollingWindowRequest::summary),
            self.simple_aggregate
                .as_ref()
                .map_or_else(|| "none".to_string(), VortexSimpleAggregateRequest::summary),
            self.sort_rows
                .as_ref()
                .map_or_else(|| "none".to_string(), VortexSortRowsRequest::summary),
            self.diagnostics.len()
        )
    }
}

fn format_fraction(value: f64) -> String {
    format!("{value:.12}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexQueryPrimitiveValue {
    Count(u64),
    Boolean(bool),
    Text(String),
    Unknown,
}
impl VortexQueryPrimitiveValue {
    #[must_use]
    pub fn as_str(&self) -> String {
        match self {
            Self::Count(v) => v.to_string(),
            Self::Boolean(v) => v.to_string(),
            Self::Text(v) => v.clone(),
            Self::Unknown => "unknown".to_string(),
        }
    }
    #[must_use]
    pub const fn is_known(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexQueryPrimitiveResult {
    pub status: VortexQueryPrimitiveStatus,
    pub mode: VortexQueryPrimitiveMode,
    pub request: VortexQueryPrimitiveRequest,
    pub value: VortexQueryPrimitiveValue,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexQueryPrimitiveResult {
    #[must_use]
    pub fn to_analysis_report(self) -> crate::VortexQueryPrimitiveAnalysisReport {
        crate::analyze_vortex_query_primitive_result(self)
    }
    #[must_use]
    pub fn metadata_answered(
        request: VortexQueryPrimitiveRequest,
        value: VortexQueryPrimitiveValue,
    ) -> Self {
        Self {
            status: VortexQueryPrimitiveStatus::MetadataAnswered,
            mode: VortexQueryPrimitiveMode::MetadataOnly,
            request,
            value,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn needs_encoded_read(
        request: VortexQueryPrimitiveRequest,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status: VortexQueryPrimitiveStatus::NeedsEncodedRead,
            mode: VortexQueryPrimitiveMode::EncodedReadRequired,
            request,
            value: VortexQueryPrimitiveValue::Unknown,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.add_diagnostic(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Warning,
            shardloom_core::DiagnosticCategory::Execution,
            "Encoded read is required for this primitive.",
            Some("vortex_query_primitive".to_string()),
            Some(reason.into()),
            Some(
                "Use metadata-only `CountAll` or wait for native encoded-read execution support."
                    .to_string(),
            ),
            shardloom_core::FallbackStatus::disabled_by_policy(),
        ));
        out
    }
    #[must_use]
    pub fn missing_metadata(
        request: VortexQueryPrimitiveRequest,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self::needs_encoded_read(request, reason);
        out.status = VortexQueryPrimitiveStatus::MissingMetadata;
        out.mode = VortexQueryPrimitiveMode::Deferred;
        out
    }
    #[must_use]
    pub fn unsupported(
        request: VortexQueryPrimitiveRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status: VortexQueryPrimitiveStatus::Unsupported,
            mode: VortexQueryPrimitiveMode::Unsupported,
            request,
            value: VortexQueryPrimitiveValue::Unknown,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature.into(),
            "Requested query primitive is unsupported for native execution.",
            Some(reason.into()),
        ));
        out
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "primitive: {}", self.request.kind.as_str());
        let _ = writeln!(text, "status: {}", self.status.as_str());
        let _ = writeln!(text, "mode: {}", self.mode.as_str());
        if self.value.is_known() {
            let _ = writeln!(text, "value: {}", self.value.as_str());
        }
        let _ = writeln!(text, "data read: {}", self.data_read);
        let _ = writeln!(text, "data decoded: {}", self.data_decoded);
        let _ = writeln!(text, "data materialized: {}", self.data_materialized);
        let _ = writeln!(text, "object-store io: {}", self.object_store_io);
        let _ = writeln!(text, "write io: {}", self.write_io);
        let _ = writeln!(text, "spill io: {}", self.spill_io_performed);
        let _ = writeln!(
            text,
            "fallback execution disabled: {}",
            !self.fallback_execution_allowed
        );
        if !self.diagnostics.is_empty() {
            let _ = writeln!(text, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(text, "- {} [{}]", d.message, d.code.as_str());
            }
        }
        text
    }
}

/// Evaluates metadata-only `CountAll` using a `VortexMetadataSummaryReport`.
/// # Errors
/// Returns an error only if `ShardLoom` detects an internal overflow conversion issue.
pub fn evaluate_vortex_count_all_from_summary(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
) -> Result<VortexQueryPrimitiveResult> {
    if request.kind != VortexQueryPrimitiveKind::CountAll {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "count_all",
            "Only `CountAll` is supported by metadata-count evaluation.",
        ));
    }
    if let Some(v) = summary.summary.row_count {
        return Ok(VortexQueryPrimitiveResult::metadata_answered(
            request,
            VortexQueryPrimitiveValue::Count(v),
        ));
    }
    if summary.summary.segments.is_empty() {
        return Ok(VortexQueryPrimitiveResult::missing_metadata(
            request,
            "no segment metadata available for CountAll evaluation",
        ));
    }
    let mut total = 0_u64;
    let mut any = false;
    for seg in &summary.summary.segments {
        let Some(rows) = seg.row_count else {
            return Ok(VortexQueryPrimitiveResult::missing_metadata(
                request,
                "segment row_count is missing",
            ));
        };
        total = total.checked_add(rows).ok_or_else(|| {
            shardloom_core::ShardLoomError::InvalidOperation(
                "row count overflow while summing segment metadata".to_string(),
            )
        })?;
        any = true;
    }
    if any {
        Ok(VortexQueryPrimitiveResult::metadata_answered(
            request,
            VortexQueryPrimitiveValue::Count(total),
        ))
    } else {
        Ok(VortexQueryPrimitiveResult::missing_metadata(
            request,
            "file and segment row_count metadata are unavailable",
        ))
    }
}

/// Evaluates metadata-only `CountWhere` using a `VortexMetadataSummaryReport`.
///
/// # Errors
/// Returns an error only if `ShardLoom` detects an internal overflow conversion issue.
pub fn evaluate_vortex_count_where_from_summary(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
) -> Result<VortexQueryPrimitiveResult> {
    if request.kind != VortexQueryPrimitiveKind::CountWhere {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "count_where",
            "Only `CountWhere` is supported by metadata-filtered count evaluation.",
        ));
    }
    let Some(predicate) = request.predicate.as_ref() else {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "count_where",
            "missing `PredicateExpr` for `CountWhere` request",
        ));
    };
    if summary.summary.segments.is_empty() {
        return Ok(VortexQueryPrimitiveResult::missing_metadata(
            request,
            "no segment metadata available for CountWhere evaluation",
        ));
    }
    let mut total = 0_u64;
    for seg in &summary.summary.segments {
        match crate::prove_predicate_from_segment_stats(predicate, seg) {
            shardloom_core::PredicateProof::AlwaysFalse { .. } => {}
            shardloom_core::PredicateProof::AlwaysTrue { .. } => {
                let Some(rows) = seg.row_count else {
                    return Ok(VortexQueryPrimitiveResult::missing_metadata(
                        request,
                        "segment row_count is required for metadata-proven true predicate",
                    ));
                };
                total = total.checked_add(rows).ok_or_else(|| {
                    shardloom_core::ShardLoomError::InvalidOperation(
                        "row count overflow while summing metadata-filtered count".to_string(),
                    )
                })?;
            }
            shardloom_core::PredicateProof::MayMatch { reason }
            | shardloom_core::PredicateProof::Unknown { reason } => {
                let mut out = VortexQueryPrimitiveResult::needs_encoded_read(request, reason);
                out.status = VortexQueryPrimitiveStatus::NeedsEncodedPredicate;
                out.mode = VortexQueryPrimitiveMode::Deferred;
                return Ok(out);
            }
            shardloom_core::PredicateProof::Unsupported { reason } => {
                return Ok(VortexQueryPrimitiveResult::unsupported(
                    request,
                    "count_where",
                    reason,
                ));
            }
        }
    }
    Ok(VortexQueryPrimitiveResult::metadata_answered(
        request,
        VortexQueryPrimitiveValue::Count(total),
    ))
}

/// Plans encoded projection intent for `ProjectColumns`/`FilterAndProject`.
/// # Errors
/// Returns an error only if `ShardLoom` observes malformed internal metadata state.
pub fn plan_vortex_encoded_projection(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
    _probe_report: Option<&crate::VortexEncodedReadProbeReport>,
) -> Result<VortexQueryPrimitiveResult> {
    if !matches!(
        request.kind,
        VortexQueryPrimitiveKind::ProjectColumns | VortexQueryPrimitiveKind::FilterAndProject
    ) {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "encoded_projection",
            "only `ProjectColumns` and `FilterAndProject` can plan encoded projection",
        ));
    }
    if request.projection.is_all() {
        return Ok(VortexQueryPrimitiveResult::needs_encoded_read(
            request,
            "projection=all requires encoded-read candidate planning",
        ));
    }
    let known_columns: std::collections::BTreeSet<&str> = summary
        .summary
        .segments
        .iter()
        .flat_map(|segment| segment.columns.iter())
        .filter_map(|column| {
            column
                .column
                .as_ref()
                .map(shardloom_core::ColumnRef::as_str)
        })
        .collect();
    if let ProjectionRequest::Columns(columns) = &request.projection {
        let missing: Vec<&str> = columns
            .iter()
            .map(shardloom_core::ColumnRef::as_str)
            .filter(|name| !known_columns.contains(*name))
            .collect();
        if !missing.is_empty() {
            let missing_text = missing.join(",");
            return Ok(VortexQueryPrimitiveResult::missing_metadata(
                request,
                format!("projection columns missing from metadata summary: {missing_text}"),
            ));
        }
    }
    let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
        request,
        "projection columns are metadata-known; encoded projection may be possible",
    );
    out.status = VortexQueryPrimitiveStatus::NeedsProjection;
    Ok(out)
}

/// Plans encoded predicate intent for `FilterPredicate`/`FilterAndProject`.
/// # Errors
/// Returns an error only if `ShardLoom` detects internal overflow while deriving metadata answers.
pub fn plan_vortex_encoded_predicate(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
    _probe_report: Option<&crate::VortexEncodedReadProbeReport>,
) -> Result<VortexQueryPrimitiveResult> {
    if !matches!(
        request.kind,
        VortexQueryPrimitiveKind::FilterPredicate | VortexQueryPrimitiveKind::FilterAndProject
    ) {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "encoded_predicate",
            "only `FilterPredicate` and `FilterAndProject` can plan encoded predicate",
        ));
    }
    let Some(predicate) = request.predicate.as_ref() else {
        return Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "encoded_predicate",
            "missing `PredicateExpr` for filter request",
        ));
    };
    let mut saw_segment = false;
    let mut saw_inconclusive = false;
    for segment in &summary.summary.segments {
        saw_segment = true;
        match crate::prove_predicate_from_segment_stats(predicate, segment) {
            shardloom_core::PredicateProof::AlwaysFalse { .. } => {}
            shardloom_core::PredicateProof::AlwaysTrue { .. }
            | shardloom_core::PredicateProof::MayMatch { .. }
            | shardloom_core::PredicateProof::Unknown { .. } => saw_inconclusive = true,
            shardloom_core::PredicateProof::Unsupported { reason } => {
                return Ok(VortexQueryPrimitiveResult::unsupported(
                    request,
                    "encoded_predicate",
                    reason,
                ));
            }
        }
    }
    if saw_segment && !saw_inconclusive {
        return Ok(VortexQueryPrimitiveResult::metadata_answered(
            request,
            VortexQueryPrimitiveValue::Boolean(false),
        ));
    }
    let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
        request,
        "metadata proof is inconclusive; encoded predicate planning is required",
    );
    out.status = VortexQueryPrimitiveStatus::NeedsEncodedPredicate;
    out.mode = VortexQueryPrimitiveMode::Deferred;
    Ok(out)
}

/// Evaluates a minimal `Vortex` query primitive against metadata summary.
/// # Errors
/// Returns an error only if metadata count evaluation overflows while summing rows.
#[allow(clippy::too_many_lines)]
pub fn evaluate_vortex_query_primitive(
    request: VortexQueryPrimitiveRequest,
    summary: &crate::VortexMetadataSummaryReport,
) -> Result<VortexQueryPrimitiveResult> {
    match request.kind {
        VortexQueryPrimitiveKind::CountAll => {
            evaluate_vortex_count_all_from_summary(request, summary)
        }
        VortexQueryPrimitiveKind::CountWhere => {
            evaluate_vortex_count_where_from_summary(request, summary)
        }
        VortexQueryPrimitiveKind::ProjectColumns => {
            plan_vortex_encoded_projection(request, summary, None)
        }
        VortexQueryPrimitiveKind::FilterPredicate => {
            plan_vortex_encoded_predicate(request, summary, None)
        }
        VortexQueryPrimitiveKind::FilterAndProject => {
            let predicate_result = plan_vortex_encoded_predicate(request.clone(), summary, None)?;
            if predicate_result.has_errors()
                || matches!(
                    predicate_result.status,
                    VortexQueryPrimitiveStatus::MetadataAnswered
                        | VortexQueryPrimitiveStatus::MissingMetadata
                )
            {
                Ok(predicate_result)
            } else {
                plan_vortex_encoded_projection(request, summary, None)
            }
        }
        VortexQueryPrimitiveKind::DistinctRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "row-level distinct requires a Vortex scan plus ShardLoom row-key de-duplication at the explicit bounded materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::DropDuplicateRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "retained-row drop_duplicates requires a Vortex scan plus ShardLoom row-key retention state at the explicit bounded materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::DuplicateMaskRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "duplicate mask requires a Vortex scan plus ShardLoom row-key state at the explicit bounded materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::TailRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "source-order tail requires a full Vortex scan plus ShardLoom final-row windowing at the explicit bounded materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::SampleRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "deterministic sample requires a Vortex scan plus ShardLoom seeded row selection at the explicit bounded materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::ExpressionProjectRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "expression projection requires a Vortex scan plus ShardLoom typed scalar rewrite at the explicit bounded materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::MeltRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "melt requires a Vortex scan plus ShardLoom scoped same-typed row expansion at the explicit bounded materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::ExplodeRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "explode requires a Vortex scan plus ShardLoom scoped list row expansion at the explicit bounded materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::PivotRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "pivot requires a Vortex scan plus ShardLoom scoped wide reshape state at the explicit bounded materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::RollingWindowRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "rolling window rows require a Vortex scan plus ShardLoom bounded source-order window state at the explicit materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::SimpleAggregate => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "simple scalar aggregate requires a Vortex scan plus ShardLoom aggregate state over projected scalar columns",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::SortRows => {
            let mut out = VortexQueryPrimitiveResult::needs_encoded_read(
                request,
                "bounded sort rows require a Vortex scan plus ShardLoom bounded order state at the explicit materialization boundary",
            );
            out.status = VortexQueryPrimitiveStatus::NeedsEncodedRead;
            out.mode = VortexQueryPrimitiveMode::Deferred;
            Ok(out)
        }
        VortexQueryPrimitiveKind::Unsupported => Ok(VortexQueryPrimitiveResult::unsupported(
            request,
            "unsupported",
            "Primitive is not supported in this phase.",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{ColumnRef, DatasetUri, SegmentId, SegmentStats};
    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/test.vortex").expect("uri")
    }
    fn empty_summary() -> crate::VortexMetadataSummaryReport {
        crate::VortexMetadataSummaryReport::unsupported("metadata_summary", "test fallback summary")
    }
    #[test]
    fn countall_no_read() {
        assert!(!VortexQueryPrimitiveKind::CountAll.requires_data_read());
    }
    #[test]
    fn countwhere_no_read() {
        assert!(!VortexQueryPrimitiveKind::CountWhere.requires_data_read());
    }
    #[test]
    fn project_may_read() {
        assert!(VortexQueryPrimitiveKind::ProjectColumns.requires_data_read());
    }
    #[test]
    fn metadata_mode_flags_false() {
        let m = VortexQueryPrimitiveMode::MetadataOnly;
        assert!(!m.reads_data() && !m.decodes_data() && !m.materializes_data());
    }
    #[test]
    fn status_meta_has_result() {
        assert!(VortexQueryPrimitiveStatus::MetadataAnswered.has_result());
    }
    #[test]
    fn unsupported_error() {
        assert!(VortexQueryPrimitiveStatus::Unsupported.is_error());
    }
    #[test]
    fn count_known() {
        assert!(VortexQueryPrimitiveValue::Count(7).is_known());
    }
    #[test]
    fn metadata_answer_side_effect_false() {
        let r = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            VortexQueryPrimitiveValue::Count(1),
        );
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn needs_read_side_effect_false() {
        let r = VortexQueryPrimitiveResult::needs_encoded_read(
            VortexQueryPrimitiveRequest::count_all(uri()),
            "x",
        );
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn unsupported_errors_no_fallback() {
        let r = VortexQueryPrimitiveResult::unsupported(
            VortexQueryPrimitiveRequest::count_all(uri()),
            "x",
            "y",
        );
        assert!(r.has_errors());
        assert!(!r.fallback_execution_allowed);
    }
    #[test]
    fn eval_count_file_row_count() {
        let mut s = empty_summary();
        s.summary.row_count = Some(11);
        let out = evaluate_vortex_count_all_from_summary(
            VortexQueryPrimitiveRequest::count_all(uri()),
            &s,
        )
        .expect("ok");
        assert_eq!(out.value, VortexQueryPrimitiveValue::Count(11));
    }
    #[test]
    fn eval_count_segments_sum() {
        let mut s = empty_summary();
        s.summary.segments = vec![
            crate::VortexSegmentMetadataSummary::unknown()
                .with_segment_id(SegmentId::new("s1").expect("id"))
                .with_row_count(2),
            crate::VortexSegmentMetadataSummary::unknown()
                .with_segment_id(SegmentId::new("s2").expect("id"))
                .with_row_count(3),
        ];
        let out = evaluate_vortex_count_all_from_summary(
            VortexQueryPrimitiveRequest::count_all(uri()),
            &s,
        )
        .expect("ok");
        assert_eq!(out.value, VortexQueryPrimitiveValue::Count(5));
    }
    #[test]
    fn eval_count_missing_metadata() {
        let s = empty_summary();
        let out = evaluate_vortex_count_all_from_summary(
            VortexQueryPrimitiveRequest::count_all(uri()),
            &s,
        )
        .expect("ok");
        assert_eq!(out.status, VortexQueryPrimitiveStatus::MissingMetadata);
    }
    #[test]
    fn eval_project_needs_read() {
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::project(uri(), ProjectionRequest::all()),
            &empty_summary(),
        )
        .expect("ok");
        assert_eq!(out.status, VortexQueryPrimitiveStatus::NeedsEncodedRead);
        assert!(out.is_side_effect_free());
    }
    #[test]
    fn eval_project_known_columns_needs_projection() {
        let mut s = empty_summary();
        let mut seg = crate::VortexSegmentMetadataSummary::unknown();
        seg.add_column(crate::VortexColumnMetadataSummary::new(
            ColumnRef::new("col1").expect("column"),
        ));
        s.summary.segments.push(seg);
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::project(
                uri(),
                ProjectionRequest::columns(vec![ColumnRef::new("col1").expect("column")]),
            ),
            &s,
        )
        .expect("ok");
        assert_eq!(out.status, VortexQueryPrimitiveStatus::NeedsProjection);
        assert!(out.is_side_effect_free());
    }
    #[test]
    fn eval_filter_inconclusive_needs_encoded_predicate() {
        let mut s = empty_summary();
        s.summary
            .segments
            .push(crate::VortexSegmentMetadataSummary::unknown());
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::filter(
                uri(),
                PredicateExpr::Compare {
                    column: ColumnRef::new("x").expect("column"),
                    op: shardloom_core::ComparisonOp::Eq,
                    value: shardloom_core::StatValue::Int64(7),
                },
            ),
            &s,
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexQueryPrimitiveStatus::NeedsEncodedPredicate
        );
        assert!(out.is_side_effect_free());
    }
    #[test]
    fn eval_filter_without_segment_stats_needs_encoded_predicate() {
        let s = empty_summary();
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::filter(
                uri(),
                PredicateExpr::Compare {
                    column: ColumnRef::new("x").expect("column"),
                    op: shardloom_core::ComparisonOp::Eq,
                    value: shardloom_core::StatValue::Int64(7),
                },
            ),
            &s,
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexQueryPrimitiveStatus::NeedsEncodedPredicate
        );
        assert_eq!(out.value, VortexQueryPrimitiveValue::Unknown);
        assert!(out.is_side_effect_free());
    }
    #[test]
    fn eval_filter_all_false_metadata_answered() {
        let mut s = empty_summary();
        let mut stats = SegmentStats::unknown();
        stats.null_count = Some(0);
        s.summary.segments = vec![seg_with_stats(Some(4), stats)];
        let out = evaluate_vortex_query_primitive(
            VortexQueryPrimitiveRequest::filter(
                uri(),
                PredicateExpr::IsNull {
                    column: ColumnRef::new("x").expect("column"),
                },
            ),
            &s,
        )
        .expect("ok");
        assert_eq!(out.status, VortexQueryPrimitiveStatus::MetadataAnswered);
        assert_eq!(out.value, VortexQueryPrimitiveValue::Boolean(false));
    }
    #[test]
    fn count_where_request_stores_predicate() {
        let req = VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::IsNull {
                column: ColumnRef::new("x").expect("column"),
            },
        );
        assert_eq!(req.kind, VortexQueryPrimitiveKind::CountWhere);
        assert!(req.predicate.is_some());
    }
    #[test]
    fn sample_fraction_builder_preserves_replacement_policy() {
        let from_count_sample =
            VortexQueryPrimitiveRequest::sample_rows(uri(), ProjectionRequest::all(), None, 10, 7)
                .with_sample_replacement(true)
                .with_sample_fraction(0.5);
        assert_eq!(from_count_sample.kind, VortexQueryPrimitiveKind::SampleRows);
        assert_eq!(from_count_sample.source_order_limit, None);
        assert_eq!(from_count_sample.sample_seed, Some(7));
        assert_eq!(from_count_sample.sample_fraction, Some(0.5));
        assert!(from_count_sample.sample_with_replacement);

        let from_fraction_sample = VortexQueryPrimitiveRequest::sample_fraction_rows(
            uri(),
            ProjectionRequest::all(),
            None,
            0.25,
            11,
        )
        .with_sample_replacement(true);
        assert_eq!(from_fraction_sample.sample_fraction, Some(0.25));
        assert_eq!(from_fraction_sample.sample_seed, Some(11));
        assert!(from_fraction_sample.sample_with_replacement);
    }
    fn seg_with_stats(
        row_count: Option<u64>,
        stats: SegmentStats,
    ) -> crate::VortexSegmentMetadataSummary {
        let mut s = crate::VortexSegmentMetadataSummary::unknown();
        s.row_count = row_count;
        let mut c = crate::VortexColumnMetadataSummary::new(ColumnRef::new("x").expect("column"));
        c.stats = stats;
        s.add_column(c);
        s
    }
    #[test]
    fn eval_count_where_all_false_returns_zero() {
        let mut s = empty_summary();
        let mut stats = SegmentStats::unknown();
        stats.null_count = Some(0);
        s.summary.segments = vec![seg_with_stats(Some(9), stats)];
        let req = VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::IsNull {
                column: ColumnRef::new("x").expect("column"),
            },
        );
        let out = evaluate_vortex_count_where_from_summary(req, &s).expect("ok");
        assert_eq!(out.value, VortexQueryPrimitiveValue::Count(0));
    }
    #[test]
    fn eval_count_where_all_true_sums_rows() {
        let mut s = empty_summary();
        let mut stats_a = SegmentStats::unknown();
        stats_a.null_count = Some(0);
        let mut stats_b = SegmentStats::unknown();
        stats_b.null_count = Some(0);
        s.summary.segments = vec![
            seg_with_stats(Some(2), stats_a),
            seg_with_stats(Some(3), stats_b),
        ];
        let req = VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").expect("column"),
            },
        );
        let out = evaluate_vortex_count_where_from_summary(req, &s).expect("ok");
        assert_eq!(out.value, VortexQueryPrimitiveValue::Count(5));
    }
    #[test]
    fn human_text_contains_flags() {
        let mut r = VortexQueryPrimitiveResult::needs_encoded_read(
            VortexQueryPrimitiveRequest::count_all(uri()),
            "x",
        );
        r.add_diagnostic(Diagnostic::no_fallback_execution("nope"));
        let t = r.to_human_text();
        assert!(t.contains("fallback execution disabled"));
        assert!(t.contains("data read: false"));
        assert!(t.contains("diagnostics:"));
    }
    #[test]
    fn side_effect_free_metadata_and_deferred() {
        let a = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            VortexQueryPrimitiveValue::Count(1),
        );
        let b = VortexQueryPrimitiveResult::missing_metadata(
            VortexQueryPrimitiveRequest::count_all(uri()),
            "missing",
        );
        assert!(a.is_side_effect_free());
        assert!(b.is_side_effect_free());
    }
}
