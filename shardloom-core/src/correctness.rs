//! Correctness and validation planning domain skeleton.
//!
#![allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::return_self_not_must_use,
    clippy::uninlined_format_args
)]

//! Correctness comes before performance. This module defines metadata-only types for
//! semantics, fixtures, diagnostics, differential baselines, and validation reports.
//! It does not execute tests, queries, external engines, or file I/O.

use crate::{
    BaselineEngine, CorrectnessValidationMode, Diagnostic, DiagnosticCategory, DiagnosticCode,
    DiagnosticSeverity, Result, ShardLoomError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticArea {
    Nulls,
    Types,
    FloatingPoint,
    Temporal,
    Strings,
    NestedData,
    MetadataOnly,
    Pruning,
    EncodedExecution,
    SelectionVectors,
    Materialization,
    Translation,
    Spill,
    ExternalEffects,
    UnsupportedDiagnostics,
}
impl SemanticArea {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Nulls => "nulls",
            Self::Types => "types",
            Self::FloatingPoint => "floating_point",
            Self::Temporal => "temporal",
            Self::Strings => "strings",
            Self::NestedData => "nested_data",
            Self::MetadataOnly => "metadata_only",
            Self::Pruning => "pruning",
            Self::EncodedExecution => "encoded_execution",
            Self::SelectionVectors => "selection_vectors",
            Self::Materialization => "materialization",
            Self::Translation => "translation",
            Self::Spill => "spill",
            Self::ExternalEffects => "external_effects",
            Self::UnsupportedDiagnostics => "unsupported_diagnostics",
        }
    }
    pub const fn canonical_label(&self) -> &'static str {
        self.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeCase {
    EmptyInput,
    SingleRow,
    AllNull,
    NoNulls,
    MixedNulls,
    HighCardinality,
    LowCardinality,
    DuplicateValues,
    SortedInput,
    UnsortedInput,
    MissingStatistics,
    ApproximateStatistics,
    DictionaryEncoded,
    SparseValidity,
    RunLengthEncoded,
    TemporalValues,
    NestedStructList,
    UnsupportedEncoding,
    UnsupportedDType,
    UnsupportedPlanShape,
    MetadataLoss,
}
impl EdgeCase {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EmptyInput => "empty_input",
            Self::SingleRow => "single_row",
            Self::AllNull => "all_null",
            Self::NoNulls => "no_nulls",
            Self::MixedNulls => "mixed_nulls",
            Self::HighCardinality => "high_cardinality",
            Self::LowCardinality => "low_cardinality",
            Self::DuplicateValues => "duplicate_values",
            Self::SortedInput => "sorted_input",
            Self::UnsortedInput => "unsorted_input",
            Self::MissingStatistics => "missing_statistics",
            Self::ApproximateStatistics => "approximate_statistics",
            Self::DictionaryEncoded => "dictionary_encoded",
            Self::SparseValidity => "sparse_validity",
            Self::RunLengthEncoded => "run_length_encoded",
            Self::TemporalValues => "temporal_values",
            Self::NestedStructList => "nested_struct_list",
            Self::UnsupportedEncoding => "unsupported_encoding",
            Self::UnsupportedDType => "unsupported_dtype",
            Self::UnsupportedPlanShape => "unsupported_plan_shape",
            Self::MetadataLoss => "metadata_loss",
        }
    }
    pub const fn canonical_label(&self) -> &'static str {
        self.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceRole {
    DecodedReference,
    ExternalOracle,
    GoldenFixture,
    GeneratedProperty,
    FuzzSeed,
}
impl ReferenceRole {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DecodedReference => "decoded_reference",
            Self::ExternalOracle => "external_oracle",
            Self::GoldenFixture => "golden_fixture",
            Self::GeneratedProperty => "generated_property",
            Self::FuzzSeed => "fuzz_seed",
        }
    }
    pub const fn is_production_execution(&self) -> bool {
        let _ = self;
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DifferentialBaseline {
    pub engine: BaselineEngine,
    pub version: Option<String>,
    pub role: ReferenceRole,
    pub notes: Option<String>,
}
impl DifferentialBaseline {
    pub fn new(engine: BaselineEngine) -> Self {
        Self {
            engine,
            version: None,
            role: ReferenceRole::ExternalOracle,
            notes: None,
        }
    }
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }
    pub fn with_notes(mut self, notes: impl Into<String>) -> Self {
        self.notes = Some(notes.into());
        self
    }
    pub fn external_correctness_oracle(engine: BaselineEngine) -> Self {
        Self::new(engine)
            .with_notes("external correctness oracle only; no runtime fallback execution")
    }
    pub const fn is_fallback_allowed(&self) -> bool {
        false
    }
    pub fn summary(&self) -> String {
        format!(
            "baseline={} role={} test/comparison only; fallback execution disabled",
            self.engine.as_str(),
            self.role.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FixtureId(String);
impl FixtureId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "fixture id cannot be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixtureFormat {
    ShardLoomNative,
    SqlLogicTestLike,
    JsonLike,
    Text,
    Generated,
    Unknown,
}
impl FixtureFormat {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ShardLoomNative => "shardloom_native",
            Self::SqlLogicTestLike => "sqllogictest_like",
            Self::JsonLike => "json_like",
            Self::Text => "text",
            Self::Generated => "generated",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedOutcome {
    Rows { row_count: Option<u64> },
    MetadataRowCount { row_count: u64 },
    EncodedCount { count: u64 },
    Diagnostic { code: DiagnosticCode },
    Unsupported { feature: String },
    MetadataOnly,
    NoSideEffects,
    NotYetDefined,
}
impl ExpectedOutcome {
    pub fn summary(&self) -> String {
        match self {
            Self::Rows { row_count } => format!("rows expected: {:?}", row_count),
            Self::MetadataRowCount { row_count } => {
                format!("metadata row count expected: {row_count}")
            }
            Self::EncodedCount { count } => format!("encoded count expected: {count}"),
            Self::Diagnostic { code } => format!("diagnostic expected: {}", code.as_str()),
            Self::Unsupported { feature } => format!("unsupported: {feature}"),
            Self::MetadataOnly => "metadata-only expectation".to_string(),
            Self::NoSideEffects => "plan-only no-side-effect expectation".to_string(),
            Self::NotYetDefined => "not yet defined".to_string(),
        }
    }
    pub const fn requires_execution(&self) -> bool {
        matches!(self, Self::Rows { .. } | Self::EncodedCount { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticExpectation {
    pub code: DiagnosticCode,
    pub category: DiagnosticCategory,
    pub severity: DiagnosticSeverity,
    pub fallback_attempted: bool,
}
impl DiagnosticExpectation {
    pub const fn new(
        code: DiagnosticCode,
        category: DiagnosticCategory,
        severity: DiagnosticSeverity,
    ) -> Self {
        Self {
            code,
            category,
            severity,
            fallback_attempted: false,
        }
    }
    pub fn from_diagnostic(diagnostic: &Diagnostic) -> Self {
        Self {
            code: diagnostic.code,
            category: diagnostic.category,
            severity: diagnostic.severity,
            fallback_attempted: diagnostic.fallback.attempted,
        }
    }
    pub fn matches(&self, diagnostic: &Diagnostic) -> bool {
        self.code == diagnostic.code
            && self.category == diagnostic.category
            && self.severity == diagnostic.severity
            && self.fallback_attempted == diagnostic.fallback.attempted
    }
    pub fn summary(&self) -> String {
        format!(
            "{} [{}:{}] fallback_attempted={}",
            self.code.as_str(),
            self.category.as_str(),
            self.severity.as_str(),
            self.fallback_attempted
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzSeed {
    pub target: String,
    pub seed: u64,
    pub reproducer: Option<String>,
}
impl FuzzSeed {
    pub fn new(target: impl Into<String>, seed: u64) -> Result<Self> {
        let target = target.into();
        if target.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "fuzz target cannot be empty".to_string(),
            ));
        }
        Ok(Self {
            target,
            seed,
            reproducer: None,
        })
    }
    pub fn with_reproducer(mut self, reproducer: impl Into<String>) -> Self {
        self.reproducer = Some(reproducer.into());
        self
    }
    pub fn summary(&self) -> String {
        format!("fuzz seed target={} seed={}", self.target, self.seed)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorrectnessFixture {
    pub id: FixtureId,
    pub format: FixtureFormat,
    pub semantic_areas: Vec<SemanticArea>,
    pub edge_cases: Vec<EdgeCase>,
    pub expected: ExpectedOutcome,
    pub source_ref: Option<String>,
    pub reference_roles: Vec<ReferenceRole>,
    pub diagnostics: Vec<Diagnostic>,
}
impl CorrectnessFixture {
    pub fn new(id: FixtureId, format: FixtureFormat) -> Self {
        Self {
            id,
            format,
            semantic_areas: vec![],
            edge_cases: vec![],
            expected: ExpectedOutcome::NotYetDefined,
            source_ref: None,
            reference_roles: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_semantic_area(&mut self, area: SemanticArea) {
        if !self.semantic_areas.contains(&area) {
            self.semantic_areas.push(area);
        }
    }
    pub fn add_edge_case(&mut self, edge_case: EdgeCase) {
        if !self.edge_cases.contains(&edge_case) {
            self.edge_cases.push(edge_case);
        }
    }
    pub fn with_expected(mut self, expected: ExpectedOutcome) -> Self {
        self.expected = expected;
        self
    }
    pub fn with_source_ref(mut self, source_ref: impl Into<String>) -> Self {
        let source_ref = source_ref.into();
        if !source_ref.trim().is_empty() {
            self.source_ref = Some(source_ref);
        }
        self
    }
    pub fn add_reference_role(&mut self, role: ReferenceRole) {
        if !self.reference_roles.contains(&role) {
            self.reference_roles.push(role);
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn covers_area(&self, area: SemanticArea) -> bool {
        self.semantic_areas.contains(&area)
    }
    pub fn covers_edge_case(&self, edge_case: EdgeCase) -> bool {
        self.edge_cases.contains(&edge_case)
    }
    pub fn has_reference_role(&self, role: ReferenceRole) -> bool {
        self.reference_roles.contains(&role)
    }
    pub fn reference_roles_are_test_only(&self) -> bool {
        self.reference_roles
            .iter()
            .all(|role| !role.is_production_execution())
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub fn summary(&self) -> String {
        format!(
            "fixture={} format={} areas={} edge_cases={}",
            self.id.as_str(),
            self.format.as_str(),
            self.semantic_areas.len(),
            self.edge_cases.len()
        )
    }
}

fn generated_fixture(
    id: &str,
    area: SemanticArea,
    edge: EdgeCase,
    expected: ExpectedOutcome,
) -> CorrectnessFixture {
    let mut fixture =
        CorrectnessFixture::new(FixtureId::new(id).expect("valid"), FixtureFormat::Generated)
            .with_expected(expected);
    fixture.add_semantic_area(area);
    fixture.add_edge_case(edge);
    fixture
}

fn vortex_metadata_footer_fixture() -> CorrectnessFixture {
    let mut fixture = CorrectnessFixture::new(
        FixtureId::new("vortex-metadata-footer-u64-20000").expect("valid"),
        FixtureFormat::ShardLoomNative,
    )
    .with_source_ref("shardloom-vortex/tests/fixtures/metadata_footer_u64_20000.vortex")
    .with_expected(ExpectedOutcome::MetadataRowCount { row_count: 20000 });
    fixture.add_semantic_area(SemanticArea::MetadataOnly);
    fixture.add_edge_case(EdgeCase::NoNulls);
    fixture.add_reference_role(ReferenceRole::GoldenFixture);
    fixture
}

fn vortex_local_encoded_count_fixture() -> CorrectnessFixture {
    let mut fixture = CorrectnessFixture::new(
        FixtureId::new("vortex-local-encoded-count-u64-20000").expect("valid"),
        FixtureFormat::ShardLoomNative,
    )
    .with_source_ref("shardloom-vortex/tests/fixtures/metadata_footer_u64_20000.vortex")
    .with_expected(ExpectedOutcome::EncodedCount { count: 20000 });
    fixture.add_semantic_area(SemanticArea::EncodedExecution);
    fixture.add_edge_case(EdgeCase::NoNulls);
    fixture.add_reference_role(ReferenceRole::GoldenFixture);
    fixture
}

fn default_external_oracle_baselines() -> Vec<DifferentialBaseline> {
    [
        BaselineEngine::Spark,
        BaselineEngine::DataFusion,
        BaselineEngine::DuckDb,
        BaselineEngine::Polars,
        BaselineEngine::Velox,
    ]
    .into_iter()
    .map(DifferentialBaseline::external_correctness_oracle)
    .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrectnessPlanStatus {
    Planned,
    NeedsReference,
    NeedsFixture,
    Unsupported,
}
impl CorrectnessPlanStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::NeedsReference => "needs_reference",
            Self::NeedsFixture => "needs_fixture",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorrectnessValidationPlan {
    pub name: String,
    pub mode: CorrectnessValidationMode,
    pub status: CorrectnessPlanStatus,
    pub fixtures: Vec<CorrectnessFixture>,
    pub baselines: Vec<DifferentialBaseline>,
    pub diagnostic_expectations: Vec<DiagnosticExpectation>,
    pub fuzz_seeds: Vec<FuzzSeed>,
    pub diagnostics: Vec<Diagnostic>,
}
impl CorrectnessValidationPlan {
    pub fn new(name: impl Into<String>, mode: CorrectnessValidationMode) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "plan name cannot be empty".to_string(),
            ));
        }
        Ok(Self {
            name,
            mode,
            status: CorrectnessPlanStatus::Planned,
            fixtures: vec![],
            baselines: vec![],
            diagnostic_expectations: vec![],
            fuzz_seeds: vec![],
            diagnostics: vec![],
        })
    }
    pub fn default_foundation_plan() -> Self {
        let mut plan = Self::new(
            "correctness-foundation",
            CorrectnessValidationMode::NotYetDefined,
        )
        .expect("valid");
        plan.add_fixture(vortex_metadata_footer_fixture());
        plan.add_fixture(vortex_local_encoded_count_fixture());
        for fixture in [
            generated_fixture(
                "null-semantics",
                SemanticArea::Nulls,
                EdgeCase::AllNull,
                ExpectedOutcome::NotYetDefined,
            ),
            generated_fixture(
                "metadata-only-correctness",
                SemanticArea::MetadataOnly,
                EdgeCase::MissingStatistics,
                ExpectedOutcome::MetadataOnly,
            ),
            generated_fixture(
                "pruning-correctness",
                SemanticArea::Pruning,
                EdgeCase::ApproximateStatistics,
                ExpectedOutcome::NotYetDefined,
            ),
            generated_fixture(
                "encoded-vs-decoded-reference",
                SemanticArea::EncodedExecution,
                EdgeCase::UnsupportedEncoding,
                ExpectedOutcome::NotYetDefined,
            ),
            generated_fixture(
                "translation-metadata-loss",
                SemanticArea::Translation,
                EdgeCase::MetadataLoss,
                ExpectedOutcome::Diagnostic {
                    code: DiagnosticCode::MetadataLoss,
                },
            ),
            generated_fixture(
                "unsupported-diagnostics",
                SemanticArea::UnsupportedDiagnostics,
                EdgeCase::UnsupportedPlanShape,
                ExpectedOutcome::Unsupported {
                    feature: "unsupported plan shape".to_string(),
                },
            ),
            generated_fixture(
                "plan-only-no-side-effects",
                SemanticArea::ExternalEffects,
                EdgeCase::EmptyInput,
                ExpectedOutcome::NoSideEffects,
            ),
            generated_fixture(
                "nested-data-edge-corpus",
                SemanticArea::NestedData,
                EdgeCase::NestedStructList,
                ExpectedOutcome::NotYetDefined,
            ),
            generated_fixture(
                "dictionary-encoded-edge-corpus",
                SemanticArea::EncodedExecution,
                EdgeCase::DictionaryEncoded,
                ExpectedOutcome::NotYetDefined,
            ),
            generated_fixture(
                "sparse-validity-edge-corpus",
                SemanticArea::SelectionVectors,
                EdgeCase::SparseValidity,
                ExpectedOutcome::NotYetDefined,
            ),
            generated_fixture(
                "run-length-edge-corpus",
                SemanticArea::EncodedExecution,
                EdgeCase::RunLengthEncoded,
                ExpectedOutcome::NotYetDefined,
            ),
            generated_fixture(
                "temporal-semantics",
                SemanticArea::Temporal,
                EdgeCase::TemporalValues,
                ExpectedOutcome::NotYetDefined,
            ),
        ] {
            plan.add_fixture(fixture);
        }
        for baseline in default_external_oracle_baselines() {
            plan.add_baseline(baseline);
        }
        plan
    }
    pub fn add_fixture(&mut self, fixture: CorrectnessFixture) {
        self.fixtures.push(fixture);
    }
    pub fn add_baseline(&mut self, baseline: DifferentialBaseline) {
        self.baselines.push(baseline);
    }
    pub fn add_diagnostic_expectation(&mut self, expectation: DiagnosticExpectation) {
        self.diagnostic_expectations.push(expectation);
    }
    pub fn add_fuzz_seed(&mut self, seed: FuzzSeed) {
        self.fuzz_seeds.push(seed);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn fixture_count(&self) -> usize {
        self.fixtures.len()
    }
    pub fn fixture_id_order(&self) -> Vec<&str> {
        self.fixtures
            .iter()
            .map(|fixture| fixture.id.as_str())
            .collect()
    }
    pub fn semantic_area_order(&self) -> Vec<&'static str> {
        let mut areas = Vec::new();
        for fixture in &self.fixtures {
            for area in &fixture.semantic_areas {
                let label = area.as_str();
                if !areas.contains(&label) {
                    areas.push(label);
                }
            }
        }
        areas
    }
    pub fn edge_case_order(&self) -> Vec<&'static str> {
        let mut edge_cases = Vec::new();
        for fixture in &self.fixtures {
            for edge_case in &fixture.edge_cases {
                let label = edge_case.as_str();
                if !edge_cases.contains(&label) {
                    edge_cases.push(label);
                }
            }
        }
        edge_cases
    }
    pub fn reference_role_order(&self) -> Vec<&'static str> {
        let mut roles = Vec::new();
        for fixture in &self.fixtures {
            for role in &fixture.reference_roles {
                let label = role.as_str();
                if !roles.contains(&label) {
                    roles.push(label);
                }
            }
        }
        for baseline in &self.baselines {
            let label = baseline.role.as_str();
            if !roles.contains(&label) {
                roles.push(label);
            }
        }
        roles
    }
    pub fn fixtures_with_source_ref_count(&self) -> usize {
        self.fixtures
            .iter()
            .filter(|fixture| fixture.source_ref.is_some())
            .count()
    }
    pub fn golden_fixture_count(&self) -> usize {
        self.fixtures
            .iter()
            .filter(|fixture| fixture.has_reference_role(ReferenceRole::GoldenFixture))
            .count()
    }
    pub fn executable_expected_output_count(&self) -> usize {
        self.fixtures
            .iter()
            .filter(|fixture| fixture.expected.requires_execution())
            .count()
    }
    pub fn not_yet_defined_fixture_count(&self) -> usize {
        self.fixtures
            .iter()
            .filter(|fixture| fixture.expected == ExpectedOutcome::NotYetDefined)
            .count()
    }
    pub fn diagnostic_expected_output_count(&self) -> usize {
        self.fixtures
            .iter()
            .filter(|fixture| matches!(fixture.expected, ExpectedOutcome::Diagnostic { .. }))
            .count()
    }
    pub fn unsupported_expected_output_count(&self) -> usize {
        self.fixtures
            .iter()
            .filter(|fixture| matches!(fixture.expected, ExpectedOutcome::Unsupported { .. }))
            .count()
    }
    pub fn required_foundation_edge_cases() -> &'static [EdgeCase] {
        &[
            EdgeCase::AllNull,
            EdgeCase::NestedStructList,
            EdgeCase::DictionaryEncoded,
            EdgeCase::SparseValidity,
            EdgeCase::RunLengthEncoded,
            EdgeCase::TemporalValues,
            EdgeCase::UnsupportedPlanShape,
        ]
    }
    pub fn covered_required_foundation_edge_case_count(&self) -> usize {
        Self::required_foundation_edge_cases()
            .iter()
            .filter(|edge_case| {
                self.fixtures
                    .iter()
                    .any(|fixture| fixture.covers_edge_case(**edge_case))
            })
            .count()
    }
    pub fn missing_required_foundation_edge_cases(&self) -> Vec<&'static str> {
        Self::required_foundation_edge_cases()
            .iter()
            .filter(|edge_case| {
                !self
                    .fixtures
                    .iter()
                    .any(|fixture| fixture.covers_edge_case(**edge_case))
            })
            .map(EdgeCase::as_str)
            .collect()
    }
    pub fn required_foundation_edge_cases_covered(&self) -> bool {
        self.missing_required_foundation_edge_cases().is_empty()
    }
    pub fn reference_roles_are_test_only(&self) -> bool {
        self.fixtures
            .iter()
            .all(CorrectnessFixture::reference_roles_are_test_only)
            && self
                .baselines
                .iter()
                .all(|baseline| !baseline.role.is_production_execution())
    }
    pub fn baseline_count(&self) -> usize {
        self.baselines.len()
    }
    pub fn has_baseline(&self, engine: BaselineEngine) -> bool {
        self.baselines
            .iter()
            .any(|baseline| baseline.engine == engine)
    }
    pub fn baselines_are_fallback_free(&self) -> bool {
        self.baselines
            .iter()
            .all(|baseline| !baseline.is_fallback_allowed())
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "Correctness validation plan: {}\nmode: {}\nstatus: {}\nfixtures: {}\nfallback execution: disabled\nexternal baselines: test/comparison only",
            self.name,
            self.mode.as_str(),
            self.status.as_str(),
            self.fixtures.len()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationResultStatus {
    NotRun,
    Passed,
    Failed,
    Unsupported,
}
impl ValidationResultStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotRun => "not_run",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_failure(&self) -> bool {
        matches!(self, Self::Failed | Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorrectnessValidationReport {
    pub plan_name: String,
    pub status: ValidationResultStatus,
    pub fixtures_checked: usize,
    pub diagnostics: Vec<Diagnostic>,
    pub notes: Vec<String>,
}
impl CorrectnessValidationReport {
    fn validated_name(plan_name: impl Into<String>) -> Result<String> {
        let n = plan_name.into();
        if n.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "plan name cannot be empty".to_string(),
            ));
        }
        Ok(n)
    }
    pub fn not_run(plan_name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            plan_name: Self::validated_name(plan_name)?,
            status: ValidationResultStatus::NotRun,
            fixtures_checked: 0,
            diagnostics: vec![],
            notes: vec![],
        })
    }
    pub fn passed(plan_name: impl Into<String>, fixtures_checked: usize) -> Result<Self> {
        Ok(Self {
            plan_name: Self::validated_name(plan_name)?,
            status: ValidationResultStatus::Passed,
            fixtures_checked,
            diagnostics: vec![],
            notes: vec![],
        })
    }
    pub fn failed(plan_name: impl Into<String>, diagnostic: Diagnostic) -> Result<Self> {
        Ok(Self {
            plan_name: Self::validated_name(plan_name)?,
            status: ValidationResultStatus::Failed,
            fixtures_checked: 0,
            diagnostics: vec![diagnostic],
            notes: vec![],
        })
    }
    pub fn add_note(&mut self, note: impl Into<String>) {
        self.notes.push(note.into());
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_failure()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "Correctness validation report: {}\nstatus: {}\nfixtures_checked: {}\nfallback execution: disabled",
            self.plan_name,
            self.status.as_str(),
            self.fixtures_checked
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FallbackStatus;
    fn sample_diag() -> Diagnostic {
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "x",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        )
    }
    #[test]
    fn semantic_area_labels_stable() {
        assert_eq!(SemanticArea::Nulls.canonical_label(), "nulls");
        assert_eq!(
            SemanticArea::EncodedExecution.canonical_label(),
            "encoded_execution"
        );
    }
    #[test]
    fn edge_case_labels_stable() {
        assert_eq!(EdgeCase::EmptyInput.canonical_label(), "empty_input");
        assert_eq!(EdgeCase::AllNull.canonical_label(), "all_null");
    }
    #[test]
    fn reference_role_is_never_production() {
        for role in [
            ReferenceRole::DecodedReference,
            ReferenceRole::ExternalOracle,
            ReferenceRole::GoldenFixture,
            ReferenceRole::GeneratedProperty,
            ReferenceRole::FuzzSeed,
        ] {
            assert!(!role.is_production_execution());
        }
    }
    #[test]
    fn differential_baseline_fallback_disabled() {
        assert!(!DifferentialBaseline::new(BaselineEngine::Spark).is_fallback_allowed());
    }
    #[test]
    fn differential_baseline_summary_mentions_policy() {
        let s = DifferentialBaseline::new(BaselineEngine::Spark).summary();
        assert!(s.contains("test/comparison only") || s.contains("fallback execution disabled"));
    }
    #[test]
    fn fixture_id_rejects_empty() {
        assert!(FixtureId::new("   ").is_err());
    }
    #[test]
    fn fuzz_seed_rejects_empty_target() {
        assert!(FuzzSeed::new("", 1).is_err());
    }
    #[test]
    fn expected_outcome_diag_no_exec() {
        assert!(
            !ExpectedOutcome::Diagnostic {
                code: DiagnosticCode::NotImplemented
            }
            .requires_execution()
        );
    }
    #[test]
    fn expected_outcome_rows_exec() {
        assert!(ExpectedOutcome::Rows { row_count: None }.requires_execution());
    }
    #[test]
    fn expected_outcome_encoded_count_exec() {
        assert!(ExpectedOutcome::EncodedCount { count: 42 }.requires_execution());
    }
    #[test]
    fn diagnostic_expectation_from_diagnostic_matches() {
        let d = sample_diag();
        let e = DiagnosticExpectation::from_diagnostic(&d);
        assert!(e.matches(&d));
    }
    #[test]
    fn fixture_covers_semantic_areas() {
        let mut f =
            CorrectnessFixture::new(FixtureId::new("a").expect("ok"), FixtureFormat::Generated);
        f.add_semantic_area(SemanticArea::Nulls);
        assert!(f.covers_area(SemanticArea::Nulls));
    }
    #[test]
    fn fixture_covers_edge_cases() {
        let mut f =
            CorrectnessFixture::new(FixtureId::new("a").expect("ok"), FixtureFormat::Generated);
        f.add_edge_case(EdgeCase::AllNull);
        assert!(f.covers_edge_case(EdgeCase::AllNull));
    }
    #[test]
    fn plan_rejects_empty_names() {
        assert!(
            CorrectnessValidationPlan::new("", CorrectnessValidationMode::NotYetDefined).is_err()
        );
    }
    #[test]
    fn plan_fallback_disabled() {
        let p = CorrectnessValidationPlan::default_foundation_plan();
        assert!(!p.fallback_execution_allowed());
    }
    #[test]
    fn plan_has_at_least_six_fixtures() {
        assert!(CorrectnessValidationPlan::default_foundation_plan().fixture_count() >= 6);
    }
    #[test]
    fn foundation_plan_exposes_coverage_inventory() {
        let plan = CorrectnessValidationPlan::default_foundation_plan();

        assert_eq!(plan.fixture_count(), 14);
        assert_eq!(plan.fixtures_with_source_ref_count(), 2);
        assert_eq!(plan.golden_fixture_count(), 2);
        assert_eq!(plan.executable_expected_output_count(), 1);
        assert_eq!(plan.not_yet_defined_fixture_count(), 8);
        assert_eq!(plan.diagnostic_expected_output_count(), 1);
        assert_eq!(plan.unsupported_expected_output_count(), 1);
        assert_eq!(plan.baseline_count(), 5);
        assert!(plan.required_foundation_edge_cases_covered());
        assert_eq!(plan.covered_required_foundation_edge_case_count(), 7);
        assert!(plan.missing_required_foundation_edge_cases().is_empty());
        assert!(plan.reference_roles_are_test_only());
        assert!(plan.baselines_are_fallback_free());
        assert_eq!(
            plan.reference_role_order(),
            vec!["golden_fixture", "external_oracle"]
        );
    }
    #[test]
    fn plan_text_mentions_fallback_disabled() {
        assert!(
            CorrectnessValidationPlan::default_foundation_plan()
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn failed_status_is_failure() {
        assert!(ValidationResultStatus::Failed.is_failure());
    }
    #[test]
    fn report_not_run_rejects_empty() {
        assert!(CorrectnessValidationReport::not_run(" ").is_err());
    }
    #[test]
    fn report_failed_has_errors() {
        let r = CorrectnessValidationReport::failed("p", sample_diag()).expect("ok");
        assert!(r.has_errors());
    }
}
