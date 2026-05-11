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
    DiagnosticSeverity, FallbackStatus, Result, ShardLoomError,
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
pub struct ReferenceArtifact {
    pub artifact_id: String,
    pub role: ReferenceRole,
    pub expected: ExpectedOutcome,
    pub semantic_profile: String,
    pub materialization_boundary: String,
    pub execution_performed: bool,
    pub fallback_attempted: bool,
}
impl ReferenceArtifact {
    pub fn decoded_reference_output(
        artifact_id: impl Into<String>,
        expected: ExpectedOutcome,
    ) -> Self {
        Self {
            artifact_id: artifact_id.into(),
            role: ReferenceRole::DecodedReference,
            expected,
            semantic_profile: "shardloom_native_test_reference".to_string(),
            materialization_boundary: "test_only_logical_reference_output".to_string(),
            execution_performed: false,
            fallback_attempted: false,
        }
    }
    pub const fn is_decoded_reference_output(&self) -> bool {
        matches!(self.role, ReferenceRole::DecodedReference)
    }
    pub const fn is_test_only(&self) -> bool {
        !self.role.is_production_execution()
            && !self.execution_performed
            && !self.fallback_attempted
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
    pub reference_artifacts: Vec<ReferenceArtifact>,
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
            reference_artifacts: vec![],
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
    pub fn add_reference_artifact(&mut self, artifact: ReferenceArtifact) {
        self.add_reference_role(artifact.role);
        self.reference_artifacts.push(artifact);
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
    pub fn decoded_reference_artifact_count(&self) -> usize {
        self.reference_artifacts
            .iter()
            .filter(|artifact| artifact.is_decoded_reference_output())
            .count()
    }
    pub fn reference_artifacts_are_test_only(&self) -> bool {
        self.reference_artifacts
            .iter()
            .all(ReferenceArtifact::is_test_only)
    }
    pub fn reference_roles_are_test_only(&self) -> bool {
        self.reference_roles
            .iter()
            .all(|role| !role.is_production_execution())
            && self.reference_artifacts_are_test_only()
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

fn add_decoded_reference_artifact(fixture: &mut CorrectnessFixture, suffix: &str) {
    fixture.add_reference_artifact(ReferenceArtifact::decoded_reference_output(
        format!("{}.decoded-reference.{suffix}", fixture.id.as_str()),
        fixture.expected.clone(),
    ));
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
    add_decoded_reference_artifact(&mut fixture, "count");
    fixture
}

fn local_primitive_struct_count_all_fixture() -> CorrectnessFixture {
    let mut fixture = CorrectnessFixture::new(
        FixtureId::new("vortex-local-count-all-struct-five").expect("valid"),
        FixtureFormat::ShardLoomNative,
    )
    .with_source_ref("shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
    .with_expected(ExpectedOutcome::EncodedCount { count: 5 });
    fixture.add_semantic_area(SemanticArea::EncodedExecution);
    fixture.add_edge_case(EdgeCase::NoNulls);
    fixture.add_reference_role(ReferenceRole::GoldenFixture);
    add_decoded_reference_artifact(&mut fixture, "count");
    fixture
}

fn local_primitive_struct_rows_fixture(
    id: &str,
    edge_case: EdgeCase,
    row_count: u64,
) -> CorrectnessFixture {
    let mut fixture = CorrectnessFixture::new(
        FixtureId::new(id).expect("valid"),
        FixtureFormat::ShardLoomNative,
    )
    .with_source_ref("shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
    .with_expected(ExpectedOutcome::Rows {
        row_count: Some(row_count),
    });
    fixture.add_semantic_area(SemanticArea::EncodedExecution);
    fixture.add_edge_case(edge_case);
    fixture.add_reference_role(ReferenceRole::GoldenFixture);
    add_decoded_reference_artifact(&mut fixture, "rows");
    fixture
}

fn add_local_primitive_foundation_fixtures(plan: &mut CorrectnessValidationPlan) {
    plan.add_fixture(local_primitive_struct_count_all_fixture());
    plan.add_fixture(local_primitive_struct_rows_fixture(
        "vortex-local-count-where-struct-five",
        EdgeCase::NoNulls,
        3,
    ));
    plan.add_fixture(local_primitive_struct_rows_fixture(
        "vortex-local-project-struct-five",
        EdgeCase::NoNulls,
        5,
    ));
    plan.add_fixture(local_primitive_struct_rows_fixture(
        "vortex-local-filter-struct-five",
        EdgeCase::NoNulls,
        3,
    ));
    plan.add_fixture(local_primitive_struct_rows_fixture(
        "vortex-local-filter-project-struct-five",
        EdgeCase::NoNulls,
        3,
    ));
}

fn prepared_encoded_rows_fixture(
    id: &str,
    primary_area: SemanticArea,
    edge_cases: &[EdgeCase],
    row_count: u64,
) -> CorrectnessFixture {
    let expected = ExpectedOutcome::Rows {
        row_count: Some(row_count),
    };
    let mut fixture =
        CorrectnessFixture::new(FixtureId::new(id).expect("valid"), FixtureFormat::Generated)
            .with_expected(expected.clone());
    fixture.add_semantic_area(primary_area);
    for edge_case in edge_cases {
        fixture.add_edge_case(*edge_case);
    }
    fixture.add_reference_role(ReferenceRole::GoldenFixture);
    add_decoded_reference_artifact(&mut fixture, "rows");
    fixture
}

fn add_prepared_encoded_foundation_fixtures(plan: &mut CorrectnessValidationPlan) {
    plan.add_fixture(prepared_encoded_rows_fixture(
        "vortex-prepared-encoded-filter-dictionary-run",
        SemanticArea::SelectionVectors,
        &[EdgeCase::DictionaryEncoded, EdgeCase::RunLengthEncoded],
        5,
    ));
    plan.add_fixture(prepared_encoded_rows_fixture(
        "vortex-prepared-encoded-projection-dictionary",
        SemanticArea::EncodedExecution,
        &[EdgeCase::DictionaryEncoded],
        3,
    ));
    plan.add_fixture(prepared_encoded_rows_fixture(
        "vortex-prepared-encoded-filter-project-selection-vector",
        SemanticArea::SelectionVectors,
        &[EdgeCase::SparseValidity, EdgeCase::RunLengthEncoded],
        5,
    ));
}

fn edge_case_executable_fixture(
    id: &str,
    primary_area: SemanticArea,
    edge_cases: &[EdgeCase],
    expected: ExpectedOutcome,
) -> CorrectnessFixture {
    let suffix = if matches!(expected, ExpectedOutcome::EncodedCount { .. }) {
        "count"
    } else {
        "rows"
    };
    let mut fixture =
        CorrectnessFixture::new(FixtureId::new(id).expect("valid"), FixtureFormat::Generated)
            .with_expected(expected);
    fixture.add_semantic_area(primary_area);
    for edge_case in edge_cases {
        fixture.add_edge_case(*edge_case);
    }
    fixture.add_reference_role(ReferenceRole::GoldenFixture);
    add_decoded_reference_artifact(&mut fixture, suffix);
    fixture
}

fn add_edge_case_executable_fixtures(plan: &mut CorrectnessValidationPlan) {
    for fixture in [
        edge_case_executable_fixture(
            "vortex-edge-count-all-empty-input",
            SemanticArea::EncodedExecution,
            &[EdgeCase::EmptyInput],
            ExpectedOutcome::EncodedCount { count: 0 },
        ),
        edge_case_executable_fixture(
            "vortex-edge-project-single-row",
            SemanticArea::EncodedExecution,
            &[EdgeCase::SingleRow],
            ExpectedOutcome::Rows { row_count: Some(1) },
        ),
        edge_case_executable_fixture(
            "vortex-edge-filter-all-null",
            SemanticArea::Nulls,
            &[EdgeCase::AllNull],
            ExpectedOutcome::Rows { row_count: Some(0) },
        ),
        edge_case_executable_fixture(
            "vortex-edge-filter-mixed-null-sparse",
            SemanticArea::SelectionVectors,
            &[EdgeCase::MixedNulls, EdgeCase::SparseValidity],
            ExpectedOutcome::Rows { row_count: Some(2) },
        ),
        edge_case_executable_fixture(
            "vortex-edge-filter-duplicate-low-cardinality",
            SemanticArea::EncodedExecution,
            &[EdgeCase::DuplicateValues, EdgeCase::LowCardinality],
            ExpectedOutcome::Rows { row_count: Some(4) },
        ),
        edge_case_executable_fixture(
            "vortex-edge-project-high-cardinality",
            SemanticArea::EncodedExecution,
            &[EdgeCase::HighCardinality],
            ExpectedOutcome::Rows {
                row_count: Some(1024),
            },
        ),
        edge_case_executable_fixture(
            "vortex-edge-filter-project-sorted-dictionary",
            SemanticArea::SelectionVectors,
            &[EdgeCase::SortedInput, EdgeCase::DictionaryEncoded],
            ExpectedOutcome::Rows { row_count: Some(3) },
        ),
        edge_case_executable_fixture(
            "vortex-edge-filter-project-unsorted-rle",
            SemanticArea::SelectionVectors,
            &[EdgeCase::UnsortedInput, EdgeCase::RunLengthEncoded],
            ExpectedOutcome::Rows { row_count: Some(3) },
        ),
        edge_case_executable_fixture(
            "vortex-edge-filter-temporal-values",
            SemanticArea::Temporal,
            &[EdgeCase::TemporalValues],
            ExpectedOutcome::Rows { row_count: Some(2) },
        ),
    ] {
        plan.add_fixture(fixture);
    }
}

fn generated_property_fixture(
    id: &str,
    primary_area: SemanticArea,
    edge_cases: &[EdgeCase],
) -> CorrectnessFixture {
    let mut fixture =
        CorrectnessFixture::new(FixtureId::new(id).expect("valid"), FixtureFormat::Generated)
            .with_expected(ExpectedOutcome::NoSideEffects);
    fixture.add_semantic_area(primary_area);
    for edge_case in edge_cases {
        fixture.add_edge_case(*edge_case);
    }
    fixture.add_reference_role(ReferenceRole::GeneratedProperty);
    fixture
}

fn add_property_fuzz_foundation(plan: &mut CorrectnessValidationPlan) {
    for fixture in [
        generated_property_fixture(
            "property-encoded-filter-selection-vector-consistency",
            SemanticArea::SelectionVectors,
            &[EdgeCase::SparseValidity, EdgeCase::MixedNulls],
        ),
        generated_property_fixture(
            "property-encoded-projection-preserves-row-order",
            SemanticArea::EncodedExecution,
            &[EdgeCase::SortedInput, EdgeCase::UnsortedInput],
        ),
        generated_property_fixture(
            "property-encoded-filter-project-composition",
            SemanticArea::SelectionVectors,
            &[EdgeCase::DictionaryEncoded, EdgeCase::RunLengthEncoded],
        ),
    ] {
        plan.add_fixture(fixture);
    }

    plan.add_fuzz_seed(
        FuzzSeed::new("encoded_filter_selection_vector", 0x5E1E_C710_0001)
            .expect("valid")
            .with_reproducer("fixture-family=selection_vector; null_policy=mixed"),
    );
    plan.add_fuzz_seed(
        FuzzSeed::new("encoded_projection_ordering", 0x5E1E_C710_0002)
            .expect("valid")
            .with_reproducer("fixture-family=projection; ordering=sorted_unsorted_pair"),
    );
    plan.add_fuzz_seed(
        FuzzSeed::new("encoded_filter_project_composition", 0x5E1E_C710_0003)
            .expect("valid")
            .with_reproducer("fixture-family=filter_project; encodings=dictionary_run_length"),
    );
}

fn default_external_oracle_baselines() -> Vec<DifferentialBaseline> {
    [
        BaselineEngine::Spark,
        BaselineEngine::DataFusion,
        BaselineEngine::DuckDb,
        BaselineEngine::Polars,
        BaselineEngine::Pandas,
        BaselineEngine::Dask,
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
        add_local_primitive_foundation_fixtures(&mut plan);
        add_prepared_encoded_foundation_fixtures(&mut plan);
        add_edge_case_executable_fixtures(&mut plan);
        add_property_fuzz_foundation(&mut plan);
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
    pub fn baseline_engine_order(&self) -> Vec<&'static str> {
        self.baselines
            .iter()
            .map(|baseline| baseline.engine.as_str())
            .collect()
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
    pub fn reference_artifact_count(&self) -> usize {
        self.fixtures
            .iter()
            .map(|fixture| fixture.reference_artifacts.len())
            .sum()
    }
    pub fn decoded_reference_output_count(&self) -> usize {
        self.fixtures
            .iter()
            .map(CorrectnessFixture::decoded_reference_artifact_count)
            .sum()
    }
    pub fn decoded_reference_artifact_id_order(&self) -> Vec<&str> {
        let mut ids = Vec::new();
        for fixture in &self.fixtures {
            for artifact in &fixture.reference_artifacts {
                if artifact.is_decoded_reference_output() {
                    ids.push(artifact.artifact_id.as_str());
                }
            }
        }
        ids
    }
    pub fn decoded_reference_output_coverage_complete(&self) -> bool {
        let mut has_executable_fixture = false;
        for fixture in &self.fixtures {
            if fixture.expected.requires_execution() {
                has_executable_fixture = true;
                if fixture.decoded_reference_artifact_count() == 0 {
                    return false;
                }
            }
        }
        has_executable_fixture
    }
    pub fn generated_property_fixture_count(&self) -> usize {
        self.fixtures
            .iter()
            .filter(|fixture| fixture.has_reference_role(ReferenceRole::GeneratedProperty))
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
    pub fn unsupported_diagnostic_fixture_count(&self) -> usize {
        self.diagnostic_expected_output_count() + self.unsupported_expected_output_count()
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
pub enum CorrectnessDifferentialHarnessStatus {
    EvidenceComplete,
    NeedsEvidence,
    UnsafeFallbackPolicy,
}
impl CorrectnessDifferentialHarnessStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvidenceComplete => "evidence_complete",
            Self::NeedsEvidence => "needs_evidence",
            Self::UnsafeFallbackPolicy => "unsafe_fallback_policy",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::UnsafeFallbackPolicy)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct CorrectnessDifferentialHarnessReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub plan_name: String,
    pub plan_mode: CorrectnessValidationMode,
    pub status: CorrectnessDifferentialHarnessStatus,
    pub fixture_count: usize,
    pub fixtures_with_source_ref_count: usize,
    pub golden_fixture_count: usize,
    pub reference_artifact_count: usize,
    pub decoded_reference_output_count: usize,
    pub decoded_reference_artifact_id_order: Vec<String>,
    pub decoded_reference_output_coverage_complete: bool,
    pub executable_expected_output_count: usize,
    pub not_yet_defined_fixture_count: usize,
    pub unsupported_diagnostic_fixture_count: usize,
    pub required_edge_case_count: usize,
    pub covered_required_edge_case_count: usize,
    pub missing_required_edge_cases: Vec<String>,
    pub baseline_count: usize,
    pub baseline_engine_order: Vec<String>,
    pub reference_role_order: Vec<String>,
    pub generated_property_fixture_count: usize,
    pub fuzz_seed_count: usize,
    pub planned_surface_count: usize,
    pub blocked_surface_count: usize,
    pub blocked_surface_order: Vec<String>,
    pub decoded_reference_outputs_required: bool,
    pub differential_oracles_required: bool,
    pub property_fuzzing_required: bool,
    pub benchmark_claim_gate_required: bool,
    pub reference_roles_test_only: bool,
    pub baselines_fallback_free: bool,
    pub production_claim_allowed: bool,
    pub benchmark_claims_blocked_by_correctness: bool,
    pub query_execution: bool,
    pub decoded_reference_execution_performed: bool,
    pub external_engine_execution: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl CorrectnessDifferentialHarnessReport {
    pub fn surface_order() -> Vec<&'static str> {
        vec![
            "fixture_manifest",
            "golden_fixtures",
            "decoded_reference_outputs",
            "differential_oracles",
            "semantic_edge_cases",
            "unsupported_diagnostics",
            "property_fuzzing",
            "benchmark_claim_gate",
        ]
    }
    pub fn required_validation_mode_order() -> Vec<&'static str> {
        vec![
            CorrectnessValidationMode::ExpectedOutput.as_str(),
            CorrectnessValidationMode::DecodedReference.as_str(),
            CorrectnessValidationMode::DifferentialComparison.as_str(),
            CorrectnessValidationMode::PropertyBased.as_str(),
            CorrectnessValidationMode::Fuzz.as_str(),
            CorrectnessValidationMode::GoldenDiagnostic.as_str(),
            CorrectnessValidationMode::UnsupportedDiagnosticOnly.as_str(),
        ]
    }
    pub fn missing_validation_mode_order(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if self.decoded_reference_output_count == 0 {
            missing.push(CorrectnessValidationMode::DecodedReference.as_str());
        }
        if self.generated_property_fixture_count == 0 {
            missing.push(CorrectnessValidationMode::PropertyBased.as_str());
        }
        if self.fuzz_seed_count == 0 {
            missing.push(CorrectnessValidationMode::Fuzz.as_str());
        }
        if self.baseline_count == 0 {
            missing.push(CorrectnessValidationMode::DifferentialComparison.as_str());
        }
        missing
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub const fn side_effect_free(&self) -> bool {
        !self.query_execution
            && !self.decoded_reference_execution_performed
            && !self.external_engine_execution
            && !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "correctness_differential_harness(status={}, planned_surfaces={}, blocked_surfaces={}, fixtures={}, golden_fixtures={}, decoded_reference_outputs={}, external_oracles={}, production_claim_allowed={}, fallback_execution=disabled)",
            self.status.as_str(),
            self.planned_surface_count,
            self.blocked_surface_count,
            self.fixture_count,
            self.golden_fixture_count,
            self.decoded_reference_output_count,
            self.baseline_count,
            self.production_claim_allowed
        )
    }
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn plan_correctness_differential_harness(
    plan: CorrectnessValidationPlan,
) -> CorrectnessDifferentialHarnessReport {
    let reference_roles_test_only = plan.reference_roles_are_test_only();
    let baselines_fallback_free = plan.baselines_are_fallback_free();
    let fixture_count = plan.fixture_count();
    let fixtures_with_source_ref_count = plan.fixtures_with_source_ref_count();
    let golden_fixture_count = plan.golden_fixture_count();
    let reference_artifact_count = plan.reference_artifact_count();
    let decoded_reference_output_count = plan.decoded_reference_output_count();
    let decoded_reference_artifact_id_order = plan
        .decoded_reference_artifact_id_order()
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let decoded_reference_output_coverage_complete =
        plan.decoded_reference_output_coverage_complete();
    let executable_expected_output_count = plan.executable_expected_output_count();
    let not_yet_defined_fixture_count = plan.not_yet_defined_fixture_count();
    let unsupported_diagnostic_fixture_count = plan.unsupported_diagnostic_fixture_count();
    let required_edge_case_count =
        CorrectnessValidationPlan::required_foundation_edge_cases().len();
    let covered_required_edge_case_count = plan.covered_required_foundation_edge_case_count();
    let missing_required_edge_cases = plan
        .missing_required_foundation_edge_cases()
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let baseline_count = plan.baseline_count();
    let baseline_engine_order = plan
        .baseline_engine_order()
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let reference_role_order = plan
        .reference_role_order()
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let generated_property_fixture_count = plan.generated_property_fixture_count();
    let fuzz_seed_count = plan.fuzz_seeds.len();

    let blocked_surface_order = correctness_harness_blocked_surfaces(
        fixture_count,
        golden_fixture_count,
        decoded_reference_output_coverage_complete,
        unsupported_diagnostic_fixture_count,
        covered_required_edge_case_count,
        required_edge_case_count,
        baseline_count,
        generated_property_fixture_count,
        fuzz_seed_count,
        not_yet_defined_fixture_count,
        reference_roles_test_only,
        baselines_fallback_free,
    );
    let blocked_surface_count = blocked_surface_order.len();
    let planned_surface_count =
        CorrectnessDifferentialHarnessReport::surface_order().len() - blocked_surface_count;
    let production_claim_allowed = blocked_surface_count == 0
        && not_yet_defined_fixture_count == 0
        && reference_roles_test_only
        && baselines_fallback_free;
    let status = if !reference_roles_test_only || !baselines_fallback_free {
        CorrectnessDifferentialHarnessStatus::UnsafeFallbackPolicy
    } else if production_claim_allowed {
        CorrectnessDifferentialHarnessStatus::EvidenceComplete
    } else {
        CorrectnessDifferentialHarnessStatus::NeedsEvidence
    };
    let diagnostics = correctness_harness_diagnostics(
        &blocked_surface_order,
        not_yet_defined_fixture_count,
        status,
    );

    CorrectnessDifferentialHarnessReport {
        schema_version: "shardloom.correctness_differential_harness.v1",
        report_id: "cg5.correctness_differential_harness.aggregate",
        plan_name: plan.name,
        plan_mode: plan.mode,
        status,
        fixture_count,
        fixtures_with_source_ref_count,
        golden_fixture_count,
        reference_artifact_count,
        decoded_reference_output_count,
        decoded_reference_artifact_id_order,
        decoded_reference_output_coverage_complete,
        executable_expected_output_count,
        not_yet_defined_fixture_count,
        unsupported_diagnostic_fixture_count,
        required_edge_case_count,
        covered_required_edge_case_count,
        missing_required_edge_cases,
        baseline_count,
        baseline_engine_order,
        reference_role_order,
        generated_property_fixture_count,
        fuzz_seed_count,
        planned_surface_count,
        blocked_surface_count,
        blocked_surface_order,
        decoded_reference_outputs_required: true,
        differential_oracles_required: true,
        property_fuzzing_required: true,
        benchmark_claim_gate_required: true,
        reference_roles_test_only,
        baselines_fallback_free,
        production_claim_allowed,
        benchmark_claims_blocked_by_correctness: !production_claim_allowed,
        query_execution: false,
        decoded_reference_execution_performed: false,
        external_engine_execution: false,
        data_read: false,
        object_store_io: false,
        write_io: false,
        fallback_execution_allowed: false,
        fallback_attempted: false,
        diagnostics,
    }
}

#[allow(clippy::too_many_arguments)]
fn correctness_harness_blocked_surfaces(
    fixture_count: usize,
    golden_fixture_count: usize,
    decoded_reference_output_coverage_complete: bool,
    unsupported_diagnostic_fixture_count: usize,
    covered_required_edge_case_count: usize,
    required_edge_case_count: usize,
    baseline_count: usize,
    generated_property_fixture_count: usize,
    fuzz_seed_count: usize,
    not_yet_defined_fixture_count: usize,
    reference_roles_test_only: bool,
    baselines_fallback_free: bool,
) -> Vec<String> {
    let mut blocked = Vec::new();
    if fixture_count == 0 {
        blocked.push("fixture_manifest".to_string());
    }
    if golden_fixture_count == 0 {
        blocked.push("golden_fixtures".to_string());
    }
    if !decoded_reference_output_coverage_complete {
        blocked.push("decoded_reference_outputs".to_string());
    }
    if baseline_count == 0 || !baselines_fallback_free {
        blocked.push("differential_oracles".to_string());
    }
    if covered_required_edge_case_count < required_edge_case_count {
        blocked.push("semantic_edge_cases".to_string());
    }
    if unsupported_diagnostic_fixture_count == 0 {
        blocked.push("unsupported_diagnostics".to_string());
    }
    if generated_property_fixture_count == 0 || fuzz_seed_count == 0 {
        blocked.push("property_fuzzing".to_string());
    }
    if not_yet_defined_fixture_count > 0
        || !decoded_reference_output_coverage_complete
        || generated_property_fixture_count == 0
        || fuzz_seed_count == 0
        || !reference_roles_test_only
        || !baselines_fallback_free
    {
        blocked.push("benchmark_claim_gate".to_string());
    }
    blocked
}

fn correctness_harness_diagnostics(
    blocked_surfaces: &[String],
    not_yet_defined_fixture_count: usize,
    status: CorrectnessDifferentialHarnessStatus,
) -> Vec<Diagnostic> {
    if status == CorrectnessDifferentialHarnessStatus::UnsafeFallbackPolicy {
        return vec![Diagnostic::new(
            DiagnosticCode::NoFallbackExecution,
            DiagnosticSeverity::Error,
            DiagnosticCategory::NoFallbackPolicy,
            "correctness harness contains a fallback-capable reference path",
            Some("correctness_differential_harness".to_string()),
            Some("Correctness references and external engines may be test oracles only.".to_string()),
            Some("Remove fallback-capable references before enabling any correctness or benchmark claim.".to_string()),
            FallbackStatus::disabled_by_policy(),
        )];
    }
    if blocked_surfaces.is_empty() {
        return Vec::new();
    }

    vec![Diagnostic::new(
        DiagnosticCode::NotImplemented,
        DiagnosticSeverity::Warning,
        DiagnosticCategory::Planning,
        "correctness harness evidence is incomplete",
        Some("correctness_differential_harness".to_string()),
        Some(format!(
            "blocked_surfaces={}; not_yet_defined_fixtures={}",
            blocked_surfaces.join(","),
            not_yet_defined_fixture_count
        )),
        Some(
            "Add decoded reference outputs, property/fuzz evidence, and resolved fixture expectations before opening production or competitive benchmark claims.".to_string(),
        ),
        FallbackStatus::disabled_by_policy(),
    )]
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

        assert_eq!(plan.fixture_count(), 34);
        assert_eq!(plan.fixtures_with_source_ref_count(), 7);
        assert_eq!(plan.golden_fixture_count(), 19);
        assert_eq!(plan.reference_artifact_count(), 18);
        assert_eq!(plan.decoded_reference_output_count(), 18);
        assert!(plan.decoded_reference_output_coverage_complete());
        assert_eq!(plan.executable_expected_output_count(), 18);
        assert_eq!(plan.not_yet_defined_fixture_count(), 8);
        assert_eq!(plan.diagnostic_expected_output_count(), 1);
        assert_eq!(plan.unsupported_expected_output_count(), 1);
        assert_eq!(plan.baseline_count(), 7);
        assert!(plan.required_foundation_edge_cases_covered());
        assert_eq!(plan.covered_required_foundation_edge_case_count(), 7);
        assert!(plan.missing_required_foundation_edge_cases().is_empty());
        assert!(plan.reference_roles_are_test_only());
        assert!(plan.baselines_are_fallback_free());
        assert_eq!(
            plan.reference_role_order(),
            vec![
                "golden_fixture",
                "decoded_reference",
                "generated_property",
                "external_oracle"
            ]
        );
    }
    #[test]
    fn correctness_harness_surfaces_evidence_gaps_without_execution() {
        let report = plan_correctness_differential_harness(
            CorrectnessValidationPlan::default_foundation_plan(),
        );

        assert_eq!(
            report.status,
            CorrectnessDifferentialHarnessStatus::NeedsEvidence
        );
        assert_eq!(
            report.schema_version,
            "shardloom.correctness_differential_harness.v1"
        );
        assert_eq!(
            report.report_id,
            "cg5.correctness_differential_harness.aggregate"
        );
        assert_eq!(report.fixture_count, 34);
        assert_eq!(report.golden_fixture_count, 19);
        assert_eq!(report.executable_expected_output_count, 18);
        assert_eq!(report.reference_artifact_count, 18);
        assert_eq!(report.decoded_reference_output_count, 18);
        assert!(report.decoded_reference_output_coverage_complete);
        assert_eq!(report.generated_property_fixture_count, 3);
        assert_eq!(report.fuzz_seed_count, 3);
        assert_eq!(report.baseline_count, 7);
        assert_eq!(report.planned_surface_count, 7);
        assert_eq!(report.blocked_surface_count, 1);
        assert_eq!(
            report.blocked_surface_order,
            vec!["benchmark_claim_gate".to_string()]
        );
        assert!(report.benchmark_claims_blocked_by_correctness);
        assert!(!report.production_claim_allowed);
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_execution);
    }
    #[test]
    fn correctness_harness_records_required_validation_modes() {
        let report = plan_correctness_differential_harness(
            CorrectnessValidationPlan::default_foundation_plan(),
        );

        assert_eq!(
            CorrectnessDifferentialHarnessReport::required_validation_mode_order(),
            vec![
                "expected_output",
                "decoded_reference",
                "differential_comparison",
                "property_based",
                "fuzz",
                "golden_diagnostic",
                "unsupported_diagnostic_only"
            ]
        );
        assert!(report.missing_validation_mode_order().is_empty());
        assert_eq!(
            report.baseline_engine_order,
            vec![
                "spark".to_string(),
                "datafusion".to_string(),
                "duckdb".to_string(),
                "polars".to_string(),
                "pandas".to_string(),
                "dask".to_string(),
                "velox".to_string()
            ]
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
