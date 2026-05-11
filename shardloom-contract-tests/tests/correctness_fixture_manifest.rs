use std::path::Path;

use shardloom_core::{
    CorrectnessFixture, CorrectnessValidationPlan, EdgeCase, ExpectedOutcome, FixtureFormat,
    ReferenceRole, SemanticArea,
};

fn fixture<'a>(plan: &'a CorrectnessValidationPlan, id: &str) -> &'a CorrectnessFixture {
    plan.fixtures
        .iter()
        .find(|fixture| fixture.id.as_str() == id)
        .expect("fixture present")
}

#[test]
fn foundation_plan_declares_checked_in_vortex_golden_fixture() {
    let plan = CorrectnessValidationPlan::default_foundation_plan();
    let fixture = fixture(&plan, "vortex-metadata-footer-u64-20000");

    assert_eq!(fixture.format, FixtureFormat::ShardLoomNative);
    assert_eq!(
        fixture.source_ref.as_deref(),
        Some("shardloom-vortex/tests/fixtures/metadata_footer_u64_20000.vortex")
    );
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root");
    let fixture_path = workspace_root.join(fixture.source_ref.as_ref().expect("source ref"));
    assert!(fixture_path.is_file(), "{fixture_path:?}");
    assert_eq!(
        fixture.expected,
        ExpectedOutcome::MetadataRowCount { row_count: 20000 }
    );
    assert!(!fixture.expected.requires_execution());
    assert!(fixture.covers_area(SemanticArea::MetadataOnly));
    assert!(fixture.covers_edge_case(EdgeCase::NoNulls));
    assert!(fixture.has_reference_role(ReferenceRole::GoldenFixture));
    assert!(fixture.reference_roles_are_test_only());
}

#[test]
fn foundation_plan_declares_local_encoded_count_reference_output() {
    let plan = CorrectnessValidationPlan::default_foundation_plan();
    let fixture = fixture(&plan, "vortex-local-encoded-count-u64-20000");

    assert_eq!(fixture.format, FixtureFormat::ShardLoomNative);
    assert_eq!(
        fixture.source_ref.as_deref(),
        Some("shardloom-vortex/tests/fixtures/metadata_footer_u64_20000.vortex")
    );
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root");
    let fixture_path = workspace_root.join(fixture.source_ref.as_ref().expect("source ref"));
    assert!(fixture_path.is_file(), "{fixture_path:?}");
    assert_eq!(
        fixture.expected,
        ExpectedOutcome::EncodedCount { count: 20000 }
    );
    assert!(fixture.expected.requires_execution());
    assert!(fixture.covers_area(SemanticArea::EncodedExecution));
    assert!(fixture.covers_edge_case(EdgeCase::NoNulls));
    assert!(fixture.has_reference_role(ReferenceRole::GoldenFixture));
    assert!(fixture.reference_roles_are_test_only());
}

#[test]
fn foundation_plan_declares_broader_local_primitive_reference_outputs() {
    let plan = CorrectnessValidationPlan::default_foundation_plan();
    let cases = [
        (
            "vortex-local-count-all-struct-five",
            ExpectedOutcome::EncodedCount { count: 5 },
        ),
        (
            "vortex-local-count-where-struct-five",
            ExpectedOutcome::Rows { row_count: Some(3) },
        ),
        (
            "vortex-local-project-struct-five",
            ExpectedOutcome::Rows { row_count: Some(5) },
        ),
        (
            "vortex-local-filter-struct-five",
            ExpectedOutcome::Rows { row_count: Some(3) },
        ),
        (
            "vortex-local-filter-project-struct-five",
            ExpectedOutcome::Rows { row_count: Some(3) },
        ),
    ];

    for (id, expected) in cases {
        let fixture = fixture(&plan, id);
        assert_eq!(fixture.format, FixtureFormat::ShardLoomNative);
        assert_eq!(
            fixture.source_ref.as_deref(),
            Some("shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
        );
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root");
        let fixture_path = workspace_root.join(fixture.source_ref.as_ref().expect("source ref"));
        assert!(fixture_path.is_file(), "{fixture_path:?}");
        assert_eq!(fixture.expected, expected);
        assert!(fixture.expected.requires_execution());
        assert!(fixture.covers_area(SemanticArea::EncodedExecution));
        assert!(fixture.covers_edge_case(EdgeCase::NoNulls));
        assert!(fixture.has_reference_role(ReferenceRole::GoldenFixture));
        assert!(fixture.reference_roles_are_test_only());
    }
}

#[test]
fn foundation_plan_declares_prepared_encoded_reference_outputs() {
    let plan = CorrectnessValidationPlan::default_foundation_plan();
    let cases = [
        (
            "vortex-prepared-encoded-filter-dictionary-run",
            ExpectedOutcome::Rows { row_count: Some(5) },
            SemanticArea::SelectionVectors,
            EdgeCase::DictionaryEncoded,
        ),
        (
            "vortex-prepared-encoded-projection-dictionary",
            ExpectedOutcome::Rows { row_count: Some(3) },
            SemanticArea::EncodedExecution,
            EdgeCase::DictionaryEncoded,
        ),
        (
            "vortex-prepared-encoded-filter-project-selection-vector",
            ExpectedOutcome::Rows { row_count: Some(5) },
            SemanticArea::SelectionVectors,
            EdgeCase::SparseValidity,
        ),
    ];

    for (id, expected, area, edge_case) in cases {
        let fixture = fixture(&plan, id);
        assert_eq!(fixture.format, FixtureFormat::Generated);
        assert_eq!(fixture.source_ref, None);
        assert_eq!(fixture.expected, expected);
        assert!(fixture.expected.requires_execution());
        assert!(fixture.covers_area(area));
        assert!(fixture.covers_edge_case(edge_case));
        assert!(fixture.has_reference_role(ReferenceRole::GoldenFixture));
        assert!(fixture.has_reference_role(ReferenceRole::DecodedReference));
        assert_eq!(fixture.decoded_reference_artifact_count(), 1);
        let artifact = &fixture.reference_artifacts[0];
        assert_eq!(artifact.artifact_id, format!("{id}.decoded-reference.rows"));
        assert_eq!(artifact.role, ReferenceRole::DecodedReference);
        assert_eq!(artifact.expected, expected);
        assert_eq!(artifact.semantic_profile, "shardloom_native_test_reference");
        assert_eq!(
            artifact.materialization_boundary,
            "test_only_logical_reference_output"
        );
        assert!(!artifact.execution_performed);
        assert!(!artifact.fallback_attempted);
        assert!(artifact.is_test_only());
        assert!(fixture.reference_roles_are_test_only());
    }
}

#[test]
fn foundation_plan_tracks_required_edge_case_fixture_families() {
    let plan = CorrectnessValidationPlan::default_foundation_plan();
    let required = [
        (SemanticArea::Nulls, EdgeCase::AllNull),
        (SemanticArea::NestedData, EdgeCase::NestedStructList),
        (SemanticArea::EncodedExecution, EdgeCase::DictionaryEncoded),
        (SemanticArea::SelectionVectors, EdgeCase::SparseValidity),
        (SemanticArea::EncodedExecution, EdgeCase::RunLengthEncoded),
        (SemanticArea::Temporal, EdgeCase::TemporalValues),
        (
            SemanticArea::UnsupportedDiagnostics,
            EdgeCase::UnsupportedPlanShape,
        ),
    ];

    for (area, edge) in required {
        assert!(
            plan.fixtures
                .iter()
                .any(|fixture| fixture.covers_area(area) && fixture.covers_edge_case(edge)),
            "missing fixture family for {} / {}",
            area.as_str(),
            edge.as_str()
        );
    }
    assert!(plan.required_foundation_edge_cases_covered());
    assert_eq!(plan.covered_required_foundation_edge_case_count(), 7);
    assert!(plan.missing_required_foundation_edge_cases().is_empty());
}

#[test]
fn reference_roles_remain_test_only_not_production_fallback() {
    let plan = CorrectnessValidationPlan::default_foundation_plan();
    let roles = [
        ReferenceRole::DecodedReference,
        ReferenceRole::ExternalOracle,
        ReferenceRole::GoldenFixture,
        ReferenceRole::GeneratedProperty,
        ReferenceRole::FuzzSeed,
    ];

    for role in roles {
        assert!(!role.is_production_execution(), "{}", role.as_str());
    }
    assert!(
        plan.fixtures
            .iter()
            .all(CorrectnessFixture::reference_roles_are_test_only)
    );
    assert!(plan.reference_roles_are_test_only());
    assert_eq!(
        plan.reference_role_order(),
        vec!["golden_fixture", "decoded_reference", "external_oracle"]
    );
    assert!(!plan.fallback_execution_allowed());
    assert!(
        plan.to_human_text()
            .contains("external baselines: test/comparison only")
    );
}

#[test]
fn foundation_plan_reports_reference_and_gap_counts() {
    let plan = CorrectnessValidationPlan::default_foundation_plan();

    assert_eq!(plan.fixture_count(), 22);
    assert_eq!(plan.fixtures_with_source_ref_count(), 7);
    assert_eq!(plan.golden_fixture_count(), 10);
    assert_eq!(plan.reference_artifact_count(), 3);
    assert_eq!(plan.decoded_reference_output_count(), 3);
    assert_eq!(
        plan.decoded_reference_artifact_id_order(),
        vec![
            "vortex-prepared-encoded-filter-dictionary-run.decoded-reference.rows",
            "vortex-prepared-encoded-projection-dictionary.decoded-reference.rows",
            "vortex-prepared-encoded-filter-project-selection-vector.decoded-reference.rows",
        ]
    );
    assert!(!plan.decoded_reference_output_coverage_complete());
    assert_eq!(plan.executable_expected_output_count(), 9);
    assert_eq!(plan.not_yet_defined_fixture_count(), 8);
    assert_eq!(plan.diagnostic_expected_output_count(), 1);
    assert_eq!(plan.unsupported_expected_output_count(), 1);
    assert_eq!(plan.baseline_count(), 7);
    assert!(plan.baselines_are_fallback_free());
}
