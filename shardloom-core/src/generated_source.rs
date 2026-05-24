//! Source-free generated-output evidence contracts.
//!
//! These contracts distinguish no-dataset smoke from generated-output execution. They define the
//! fields runtime slices must emit when `ShardLoom` creates output without reading an input
//! dataset. Current support is limited to scoped local JSONL/CSV fixture smokes; broader SQL,
//! `DataFrame`, object-store, Foundry, production, and performance claims remain blocked.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedSourceCaseKind {
    NoDatasetSmoke,
    UserGeneratedSource,
    EngineNativeGeneratedSource,
}

impl GeneratedSourceCaseKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoDatasetSmoke => "no_dataset_smoke",
            Self::UserGeneratedSource => "user_generated_source",
            Self::EngineNativeGeneratedSource => "engine_native_generated_source",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedSourceSupportStatus {
    SmokeOnly,
    FixtureSmokeSupported,
    ReportOnly,
    PlannedRuntime,
}

impl GeneratedSourceSupportStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SmokeOnly => "smoke_only",
            Self::FixtureSmokeSupported => "fixture_smoke_supported",
            Self::ReportOnly => "report_only",
            Self::PlannedRuntime => "planned_runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedSourceCertificateStatus {
    NotApplicableNoGeneratedRows,
    NotEmittedReportOnly,
    RequiredForRuntime,
}

impl GeneratedSourceCertificateStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicableNoGeneratedRows => "not_applicable_no_generated_rows",
            Self::NotEmittedReportOnly => "not_emitted_report_only",
            Self::RequiredForRuntime => "required_for_runtime",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct GeneratedSourceCertificateContractRow {
    pub case_kind: GeneratedSourceCaseKind,
    pub support_status: GeneratedSourceSupportStatus,
    pub generated_source_certificate_status: GeneratedSourceCertificateStatus,
    pub input_dataset_count: u64,
    pub source_io_performed: bool,
    pub generated_source_created: bool,
    pub output_io_performed: bool,
    pub source_native_io_certificate_status: &'static str,
    pub output_native_io_certificate_status: &'static str,
    pub required_generator_kinds: &'static str,
    pub required_evidence_fields: &'static str,
    pub blocker_id: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl GeneratedSourceCertificateContractRow {
    #[must_use]
    pub const fn no_dataset_smoke() -> Self {
        Self {
            case_kind: GeneratedSourceCaseKind::NoDatasetSmoke,
            support_status: GeneratedSourceSupportStatus::SmokeOnly,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::NotApplicableNoGeneratedRows,
            input_dataset_count: 0,
            source_io_performed: false,
            generated_source_created: false,
            output_io_performed: false,
            source_native_io_certificate_status: "not_applicable_no_source_dataset",
            output_native_io_certificate_status: "not_emitted_no_output_data",
            required_generator_kinds: "none",
            required_evidence_fields: "input_dataset_count,source_io_performed,generated_source_created,output_io_performed,generated_source_certificate_status,claim_gate_status",
            blocker_id: "gar-gen-1.no_dataset_smoke_not_generated_output",
            claim_gate_status: "smoke_only",
            claim_boundary: "No-dataset smoke is a status/capability proof only; it creates no generated rows, no source Native I/O certificate, and no output data claim.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn user_generated_source() -> Self {
        Self {
            case_kind: GeneratedSourceCaseKind::UserGeneratedSource,
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::RequiredForRuntime,
            input_dataset_count: 0,
            source_io_performed: false,
            generated_source_created: true,
            output_io_performed: true,
            source_native_io_certificate_status: "not_applicable_no_source_dataset",
            output_native_io_certificate_status: "required_for_runtime_output",
            required_generator_kinds: "python_rows(runtime_local_jsonl_csv_smoke),literal_table(runtime_local_jsonl_csv_smoke),calendar(runtime_local_jsonl_csv_smoke)",
            required_evidence_fields: "generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,generation_deterministic,output_io_performed,output_native_io_certificate_status",
            blocker_id: "none_scoped_local_jsonl_csv_smoke_only",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "User-generated source support is limited to scoped local user_rows, literal_table, and calendar JSONL/CSV fixture smokes until broader runtime and sink evidence exists.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn engine_native_generated_source() -> Self {
        Self {
            case_kind: GeneratedSourceCaseKind::EngineNativeGeneratedSource,
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::RequiredForRuntime,
            input_dataset_count: 0,
            source_io_performed: false,
            generated_source_created: true,
            output_io_performed: true,
            source_native_io_certificate_status: "not_applicable_no_source_dataset",
            output_native_io_certificate_status: "required_for_runtime_output",
            required_generator_kinds: "range(runtime_local_jsonl_csv_smoke),sequence(runtime_local_jsonl_csv_smoke),values(report_only),literal_table(report_only),calendar(report_only),synthetic(report_only)",
            required_evidence_fields: "generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,generated_source_seed,generation_deterministic,output_io_performed,output_native_io_certificate_status",
            blocker_id: "none_scoped_local_range_sequence_jsonl_csv_smoke_only",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Engine-native generated source support is limited to scoped local range and sequence JSONL/CSV fixture smokes; values, synthetic, broader SQL/DataFrame, object-store, and Foundry runtime remain blocked/report-only.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.fallback_attempted && !self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct GeneratedSourceCertificateContractReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub generated_source_certificate_schema_version: &'static str,
    pub support_status_vocabulary: &'static str,
    pub required_field_order: Vec<&'static str>,
    pub rows: Vec<GeneratedSourceCertificateContractRow>,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub object_store_io_performed: bool,
    pub foundry_runtime_invoked: bool,
    pub broad_sql_dataframe_claim_allowed: bool,
    pub claim_gate_status: &'static str,
}

impl GeneratedSourceCertificateContractReport {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.generated_source_certificate_contract.v1",
            report_id: "gar-gen-1.generated_source_certificate_contract",
            generated_source_certificate_schema_version: "shardloom.generated_source_certificate.v1",
            support_status_vocabulary: "smoke_only,fixture_smoke_supported,report_only,planned_runtime",
            required_field_order: vec![
                "input_dataset_count",
                "source_io_performed",
                "generated_source_created",
                "generated_source_kind",
                "generated_source_schema_digest",
                "generated_source_row_count",
                "generated_source_plan_digest",
                "generated_source_seed",
                "generation_deterministic",
                "output_io_performed",
                "output_native_io_certificate_status",
                "generated_source_certificate_status",
                "fallback_attempted",
                "external_engine_invoked",
                "claim_gate_status",
            ],
            rows: vec![
                GeneratedSourceCertificateContractRow::no_dataset_smoke(),
                GeneratedSourceCertificateContractRow::user_generated_source(),
                GeneratedSourceCertificateContractRow::engine_native_generated_source(),
            ],
            fallback_attempted: false,
            external_engine_invoked: false,
            object_store_io_performed: false,
            foundry_runtime_invoked: false,
            broad_sql_dataframe_claim_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub fn case_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.case_kind.as_str()).collect()
    }

    #[must_use]
    pub fn row_for(
        &self,
        case_kind: GeneratedSourceCaseKind,
    ) -> Option<&GeneratedSourceCertificateContractRow> {
        self.rows.iter().find(|row| row.case_kind == case_kind)
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self
                .rows
                .iter()
                .all(GeneratedSourceCertificateContractRow::fallback_free)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct GeneratedSourceApiAdmissionRow {
    pub row_id: &'static str,
    pub user_visible_surface: &'static str,
    pub support_status: GeneratedSourceSupportStatus,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub write_io: bool,
    pub source_io_performed: bool,
    pub generated_source_created: bool,
    pub blocker_id: &'static str,
    pub required_evidence: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
}

impl GeneratedSourceApiAdmissionRow {
    #[must_use]
    pub const fn python_ctx_from_rows() -> Self {
        Self {
            row_id: "python_ctx_from_rows",
            user_visible_surface: "Python ctx.from_rows([...]).write(local_jsonl_or_csv)",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_jsonl_csv_smoke_only",
            required_evidence: "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Only scoped local user-row JSONL/CSV generated-output fixture smoke is admitted; no broad DataFrame, SQL, object-store, Foundry, production, or performance claim.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn python_ctx_range() -> Self {
        Self {
            row_id: "python_ctx_range",
            user_visible_surface: "Python ctx.range(...).write(local_jsonl_or_csv)",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_range_jsonl_csv_smoke_only",
            required_evidence: "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Only scoped local range JSONL/CSV generated-output fixture smoke is admitted; broad DataFrame, object-store, Foundry, production, and performance claims remain blocked.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn python_ctx_sequence() -> Self {
        Self {
            row_id: "python_ctx_sequence",
            user_visible_surface: "Python ctx.sequence(...).write(local_jsonl_or_csv)",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_sequence_jsonl_csv_smoke_only",
            required_evidence: "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Only scoped local sequence JSONL/CSV generated-output fixture smoke is admitted; broad DataFrame, object-store, Foundry, production, and performance claims remain blocked.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn python_ctx_literal_table() -> Self {
        Self {
            row_id: "python_ctx_literal_table",
            user_visible_surface: "Python ctx.literal_table([...]).write(local_jsonl_or_csv)",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_literal_table_jsonl_csv_smoke_only",
            required_evidence: "literal_table_generator_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Only scoped local literal-table JSONL/CSV generated-output fixture smoke is admitted; broad DataFrame, object-store, Foundry, production, and performance claims remain blocked.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn python_ctx_calendar() -> Self {
        Self {
            row_id: "python_ctx_calendar",
            user_visible_surface: "Python ctx.calendar(start,end).write(local_jsonl_or_csv)",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_calendar_jsonl_csv_smoke_only",
            required_evidence: "calendar_generator_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Only scoped local calendar/date-dimension JSONL/CSV generated-output fixture smoke is admitted; SQL/DataFrame generated-source runtime, object-store, Foundry, production, and performance claims remain blocked.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn python_generated_source_write() -> Self {
        Self {
            row_id: "python_generated_source_write",
            user_visible_surface: "Python GeneratedRowsSource/GeneratedRangeSource.write(local_jsonl_or_csv)",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_supported_generated_source_write_smokes_only",
            required_evidence: "generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Generated-source write is admitted only for supported local user_rows, literal_table, calendar, range, sequence, SQL VALUES, SQL literal SELECT, and SQL generate_series/range JSONL/CSV smokes; unsupported generator kinds remain blocked/report-only.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn sql_literal_select() -> Self {
        Self {
            row_id: "sql_literal_select",
            user_visible_surface: "SQL SELECT literal expressions",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_sql_literal_select_jsonl_csv_smoke_only",
            required_evidence: "sql_parser,sql_binder,sql_planner,literal_projection_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "SQL literal SELECT is admitted only for scoped source-free local JSONL/CSV generated-output fixture smokes; no broad SQL runtime claim.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn sql_values() -> Self {
        Self {
            row_id: "sql_values",
            user_visible_surface: "SQL VALUES (...)",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_sql_values_jsonl_csv_smoke_only",
            required_evidence: "sql_parser,sql_binder,values_table_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "SQL VALUES is admitted only for scoped source-free local JSONL/CSV generated-output fixture smokes; no broad SQL runtime claim.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn sql_source_free_projection() -> Self {
        Self {
            row_id: "sql_source_free_projection",
            user_visible_surface: "SQL source-free range projection",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_sql_range_projection_jsonl_csv_smoke_only",
            required_evidence: "sql_parser,sql_binder,sql_planner,range_projection_expression_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Only scoped source-free range-generator projections over the generated value column with admitted int64 expressions are supported; arbitrary SQL source-free projection remains blocked.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn sql_generate_series_range() -> Self {
        Self {
            row_id: "sql_generate_series_range",
            user_visible_surface: "SQL generate_series/range",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_sql_generate_series_range_jsonl_csv_smoke_only",
            required_evidence: "sql_parser,sql_binder,sql_table_function_contract,range_generator_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "SQL generate_series/range is admitted only for SELECT * plus scoped value-column/int64 projections from generate_series/range(...) local JSONL/CSV generated-output fixture smokes; no broad SQL runtime claim.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn dataframe_source_free_projection() -> Self {
        Self {
            row_id: "dataframe_source_free_projection",
            user_visible_surface: "DataFrame source-free projection",
            support_status: GeneratedSourceSupportStatus::ReportOnly,
            runtime_execution: false,
            data_read: false,
            write_io: false,
            source_io_performed: false,
            generated_source_created: false,
            blocker_id: "gar-gen-1.dataframe_source_free_projection_runtime_not_implemented",
            required_evidence: "typed_expression_contract,projection_plan_digest,generated_source_certificate,execution_certificate",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "DataFrame source-free projection is report-only outside the scoped local user_rows and range write smokes.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn dataframe_generated_with_column() -> Self {
        Self {
            row_id: "dataframe_generated_with_column",
            user_visible_surface: "Scoped generated DataFrame with_column",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            blocker_id: "none_scoped_local_generated_with_column_jsonl_csv_smoke_only",
            required_evidence: "generated_row_literal_projection,range_projection_expression_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Only scoped generated-row literal with_column and generated range int64 with_column workflows before local output are admitted; broad expression-backed DataFrame generation remains blocked.",
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn is_python(&self) -> bool {
        self.row_id.starts_with("python_")
    }

    #[must_use]
    pub fn is_sql(&self) -> bool {
        self.row_id.starts_with("sql_")
    }

    #[must_use]
    pub fn is_dataframe(&self) -> bool {
        self.row_id.starts_with("dataframe_")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct GeneratedSourceApiAdmissionMatrix {
    pub schema_version: &'static str,
    pub matrix_id: &'static str,
    pub rows: Vec<GeneratedSourceApiAdmissionRow>,
    pub support_status_vocabulary: &'static str,
    pub claim_gate_status: &'static str,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub write_io: bool,
    pub source_io_performed: bool,
    pub generated_source_created: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub broad_sql_dataframe_claim_allowed: bool,
}

impl GeneratedSourceApiAdmissionMatrix {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.generated_source_api_admission.v1",
            matrix_id: "gar-gen-1e.source_free_api_admission",
            rows: vec![
                GeneratedSourceApiAdmissionRow::python_ctx_from_rows(),
                GeneratedSourceApiAdmissionRow::python_ctx_range(),
                GeneratedSourceApiAdmissionRow::python_ctx_sequence(),
                GeneratedSourceApiAdmissionRow::python_ctx_literal_table(),
                GeneratedSourceApiAdmissionRow::python_ctx_calendar(),
                GeneratedSourceApiAdmissionRow::python_generated_source_write(),
                GeneratedSourceApiAdmissionRow::sql_literal_select(),
                GeneratedSourceApiAdmissionRow::sql_values(),
                GeneratedSourceApiAdmissionRow::sql_source_free_projection(),
                GeneratedSourceApiAdmissionRow::sql_generate_series_range(),
                GeneratedSourceApiAdmissionRow::dataframe_source_free_projection(),
                GeneratedSourceApiAdmissionRow::dataframe_generated_with_column(),
            ],
            support_status_vocabulary: "smoke_only,fixture_smoke_supported,report_only,planned_runtime",
            claim_gate_status: "not_claim_grade",
            runtime_execution: true,
            data_read: false,
            write_io: true,
            source_io_performed: false,
            generated_source_created: true,
            fallback_attempted: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
            broad_sql_dataframe_claim_allowed: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn python_row_order(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.is_python())
            .map(|row| row.row_id)
            .collect()
    }

    #[must_use]
    pub fn sql_row_order(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.is_sql())
            .map(|row| row.row_id)
            .collect()
    }

    #[must_use]
    pub fn dataframe_row_order(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.is_dataframe())
            .map(|row| row.row_id)
            .collect()
    }

    #[must_use]
    pub fn blocker_ids(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.blocker_id).collect()
    }

    #[must_use]
    pub fn required_evidence(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.required_evidence).collect()
    }

    #[must_use]
    pub fn row_for(&self, row_id: &str) -> Option<&GeneratedSourceApiAdmissionRow> {
        self.rows.iter().find(|row| row.row_id == row_id)
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && !self.fallback_execution_allowed
            && self
                .rows
                .iter()
                .all(GeneratedSourceApiAdmissionRow::fallback_free)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeneratedSourceEvidenceAlignmentRow {
    pub row_id: &'static str,
    pub user_visible_surface: &'static str,
    pub source_free_case: &'static str,
    pub support_status: GeneratedSourceSupportStatus,
    pub runtime_execution: bool,
    pub generated_source_certificate_status: GeneratedSourceCertificateStatus,
    pub output_native_io_certificate_status: &'static str,
    pub openlineage_facet_status: &'static str,
    pub opentelemetry_span_status: &'static str,
    pub bayesian_confidence_status: &'static str,
    pub foundry_boundary_ref: &'static str,
    pub blocker_id: &'static str,
    pub required_evidence: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl GeneratedSourceEvidenceAlignmentRow {
    #[must_use]
    pub const fn no_dataset_smoke() -> Self {
        Self {
            row_id: "no_dataset_smoke",
            user_visible_surface: "No-dataset smoke",
            source_free_case: "no_dataset_smoke",
            support_status: GeneratedSourceSupportStatus::SmokeOnly,
            runtime_execution: false,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::NotApplicableNoGeneratedRows,
            output_native_io_certificate_status: "not_emitted_no_output_data",
            openlineage_facet_status: "not_emitted_no_generated_rows",
            opentelemetry_span_status: "not_emitted_smoke_only",
            bayesian_confidence_status: "not_applicable_smoke_only",
            foundry_boundary_ref: "not_applicable",
            blocker_id: "gar-novel-1a.no_dataset_smoke_not_generated_output",
            required_evidence: "no_dataset_smoke_status,capability_envelope,no_fallback_evidence",
            claim_gate_status: "smoke_only",
            claim_boundary: "No-dataset smoke proves import/capability posture only; it creates no generated rows, no source certificate, and no output claim.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn python_generated_source() -> Self {
        Self {
            row_id: "python_generated_source_write",
            user_visible_surface: "Python ctx.from_rows/range local JSONL/CSV write",
            source_free_case: "user_generated_source_or_engine_native_generated_source",
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            runtime_execution: true,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::RequiredForRuntime,
            output_native_io_certificate_status: "required_for_runtime_output",
            openlineage_facet_status: "report_only_generated_source_facet_ref",
            opentelemetry_span_status: "report_only_result_sink_span_ref",
            bayesian_confidence_status: "advisory_ref_only",
            foundry_boundary_ref: "not_applicable_local_output",
            blocker_id: "none_scoped_local_jsonl_csv_smoke_only",
            required_evidence: "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Scoped local JSONL/CSV fixture smoke only; lineage, telemetry, and confidence refs are report-only and cannot upgrade the claim.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn sql_dataframe_source_free() -> Self {
        Self {
            row_id: "sql_dataframe_source_free",
            user_visible_surface: "SQL/DataFrame source-free rows",
            source_free_case: "sql_dataframe_report_only",
            support_status: GeneratedSourceSupportStatus::ReportOnly,
            runtime_execution: false,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::NotEmittedReportOnly,
            output_native_io_certificate_status: "not_emitted_report_only",
            openlineage_facet_status: "mapped_report_only_no_event",
            opentelemetry_span_status: "mapped_report_only_no_export",
            bayesian_confidence_status: "advisory_schema_only",
            foundry_boundary_ref: "not_applicable",
            blocker_id: "gar-novel-1a.sql_dataframe_runtime_not_implemented",
            required_evidence: "parser_binder_or_dataframe_plan,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "SQL/DataFrame generated-output support is report-only; no parser, planner, DataFrame runtime, row generation, or output write is executed.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn foundry_generated_output() -> Self {
        Self {
            row_id: "foundry_generated_output",
            user_visible_surface: "Foundry generated-output proof boundary",
            source_free_case: "foundry_report_only",
            support_status: GeneratedSourceSupportStatus::ReportOnly,
            runtime_execution: false,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::NotEmittedReportOnly,
            output_native_io_certificate_status: "not_emitted_report_only",
            openlineage_facet_status: "mapped_report_only_no_event",
            opentelemetry_span_status: "mapped_report_only_no_export",
            bayesian_confidence_status: "not_applicable_until_runtime_proof",
            foundry_boundary_ref: "shardloom.foundry_generated_output_boundary.v1",
            blocker_id: "gar-gen-1f.foundry_output_api_not_invoked",
            required_evidence: "foundry_output_api_evidence,result_dataset_written,evidence_dataset_written,generated_source_certificate,output_native_io_certificate,no_fallback_evidence",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "Foundry generated-output remains a future validation target; current proof invokes no Foundry runtime, Spark, output API, direct S3, or object-store write.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.fallback_attempted && !self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct GeneratedSourceEvidenceAlignmentReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub docs_ref: &'static str,
    pub generated_source_contract_ref: &'static str,
    pub generated_source_api_admission_ref: &'static str,
    pub openlineage_facets_ref: &'static str,
    pub opentelemetry_spans_ref: &'static str,
    pub bayesian_confidence_ref: &'static str,
    pub rows: Vec<GeneratedSourceEvidenceAlignmentRow>,
    pub openlineage_export_enabled: bool,
    pub opentelemetry_export_enabled: bool,
    pub opentelemetry_network_exporter_enabled: bool,
    pub bayesian_confidence_enabled: bool,
    pub foundry_runtime_invoked: bool,
    pub object_store_io_performed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
}

impl GeneratedSourceEvidenceAlignmentReport {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.generated_source_evidence_alignment.v1",
            report_id: "gar-novel-1a.generated_source_cross_surface_alignment",
            docs_ref: "docs/architecture/evidence-native-generated-execution-observability-confidence.md",
            generated_source_contract_ref: "shardloom.generated_source_certificate_contract.v1",
            generated_source_api_admission_ref: "shardloom.generated_source_api_admission.v1",
            openlineage_facets_ref: "GAR-NOVEL-1B.report_only_facets",
            opentelemetry_spans_ref: "GAR-NOVEL-1C.report_only_spans",
            bayesian_confidence_ref: "GAR-NOVEL-1D.report_only_confidence",
            rows: vec![
                GeneratedSourceEvidenceAlignmentRow::no_dataset_smoke(),
                GeneratedSourceEvidenceAlignmentRow::python_generated_source(),
                GeneratedSourceEvidenceAlignmentRow::sql_dataframe_source_free(),
                GeneratedSourceEvidenceAlignmentRow::foundry_generated_output(),
            ],
            openlineage_export_enabled: false,
            opentelemetry_export_enabled: false,
            opentelemetry_network_exporter_enabled: false,
            bayesian_confidence_enabled: false,
            foundry_runtime_invoked: false,
            object_store_io_performed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self
                .rows
                .iter()
                .all(GeneratedSourceEvidenceAlignmentRow::fallback_free)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GeneratedSourceApiAdmissionMatrix, GeneratedSourceCaseKind,
        GeneratedSourceCertificateContractReport, GeneratedSourceEvidenceAlignmentReport,
    };

    #[test]
    fn report_only_contract_separates_no_dataset_smoke_from_generated_output() {
        let report = GeneratedSourceCertificateContractReport::report_only();
        assert_eq!(
            report.case_order(),
            vec![
                "no_dataset_smoke",
                "user_generated_source",
                "engine_native_generated_source"
            ]
        );
        assert!(report.all_rows_fallback_free());
        assert!(!report.broad_sql_dataframe_claim_allowed);
        assert!(!report.object_store_io_performed);
        assert!(!report.foundry_runtime_invoked);

        let smoke = report
            .row_for(GeneratedSourceCaseKind::NoDatasetSmoke)
            .expect("no-dataset smoke row");
        assert!(!smoke.generated_source_created);
        assert!(!smoke.output_io_performed);
        assert_eq!(
            smoke.generated_source_certificate_status.as_str(),
            "not_applicable_no_generated_rows"
        );

        let user_rows = report
            .row_for(GeneratedSourceCaseKind::UserGeneratedSource)
            .expect("user generated source row");
        assert_eq!(
            user_rows.generated_source_certificate_status.as_str(),
            "required_for_runtime"
        );
        assert_eq!(user_rows.support_status.as_str(), "fixture_smoke_supported");
        assert!(user_rows.generated_source_created);
        assert!(user_rows.output_io_performed);
        assert!(
            user_rows
                .required_generator_kinds
                .contains("literal_table(runtime_local_jsonl_csv_smoke)")
        );
        assert!(
            user_rows
                .required_generator_kinds
                .contains("calendar(runtime_local_jsonl_csv_smoke)")
        );
        assert_eq!(user_rows.claim_gate_status, "fixture_smoke_only");

        let engine_range = report
            .row_for(GeneratedSourceCaseKind::EngineNativeGeneratedSource)
            .expect("engine-native generated source row");
        assert_eq!(
            engine_range.generated_source_certificate_status.as_str(),
            "required_for_runtime"
        );
        assert_eq!(
            engine_range.support_status.as_str(),
            "fixture_smoke_supported"
        );
        assert!(engine_range.generated_source_created);
        assert!(engine_range.output_io_performed);
        assert_eq!(engine_range.claim_gate_status, "fixture_smoke_only");
        assert!(engine_range.required_generator_kinds.contains("range("));
        assert!(engine_range.required_generator_kinds.contains("sequence("));
        assert!(
            engine_range
                .claim_boundary
                .contains("range and sequence JSONL/CSV")
        );
    }

    #[test]
    fn api_admission_matrix_classifies_supported_and_report_only_source_free_forms() {
        let matrix = GeneratedSourceApiAdmissionMatrix::report_only();
        assert_eq!(
            matrix.python_row_order(),
            vec![
                "python_ctx_from_rows",
                "python_ctx_range",
                "python_ctx_sequence",
                "python_ctx_literal_table",
                "python_ctx_calendar",
                "python_generated_source_write",
            ]
        );
        assert_eq!(
            matrix.sql_row_order(),
            vec![
                "sql_literal_select",
                "sql_values",
                "sql_source_free_projection",
                "sql_generate_series_range",
            ]
        );
        assert_eq!(
            matrix.dataframe_row_order(),
            vec![
                "dataframe_source_free_projection",
                "dataframe_generated_with_column",
            ]
        );
        assert!(matrix.all_rows_fallback_free());
        assert!(!matrix.data_read);
        assert!(!matrix.source_io_performed);
        assert!(!matrix.broad_sql_dataframe_claim_allowed);

        let from_rows = matrix
            .row_for("python_ctx_from_rows")
            .expect("python from_rows row");
        assert_eq!(from_rows.support_status.as_str(), "fixture_smoke_supported");
        assert!(from_rows.runtime_execution);
        assert!(from_rows.write_io);
        assert!(from_rows.generated_source_created);

        let literal_table = matrix
            .row_for("python_ctx_literal_table")
            .expect("python literal_table row");
        assert_eq!(
            literal_table.support_status.as_str(),
            "fixture_smoke_supported"
        );
        assert!(literal_table.runtime_execution);
        assert!(literal_table.write_io);
        assert!(literal_table.generated_source_created);

        let calendar = matrix
            .row_for("python_ctx_calendar")
            .expect("python calendar row");
        assert_eq!(calendar.support_status.as_str(), "fixture_smoke_supported");
        assert!(calendar.runtime_execution);
        assert!(calendar.write_io);
        assert!(calendar.generated_source_created);

        let sequence = matrix
            .row_for("python_ctx_sequence")
            .expect("python sequence row");
        assert_eq!(sequence.support_status.as_str(), "fixture_smoke_supported");
        assert!(sequence.runtime_execution);
        assert!(sequence.write_io);
        assert!(sequence.generated_source_created);

        let sql_values = matrix.row_for("sql_values").expect("sql values row");
        assert_eq!(
            sql_values.support_status.as_str(),
            "fixture_smoke_supported"
        );
        assert!(sql_values.runtime_execution);
        assert!(sql_values.write_io);
        assert!(sql_values.generated_source_created);
        assert_eq!(
            sql_values.blocker_id,
            "none_scoped_local_sql_values_jsonl_csv_smoke_only"
        );
    }

    #[test]
    fn evidence_alignment_report_links_generated_source_to_export_refs_without_execution() {
        let report = GeneratedSourceEvidenceAlignmentReport::report_only();
        assert_eq!(
            report.row_order(),
            vec![
                "no_dataset_smoke",
                "python_generated_source_write",
                "sql_dataframe_source_free",
                "foundry_generated_output",
            ]
        );
        assert!(report.all_rows_fallback_free());
        assert!(!report.openlineage_export_enabled);
        assert!(!report.opentelemetry_export_enabled);
        assert!(!report.opentelemetry_network_exporter_enabled);
        assert!(!report.bayesian_confidence_enabled);
        assert!(!report.foundry_runtime_invoked);
        assert!(!report.object_store_io_performed);
        assert_eq!(report.claim_gate_status, "not_claim_grade");

        let foundry = report
            .rows
            .iter()
            .find(|row| row.row_id == "foundry_generated_output")
            .expect("foundry generated output row");
        assert_eq!(
            foundry.foundry_boundary_ref,
            "shardloom.foundry_generated_output_boundary.v1"
        );
        assert_eq!(foundry.claim_gate_status, "not_claim_grade");
        assert!(!foundry.runtime_execution);
    }
}
