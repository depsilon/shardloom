//! Reports describe the producer that actually ran, including retained state and
//! possible provider filter work when the provider delivers no output batches.

use super::{
    LocalVortexScan, PreparedVortexUnary, Result, VortexLocalPrimitiveExecutionReport,
    VortexQueryPrimitiveKind as Kind, failed, vortex_error,
};

#[derive(Clone, Copy, Default)]
pub(in crate::local_primitives) struct StateUsage {
    pub(in crate::local_primitives) items: usize,
    pub(in crate::local_primitives) all_input_retained: bool,
}

impl PreparedVortexUnary {
    pub(super) fn execution_report(
        &self,
        scan: &LocalVortexScan,
        usage: StateUsage,
    ) -> Result<VortexLocalPrimitiveExecutionReport> {
        let mut report = match self.bound.request.kind {
            Kind::DistinctRows => super::super::distinct_rows_report(self.bound.request.kind, scan),
            Kind::DropDuplicateRows => {
                super::super::drop_duplicate_rows_report(scan, &self.bound.request)
            }
            Kind::DuplicateMaskRows => {
                super::super::duplicate_mask_rows_report(scan, &self.bound.request)
            }
            Kind::TailRows => super::super::tail_rows_report(self.bound.request.kind, scan),
            Kind::SampleRows => super::super::sample_rows_report(&self.bound.request, scan),
            Kind::RollingWindowRows => {
                super::super::rolling_window_rows_report(&self.bound.request, scan)
            }
            Kind::ExpressionProjectRows => {
                super::super::expression_project_rows_report(&self.bound.request, scan)
            }
            Kind::MeltRows => super::super::melt_rows_report(&self.bound.request, scan),
            Kind::ExplodeRows => super::super::explode_rows_report(&self.bound.request, scan),
            Kind::PivotRows => super::super::pivot_rows_report(&self.bound.request, scan),
            _ => Err(failed("operator report is not admitted")),
        }?
        .with_physical_policy(self.physical_policy.clone());
        let provider_work =
            scan.empty_provider_filter_work(self.bound.request.predicate.as_ref())?;
        let data_work = scan.data_read() || provider_work;
        report.full_stream_collected = usage.all_input_retained;
        report.data_read = data_work;
        report.data_decoded = data_work;
        report.data_materialized = data_work;
        report.row_read = data_work;
        report.materialization_boundary_reported = data_work;
        if provider_work && let Some(summary) = &mut report.result_summary {
            summary.push_str(" scan_side_effect_scope=provider_filter_may_read_decode_materialize_not_observed_bytes");
        }
        if self.bound.request.kind != Kind::TailRows {
            let family = match self.bound.request.kind {
                Kind::SampleRows if self.bound.request.sample_with_replacement => {
                    "sample_replacement_population"
                }
                Kind::SampleRows => "bounded_top_k_sample_state",
                Kind::ExpressionProjectRows => "expression_rewrite_state",
                Kind::MeltRows => "bounded_melt_row_references",
                Kind::ExplodeRows => "bounded_explode_row_references",
                Kind::PivotRows => "reserved_sparse_pivot_state",
                _ => &report.state_budget.state_family,
            }
            .to_owned();
            report.state_budget = reserved_state_budget(family, &report.state_budget, usage)?;
            report.state_budget.state_budget_status = "reserved_within_operator_budget".into();
            report.state_budget.state_pressure_class = "reserved_within_operator_budget".into();
            report.state_budget.diagnostic_code = "none".into();
            report.state_budget.next_action = "none".into();
        }
        Ok(report)
    }
}

fn reserved_state_budget(
    family: String,
    prior: &super::super::VortexLocalPrimitiveStateBudgetReport,
    usage: StateUsage,
) -> Result<super::super::VortexLocalPrimitiveStateBudgetReport> {
    // Preserve the operator's public scheduling and pressure identifiers when
    // adding evidence for the newly owned state and result buffers.
    let mut units = prior
        .capillary_work_units
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    for unit in ["vortex_scan", "source_order_boundary"] {
        if !units.contains(&unit) {
            units.push(unit);
        }
    }
    let mut signals = prior
        .pulseweave_pressure_signals
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    for signal in [
        "retained_state_items",
        "reserved_operator_bytes",
        "native_buffer_bytes",
    ] {
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(
        super::super::VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
            family,
            units,
            signals,
            u64::try_from(usage.items).map_err(vortex_error)?,
            None,
            "owned_unary_state_and_result_buffers_excludes_untracked_provider_metadata_and_process_rss",
        ),
    )
}
