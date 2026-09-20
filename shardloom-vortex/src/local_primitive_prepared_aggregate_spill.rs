//! Prepared entry into the same native run operators as ordinary aggregation.
//! Only the source is retained; each spill attempt builds fresh state and runs.

use super::super::{
    aggregate_scan_runtime::AggregateScanRuntime as _, exact_distinct_pairs,
    weighted_count_spill_admission, weighted_count_spill_query,
};
use super::{
    LocalVortexAggregateScan, NativeExecutionContext, PreparedVortexAggregate, Result,
    VortexQueryPrimitiveRequest, failed, required_simple_aggregate,
};
use shardloom_exec::compute_pool::CancellationToken;
use std::sync::Arc;
use vortex::{
    array::dtype::DType, file::VortexFile, io::runtime::current::CurrentThreadRuntime,
    session::VortexSession,
};

pub(super) fn validate_request(request: &VortexQueryPrimitiveRequest) -> Result<()> {
    if weighted_count_spill_admission::request_admitted(request)
        || exact_distinct_pairs::workers::request_may_be_admitted(request)
    {
        Ok(())
    } else {
        Err(failed(
            "explicit spill requires an admitted native COUNT or exact integer DISTINCT request",
        ))
    }
}

pub(super) fn validate_schema(request: &VortexQueryPrimitiveRequest, dtype: &DType) -> Result<()> {
    if weighted_count_spill_admission::request_admitted(request) {
        weighted_count_spill_admission::admit(request, dtype).map(|_| ())
    } else if exact_distinct_pairs::workers::request_schema_may_be_admitted(request, dtype) {
        Ok(())
    } else {
        Err(failed(
            "explicit DISTINCT spill requires nonnullable integer group and value fields",
        ))
    }
}

impl PreparedVortexAggregate {
    pub(super) fn read_spill(
        &self,
        context: Option<&NativeExecutionContext<'_>>,
    ) -> Result<LocalVortexAggregateScan> {
        let uri = self
            .request
            .source_uri
            .as_ref()
            .ok_or_else(|| failed("prepared source URI is absent"))?;
        if weighted_count_spill_admission::request_admitted(&self.request) {
            self.run_spill(context, |file, session, runtime| {
                weighted_count_spill_query::execute(
                    uri,
                    &self.request,
                    self.policy,
                    file,
                    session,
                    runtime,
                    self.session.memory(),
                    self.worker_pool,
                )
            })
        } else {
            self.run_spill(context, |file, session, runtime| {
                exact_distinct_pairs::spill_query::execute(
                    uri,
                    &self.request,
                    self.policy,
                    file,
                    session,
                    runtime,
                    self.session.memory(),
                )
            })
        }
    }

    fn run_spill<T>(
        &self,
        context: Option<&NativeExecutionContext<'_>>,
        execute: impl FnOnce(
            &VortexFile,
            &VortexSession,
            &CurrentThreadRuntime,
        ) -> Result<(LocalVortexAggregateScan, T)>,
    ) -> Result<LocalVortexAggregateScan> {
        let spill = required_simple_aggregate(&self.request)?
            .spill
            .as_ref()
            .ok_or_else(|| failed("prepared spill policy is absent"))?;
        let cancellation = CancellationToken::from_shared_flag(Arc::clone(&spill.cancellation));
        let run = |file: &VortexFile, context: &NativeExecutionContext<'_>| {
            cancellation.check()?;
            let providers = self
                .temporary_provider_drivers
                .then(|| context.runtime().provider_drivers(context.cpu_lanes()))
                .transpose()?;
            let result = execute(file, context.native_session(), context.runtime())?;
            let drivers = providers.as_ref().map_or(0, |(_, count)| *count);
            drop(providers);
            cancellation.check()?;
            Ok((result, drivers))
        };
        let ((mut scan, owner), drivers) = match context {
            Some(context) => self.source.with_admitted_native_execution(context, run),
            None => self
                .source
                .with_native_execution_controlled(&cancellation, run),
        }?;
        // The returned owner keeps operator/result reservations until the source
        // boundary has validated the entire generation and joined its drivers.
        drop(owner);
        scan.restored_provider_background_workers =
            scan.restored_provider_background_workers.max(drivers);
        Ok(scan)
    }
}
