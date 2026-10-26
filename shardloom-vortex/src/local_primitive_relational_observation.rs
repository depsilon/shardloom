//! Action-local metric completion. Provisional metrics never escape a failure.

use super::{
    BoundObservation, CompletedVortexRelationalObservation, Metrics, NativeExecutionContext,
    ReservedVec, Result, failed, native_relational_aggregate,
};
use crate::local_primitives::collect::{JsonRows, serialize_result_schema};

impl Metrics<'_> {
    pub(super) fn complete_observation(
        &self,
        observation: &BoundObservation,
        aggregate: &native_relational_aggregate::Aggregate<'_>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        context.check_cancelled()?;
        let mut output = JsonRows::new(context.memory(), 8 << 20, true)?;
        let mut schema = None;
        aggregate.finish(context, 1, &mut |array| {
            if array.len() != 1 || schema.is_some() {
                return Err(failed("observation must produce exactly one metric row"));
            }
            schema = Some(serialize_result_schema(array.dtype(), context.memory())?);
            output.append_native(&array, context)
        })?;
        let mut completed = self.observations.borrow_mut();
        if completed.is_none() {
            *completed = Some(ReservedVec::new(context.memory())?);
        }
        let completed = completed.as_mut().expect("initialized observations");
        if completed
            .values
            .iter()
            .any(|prior| prior.name == observation.name)
        {
            return Err(failed(
                "an observation cannot execute repeatedly within one action",
            ));
        }
        let metadata = context.memory().reserve(observation.name.len() as u64)?;
        completed.push(CompletedVortexRelationalObservation {
            name: observation.name.clone(),
            result_jsonl: output.finish()?,
            result_schema_json: schema.ok_or_else(|| failed("observation schema absent"))?,
            _metadata: metadata,
        })?;
        context.check_cancelled()
    }
}
