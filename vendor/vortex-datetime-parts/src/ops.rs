// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// ShardLoom local change: read full-domain i64 days and check narrower components.

use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::ProbeState;
use vortex_array::dtype::DType;
use vortex_array::extension::datetime::Timestamp;
use vortex_array::scalar::Scalar;
use vortex_array::vtable::OperationsVTable;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;

use crate::DateTimeParts;
use crate::DateTimePartsSlots;
use crate::timestamp;
use crate::timestamp::TimestampParts;

impl OperationsVTable<DateTimeParts> for DateTimeParts {
    type ProbeState = ();

    fn probe_scalar(
        state: &mut ProbeState<'_, DateTimeParts>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let array = state.array();
        let DType::Extension(ext) = array.dtype().clone() else {
            vortex_panic!(
                "DateTimePartsArray must have extension dtype, found {}",
                array.dtype()
            );
        };

        let Some(options) = ext.metadata_opt::<Timestamp>() else {
            vortex_panic!(Compute: "must decode TemporalMetadata from extension metadata");
        };

        if !state.is_valid(index, ctx)? {
            return Ok(Scalar::null(DType::Extension(ext)));
        }

        let days = part_at(state, DateTimePartsSlots::DAYS, "days", index, ctx)?;
        let seconds = part_at(state, DateTimePartsSlots::SECONDS, "seconds", index, ctx)?;
        let subseconds = part_at(
            state,
            DateTimePartsSlots::SUBSECONDS,
            "subseconds",
            index,
            ctx,
        )?;

        let ts = timestamp::combine(
            TimestampParts {
                days,
                seconds: seconds
                    .try_into()
                    .map_err(|_| vortex_err!("timestamp seconds do not fit i32"))?,
                subseconds: subseconds
                    .try_into()
                    .map_err(|_| vortex_err!("timestamp subseconds do not fit i32"))?,
            },
            options.unit,
        );

        Ok(Scalar::extension::<Timestamp>(
            options.clone(),
            Scalar::primitive(ts, ext.storage_dtype().nullability()),
        ))
    }

    fn scalar_at(
        array: ArrayView<'_, DateTimeParts>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        Self::probe_scalar(&mut ProbeState::once(array), index, ctx)
    }
}

/// Reads one timestamp part out of `slot`, through the probe so a repeated read keeps the
/// child's preparation.
fn part_at(
    state: &mut ProbeState<'_, DateTimeParts>,
    slot: usize,
    name: &'static str,
    index: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<i64> {
    state
        .slot(slot)?
        .ok_or_else(|| vortex_err!("DateTimeParts {name} slot is missing"))?
        .execute_scalar(index, ctx)?
        .as_primitive()
        .as_::<i64>()
        .ok_or_else(|| vortex_err!("timestamp {name} must be non-null at a valid row"))
}
