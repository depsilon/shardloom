//! Prepared region selection becomes execution evidence only at reader EOF.
//! Counts describe physical regions per completed read phase, not unique source
//! regions across repeated reads, inferred byte savings, or predicate evaluation.

use super::{LocalColumnarSource, NativeExecutionContext, Result, SourceDemand, failed};
use shardloom_core::native_io_decisions::{
    NativeIoCapability, NativeIoDecisionOutcome, NativeIoDecisionReason, NativeIoObservedWork,
};

#[derive(Clone, Copy)]
pub(super) enum RegionSelection {
    Unavailable,
    Regions {
        total: u64,
        excluded: u64,
        admission: NativeIoDecisionReason,
        decline: NativeIoDecisionReason,
    },
}

impl RegionSelection {
    pub(super) fn demand(
        source: &LocalColumnarSource,
        demand: &SourceDemand,
        retained: usize,
    ) -> Result<Option<Self>> {
        if !demand.predicates.iter().any(Option::is_some) {
            return Ok(None);
        }
        if !source.is_parquet()
            && source
                .other
                .as_ref()
                .is_none_or(|source| source.format != "orc")
        {
            return Ok(Some(Self::Unavailable));
        }
        let excluded = source
            .row_groups_total
            .checked_sub(retained)
            .ok_or_else(|| failed("selected region count exceeds the held inventory"))?;
        Ok(Some(Self::Regions {
            total: source.row_groups_total as u64,
            excluded: excluded as u64,
            admission: NativeIoDecisionReason::ExclusionProved,
            decline: if demand.predicates.iter().any(Option::is_none) {
                NativeIoDecisionReason::SharedSourceUsage
            } else {
                NativeIoDecisionReason::ExclusionNotProved
            },
        }))
    }

    pub(super) fn selected(total: usize, retained: usize) -> Result<Self> {
        let excluded = total
            .checked_sub(retained)
            .ok_or_else(|| failed("selected payload regions exceed admitted rank regions"))?;
        Ok(Self::Regions {
            total: total as u64,
            excluded: excluded as u64,
            admission: NativeIoDecisionReason::ValidatedRowSelection,
            decline: NativeIoDecisionReason::RequiredValueEvaluation,
        })
    }

    pub(super) fn complete(self, context: &NativeExecutionContext<'_>) -> Result<()> {
        let Self::Regions {
            total,
            excluded,
            admission,
            decline,
        } = self
        else {
            return context.record_capability_decision(
                NativeIoCapability::RegionExclusion,
                NativeIoDecisionOutcome::Declined,
                NativeIoDecisionReason::SourceCapabilityUnavailable,
                NativeIoObservedWork::default(),
            );
        };
        for (count, outcome, reason, excluded_regions) in [
            (
                excluded,
                NativeIoDecisionOutcome::Executed,
                admission,
                excluded,
            ),
            (
                total - excluded,
                NativeIoDecisionOutcome::Declined,
                decline,
                0,
            ),
        ] {
            if count > 0 {
                context.record_capability_decisions(
                    NativeIoCapability::RegionExclusion,
                    count,
                    outcome,
                    reason,
                    NativeIoObservedWork {
                        regions: Some(count),
                        excluded_regions: Some(excluded_regions),
                        ..NativeIoObservedWork::default()
                    },
                )?;
            }
        }
        Ok(())
    }
}
