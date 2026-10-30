//! Share lowered immutable definitions without retaining any execution result.

use super::{Lowered, NativeResult, VortexRelationalPreparation, unsupported_sql_error};
use shardloom_exec::live_memory::MemoryLease;
use std::{collections::BTreeMap, sync::Arc};

/// Scoped to one complete lowering. The owning syntax/declaration remains live
/// throughout the call; the retained scope token also prevents address reuse.
pub(crate) struct LoweringMemo {
    entries: BTreeMap<(usize, usize, u8), (Arc<()>, Lowered)>,
    metadata: MemoryLease,
}

impl LoweringMemo {
    pub(crate) fn new(schemas: &VortexRelationalPreparation<'_>) -> NativeResult<Self> {
        Ok(Self {
            entries: BTreeMap::new(),
            metadata: schemas.reserve_input_scratch(0)?,
        })
    }

    pub(crate) fn get(&self, scope: &Arc<()>, position: usize, kind: u8) -> Option<Lowered> {
        self.entries
            .get(&(Arc::as_ptr(scope) as usize, position, kind))
            .map(|(_, value)| value.clone())
    }

    pub(crate) fn insert(
        &mut self,
        scope: &Arc<()>,
        position: usize,
        kind: u8,
        value: &Lowered,
    ) -> NativeResult<()> {
        let bytes = value.columns.iter().try_fold(4096_u64, |bytes, name| {
            u64::try_from(name.len())
                .ok()
                .and_then(|n| n.checked_add(4096))
                .and_then(|n| bytes.checked_add(n))
                .ok_or_else(|| unsupported_sql_error("shared declaration metadata size overflow"))
        })?;
        self.metadata.resize(
            self.metadata.bytes().checked_add(bytes).ok_or_else(|| {
                unsupported_sql_error("shared declaration metadata size overflow")
            })?,
        )?;
        if self
            .entries
            .insert(
                (Arc::as_ptr(scope) as usize, position, kind),
                (Arc::clone(scope), value.clone()),
            )
            .is_some()
        {
            return Err(unsupported_sql_error(
                "shared declaration was lowered twice before publication",
            ));
        }
        Ok(())
    }
}
