//! A completed inner relation, with scalar and predicate state kept distinct.

#[cfg(feature = "vortex-write")]
use super::Kind;
use super::{ArrayRef, Batch, DType, NativeExecutionContext, Result, Results, Spec, Subquery};

#[cfg(feature = "vortex-write")]
#[path = "native_relational_subquery_spill.rs"]
pub(super) mod spill;

#[derive(Default)]
pub(in crate::local_primitives) struct Report {
    pub(in crate::local_primitives) stages: u64,
    pub(in crate::local_primitives) spilled_stages: u64,
    pub(in crate::local_primitives) input_rows: u64,
    pub(in crate::local_primitives) indexed_rows: u64,
    pub(in crate::local_primitives) probe_rows: u64,
    pub(in crate::local_primitives) candidate_rows: u64,
    pub(in crate::local_primitives) lookup_blocks: u64,
}

#[allow(clippy::large_enum_variant)] // One bounded state owner, without another heap allocation.
pub(in crate::local_primitives) enum Ready<'a> {
    Resident(Subquery<'a>),
    #[cfg(feature = "vortex-write")]
    Ordered(spill::Predicate<'a>),
}

impl<'a> Ready<'a> {
    pub(in crate::local_primitives) fn empty(
        spec: &'a Spec,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Subquery::new(spec, context.memory()).map(Self::Resident)
    }

    pub(in crate::local_primitives) fn build(
        spec: &'a Spec,
        fields: &[(String, DType)],
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        #[cfg(feature = "vortex-write")] state: Option<
            &'a crate::local_primitives::native_relational_spill::State,
        >,
        input: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    ) -> Result<Self> {
        #[cfg(feature = "vortex-write")]
        if let Some(state) = state
            && !matches!(spec.kind, Kind::Scalar { .. })
        {
            return spill::build(spec, fields, state, context, batch_rows, input)
                .map(Self::Ordered);
        }
        #[cfg(not(feature = "vortex-write"))]
        let _ = (fields, batch_rows);
        let mut resident = Subquery::new(spec, context.memory())?;
        input(&mut |array| resident.build(array, context))?;
        Ok(Self::Resident(resident))
    }

    pub(in crate::local_primitives) fn push_result(
        &mut self,
        values: &mut Results,
        left: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        match self {
            Self::Resident(resident) => values.push(resident, left, row, context),
            #[cfg(feature = "vortex-write")]
            Self::Ordered(ordered) => {
                let Results::Predicate(values) = values else {
                    return Err(super::failed("ordered predicate cannot produce a scalar"));
                };
                values.values[row] = ordered.result(left, row, context)?;
                Ok(())
            }
        }
    }

    pub(in crate::local_primitives) fn consume(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Resident(resident) => resident.consume(array, context, consume),
            #[cfg(feature = "vortex-write")]
            Self::Ordered(ordered) => {
                let selected = ordered.spec.selected_rows(&array, context)?;
                ordered.consume_selected(array, &selected.values, context, consume)
            }
        }
    }

    pub(in crate::local_primitives) fn consume_selected(
        &mut self,
        array: ArrayRef,
        selected: &[usize],
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Resident(resident) => {
                resident.consume_selected(array, selected, context, consume)
            }
            #[cfg(feature = "vortex-write")]
            Self::Ordered(ordered) => ordered.consume_selected(array, selected, context, consume),
        }
    }

    pub(in crate::local_primitives) fn finish(
        self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Report> {
        context.check_cancelled()?;
        match self {
            Self::Resident(_) => Ok(Report::default()),
            #[cfg(feature = "vortex-write")]
            Self::Ordered(ordered) => ordered.finish(context),
        }
    }
}
