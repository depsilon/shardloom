//! Numeric-only, post-coalescing compression for the isolated ingest candidate.
//!
//! The upstream dictionary probe decides a layout; it does not preserve a
//! non-dictionary compressed result. This adapter uses upstream compression on
//! the actual emitted chunks, after the last canonicalizing repartition.

use std::{sync::Arc, time::Instant};

use futures::future::BoxFuture;
use vortex::utils::aliases::hash_set::HashSet;
use vortex::{
    array::{
        ArrayId, ArrayRef, ExecutionCtx, VTable as _,
        arrays::{Dict, Primitive},
        dtype::DType,
        session::ArraySessionExt as _,
    },
    compressor::{BtrBlocksCompressor, BtrBlocksCompressorBuilder, CompressionSessionExt as _},
    editions::{ComponentKind, EditionSessionExt as _},
    error::VortexResult,
    layout::{
        LayoutRef, LayoutStrategy, LayoutWriterContext,
        layouts::compressed::{CompressingStrategy, CompressorPlugin},
        segments::SegmentSinkRef,
        sequence::{SendableSequentialStream, SequencePointer},
    },
    session::VortexSession,
};

use super::{IngestStageTimings, Stage};
use vortex_compressor::scheme::SchemeExt as _;

/// Keeps text, validity, dictionary codes/values, and statistics on their
/// existing routes. Only the non-Dict primitive data fallback enters this node.
#[derive(Clone)]
pub(super) struct NumericDataStrategy {
    numeric: Arc<dyn LayoutStrategy>,
    other: Arc<dyn LayoutStrategy>,
}

impl NumericDataStrategy {
    pub(super) fn new(
        child: impl LayoutStrategy,
        timings: IngestStageTimings,
        session: &VortexSession,
    ) -> Self {
        let other: Arc<dyn LayoutStrategy> = Arc::new(child);
        // One outstanding compression job per leaf. This does not claim a
        // global writer task limit: upstream field/zone concurrency still applies.
        let numeric = Arc::new(
            CompressingStrategy::new(Arc::clone(&other), NumericCompressor::new(timings, session))
                .with_concurrency(1)
                // BtrBlocks computes its required statistics inside the measured
                // call. Avoid an additional unmeasured Stat::all() pass here;
                // existing Zoned and file-level statistics remain unchanged.
                .with_stats(&[]),
        );
        Self { numeric, other }
    }
}

impl LayoutStrategy for NumericDataStrategy {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        ctx: LayoutWriterContext,
        segment_sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        eof: SequencePointer,
        session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        let child = if matches!(stream.dtype(), DType::Primitive(..)) {
            &self.numeric
        } else {
            &self.other
        };
        child.write_stream(ctx, segment_sink, stream, eof, session)
    }
}

struct NumericCompressor {
    compressor: BtrBlocksCompressor,
    allowed: HashSet<ArrayId>,
    timings: IngestStageTimings,
}

impl NumericCompressor {
    fn new(timings: IngestStageTimings, session: &VortexSession) -> Self {
        let allowed = session
            .enabled_component_ids(ComponentKind::Array)
            .into_iter()
            .collect::<Vec<_>>();
        // The umbrella provider exposes encoding admission, not scheme-ID
        // constructors. Removing Dict from the compressor output whitelist
        // removes both integer and floating dictionary schemes without adding
        // a direct provider dependency. Keep the full edition set for existing
        // serialized arrays that are already encoded and can be preserved.
        let compression_allowed = allowed
            .iter()
            .filter(|id| **id != Dict.id())
            .copied()
            .collect();
        Self {
            compressor: restricted_compressor(session, &compression_allowed),
            allowed: allowed.into_iter().collect(),
            timings,
        }
    }

    fn can_preserve(&self, chunk: &ArrayRef, session: &VortexSession) -> VortexResult<bool> {
        if chunk.is::<Primitive>() {
            return Ok(false);
        }
        let mut pending = vec![chunk.clone()];
        while let Some(array) = pending.pop() {
            // A physical encoding can have several wire forms. Ask its
            // serializer for this array's actual form and children; admitting
            // any registered variant would silently admit newer file layouts.
            let Some(serialized) = session.array_serialize(&array)? else {
                return Ok(false);
            };
            if !self.allowed.contains(&serialized.serialized_id) {
                return Ok(false);
            }
            pending.extend(serialized.children);
        }
        Ok(true)
    }
}

impl CompressorPlugin for NumericCompressor {
    fn compress_chunk(&self, chunk: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        let start = Instant::now();
        let preserve = self.can_preserve(chunk, ctx.session())?;
        let result = if preserve {
            Ok(chunk.clone())
        } else {
            self.compressor.compress(chunk, ctx)
        };
        self.timings.record(
            if preserve {
                Stage::NumericPreserve
            } else {
                Stage::NumericCompress
            },
            start.elapsed(),
            chunk.len() as u64,
            chunk.nbytes(),
            result.as_ref().map_or(0, ArrayRef::nbytes),
        );
        result
    }
}

/// Retain the previous caller-specific output restriction in addition to the
/// provider's registered-scheme and writer-edition checks.
pub(super) fn restricted_compressor(
    session: &VortexSession,
    allowed_encodings: &HashSet<ArrayId>,
) -> BtrBlocksCompressor {
    let excluded = session
        .compression()
        .schemes()
        .iter()
        .filter(|scheme| {
            scheme
                .produced_encodings()
                .iter()
                .any(|id| !allowed_encodings.contains(id))
        })
        .map(|scheme| scheme.id())
        .collect::<Vec<_>>();
    BtrBlocksCompressorBuilder::from_session(session)
        .exclude_schemes(excluded)
        .build()
}

pub(super) fn measured_probe(
    session: &VortexSession,
    allowed_encodings: &HashSet<ArrayId>,
    timings: IngestStageTimings,
) -> Arc<dyn CompressorPlugin> {
    let dictionary_admitted = allowed_encodings.contains(&Dict.id());
    let compressor = restricted_compressor(session, allowed_encodings);
    Arc::new(move |chunk: &ArrayRef, ctx: &mut ExecutionCtx| {
        // DictStrategy uses only the root Dict decision and discards every
        // other compressed result. With Dict excluded, probing a canonical
        // primitive cannot change that decision, but still canonicalizes,
        // compacts and gathers constant-detection statistics upstream. Leave
        // encoded inputs on the provider route: they may have a Dict root or
        // require execution before the decision can be made safely.
        if !dictionary_admitted && chunk.is::<Primitive>() {
            return Ok(chunk.clone());
        }
        let start = Instant::now();
        let result = compressor.compress(chunk, ctx);
        if matches!(chunk.dtype(), DType::Primitive(..)) {
            timings.record(
                Stage::NumericProbe,
                start.elapsed(),
                chunk.len() as u64,
                chunk.nbytes(),
                result.as_ref().map_or(0, ArrayRef::nbytes),
            );
        }
        result
    })
}

#[cfg(test)]
#[path = "vortex_ingest_numeric_encoding_tests.rs"]
mod tests;
