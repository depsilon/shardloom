//! Preserve economical input dictionaries before the writer's canonicalizing
//! repartition. Each input remains an independent dictionary epoch and zone.

use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use futures::{StreamExt as _, future::BoxFuture, stream};
use vortex::{
    array::{
        ArrayId, ArrayRef, ExecutionCtx, IntoArray as _, VTable as _,
        arrays::{Dict, DictArray, dict::DictArraySlotsExt as _},
        builtins::ArrayBuiltins as _,
        dtype::{DType, PType},
    },
    compressor::{BtrBlocksCompressor, BtrBlocksCompressorBuilder},
    editions::{ComponentKind, EditionSessionExt as _},
    error::{VortexResult, vortex_err},
    layout::{
        LayoutRef, LayoutStrategy, LayoutStrategyEncodingValidator, LayoutWriterContext,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy,
            compressed::{CompressingStrategy, CompressorPlugin},
            flat::writer::FlatLayoutStrategy,
            zoned::writer::{ZonedLayoutOptions, ZonedStrategy},
        },
        segments::SegmentSinkRef,
        sequence::{
            SendableSequentialStream, SequencePointer, SequentialStreamAdapter,
            SequentialStreamExt as _,
        },
    },
    session::VortexSession,
};

use super::{IngestStageTimings, Stage};

pub(super) struct DictionaryPreservingStrategy {
    retained: Arc<dyn LayoutStrategy>,
    text: Arc<dyn LayoutStrategy>,
}

impl DictionaryPreservingStrategy {
    pub(super) fn new(
        retained: impl LayoutStrategy,
        row_block_size: usize,
        timings: IngestStageTimings,
        session: &VortexSession,
    ) -> Self {
        let retained: Arc<dyn LayoutStrategy> = Arc::new(retained);
        let allowed = session.enabled_component_ids(ComponentKind::Array);
        let dictionary_admitted = allowed.contains(&Dict.id());
        let allowed_values = allowed.clone();
        let compression_allowed = allowed
            .iter()
            .filter(|id| **id != Dict.id())
            .copied()
            .collect();
        let flat: Arc<dyn LayoutStrategy> = Arc::new(LayoutStrategyEncodingValidator::new(
            FlatLayoutStrategy::default(),
            allowed.into_iter().collect(),
        ));
        let data: Arc<dyn LayoutStrategy> = Arc::new(
            CompressingStrategy::new(
                Arc::clone(&flat),
                DictionaryCodes {
                    compressor: BtrBlocksCompressorBuilder::default()
                        .retain_allowed_encodings(&compression_allowed)
                        .build(),
                    timings,
                },
            )
            .with_concurrency(1)
            .with_stats(&[]),
        );
        Self {
            retained: Arc::clone(&retained),
            text: Arc::new(ChunkedLayoutStrategy::new(DictionaryChunk {
                retained,
                flat,
                data,
                dictionary_admitted,
                allowed_values,
                row_block_size: row_block_size.max(1),
            })),
        }
    }
}

impl LayoutStrategy for DictionaryPreservingStrategy {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        ctx: LayoutWriterContext,
        sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        eof: SequencePointer,
        session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        if stream.dtype().is_utf8() {
            self.text.write_stream(ctx, sink, stream, eof, session)
        } else {
            self.retained.write_stream(ctx, sink, stream, eof, session)
        }
    }
}

struct DictionaryChunk {
    retained: Arc<dyn LayoutStrategy>,
    flat: Arc<dyn LayoutStrategy>,
    data: Arc<dyn LayoutStrategy>,
    dictionary_admitted: bool,
    allowed_values: Vec<ArrayId>,
    row_block_size: usize,
}

impl LayoutStrategy for DictionaryChunk {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        ctx: LayoutWriterContext,
        sink: SegmentSinkRef,
        mut input: SendableSequentialStream,
        eof: SequencePointer,
        session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let dtype = input.dtype().clone();
            let Some((sequence, chunk)) = input.next().await.transpose()? else {
                return self
                    .retained
                    .write_stream(ctx, sink, input, eof, session)
                    .await;
            };
            // The enclosing native Chunked strategy guarantees one input here.
            // Check that invariant before publishing any segment.
            if input.next().await.transpose()?.is_some() {
                return Err(vortex_err!(
                    "dictionary-preserving leaf requires one input chunk"
                ));
            }
            let rows = chunk.len();
            let admitted = self.dictionary_admitted && chunk.dtype().is_utf8()
                && chunk.is::<Dict>() && rows > 0 && rows <= self.row_block_size
                && u32::try_from(chunk.as_::<Dict>().values().len()).is_ok()
                && chunk.as_::<Dict>().values().depth_first_traversal().all(|value| {
                    value.is_canonical() || self.allowed_values.contains(&value.encoding_id())
                })
                // Even excluding external string bytes, a canonical view costs
                // 16 bytes per row. This is an admission bound, not a byte claim.
                && chunk.nbytes() < (rows as u64).saturating_mul(16);
            let input = SequentialStreamAdapter::new(dtype, stream::iter([Ok((sequence, chunk))]))
                .sendable();
            if !admitted {
                return self
                    .retained
                    .write_stream(ctx, sink, input, eof, session)
                    .await;
            }
            // One zone per original chunk prevents statistics or dictionary IDs
            // crossing epochs, including short input batches.
            ZonedStrategy::new(
                Arc::clone(&self.data),
                Arc::clone(&self.flat),
                ZonedLayoutOptions {
                    block_size: NonZeroUsize::new(rows).expect("admitted nonempty dictionary"),
                    concurrency: NonZeroUsize::new(1).expect("positive concurrency"),
                    ..Default::default()
                },
            )
            .write_stream(ctx, sink, input, eof, session)
            .await
        })
    }
}

struct DictionaryCodes {
    compressor: BtrBlocksCompressor,
    timings: IngestStageTimings,
}

impl CompressorPlugin for DictionaryCodes {
    fn compress_chunk(&self, chunk: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        let started = Instant::now();
        let dictionary = chunk
            .as_opt::<Dict>()
            .ok_or_else(|| vortex_err!("admitted dictionary lost its native encoding"))?;
        // Arrow dictionaries use signed keys; native direct consumers accept
        // unsigned codes. Native cast checks live values and preserves nulls.
        let codes = dictionary.codes().cast(DType::Primitive(
            PType::U32,
            dictionary.codes().dtype().nullability(),
        ))?;
        let codes = self.compressor.compress(&codes, ctx)?;
        let result = DictArray::try_new(codes, dictionary.values().clone())?.into_array();
        self.timings.record(
            Stage::TextDictionaryPreserve,
            started.elapsed(),
            chunk.len() as u64,
            chunk.nbytes(),
            result.nbytes(),
        );
        Ok(result)
    }
}

#[cfg(all(
    test,
    feature = "universal-format-io",
    feature = "vortex-local-primitives"
))]
#[path = "vortex_ingest_dictionary_preservation_tests.rs"]
mod tests;

#[cfg(all(
    test,
    feature = "universal-format-io",
    feature = "vortex-local-primitives"
))]
#[path = "vortex_ingest_source_dictionary_bench.rs"]
mod source_dictionary_bench;
