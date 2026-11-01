// SPDX-License-Identifier: Apache-2.0
//! Admission for decoder preparation, header storage and record payloads.

use crate::errors::AvroError;
use arrow_schema::ArrowError;
use std::{fmt::Debug, sync::Arc};

/// One independently owned reservation, released when its owner drops.
pub trait DecoderMemoryReservation: Debug + Send + Sync {}

/// Caller-owned admission for decoder preparation, headers and decoded records.
///
/// Each allocation obtains a separate credit before allocation. Replacements
/// overlap the old credit; flushed Arrow buffers retain their own credits across
/// clones, slices and reader destruction. Header clones share admitted storage.
/// OCF JSON parsing has a temporary source-shaped capacity reservation. Codec,
/// projection, decoder and Arrow schema structures have a retained capacity
/// owner; callers retaining their schema or batch metadata beyond the decoder
/// must also retain `schema_memory_reservation()`. These capacities are
/// conservative bounds for named structures, not an allocator/RSS bound.
/// Controlled expanded schemas are limited to depth 128 before construction.
/// Input buffering, raw stream schema-store parsing/cache storage and OCF
/// block/codec storage require their separate admission boundaries.
pub trait DecoderMemory: Debug + Send + Sync {
    /// Admit an independent allocation of `bytes`, or fail before allocating.
    fn reserve(&self, bytes: usize) -> Result<Box<dyn DecoderMemoryReservation>, ArrowError>;
}

pub(crate) type Memory = Option<Arc<dyn DecoderMemory>>;

pub(crate) fn invalid(message: &str) -> AvroError {
    AvroError::ParseError(message.to_string())
}
