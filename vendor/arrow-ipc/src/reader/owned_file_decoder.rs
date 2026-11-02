// SPDX-License-Identifier: Apache-2.0
//! Caller-owned file/stream message decoding with explicit dictionary owners.

use super::{
    DecoderMemory, DecoderMemoryReservation, FileDecoder, OwnedRecordBatch, OwnedSchema,
    RecordBatchDecoder, array_structure, block_body,
    decoder_memory::invalid,
    dictionary_delta,
    schema_owned::{add, table, times},
};
use arrow_array::ArrayRef;
use arrow_buffer::Buffer;
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema};
use std::{collections::HashMap, mem::size_of, sync::Arc};

#[cfg(test)]
#[path = "owned_file_decoder_tests.rs"]
mod tests;

/// Decode caller-owned IPC messages while retaining schema, dictionary and
/// returned array-structure credits. The caller admits physical message bytes;
/// decompression, alignment copies and dictionary deltas use `memory`.
///
/// Returned batches carry their schema/structure owners independently of this
/// decoder. Their payload buffers retain the input and codec owners. No row
/// resizing or process allocator/RSS guarantee is implied by this API.
#[derive(Debug)]
pub struct OwnedFileDecoder {
    // Drop arrays and their metadata before refunding associated reservations.
    inner: FileDecoder,
    dictionary_credits: HashMap<i64, Arc<dyn DecoderMemoryReservation>>,
    schema: OwnedSchema,
    memory: Arc<dyn DecoderMemory>,
    _credit: Box<dyn DecoderMemoryReservation>,
}

fn fields<'a>(dtype: &'a DataType, visit: &mut impl FnMut(&'a Field)) {
    match dtype {
        DataType::Struct(children) => {
            for child in children {
                field(child, visit);
            }
        }
        DataType::Union(children, _) => {
            for (_, child) in children.iter() {
                field(child, visit);
            }
        }
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::ListView(child)
        | DataType::LargeListView(child)
        | DataType::FixedSizeList(child, _)
        | DataType::Map(child, _)
        | DataType::RunEndEncoded(_, child) => field(child, visit),
        DataType::Dictionary(_, value) => fields(value, visit),
        _ => {}
    }
}

fn field<'a>(input: &'a Field, visit: &mut impl FnMut(&'a Field)) {
    visit(input);
    fields(input.data_type(), visit);
}

impl OwnedFileDecoder {
    /// Admit decoder maps and projection before construction. `schema` must
    /// have been admitted by [`super::read_schema_with_memory`].
    pub fn new(
        schema: OwnedSchema,
        version: crate::MetadataVersion,
        projection: Option<&[usize]>,
        memory: Arc<dyn DecoderMemory>,
    ) -> Result<Self, ArrowError> {
        if projection.is_some_and(|p| {
            p.iter()
                .any(|index| *index >= schema.schema().fields().len())
        }) {
            return Err(invalid("IPC projection index is out of range"));
        }
        let mut count = 0_usize;
        for input in schema.schema().fields() {
            field(input, &mut |f| {
                count += usize::from(matches!(f.data_type(), DataType::Dictionary(_, _)));
            });
        }
        let bytes = add(
            size_of::<Self>(),
            add(
                table::<(i64, ArrayRef)>(count)?,
                table::<(i64, Arc<dyn DecoderMemoryReservation>)>(count)?,
            )?,
        )?;
        let bytes = add(
            bytes,
            times(projection.map_or(0, <[usize]>::len), size_of::<usize>())?,
        )?;
        let credit = memory.reserve(bytes)?;
        let mut inner = FileDecoder::new(schema.schema().clone(), version);
        inner.dictionaries = HashMap::with_capacity(count);
        inner.projection = projection.map(<[usize]>::to_vec);
        Ok(Self {
            inner,
            dictionary_credits: HashMap::with_capacity(count),
            schema,
            memory,
            _credit: credit,
        })
    }

    /// Borrow the admitted full schema. Keep its independent owner if detached.
    pub fn schema(&self) -> &OwnedSchema {
        &self.schema
    }

    /// Require aligned input instead of admitting a necessary payload copy.
    pub fn with_require_alignment(mut self, required: bool) -> Self {
        self.inner.require_alignment = required;
        self
    }

    /// Read a complete dictionary message, atomically replacing the current
    /// generation only after decode/concatenation and admission succeed.
    pub fn read_dictionary(
        &mut self,
        block: &crate::Block,
        input: &Buffer,
    ) -> Result<(), ArrowError> {
        let body = block_body(block, input)?;
        let message = self.inner.read_message(input)?;
        self.read_dictionary_message(&message, &body)
    }

    /// Read separately owned, verified message metadata and its exact body.
    /// This avoids re-reading or copying an entire stream message after its
    /// body length has been discovered. Dictionary changes remain atomic.
    pub fn read_dictionary_message(
        &mut self,
        message: &crate::Message<'_>,
        body: &Buffer,
    ) -> Result<(), ArrowError> {
        self.validate_message(message, body)?;
        let batch = message
            .header_as_dictionary_batch()
            .ok_or_else(|| invalid("expected IPC dictionary message"))?;
        let id = batch.id();
        let mut value_type = None;
        for input in self.schema.schema().fields() {
            field(input, &mut |f| {
                #[allow(deprecated)]
                if value_type.is_none() && f.dict_id() == Some(id) {
                    if let DataType::Dictionary(_, value) = f.data_type() {
                        value_type = Some(value.as_ref());
                    }
                }
            });
        }
        let value_type =
            value_type.ok_or_else(|| invalid("IPC dictionary id is absent from schema"))?;
        let encoded = batch
            .data()
            .ok_or_else(|| invalid("IPC dictionary has no value batch"))?;
        let schema_capacity = add(
            size_of::<Schema>() + 4 * size_of::<Field>() + 4 * size_of::<FieldRef>(),
            array_structure::node(value_type, 0, 0, 1)?.retained,
        )?;
        let schema_credit = self.memory.reserve(schema_capacity)?;
        let schema = Arc::new(Schema::new(vec![Field::new("", value_type.clone(), true)]));
        let decoded = RecordBatchDecoder::try_new(
            body,
            encoded,
            schema,
            &self.inner.dictionaries,
            &message.version(),
        )?
        .with_require_alignment(self.inner.require_alignment)
        .with_decoder_memory(self.memory.clone())
        .read_owned_record_batch()?;
        let (decoded, credit) = decoded.into_parts();
        let values = decoded.column(0).clone();
        drop(decoded);
        drop(schema_credit);
        let (values, credit) = if batch.isDelta() {
            let existing = self
                .inner
                .dictionaries
                .get(&id)
                .ok_or_else(|| invalid("IPC dictionary delta has no prior generation"))?;
            let combined = dictionary_delta::concatenate(existing, &values, self.memory.as_ref())?;
            // The decoded delta payload must drop before its structure credit.
            drop(values);
            drop(credit);
            combined
        } else {
            (values, credit)
        };
        self.inner.dictionaries.insert(id, values);
        self.dictionary_credits.insert(id, credit);
        Ok(())
    }

    /// Decode a complete record-batch message with independently retained
    /// schema/array owners. Dictionary buffers remain valid across replacement.
    pub fn read_record_batch(
        &self,
        block: &crate::Block,
        input: &Buffer,
    ) -> Result<Option<OwnedRecordBatch>, ArrowError> {
        let body = block_body(block, input)?;
        let message = self.inner.read_message(input)?;
        self.read_record_batch_message(&message, &body)
    }

    /// Decode separately owned, verified message metadata and its exact body.
    /// The returned batch owns schema/structure credits independently of both
    /// the decoder and the metadata input.
    pub fn read_record_batch_message(
        &self,
        message: &crate::Message<'_>,
        body: &Buffer,
    ) -> Result<Option<OwnedRecordBatch>, ArrowError> {
        self.validate_message(message, body)?;
        if message.header_type() == crate::MessageHeader::NONE {
            return Ok(None);
        }
        let batch = message
            .header_as_record_batch()
            .ok_or_else(|| invalid("expected IPC record batch message"))?;
        let mut decoded = RecordBatchDecoder::try_new(
            body,
            batch,
            self.inner.schema.clone(),
            &self.inner.dictionaries,
            &message.version(),
        )?
        .with_projection(self.inner.projection.as_deref())
        .with_require_alignment(self.inner.require_alignment)
        .with_decoder_memory(self.memory.clone())
        .read_owned_record_batch()?;
        decoded.credit = Arc::new(LinkedCredit {
            _arrays: decoded.credit,
            _schema: self.schema.memory_reservation(),
        });
        Ok(Some(decoded))
    }

    fn validate_message(
        &self,
        message: &crate::Message<'_>,
        body: &Buffer,
    ) -> Result<(), ArrowError> {
        if self.inner.version != crate::MetadataVersion::V1
            && message.version() != self.inner.version
        {
            return Err(invalid("IPC metadata versions mismatch"));
        }
        if usize::try_from(message.bodyLength()).ok() != Some(body.len()) {
            return Err(invalid("IPC message body length mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct LinkedCredit {
    _arrays: Arc<dyn DecoderMemoryReservation>,
    _schema: Arc<dyn DecoderMemoryReservation>,
}

impl DecoderMemoryReservation for LinkedCredit {}
