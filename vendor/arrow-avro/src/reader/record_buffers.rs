// SPDX-License-Identifier: Apache-2.0
//! Fallible record builders whose allocations move with their Arrow buffers.

use super::decoder_memory::{DecoderMemoryReservation, Memory, invalid};
use crate::errors::AvroError;
use arrow_array::{PrimitiveArray, types::ArrowPrimitiveType};
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use bytes::Bytes;
use std::{fmt::Debug, mem::size_of, ops::Deref};

#[derive(Debug)]
pub(crate) struct Values<T> {
    values: Vec<T>,
    // Free the allocation before refunding its independent credit.
    credit: Option<Box<dyn DecoderMemoryReservation>>,
    memory: Memory,
}

impl<T> Default for Values<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            credit: None,
            memory: None,
        }
    }
}

impl<T: Copy> Values<T> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn set_memory(&mut self, memory: &Memory) -> Result<(), AvroError> {
        if self.values.capacity() != 0 {
            return Err(invalid(
                "Avro decoder memory must be attached before decoding",
            ));
        }
        self.memory = memory.clone();
        Ok(())
    }

    pub(crate) fn memory(&self) -> Memory {
        self.memory.clone()
    }

    fn reserve(&mut self, additional: usize) -> Result<(), AvroError> {
        let required = self
            .values
            .len()
            .checked_add(additional)
            .ok_or_else(|| invalid("Avro decoder length overflow"))?;
        if required <= self.values.capacity() {
            return Ok(());
        }
        let capacity = required.max(
            self.values
                .capacity()
                .checked_mul(2)
                .ok_or_else(|| invalid("Avro decoder capacity overflow"))?,
        );
        let bytes = capacity
            .checked_mul(size_of::<T>())
            .ok_or_else(|| invalid("Avro decoder allocation overflow"))?;
        // Old payload/credit remain live until the complete replacement exists.
        let credit = self
            .memory
            .as_ref()
            .map(|memory| memory.reserve(bytes))
            .transpose()?;
        let mut replacement = Vec::new();
        replacement
            .try_reserve_exact(capacity)
            .map_err(|error| AvroError::External(Box::new(error)))?;
        if replacement.capacity() > capacity {
            return Err(invalid("Avro decoder allocator exceeded admitted capacity"));
        }
        replacement.extend_from_slice(&self.values);
        self.values = replacement;
        self.credit = credit;
        Ok(())
    }

    pub(crate) fn push(&mut self, value: T) -> Result<(), AvroError> {
        self.reserve(1)?;
        self.values.push(value);
        Ok(())
    }

    pub(crate) fn extend_from_slice(&mut self, values: &[T]) -> Result<(), AvroError> {
        self.reserve(values.len())?;
        self.values.extend_from_slice(values);
        Ok(())
    }

    pub(crate) fn extend(&mut self, values: impl IntoIterator<Item = T>) -> Result<(), AvroError> {
        for value in values {
            self.push(value)?;
        }
        Ok(())
    }

    pub(crate) fn resize(&mut self, length: usize, value: T) -> Result<(), AvroError> {
        self.reserve(length.saturating_sub(self.values.len()))?;
        self.values.resize(length, value);
        Ok(())
    }

    pub(crate) fn take_owned(&mut self) -> Self {
        let empty = Self {
            memory: self.memory.clone(),
            ..Self::default()
        };
        std::mem::replace(self, empty)
    }
}

impl<T: ArrowNativeType> Values<T> {
    pub(crate) fn take_buffer(&mut self) -> Buffer {
        let buffer = Buffer::from_vec(std::mem::take(&mut self.values));
        match self.credit.take() {
            Some(credit) => Buffer::from(Bytes::from_owner(OwnedBuffer {
                buffer,
                _credit: credit,
            })),
            None => buffer,
        }
    }
}

impl<T> Deref for Values<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.values
    }
}

struct OwnedBuffer {
    buffer: Buffer,
    _credit: Box<dyn DecoderMemoryReservation>,
}

impl AsRef<[u8]> for OwnedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

#[derive(Debug, Default)]
pub(crate) struct Bits {
    data: Values<u8>,
    length: usize,
}

impl Bits {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    pub(crate) fn set_memory(&mut self, memory: &Memory) -> Result<(), AvroError> {
        self.data.set_memory(memory)
    }
    pub(crate) fn append(&mut self, value: bool) -> Result<(), AvroError> {
        self.append_n(1, value)
    }
    fn append_n(&mut self, count: usize, value: bool) -> Result<(), AvroError> {
        let length = self
            .length
            .checked_add(count)
            .ok_or_else(|| invalid("Avro bitmap length overflow"))?;
        let bytes = length
            .checked_add(7)
            .ok_or_else(|| invalid("Avro bitmap capacity overflow"))?
            / 8;
        self.data.resize(bytes, 0)?;
        if value {
            for bit in self.length..length {
                self.data.values[bit / 8] |= 1 << (bit % 8);
            }
        }
        self.length = length;
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> BooleanBuffer {
        BooleanBuffer::new(self.data.take_buffer(), 0, std::mem::take(&mut self.length))
    }
}

#[derive(Debug, Default)]
pub(crate) struct Nulls {
    bits: Option<Bits>,
    length: usize,
    memory: Memory,
}

impl Nulls {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    pub(crate) fn set_memory(&mut self, memory: &Memory) -> Result<(), AvroError> {
        if self.length != 0 {
            return Err(invalid("Avro null memory must precede decoding"));
        }
        self.memory = memory.clone();
        Ok(())
    }
    pub(crate) fn append(&mut self, value: bool) -> Result<(), AvroError> {
        let length = self
            .length
            .checked_add(1)
            .ok_or_else(|| invalid("Avro null length overflow"))?;
        if !value && self.bits.is_none() {
            let mut bits = Bits::new();
            bits.set_memory(&self.memory)?;
            bits.append_n(self.length, true)?;
            self.bits = Some(bits);
        }
        if let Some(bits) = &mut self.bits {
            bits.append(value)?;
        }
        self.length = length;
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> Option<NullBuffer> {
        self.length = 0;
        self.bits
            .take()
            .map(|mut bits| NullBuffer::new(bits.finish()))
    }
}

#[derive(Debug, Default)]
pub(crate) struct Offsets {
    values: Values<i32>,
    current: usize,
}

impl Offsets {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    pub(crate) fn set_memory(&mut self, memory: &Memory) -> Result<(), AvroError> {
        self.values.set_memory(memory)
    }
    pub(crate) fn push_length(&mut self, length: usize) -> Result<(), AvroError> {
        let next = self
            .current
            .checked_add(length)
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| invalid("Avro record offset overflow"))?;
        if self.values.is_empty() {
            self.values.push(0)?;
        }
        self.values.push(next)?;
        self.current = next as usize;
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> Result<OffsetBuffer<i32>, AvroError> {
        if self.values.is_empty() {
            self.values.push(0)?;
        }
        self.current = 0;
        Ok(OffsetBuffer::new(self.values.take_buffer().into()))
    }
}

#[derive(Debug)]
pub(crate) struct PrimitiveBuilder<T: ArrowPrimitiveType> {
    values: Values<T::Native>,
}

impl<T: ArrowPrimitiveType> PrimitiveBuilder<T> {
    pub(crate) fn new() -> Self {
        Self {
            values: Values::new(),
        }
    }
    pub(crate) fn set_memory(&mut self, memory: &Memory) -> Result<(), AvroError> {
        self.values.set_memory(memory)
    }
    pub(crate) fn append_value(&mut self, value: T::Native) -> Result<(), AvroError> {
        self.values.push(value)
    }
    pub(crate) fn append_null(&mut self) -> Result<(), AvroError> {
        self.values.push(T::Native::default())
    }
    pub(crate) fn finish(&mut self) -> PrimitiveArray<T> {
        PrimitiveArray::new(ScalarBuffer::from(self.values.take_buffer()), None)
    }
}

impl<T: arrow_array::types::DecimalType> PrimitiveBuilder<T> {
    pub(crate) fn with_precision_and_scale(
        self,
        precision: u8,
        scale: i8,
    ) -> Result<Self, AvroError> {
        arrow_array::types::validate_decimal_precision_and_scale::<T>(precision, scale)?;
        Ok(self)
    }
}
