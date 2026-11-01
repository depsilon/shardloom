// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom provider extension implementing Apache ORC v1 streams.

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::Decimal128Type;
use bytes::{BufMut, BytesMut};
use snafu::ensure;

use crate::encoding::boolean::BooleanEncoder;
use crate::encoding::integer::{rle_v2::RleV2Encoder, SignedEncoding, UnsignedEncoding};
use crate::encoding::PrimitiveValueEncoder;
use crate::error::{Result, UnexpectedSnafu};
use crate::memory::EstimateMemory;

use super::column::ColumnStripeEncoder;
use super::{ColumnEncoding, Stream, StreamType};

#[derive(Default)]
struct Presence {
    encoder: Option<BooleanEncoder>,
    rows: usize,
}

impl Presence {
    fn append(&mut self, array: &dyn Array) {
        if let Some(nulls) = array.nulls() {
            let encoder = self.encoder.get_or_insert_with(|| {
                let mut encoder = BooleanEncoder::new();
                encoder.extend_present(self.rows);
                encoder
            });
            encoder.extend(nulls);
        } else if let Some(encoder) = &mut self.encoder {
            encoder.extend_present(array.len());
        }
        self.rows += array.len();
    }

    fn finish(&mut self, streams: &mut Vec<Stream>) {
        if let Some(mut encoder) = self.encoder.take() {
            streams.push(Stream {
                kind: StreamType::Present,
                bytes: encoder.finish(),
            });
        }
        self.rows = 0;
    }
}

impl EstimateMemory for Presence {
    fn estimate_memory_size(&self) -> usize {
        self.encoder
            .as_ref()
            .map_or(0, EstimateMemory::estimate_memory_size)
    }
}

pub(super) struct DecimalColumnEncoder {
    data: BytesMut,
    scales: RleV2Encoder<i64, SignedEncoding>,
    presence: Presence,
    precision: u8,
    scale: i8,
}

impl DecimalColumnEncoder {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self {
            data: BytesMut::new(),
            scales: RleV2Encoder::new(),
            presence: Presence::default(),
            precision,
            scale,
        }
    }
}

impl EstimateMemory for DecimalColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.data.capacity()
            + self.scales.estimate_memory_size()
            + self.presence.estimate_memory_size()
    }
}

impl ColumnStripeEncoder for DecimalColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let values = array.as_primitive::<Decimal128Type>();
        let bound = 10_u128.pow(u32::from(self.precision));
        // Validate the complete admitted batch before changing any stream.
        for value in values.iter().flatten() {
            ensure!(
                value.unsigned_abs() < bound,
                UnexpectedSnafu {
                    msg: "decimal value exceeds declared ORC precision"
                }
            );
        }
        self.presence.append(values);
        for value in values.iter().flatten() {
            let bits = u128::from_le_bytes(value.to_le_bytes());
            let sign = if value < 0 { u128::MAX } else { 0 };
            let mut encoded = (bits << 1) ^ sign;
            while encoded >= 128 {
                self.data.put_u8((encoded & 127) as u8 | 128);
                encoded >>= 7;
            }
            self.data.put_u8(encoded as u8);
            self.scales.write_one(i64::from(self.scale));
        }
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        let mut streams = vec![
            Stream {
                kind: StreamType::Data,
                bytes: self.data.split().freeze(),
            },
            Stream {
                kind: StreamType::Secondary,
                bytes: self.scales.take_inner(),
            },
        ];
        self.presence.finish(&mut streams);
        streams
    }
}

pub(super) struct StructColumnEncoder {
    children: Vec<Box<dyn ColumnStripeEncoder>>,
    presence: Presence,
}

impl StructColumnEncoder {
    pub fn new(children: Vec<Box<dyn ColumnStripeEncoder>>) -> Self {
        Self {
            children,
            presence: Presence::default(),
        }
    }
}

impl EstimateMemory for StructColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.presence.estimate_memory_size()
            + self
                .children
                .iter()
                .map(|child| child.estimate_memory_size())
                .sum::<usize>()
    }
}

impl ColumnStripeEncoder for StructColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_struct();
        self.presence.append(array);
        for (values, encoder) in array.columns().iter().zip(&mut self.children) {
            if array.null_count() == 0 {
                encoder.encode_array(values)?;
                continue;
            }
            // Slices borrow the existing child buffers; no filtered child
            // representation or population-sized selection vector is needed.
            let mut row = 0;
            while row < array.len() {
                if array.is_null(row) {
                    row += 1;
                    continue;
                }
                let start = row;
                while row < array.len() && array.is_valid(row) {
                    row += 1;
                }
                encoder.encode_array(&values.slice(start, row - start))?;
            }
        }
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::Direct
    }

    fn finish(&mut self) -> Vec<Stream> {
        let mut streams = Vec::new();
        self.presence.finish(&mut streams);
        streams
    }

    fn finish_columns(&mut self) -> Vec<(ColumnEncoding, Vec<Stream>)> {
        let mut columns = vec![(self.column_encoding(), self.finish())];
        for child in &mut self.children {
            columns.extend(child.finish_columns());
        }
        columns
    }
}

pub(super) struct ListColumnEncoder {
    child: Box<dyn ColumnStripeEncoder>,
    lengths: RleV2Encoder<i64, UnsignedEncoding>,
    presence: Presence,
    large: bool,
}

impl ListColumnEncoder {
    pub fn new(child: Box<dyn ColumnStripeEncoder>, large: bool) -> Self {
        Self {
            child,
            lengths: RleV2Encoder::new(),
            presence: Presence::default(),
            large,
        }
    }

    fn encode_list<O: arrow::array::OffsetSizeTrait>(
        &mut self,
        array: &arrow::array::GenericListArray<O>,
    ) -> Result<()> {
        self.presence.append(array);
        let offsets = array.value_offsets();
        let mut pending: Option<(usize, usize)> = None;
        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let start = offsets[row].as_usize();
            let end = offsets[row + 1].as_usize();
            let length = i64::try_from(end - start).map_err(|_| {
                UnexpectedSnafu {
                    msg: "list length exceeds ORC integer range",
                }
                .build()
            })?;
            self.lengths.write_one(length);
            if start == end {
                continue;
            }
            match pending {
                Some((first, last)) if last == start => pending = Some((first, end)),
                Some((first, last)) => {
                    self.child
                        .encode_array(&array.values().slice(first, last - first))?;
                    pending = Some((start, end));
                }
                None => pending = Some((start, end)),
            }
        }
        if let Some((first, last)) = pending {
            self.child
                .encode_array(&array.values().slice(first, last - first))?;
        }
        Ok(())
    }
}

impl EstimateMemory for ListColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.child.estimate_memory_size()
            + self.lengths.estimate_memory_size()
            + self.presence.estimate_memory_size()
    }
}

impl ColumnStripeEncoder for ListColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        if self.large {
            self.encode_list(array.as_list::<i64>())
        } else {
            self.encode_list(array.as_list::<i32>())
        }
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        let mut streams = vec![Stream {
            kind: StreamType::Length,
            bytes: self.lengths.take_inner(),
        }];
        self.presence.finish(&mut streams);
        streams
    }

    fn finish_columns(&mut self) -> Vec<(ColumnEncoding, Vec<Stream>)> {
        let mut columns = vec![(self.column_encoding(), self.finish())];
        columns.extend(self.child.finish_columns());
        columns
    }
}
