// SPDX-License-Identifier: Apache-2.0
//! Structural overlap bound for the admitted decoder graph. Payload/run buffers
//! retain their independent credits; this does not model allocator overhead/RSS.
use super::*;
use std::mem::size_of;

pub(crate) fn metadata_per_node() -> usize {
    use crate::compression::Decompressor;
    use crate::encoding::{
        boolean::BooleanDecoder,
        byte::ByteRleDecoder,
        integer::{rle_v1::RleV1Decoder, rle_v2::RleV2Decoder, SignedEncoding},
        timestamp::TimestampNanosecondAsDecimalDecoder,
    };
    use arrow::array::{
        ArrayData, BinaryArray, BooleanArray, Decimal128Array, Int64Array, ListArray, MapArray,
        StringArray, StructArray, UnionArray,
    };

    // Each logical node creates one array decoder. Timestamp/dictionary paths
    // can overlap a present stream, two value/length streams and a wrapper.
    let array_decoder = [
        size_of::<Int64ArrayDecoder>(),
        size_of::<BooleanArrayDecoder>(),
        size_of::<decimal::DecimalArrayDecoder>(),
        size_of::<string::DictionaryStringArrayDecoder>(),
        size_of::<string::GenericByteArrayDecoder<arrow::datatypes::GenericStringType<i32>>>(),
        size_of::<ListArrayDecoder>(),
        size_of::<MapArrayDecoder>(),
        size_of::<StructArrayDecoder>(),
        size_of::<UnionArrayDecoder>(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    let value_decoder = [
        size_of::<BooleanDecoder<Decompressor>>(),
        size_of::<ByteRleDecoder<Decompressor>>(),
        size_of::<FloatDecoder<f64, Decompressor>>(),
        size_of::<RleV1Decoder<i64, Decompressor, SignedEncoding>>(),
        size_of::<RleV2Decoder<i64, Decompressor, SignedEncoding>>(),
        size_of::<TimestampNanosecondAsDecimalDecoder>(),
        size_of::<crate::encoding::decimal::UnboundedVarintStreamDecoder<Decompressor>>(),
        decimal::structure_bytes(),
        timestamp::structure_bytes(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    let output = [
        size_of::<Int64Array>(),
        size_of::<Decimal128Array>(),
        size_of::<BooleanArray>(),
        size_of::<StringArray>(),
        size_of::<BinaryArray>(),
        size_of::<ListArray>(),
        size_of::<MapArray>(),
        size_of::<StructArray>(),
        size_of::<UnionArray>(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0);

    // Arc allocation headers, trait-object handles and child vectors can
    // overlap during construction and Arrow's checked ArrayData conversion.
    // An edge belongs to one child, so these per-node terms also cover wide
    // structs/unions without a fixed field/row-count allowance.
    array_decoder
        + 4 * (value_decoder + size_of::<[usize; 8]>())
        + 2 * (output + size_of::<ArrayData>())
        + 8 * size_of::<ArrayRef>()
        + 2 * size_of::<arrow::datatypes::Field>()
        + 16 * size_of::<usize>()
}
