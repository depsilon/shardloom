// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom preallocation, retained-result and error-order controls.

use super::*;
use crate::reader::{DecoderMemory, DecoderMemoryReservation};
use arrow_schema::ArrowError;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct Pool {
    used: AtomicUsize,
    peak: AtomicUsize,
    limit: usize,
    cancelled: AtomicBool,
}

impl Pool {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            limit,
            cancelled: AtomicBool::new(false),
        })
    }
    fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }
    fn memory(self: &Arc<Self>) -> Memory {
        Some(Arc::new(Factory(self.clone())))
    }
}

#[derive(Debug)]
struct Factory(Arc<Pool>);
#[derive(Debug)]
struct Credit {
    pool: Arc<Pool>,
    bytes: usize,
}
impl DecoderMemoryReservation for Credit {}
impl Drop for Credit {
    fn drop(&mut self) {
        self.pool.used.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}
impl DecoderMemory for Factory {
    fn reserve(&self, bytes: usize) -> Result<Box<dyn DecoderMemoryReservation>, ArrowError> {
        if self.0.cancelled.load(Ordering::SeqCst) {
            return Err(ArrowError::MemoryError(
                "record control cancelled".to_string(),
            ));
        }
        let next = self
            .0
            .used()
            .checked_add(bytes)
            .filter(|total| *total <= self.0.limit)
            .ok_or_else(|| ArrowError::MemoryError("record control limit".to_string()))?;
        self.0.used.store(next, Ordering::SeqCst);
        self.0.peak.fetch_max(next, Ordering::SeqCst);
        Ok(Box::new(Credit {
            pool: self.0.clone(),
            bytes,
        }))
    }
}

fn encoded_bytes(value: &[u8]) -> Vec<u8> {
    assert!(value.len() < 64);
    let mut out = vec![u8::try_from(value.len() * 2).unwrap()];
    out.extend_from_slice(value);
    out
}

#[test]
fn record_payloads_keep_their_credit_after_decoder_and_parent_array_drop() {
    let text = "a long 東京 value beyond inline storage";
    let mut cases: Vec<(Codec, Vec<u8>, ArrayRef)> = vec![
        (
            Codec::Boolean,
            vec![1],
            Arc::new(BooleanArray::from(vec![true])),
        ),
        (Codec::Int32, vec![84], Arc::new(Int32Array::from(vec![42]))),
        (
            Codec::Int64,
            vec![83],
            Arc::new(Int64Array::from(vec![-42])),
        ),
        (
            Codec::Float32,
            1.25_f32.to_le_bytes().to_vec(),
            Arc::new(Float32Array::from(vec![1.25])),
        ),
        (
            Codec::Float64,
            (-2.5_f64).to_le_bytes().to_vec(),
            Arc::new(Float64Array::from(vec![-2.5])),
        ),
        (Codec::Date32, vec![2], Arc::new(Date32Array::from(vec![1]))),
        (
            Codec::TimeMillis,
            vec![4],
            Arc::new(Time32MillisecondArray::from(vec![2])),
        ),
        (
            Codec::TimeMicros,
            vec![6],
            Arc::new(Time64MicrosecondArray::from(vec![3])),
        ),
        (
            Codec::TimestampNanos(None),
            vec![8],
            Arc::new(TimestampNanosecondArray::from(vec![4])),
        ),
        (
            Codec::Utf8,
            encoded_bytes(text.as_bytes()),
            Arc::new(StringArray::from(vec![text])),
        ),
        (
            Codec::Utf8View,
            encoded_bytes(text.as_bytes()),
            Arc::new(StringViewArray::from(vec![text])),
        ),
        (
            Codec::Binary,
            encoded_bytes(&[0, 255, 4]),
            Arc::new(BinaryArray::from(vec![&[0, 255, 4][..]])),
        ),
        (
            Codec::Fixed(3),
            vec![3, 2, 1],
            Arc::new(FixedSizeBinaryArray::try_from_iter([&[3, 2, 1][..]].into_iter()).unwrap()),
        ),
        (
            Codec::Decimal(20, Some(2), None),
            encoded_bytes(&[0x7b]),
            Arc::new(
                Decimal128Array::from(vec![123])
                    .with_precision_and_scale(20, 2)
                    .unwrap(),
            ),
        ),
        (
            Codec::Decimal(50, Some(3), None),
            encoded_bytes(&[0xff]),
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(-1)])
                    .with_precision_and_scale(50, 3)
                    .unwrap(),
            ),
        ),
    ];
    let mut interval = Vec::new();
    for value in [1_u32, 2, 3] {
        interval.extend_from_slice(&value.to_le_bytes());
    }
    cases.push((
        Codec::Interval,
        interval,
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(1, 2, 3_000_000),
        ])),
    ));
    for (codec, payload, expected) in cases {
        let pool = Pool::new(1 << 20);
        let dtype = AvroDataType::new(codec, Default::default(), None);
        let mut decoder = Decoder::try_new(&dtype).unwrap();
        decoder.set_memory(&pool.memory()).unwrap();
        assert_eq!(pool.used(), 0, "builders start empty");
        let mut held = Vec::new();
        for _ in 0..3 {
            decoder.decode(&mut AvroCursor::new(&payload)).unwrap();
            let array = decoder.flush(None).unwrap();
            assert_eq!(array.to_data(), expected.to_data(), "{dtype:?}");
            let before = pool.used();
            assert!(before > 0);
            held.push(array.slice(0, 1));
            drop(array);
            assert_eq!(pool.used(), before, "slice keeps complete owner");
        }
        drop(decoder);
        assert!(pool.used() > 0, "outputs own credits independently");
        for array in &held {
            assert_eq!(array.to_data(), expected.to_data());
        }
        drop(held);
        assert_eq!(pool.used(), 0, "{dtype:?}");
        assert!(pool.peak.load(Ordering::SeqCst) <= pool.limit);
    }
}

#[test]
fn nested_list_map_enum_and_nullable_payloads_keep_all_owners() {
    let inner = Arc::new(AvroDataType::new(Codec::Int64, Default::default(), None));
    for (codec, payload) in [
        (Codec::List(inner.clone()), vec![4, 6, 10, 0]),
        (Codec::Map(inner), vec![2, 2, b'k', 14, 0]),
        (
            Codec::Enum(vec!["zero".to_string(), "one".to_string()].into()),
            vec![2],
        ),
    ] {
        let pool = Pool::new(4096);
        let dtype = AvroDataType::new(codec, Default::default(), Some(Nullability::NullFirst));
        let mut decoder = Decoder::try_new(&dtype).unwrap();
        decoder.set_memory(&pool.memory()).unwrap();
        decoder.decode(&mut AvroCursor::new(&[0])).unwrap();
        let mut present = vec![2];
        present.extend_from_slice(&payload);
        decoder.decode(&mut AvroCursor::new(&present)).unwrap();
        let array = decoder.flush(None).unwrap();
        assert_eq!(array.len(), 2);
        assert!(array.is_null(0));
        assert!(array.is_valid(1));
        match dtype.codec() {
            Codec::List(_) => {
                let list = array.as_any().downcast_ref::<ListArray>().unwrap();
                let values = list.value(1);
                assert_eq!(
                    values
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .as_ref(),
                    &[3, 5]
                );
            }
            Codec::Map(_) => {
                let map = array.as_any().downcast_ref::<MapArray>().unwrap();
                assert_eq!(
                    map.keys()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0),
                    "k"
                );
                assert_eq!(
                    map.values()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    7
                );
            }
            Codec::Enum(_) => {
                let dict = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap();
                assert_eq!(dict.keys().value(1), 1);
                assert_eq!(
                    dict.values()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(1),
                    "one"
                );
            }
            _ => unreachable!(),
        }
        drop(decoder);
        assert!(pool.used() > 0);
        drop(array);
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn growth_denial_preserves_old_credit_and_refunds_on_drop() {
    let pool = Pool::new(23);
    let dtype = AvroDataType::new(Codec::Int64, Default::default(), None);
    let mut decoder = Decoder::try_new(&dtype).unwrap();
    decoder.set_memory(&pool.memory()).unwrap();
    decoder.decode(&mut AvroCursor::new(&[2])).unwrap();
    assert_eq!(pool.used(), 8);
    let error = decoder.decode(&mut AvroCursor::new(&[4])).unwrap_err();
    assert!(error.to_string().contains("record control limit"));
    assert_eq!(
        pool.used(),
        8,
        "denied 8+16 replacement leaves old owner intact"
    );
    drop(decoder);
    assert_eq!(pool.used(), 0);
}

#[test]
fn null_bitmaps_offsets_and_views_deny_before_uncredited_growth() {
    for codec in [Codec::Boolean, Codec::Utf8, Codec::Utf8View, Codec::Binary] {
        let pool = Pool::new(0);
        let dtype = AvroDataType::new(codec, Default::default(), Some(Nullability::NullFirst));
        let mut decoder = Decoder::try_new(&dtype).unwrap();
        decoder.set_memory(&pool.memory()).unwrap();
        assert!(
            decoder
                .decode(&mut AvroCursor::new(&[0]))
                .unwrap_err()
                .to_string()
                .contains("record control limit")
        );
        drop(decoder);
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.peak.load(Ordering::SeqCst), 0);
    }
    let pool = Pool::new(1024);
    let dtype = AvroDataType::new(Codec::Utf8View, Default::default(), None);
    let mut decoder = Decoder::try_new(&dtype).unwrap();
    decoder.set_memory(&pool.memory()).unwrap();
    decoder
        .decode(&mut AvroCursor::new(&encoded_bytes(b"abc")))
        .unwrap();
    pool.cancelled.store(true, Ordering::SeqCst);
    assert!(
        decoder
            .flush(None)
            .unwrap_err()
            .to_string()
            .contains("record control cancelled")
    );
    drop(decoder);
    assert_eq!(pool.used(), 0);
}
