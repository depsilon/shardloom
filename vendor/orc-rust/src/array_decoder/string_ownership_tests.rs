// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::array_decoder::ownership_tests::{budget, Literal};

fn direct(lengths: Vec<i64>, bytes: &'static [u8], memory: Memory) -> DirectStringArrayDecoder {
    DirectStringArrayDecoder::new(
        Box::new(Decompressor::new(
            bytes::Bytes::from_static(bytes),
            None,
            vec![],
        )),
        Box::new(Literal::new(lengths)),
        None,
        memory,
    )
}

#[test]
fn direct_and_dictionary_strings_keep_payload_credit_after_decoder_drop() {
    let (memory, counters) = budget(256);
    let dictionary = Arc::new(
        direct(vec![1, 2, 0], b"a\xc3\xa9", memory.clone())
            .next_byte_batch(3, None)
            .unwrap(),
    );
    assert_eq!(
        dictionary.iter().collect::<Vec<_>>(),
        vec![Some("a"), Some("é"), Some("")]
    );
    assert_eq!(counters.lock().unwrap().live, 19);
    let indexes = Int64ArrayDecoder::new(Box::new(Literal::new(vec![1, 0, 1, 2])), None, memory);
    let mut decoder = DictionaryStringArrayDecoder::new(indexes, dictionary.clone()).unwrap();
    let parent = NullBuffer::from(vec![true, false, true, true, true]);
    let array = decoder.next_batch(5, Some(&parent)).unwrap();
    let values = array.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some("é"), None, Some("a"), Some("é"), Some("")]
    );
    let slice = array.slice(1, 3);
    drop(array);
    drop(decoder);
    drop(dictionary);
    assert_eq!(counters.lock().unwrap().live, 29);
    assert_eq!(
        slice
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some("a"), Some("é")]
    );
    drop(slice);
    assert_eq!(counters.lock().unwrap().live, 0);
}

#[test]
fn invalid_lengths_truncation_and_dictionary_keys_fail_without_retained_credit() {
    let (memory, counters) = budget(256);
    for lengths in [vec![-1], vec![i64::MAX, 1], vec![i32::MAX as i64, 1]] {
        let count = lengths.len();
        assert!(direct(lengths, b"", memory.clone())
            .next_batch(count, None)
            .is_err());
        assert_eq!(counters.lock().unwrap().live, 0);
    }
    assert!(direct(vec![3], b"ab", memory.clone())
        .next_batch(1, None)
        .is_err());
    assert!(direct(vec![3], b"ab", memory.clone())
        .skip_values(1, None)
        .is_err());
    assert!(direct(vec![-1], b"", memory.clone())
        .skip_values(1, None)
        .is_err());
    assert_eq!(counters.lock().unwrap().live, 0);
    for key in [-1, 2, i64::MAX] {
        let dictionary = Arc::new(
            direct(vec![1], b"a", memory.clone())
                .next_byte_batch(1, None)
                .unwrap(),
        );
        let indexes =
            Int64ArrayDecoder::new(Box::new(Literal::new(vec![key])), None, memory.clone());
        let mut decoder = DictionaryStringArrayDecoder::new(indexes, dictionary).unwrap();
        assert!(decoder.next_batch(1, None).is_err());
        drop(decoder);
        assert_eq!(counters.lock().unwrap().live, 0);
    }
}

#[test]
fn dictionary_expansion_is_admitted_before_payload_allocation() {
    let (memory, counters) = budget(96);
    let dictionary = Arc::new(
        direct(vec![16], b"abcdefghijklmnop", memory.clone())
            .next_byte_batch(1, None)
            .unwrap(),
    );
    let indexes = Int64ArrayDecoder::new(Box::new(Literal::new(vec![0, 0, 0])), None, memory);
    let mut decoder = DictionaryStringArrayDecoder::new(indexes, dictionary).unwrap();
    assert!(decoder.next_batch(3, None).is_err());
    assert_eq!(counters.lock().unwrap().live, 24);
    assert!(counters.lock().unwrap().peak <= 96);
    drop(decoder);
    assert_eq!(counters.lock().unwrap().live, 0);
}
