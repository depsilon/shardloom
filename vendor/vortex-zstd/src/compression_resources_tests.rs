// SPDX-License-Identifier: Apache-2.0
// ShardLoom local provider regression tests.

use super::*;
use vortex_array::memory::DefaultHostAllocator;

fn training_input() -> (Vec<u8>, Vec<usize>) {
    let input = (0i64..4096)
        .flat_map(|index| (index * 37 - 4096).to_le_bytes())
        .collect::<Vec<_>>();
    let sample_sizes = vec![1024; 32];
    (input, sample_sizes)
}

#[test]
fn admitted_training_preserves_the_original_default_dictionary_bytes() {
    let (input, sample_sizes) = training_input();
    let expected = zstd::dict::from_continuous(&input, &sample_sizes, 327).unwrap();
    let admitted = train_dictionary(&input, &sample_sizes, 327, &DefaultHostAllocator).unwrap();
    assert_eq!(admitted.as_slice(), expected);
}

#[test]
fn admitted_compression_preserves_original_frames_and_covers_context_growth() {
    let (input, sample_sizes) = training_input();
    let dictionary = zstd::dict::from_continuous(&input, &sample_sizes, 327).unwrap();
    let larger = input.repeat(5);
    for level in [i32::MIN, -1000, -1, 0, 3, 9, 19, 22, i32::MAX] {
        for dictionary in [None, Some(dictionary.as_slice())] {
            let mut original =
                Compressor::with_dictionary(level, dictionary.unwrap_or_default()).unwrap();
            let mut admitted =
                AdmittedCompressor::new(level, dictionary, &DefaultHostAllocator).unwrap();
            // Reuse the context across growth and shrinkage, including an empty
            // frame. The provider's post-call size check must remain satisfied.
            for input in [
                &input[..7],
                &larger[..],
                &input[..1],
                &input[..1024],
                &input[..0],
            ] {
                let expected = original.compress(input).unwrap();
                let mut output = vec![0; compress_bound(input.len()).unwrap()];
                let written = admitted.compress(input, &mut output).unwrap();
                assert_eq!(&output[..written], expected, "level {level}");
            }
        }
    }
}

#[test]
fn admission_rejects_invalid_training_extents_and_checked_size_domains() {
    for (input_len, sample_sizes, dictionary_capacity, message) in [
        (9, vec![8], 256, "sample extents"),
        (8, vec![usize::MAX, 9], 256, "sample size overflow"),
        (7, vec![7], 256, "size domain"),
        (64, vec![8; 8], usize::MAX, "dictionary size overflow"),
        (8, vec![1; 8], 256, "training partition"),
        (64, vec![32; 2], 256, "sample split"),
        (
            u32::MAX as usize,
            vec![u32::MAX as usize],
            256,
            "size domain",
        ),
    ] {
        let error = training_heap_bound(input_len, &sample_sizes, dictionary_capacity).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }
    assert!(compress_bound(usize::MAX).is_err());
}

#[test]
fn training_preserves_the_original_provider_failure() {
    let (input, sample_sizes) = training_input();
    let original = zstd::dict::from_continuous(&input, &sample_sizes, 1).unwrap_err();
    let admitted = train_dictionary(&input, &sample_sizes, 1, &DefaultHostAllocator).unwrap_err();
    assert!(
        admitted.to_string().contains(&original.to_string()),
        "{admitted}"
    );
}
