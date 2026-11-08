// SPDX-License-Identifier: Apache-2.0

use prost::Message as _;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::{IntoArray, VortexSessionExecute};
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;

use super::{ViewLen, ZstdData, collect_valid_vbv};

fn original_payload_and_indices(values: &[Option<Vec<u8>>]) -> (ByteBuffer, Vec<usize>) {
    let mut payload = Vec::new();
    let mut indices = Vec::new();
    for value in values.iter().flatten() {
        indices.push(payload.len());
        payload.extend_from_slice(&ViewLen::try_from(value.len()).unwrap().to_le_bytes());
        payload.extend_from_slice(value);
    }
    (ByteBuffer::copy_from(payload.as_slice()), indices)
}

fn compare_original_representation(values: &[Option<Vec<u8>>], sliced: bool) -> VortexResult<()> {
    let session = vortex_session::VortexSession::empty();
    let mut ctx = session.create_execution_ctx();
    let input = if sliced {
        let mut padded = vec![Some(b"unselected prefix".to_vec())];
        padded.extend_from_slice(values);
        padded.push(Some(b"unselected suffix".to_vec()));
        VarBinViewArray::from_iter_nullable_bin(padded.iter().map(Option::as_deref))
            .into_array()
            .slice(1..values.len() + 1)?
            .execute::<VarBinViewArray>(&mut ctx)?
    } else {
        VarBinViewArray::from_iter_nullable_bin(values.iter().map(Option::as_deref))
    };
    let (payload, indices) = original_payload_and_indices(values);
    for width in [0, 1, 2, 7, 8_192, usize::MAX] {
        let effective = if width == 0 {
            indices.len().max(1)
        } else {
            width.min(indices.len().max(1))
        };
        let starts: Vec<_> = indices.iter().step_by(effective).copied().collect();
        let packed = collect_valid_vbv(&input, width, &mut ctx)?;
        assert_eq!(packed.payload, payload);
        assert_eq!(packed.frame_byte_starts.as_slice(), starts);
        assert_eq!(packed.n_values, indices.len());
        assert_eq!(packed.values_per_frame, effective);
        for dictionary in [false, true] {
            let expected = ZstdData::compress_values(
                &payload,
                &starts,
                3,
                effective,
                indices.len(),
                dictionary,
                &ctx.host_allocator(),
            )?;
            let actual = ZstdData::from_var_bin_view_impl(&input, 3, width, dictionary, &mut ctx)?;
            assert_eq!(actual.storage.frames, expected.frames);
            assert_eq!(actual.storage.dictionary, expected.dictionary);
            assert_eq!(
                actual.storage.metadata.encode_to_vec(),
                expected.metadata.encode_to_vec()
            );
        }
    }
    Ok(())
}

#[test]
fn sparse_frames_preserve_empty_null_binary_and_sliced_payloads() -> VortexResult<()> {
    let cases = [
        vec![],
        vec![None, None],
        vec![Some(vec![])],
        vec![
            Some(vec![]),
            None,
            Some(vec![0, 255, 128]),
            Some(b"longer-than-inline-view".to_vec()),
        ],
    ];
    for values in &cases {
        for sliced in [false, true] {
            compare_original_representation(values, sliced)?;
        }
    }
    Ok(())
}

#[test]
fn sparse_frames_preserve_dictionary_training_and_partial_frames() -> VortexResult<()> {
    let values: Vec<_> = (0..513)
        .map(|index| {
            (index % 11 != 0).then(|| {
                format!("dictionary sample λ {index:05} ")
                    .repeat(4)
                    .into_bytes()
            })
        })
        .collect();
    compare_original_representation(&values, true)
}

#[test]
fn sparse_index_has_only_native_writer_frame_boundaries() -> VortexResult<()> {
    let input = VarBinViewArray::from_iter_str(std::iter::repeat_n("", 16_385));
    let mut ctx = vortex_session::VortexSession::empty().create_execution_ctx();
    let packed = collect_valid_vbv(&input, 8_192, &mut ctx)?;
    assert_eq!(packed.n_values, 16_385);
    assert_eq!(packed.payload.len(), 16_385 * size_of::<ViewLen>());
    assert_eq!(packed.frame_byte_starts.as_slice(), &[0, 32_768, 65_536]);
    assert_eq!(
        packed.frame_byte_starts.len() * size_of::<usize>(),
        3 * size_of::<usize>()
    );
    Ok(())
}
