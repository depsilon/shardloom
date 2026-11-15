// SPDX-License-Identifier: Apache-2.0
// ShardLoom modification: active compressed slice regression coverage.

use fsst::Symbol;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinArray;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::scalar_fn::fns::like::LikeKernel;
use vortex_array::scalar_fn::fns::like::LikeOptions;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

use crate::FSST;
use crate::FSSTArrayExt;
use crate::FSSTArraySlotsExt;
use crate::like_observation::LikeObservation;

#[test]
fn prefilter_uses_only_active_codes_for_slices() -> VortexResult<()> {
    for chunked_offsets in [false, true] {
        let session = VortexSession::empty();
        crate::initialize(&session);
        let mut ctx = session.create_execution_ctx();
        // Six rows: large matching parent, "a", null, empty, "z", large
        // matching parent. The middle slices retain both parent byte buffers.
        let mut bytes = vec![0; 131_074];
        bytes[65_535] = 1;
        bytes[65_537] = 1;
        bytes[131_073] = 1;
        let offsets =
            PrimitiveArray::from_iter([0u64, 65_536, 65_537, 65_537, 65_537, 65_538, 131_074])
                .into_array();
        let offsets = if chunked_offsets {
            ChunkedArray::from_iter([offsets.slice(0..3)?, offsets.slice(3..7)?]).into_array()
        } else {
            offsets
        };
        let codes = VarBinArray::new(
            offsets,
            Buffer::from(bytes),
            DType::Binary(Nullability::Nullable),
            Validity::from_iter([true, true, false, true, true, true]),
            &mut ctx,
        );
        let array = FSST::try_new(
            DType::Utf8(Nullability::Nullable),
            buffer![
                Symbol::from_slice(&[b'a', 0, 0, 0, 0, 0, 0, 0]),
                Symbol::from_slice(&[b'z', 0, 0, 0, 0, 0, 0, 0]),
            ],
            buffer![1u8, 1],
            codes,
            PrimitiveArray::from_iter([65_536u64, 1, 0, 0, 1, 65_536]).into_array(),
            &mut ctx,
        )?;

        for (range, expected, rejected) in [
            (1..2, vec![Some(false)], true),
            (1..4, vec![Some(false), None, Some(false)], true),
            (2..3, vec![None], true),
            (3..4, vec![Some(false)], true),
            (2..2, vec![], true),
            (4..5, vec![Some(true)], false),
            (
                1..5,
                vec![Some(false), None, Some(false), Some(true)],
                false,
            ),
        ] {
            let sliced = if range.is_empty() || range == (2..3) {
                // Generic Slice canonicalizes empty/all-null arrays. Construct
                // their valid FSST views explicitly to test this kernel too.
                let codes = VarBinArray::new(
                    array.codes_offsets().slice(range.start..range.end + 1)?,
                    array.codes_bytes().clone(),
                    DType::Binary(Nullability::Nullable),
                    if range.is_empty() {
                        Validity::AllValid
                    } else {
                        Validity::AllInvalid
                    },
                    &mut ctx,
                );
                FSST::try_new_with_symbol_table(
                    array.dtype().clone(),
                    array.symbol_table(),
                    codes,
                    array.uncompressed_lengths().slice(range)?,
                    &mut ctx,
                )?
            } else {
                array
                    .slice(range)?
                    .try_downcast::<FSST>()
                    .map_err(|_| vortex_err!("nonempty mixed/valid slice must preserve FSST"))?
            };
            assert_eq!(sliced.codes().bytes().len(), 131_074);
            let observer = LikeObservation::default();
            session.register(observer.clone());
            let result = <FSST as LikeKernel>::like(
                sliced.as_view(),
                &ConstantArray::new("%z%", sliced.len()).into_array(),
                LikeOptions::default(),
                &mut ctx,
            )?
            .unwrap();
            assert_arrays_eq!(result, BoolArray::from_iter(expected), &mut ctx);
            let work = observer.snapshot();
            assert_eq!(work.calls, 1);
            assert_eq!(work.prefilter_calls, 1);
            assert_eq!(work.block_rejections, u64::from(rejected));
            assert_eq!(work.matcher_constructions, u64::from(!rejected));
            assert!(!work.overflowed);
        }
    }
    Ok(())
}
