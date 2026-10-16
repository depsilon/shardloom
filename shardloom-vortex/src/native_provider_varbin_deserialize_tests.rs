// SPDX-License-Identifier: Apache-2.0
//! Host validation and opaque-device handle ownership at native deserialization.

use super::*;
use std::mem::size_of;
use vortex::{
    VortexSessionDefault as _,
    array::{
        VortexSessionExecute as _,
        arrays::{BoolArray, varbinview::BinaryView},
        buffer::DeviceBuffer,
        dtype::Nullability,
        memory::MemorySessionExt as _,
    },
    buffer::{Buffer, ByteBuffer},
};

fn session(memory: &LiveMemoryPool) -> VortexSession {
    VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ))
}

#[test]
fn native_varbin_deserialize_host_table_exact_grant_and_one_byte_short() {
    let raw = VarBinViewArray::from_iter_str(["long outlined native value", "tiny"]);
    let buffers = raw.as_ref().buffer_handles();
    let peak = size_of::<ByteBuffer>().max(2 * size_of::<BufferHandle>()) as u64;
    for grant in [peak - 1, peak] {
        let memory = LiveMemoryPool::new(grant).unwrap();
        let session = session(&memory);
        let result = VarBinView.deserialize(
            raw.dtype(),
            raw.len(),
            &[],
            &buffers,
            &Vec::<ArrayRef>::new(),
            &session,
        );
        if grant == peak {
            let output = VarBinViewArray::try_from_parts(result.unwrap()).unwrap();
            assert_eq!(output.bytes_at(0), raw.bytes_at(0));
            assert!(!Arc::ptr_eq(output.data_buffers(), raw.data_buffers()));
            assert_eq!(
                memory.snapshot().reserved_bytes,
                size_of::<BufferHandle>() as u64
            );
            assert_eq!(memory.snapshot().peak_reserved_bytes, peak);
            drop(session);
            assert_eq!(
                memory.snapshot().reserved_bytes,
                size_of::<BufferHandle>() as u64
            );
            drop(output);
        } else {
            assert!(crate::owned_buffers::is_owned_reservation_denial(
                &result.err().unwrap()
            ));
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_varbin_deserialize_rejects_malformed_shape_and_host_views() {
    let raw = VarBinViewArray::from_iter_str(["long outlined native value", "tiny"]);
    let buffers = raw.as_ref().buffer_handles();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let children = Vec::<ArrayRef>::new();
    for (len, metadata, input) in [
        (raw.len(), &[1u8][..], &buffers[..]),
        (1, &[][..], &buffers[..]),
        (usize::MAX, &[][..], &buffers[..]),
        (raw.len(), &[][..], &[][..]),
    ] {
        assert!(
            VarBinView
                .deserialize(raw.dtype(), len, metadata, input, &children, &session)
                .is_err()
        );
        assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    }
    for view in [
        BinaryView::new_ref(13, *b"long", 1, 0),
        BinaryView::new_ref(13, *b"long", 0, u32::MAX),
        BinaryView::new_ref(13, *b"nope", 0, 0),
        BinaryView::make_view(&[0xff], 0, 0),
    ] {
        let views: Buffer<BinaryView> = [view].into_iter().collect();
        let input = [
            buffers[0].clone(),
            BufferHandle::new_host(views.into_byte_buffer()),
        ];
        assert!(
            VarBinView
                .deserialize(raw.dtype(), 1, &[], &input, &children, &session)
                .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    // A longer validity must be rejected before null-view normalization indexes
    // into the shorter view buffer; neither direction may panic or publish data.
    for validity_len in [1, 3] {
        let validity =
            Validity::Array(BoolArray::from_iter(vec![false; validity_len]).into_array());
        let result = VarBinViewArray::try_new_from_buffers(
            Buffer::from_iter(raw.views().iter().copied()),
            &[raw.data_buffers()[0].as_host().clone()],
            raw.dtype().as_nullable(),
            validity,
            &mut session.create_execution_ctx(),
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("validity length differs")
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UnreadableDevice(usize);

impl DeviceBuffer for UnreadableDevice {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn len(&self) -> usize {
        self.0
    }
    fn alignment(&self) -> Alignment {
        Alignment::new(256)
    }
    fn copy_to_host_sync(&self, _alignment: Alignment) -> VortexResult<ByteBuffer> {
        panic!("buffer-table construction must not copy device data")
    }
    fn copy_to_host(
        &self,
        _alignment: Alignment,
    ) -> VortexResult<futures::future::BoxFuture<'static, VortexResult<ByteBuffer>>> {
        panic!("buffer-table construction must not schedule a device copy")
    }
    fn slice(&self, range: std::ops::Range<usize>) -> Arc<dyn DeviceBuffer> {
        assert!(range.start <= range.end && range.end <= self.0);
        Arc::new(Self(range.end - range.start))
    }
    fn aligned(self: Arc<Self>, alignment: Alignment) -> VortexResult<Arc<dyn DeviceBuffer>> {
        vortex_ensure!(*alignment <= 256, "test device alignment exceeded");
        Ok(self)
    }
}

#[test]
fn native_varbin_deserialize_device_handles_are_admitted_without_device_access() {
    let table = size_of::<BufferHandle>() as u64;
    let memory = LiveMemoryPool::new(2 * table).unwrap();
    let session = session(&memory);
    let buffers = [
        BufferHandle::new_device(Arc::new(UnreadableDevice(28))),
        BufferHandle::new_device(Arc::new(UnreadableDevice(32))),
    ];
    let dtype = DType::Utf8(Nullability::NonNullable);
    let output = VarBinViewArray::try_from_parts(
        VarBinView
            .deserialize(&dtype, 2, &[], &buffers, &Vec::<ArrayRef>::new(), &session)
            .unwrap(),
    )
    .unwrap();
    assert!(output.views_handle().is_on_device());
    assert!(output.data_buffers()[0].is_on_device());
    assert!(Arc::ptr_eq(
        output.data_buffers()[0].as_device(),
        buffers[0].as_device()
    ));
    assert_eq!(output.metadata_capacity_bytes() as u64, table);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 2 * table);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, table);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
