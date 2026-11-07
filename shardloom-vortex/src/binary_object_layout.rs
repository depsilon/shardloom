//! A native table child plus a checked directory of lazy raw object segments.

use std::sync::Arc;
use vortex::{
    array::{DeserializeMetadata, SerializeMetadata, dtype::DType, memory::HostMemoryReservation},
    error::{VortexResult, vortex_ensure, vortex_err},
    layout::{
        Layout, LayoutChildType, LayoutDeserializeArgs, LayoutEncoding, LayoutId, LayoutParts,
        LayoutReaderContext, LayoutReaderRef, LayoutRef, VTable, layout_children,
        segments::{SegmentId, SegmentSource},
        session::LayoutSessionExt as _,
    },
    session::{VortexSession, registry::CachedId},
};

pub(crate) const ID: &str = "shardloom.binary-objects.v1";
const MAGIC: &[u8; 8] = b"SLBOLAY1";
pub(super) const DIRECTORY_MAGIC: &[u8; 8] = b"SLBOIDX1";

/// The layout codec has no variable-size metadata allocation. Directory bytes
/// live in an ordinary segment and are admitted by the resident owner first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Header {
    pub(super) objects: u64,
    pub(super) chunks: u64,
    pub(super) directory_bytes: u64,
    pub(super) digest: [u8; 32],
}

impl Header {
    pub(super) fn directory_size(objects: u64, chunks: u64) -> VortexResult<u64> {
        objects
            .checked_mul(48)
            .and_then(|n| chunks.checked_mul(32).and_then(|c| n.checked_add(c)))
            .and_then(|n| n.checked_add(DIRECTORY_MAGIC.len() as u64))
            .filter(|n| u32::try_from(*n).is_ok())
            .ok_or_else(|| vortex_err!("SL-BINARY-OBJECT-DIRECTORY: directory size overflows"))
    }

    fn validate(&self) -> VortexResult<()> {
        vortex_ensure!(
            self.chunks < u64::from(u32::MAX)
                && self.directory_bytes == Self::directory_size(self.objects, self.chunks)?,
            "SL-BINARY-OBJECT-DIRECTORY: invalid object directory geometry"
        );
        Ok(())
    }
}

impl SerializeMetadata for Header {
    fn serialize(self) -> Vec<u8> {
        let mut raw = Vec::with_capacity(64);
        raw.extend_from_slice(MAGIC);
        raw.extend_from_slice(&self.objects.to_le_bytes());
        raw.extend_from_slice(&self.chunks.to_le_bytes());
        raw.extend_from_slice(&self.directory_bytes.to_le_bytes());
        raw.extend_from_slice(&self.digest);
        raw
    }
}

impl DeserializeMetadata for Header {
    type Output = Self;
    fn deserialize(raw: &[u8]) -> VortexResult<Self> {
        vortex_ensure!(
            raw.len() == 64 && &raw[..8] == MAGIC,
            "SL-BINARY-OBJECT-DIRECTORY: invalid object layout version or header length"
        );
        let header = Self {
            objects: u64::from_le_bytes(raw[8..16].try_into().expect("checked width")),
            chunks: u64::from_le_bytes(raw[16..24].try_into().expect("checked width")),
            directory_bytes: u64::from_le_bytes(raw[24..32].try_into().expect("checked width")),
            digest: raw[32..64].try_into().expect("checked width"),
        };
        header.validate()?;
        Ok(header)
    }
}

#[derive(Clone, Debug)]
pub(super) struct Objects;

#[derive(Clone, Debug)]
pub(super) struct Data {
    pub(super) header: Header,
    // The layout, its segment vector and constant codec workspace share this
    // reservation through footer clones and the final reader/layout owner.
    _credit: Arc<HostMemoryReservation>,
}

pub(super) fn reserve(
    session: &VortexSession,
    chunks: usize,
) -> VortexResult<HostMemoryReservation> {
    use vortex::array::memory::HostMemorySessionExt as _;
    session
        .host_allocator()
        .reserve_external(reserved_bytes(chunks)?)
}

pub(super) fn reserved_bytes(chunks: usize) -> VortexResult<usize> {
    // Segment identifiers, provider segment specs and the serialized footer
    // can coexist. Admit their bounded metadata copies before the first write.
    let bytes = chunks
        .checked_add(1)
        .and_then(|n| n.checked_mul(256))
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| vortex_err!("SL-BINARY-OBJECT-RESOURCE: object layout size overflows"))?;
    Ok(bytes)
}

impl VTable for Objects {
    type LayoutData = Data;
    type Metadata = Header;

    fn id(&self) -> LayoutId {
        static NAME: CachedId = CachedId::new(ID);
        *NAME
    }

    fn metadata(layout: &Layout<Self>) -> Header {
        layout.header
    }

    fn deserialize(&self, args: &LayoutDeserializeArgs<'_>, header: &Header) -> VortexResult<Data> {
        header.validate()?;
        vortex_ensure!(
            args.children.nchildren() == 1,
            "SL-BINARY-OBJECT-DIRECTORY: object layout requires one table child"
        );
        vortex_ensure!(
            args.children.child_row_count(0) == args.row_count,
            "SL-BINARY-OBJECT-DIRECTORY: object layout child row count differs"
        );
        vortex_ensure!(
            crate::native_binary_type::contains(args.dtype),
            "SL-BINARY-OBJECT-DIRECTORY: object layout has no object column"
        );
        let chunks = usize::try_from(header.chunks)?;
        vortex_ensure!(
            args.segment_ids.len() == chunks + 1,
            "SL-BINARY-OBJECT-DIRECTORY: object segment count differs"
        );
        // The child adapter validates its dtype when materialized. It remains
        // lazy here; the wrapper never reads object data to scan table columns.
        Ok(Data {
            header: *header,
            _credit: Arc::new(reserve(args.session, chunks)?),
        })
    }

    fn child_dtype(layout: &Layout<Self>, slot: usize) -> VortexResult<DType> {
        vortex_ensure!(slot == 0, "object layout has only one table child");
        Ok(layout.dtype().clone())
    }

    fn child_type(_: &Layout<Self>, _: usize) -> LayoutChildType {
        LayoutChildType::Transparent("table".into())
    }

    fn new_reader(
        layout: &Layout<Self>,
        name: Arc<str>,
        segment_source: Arc<dyn SegmentSource>,
        session: &VortexSession,
        ctx: &LayoutReaderContext,
    ) -> VortexResult<LayoutReaderRef> {
        layout.children().child(0, layout.dtype())?.dyn_new_reader(
            name,
            segment_source,
            session,
            ctx,
        )
    }
}

pub(super) fn wrap(
    child: LayoutRef,
    ids: Vec<SegmentId>,
    header: Header,
    credit: HostMemoryReservation,
) -> LayoutRef {
    LayoutParts::new(
        Objects,
        child.dyn_dtype().clone(),
        child.dyn_row_count(),
        ids,
        layout_children(vec![child]),
        Data {
            header,
            _credit: Arc::new(credit),
        },
    )
    .into_layout()
}

pub(crate) fn register(session: &VortexSession) {
    session.layouts().register(&Objects as &dyn LayoutEncoding);
}
