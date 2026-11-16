//! Geometry dtype edition; frozen upstream array/layout editions stay unchanged.

use vortex::{
    editions::{
        Edition, EditionDeclaration, EditionFamily, EditionId, EditionMember,
        EditionSessionExt as _,
    },
    session::VortexSession,
};
const ID: EditionId = EditionId::new("shardloomspatial", 2026, 11, 0);
static FAMILY: EditionFamily = EditionFamily {
    name: "shardloomspatial",
    origin: "shardloom",
    doc: "GeoArrow 2D Point, LineString and Polygon over native coordinates and offsets.",
};
static DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: ID,
        immutable: true,
        min_library_version: None,
    },
    added: &[
        EditionMember::dtype(&"geoarrow.point"),
        EditionMember::dtype(&"geoarrow.linestring"),
        EditionMember::dtype(&"geoarrow.polygon"),
    ],
};
pub(super) fn register(session: &VortexSession) {
    if session.editions().find(&ID).is_none() {
        session
            .editions()
            .declare_family(&FAMILY)
            .expect("constant spatial edition family");
        session
            .register_edition(&DECLARATION)
            .expect("constant spatial edition declaration");
    }
    session
        .enable_edition(ID)
        .expect("registered spatial edition");
}
