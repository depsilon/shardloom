//! Two payload identities; core array/layout editions remain unchanged.

use vortex::{
    editions::{
        Edition, EditionDeclaration, EditionFamily, EditionId, EditionMember,
        EditionSessionExt as _,
    },
    session::VortexSession,
};
const ID: EditionId = EditionId::new("shardloomvector", 2026, 11, 0);
static FAMILY: EditionFamily = EditionFamily {
    name: "shardloomvector",
    origin: "shardloom",
    doc: "Versioned sparse and affine-quantized payloads over native structs and lists.",
};
static DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: ID,
        min_library_version: None,
    },
    added: &[
        EditionMember::dtype(&"shardloom.sparse_vector.v1"),
        EditionMember::dtype(&"shardloom.quantized_vector.v1"),
    ],
};
pub(super) fn register(session: &VortexSession) {
    if session.editions().find(&ID).is_none() {
        session
            .editions()
            .declare_family(&FAMILY)
            .expect("constant vector edition family");
        session
            .register_edition(&DECLARATION)
            .expect("constant vector edition declaration");
    }
    session
        .enable_edition(ID)
        .expect("registered vector edition");
}
