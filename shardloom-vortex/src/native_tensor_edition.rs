//! Separate tensor wire admission; the frozen Vortex core edition is unchanged.

use vortex::{
    editions::{
        Edition, EditionDeclaration, EditionFamily, EditionId, EditionMember,
        EditionSessionExt as _,
    },
    session::VortexSession,
};

const ID: EditionId = EditionId::new("shardloomtensor", 2026, 11, 0);
static FAMILY: EditionFamily = EditionFamily {
    name: "shardloomtensor",
    origin: "shardloom",
    doc: "Checked fixed-shape tensors over native lists. Readers require shardloom.fixed_shape_tensor.v1.",
};
static DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: ID,
        min_library_version: None,
    },
    added: &[EditionMember::dtype(&"shardloom.fixed_shape_tensor.v1")],
};
pub(super) fn register(session: &VortexSession) {
    if session.editions().find(&ID).is_none() {
        session
            .editions()
            .declare_family(&FAMILY)
            .expect("constant tensor edition family");
        session
            .register_edition(&DECLARATION)
            .expect("constant tensor edition declaration");
    }
    session
        .enable_edition(ID)
        .expect("registered tensor edition");
}
