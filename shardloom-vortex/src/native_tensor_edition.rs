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
    doc: "Checked tensors over native lists and structs. Readers require the corresponding ShardLoom tensor dtype.",
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

const VARIABLE_ID: EditionId = EditionId::new("shardloomtensor", 2026, 11, 1);
static VARIABLE_DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: VARIABLE_ID,
        min_library_version: None,
    },
    added: &[EditionMember::dtype(&"shardloom.variable_shape_tensor.v1")],
};

pub(crate) fn register_variable(session: &VortexSession) {
    register(session);
    if session.editions().find(&VARIABLE_ID).is_none() {
        session
            .register_edition(&VARIABLE_DECLARATION)
            .expect("constant variable tensor edition");
    }
    session
        .enable_edition(VARIABLE_ID)
        .expect("registered variable tensor edition");
}
