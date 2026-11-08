//! First-party embedding wire admission without changing frozen Vortex core.

use vortex::{
    editions::{
        Edition, EditionDeclaration, EditionFamily, EditionId, EditionMember,
        EditionSessionExt as _,
    },
    session::VortexSession,
};

const ID: EditionId = EditionId::new("shardloomembedding", 2026, 11, 0);
static FAMILY: EditionFamily = EditionFamily {
    name: "shardloomembedding",
    origin: "shardloom",
    doc: "ShardLoom dense Float16/Float32/Float64 embedding identity over fixed-size Vortex lists. Readers require shardloom.embedding.v1.",
};
static DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: ID,
        immutable: true,
        min_library_version: None,
    },
    added: &[EditionMember::dtype(&"shardloom.embedding.v1")],
};

pub(super) fn register(session: &VortexSession) {
    if session.editions().find(&ID).is_none() {
        session
            .editions()
            .declare_family(&FAMILY)
            .expect("constant embedding edition family");
        session
            .register_edition(&DECLARATION)
            .expect("constant embedding edition declaration");
    }
    session
        .enable_edition(ID)
        .expect("registered embedding edition");
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::{VortexSessionDefault as _, editions::ComponentKind};

    #[test]
    fn dense_embedding_edition_only_adds_its_dtype() {
        let session = VortexSession::default();
        let arrays = session.enabled_component_ids(ComponentKind::Array);
        let layouts = session.enabled_component_ids(ComponentKind::Layout);
        let aggregates = session.enabled_component_ids(ComponentKind::Aggregate);
        let mut dtypes = session.enabled_component_ids(ComponentKind::DType);
        super::super::register(&session);
        super::super::register(&session);
        session.editions().validate().unwrap();
        assert_eq!(session.enabled_component_ids(ComponentKind::Array), arrays);
        assert_eq!(
            session.enabled_component_ids(ComponentKind::Layout),
            layouts
        );
        assert_eq!(
            session.enabled_component_ids(ComponentKind::Aggregate),
            aggregates
        );
        dtypes.push("shardloom.embedding.v1".into());
        dtypes.sort_unstable();
        assert_eq!(session.enabled_component_ids(ComponentKind::DType), dtypes);
    }
}
