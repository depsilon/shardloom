//! First-party wire admission for binary descriptors and owned native segments.

use vortex::{
    editions::{
        Edition, EditionDeclaration, EditionFamily, EditionId, EditionMember,
        EditionSessionExt as _,
    },
    session::VortexSession,
};

const ID: EditionId = EditionId::new("shardloombinary", 2026, 11, 0);
static FAMILY: EditionFamily = EditionFamily {
    name: "shardloombinary",
    origin: "shardloom",
    doc: "ShardLoom binary-object descriptors and lazy owned payload segments in one native file.",
};
static DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: ID,
        immutable: true,
        min_library_version: None,
    },
    added: &[
        EditionMember::dtype(&"shardloom.binary-object.v1"),
        EditionMember::layout(&"shardloom.binary-objects.v1"),
    ],
};

pub(super) fn register(session: &VortexSession) {
    if session.editions().find(&ID).is_none() {
        session
            .editions()
            .declare_family(&FAMILY)
            .expect("constant binary-object edition family");
        session
            .register_edition(&DECLARATION)
            .expect("constant binary-object edition declaration");
    }
    session
        .enable_edition(ID)
        .expect("registered binary-object edition");
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::{VortexSessionDefault as _, editions::ComponentKind};

    #[test]
    fn binary_object_edition_only_adds_its_dtype_and_layout() {
        let session = VortexSession::default();
        let arrays = session.enabled_component_ids(ComponentKind::Array);
        let aggregates = session.enabled_component_ids(ComponentKind::Aggregate);
        let mut dtypes = session.enabled_component_ids(ComponentKind::DType);
        let mut layouts = session.enabled_component_ids(ComponentKind::Layout);
        super::super::register(&session);
        super::super::register(&session);
        session.editions().validate().unwrap();
        assert_eq!(session.enabled_component_ids(ComponentKind::Array), arrays);
        assert_eq!(
            session.enabled_component_ids(ComponentKind::Aggregate),
            aggregates
        );
        dtypes.push("shardloom.binary-object.v1".into());
        layouts.push("shardloom.binary-objects.v1".into());
        dtypes.sort_unstable();
        layouts.sort_unstable();
        assert_eq!(session.enabled_component_ids(ComponentKind::DType), dtypes);
        assert_eq!(
            session.enabled_component_ids(ComponentKind::Layout),
            layouts
        );
    }
}
