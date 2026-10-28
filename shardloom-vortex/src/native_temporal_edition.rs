//! First-party duration wire admission, separate from Vortex's frozen core.

use vortex::{
    editions::{
        Edition, EditionDeclaration, EditionFamily, EditionId, EditionMember,
        EditionSessionExt as _,
    },
    session::VortexSession,
};

const ID: EditionId = EditionId::new("shardloomtemporal", 2026, 10, 0);
static FAMILY: EditionFamily = EditionFamily {
    name: "shardloomtemporal",
    origin: "shardloom",
    doc: "ShardLoom fixed-duration extension metadata over native Vortex integer storage. Readers require the registered shardloom.duration.v1 extension.",
};
static DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: ID,
        min_library_version: None,
    },
    added: &[EditionMember::dtype(&"shardloom.duration.v1")],
};

pub(super) fn register(session: &VortexSession) {
    if session.editions().find(&ID).is_none() {
        session
            .editions()
            .declare_family(&FAMILY)
            .expect("constant temporal edition family");
        session
            .register_edition(&DECLARATION)
            .expect("constant temporal edition declaration");
    }
    // The native-temporal feature explicitly admits only this first-party dtype.
    // Core array/layout/aggregate membership and optional provider codecs stay unchanged.
    session
        .enable_edition(ID)
        .expect("registered temporal edition");
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::VortexSessionDefault as _;
    use vortex::editions::{CORE_2026_08_3, ComponentKind};

    #[test]
    fn temporal_edition_only_adds_duration_and_preserves_frozen_core() {
        let session = VortexSession::default();
        let arrays = session.enabled_component_ids(ComponentKind::Array);
        let layouts = session.enabled_component_ids(ComponentKind::Layout);
        let aggregates = session.enabled_component_ids(ComponentKind::Aggregate);
        let mut dtypes = session.enabled_component_ids(ComponentKind::DType);
        super::super::register(&session);
        super::super::register(&session);
        ID.validate().unwrap();
        session.editions().validate().unwrap();
        assert!(
            session
                .enabled_editions()
                .editions()
                .contains(&CORE_2026_08_3)
        );
        assert!(!session.editions().find(&CORE_2026_08_3).unwrap().is_draft());
        assert!(session.editions().find(&ID).unwrap().is_draft());
        assert_eq!(session.enabled_component_ids(ComponentKind::Array), arrays);
        assert_eq!(
            session.enabled_component_ids(ComponentKind::Layout),
            layouts
        );
        assert_eq!(
            session.enabled_component_ids(ComponentKind::Aggregate),
            aggregates
        );
        dtypes.push("shardloom.duration.v1".into());
        dtypes.sort_unstable();
        assert_eq!(session.enabled_component_ids(ComponentKind::DType), dtypes);
    }
}
