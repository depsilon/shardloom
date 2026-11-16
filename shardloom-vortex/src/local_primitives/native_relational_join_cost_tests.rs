use super::*;

#[test]
fn join_cost_nested_and_reentrant_consumers_are_disjoint() {
    let mut state = State::default();
    state.report.node_count = 2;
    let parent_input = Frame {
        node: Some(0),
        phase: Phase::ProbeInput,
    };
    let child_probe = Frame {
        node: Some(1),
        phase: Phase::ProbeMatch,
    };
    let child_payload = Frame {
        node: Some(1),
        phase: Phase::Payload,
    };
    let child_consumer = Frame {
        node: Some(1),
        phase: Phase::Downstream,
    };
    let parent_probe = Frame {
        node: Some(0),
        phase: Phase::ProbeMatch,
    };
    let parent_payload = Frame {
        node: Some(0),
        phase: Phase::Payload,
    };
    let sink = Frame {
        node: None,
        phase: Phase::SinkExcluded,
    };
    for (frame, time) in [
        (parent_input, 0),
        (child_probe, 10),
        (child_payload, 20),
        (child_consumer, 25),
        (parent_probe, 30),
        (parent_payload, 40),
        (sink, 45),
    ] {
        state.enter(frame, time);
    }
    for (frame, time) in [
        (sink, 55),
        (parent_payload, 60),
        (parent_probe, 70),
        (child_consumer, 80),
        (child_payload, 85),
        (child_probe, 90),
        (parent_input, 100),
    ] {
        state.leave(frame, time);
    }
    let parent = &state.report.nodes[0];
    let child = &state.report.nodes[1];
    assert_eq!(parent.nanos[Phase::ProbeInput as usize], 20);
    assert_eq!(parent.nanos[Phase::ProbeMatch as usize], 20);
    assert_eq!(parent.nanos[Phase::Payload as usize], 10);
    assert_eq!(child.nanos[Phase::ProbeMatch as usize], 15);
    assert_eq!(child.nanos[Phase::Payload as usize], 10);
    assert_eq!(child.nanos[Phase::Downstream as usize], 15);
    assert_eq!(
        state.report.excluded_nanos[Phase::SinkExcluded as usize],
        10
    );
    assert_eq!(
        parent.nanos.iter().sum::<u128>() + child.nanos.iter().sum::<u128>(),
        90
    );
    assert_eq!(state.depth, 0);
}

#[test]
fn join_cost_idle_stale_node_and_scope_do_not_enter_a_later_session() {
    let inactive = node("inner", 1, false, false);
    drop(inactive.scope(Phase::Setup));
    drop(scope(Phase::Payload));
    let first = Session::start();
    let stale_node = node("inner", 1, false, false);
    let stale_scope = stale_node.scope(Phase::BuildInput);
    drop(first);
    let second = Session::start();
    let current = node("left", 2, false, true);
    let current_scope = current.scope(Phase::ProbeMatch);
    drop(stale_scope);
    drop(stale_node.scope(Phase::BuildState));
    stale_node.input(true, 999);
    inactive.input(false, 999);
    drop(current_scope);
    let report = second.finish();
    assert_eq!(report.node_count, 1);
    assert_eq!(report.nodes[0].calls.iter().sum::<u64>(), 1);
    assert_eq!(report.nodes[0].build_rows + report.nodes[0].probe_rows, 0);
}

#[test]
fn join_cost_key_preparation_only_belongs_to_build_or_probe_work() {
    let session = Session::start();
    let owner = node("left_semi", 1, true, false);
    drop(key_preparation());
    for phase in [
        Phase::BuildInput,
        Phase::BuildState,
        Phase::ProbeMatch,
        Phase::Payload,
    ] {
        let outer = owner.scope(phase);
        drop(key_preparation());
        drop(outer);
    }
    let report = session.finish();
    assert_eq!(report.nodes[0].calls[Phase::BuildKeys as usize], 1);
    assert_eq!(report.nodes[0].calls[Phase::ProbeKeys as usize], 1);
    assert_eq!(report.nodes[0].calls.iter().sum::<u64>(), 6);
}

#[test]
fn join_cost_error_and_panic_unwinding_release_all_scopes() {
    fn fail(owner: Node) -> Result<(), ()> {
        let _outer = owner.scope(Phase::BuildState);
        let _inner = key_preparation();
        Err(())
    }
    let session = Session::start();
    let owner = node("inner", 1, false, false);
    assert!(fail(owner).is_err());
    let panic = std::panic::catch_unwind(|| {
        let _probe = owner.scope(Phase::ProbeMatch);
        let _payload = scope(Phase::Payload);
        panic!("consumer failure control");
    });
    assert!(panic.is_err());
    assert_eq!(session.finish().nodes[0].calls.iter().sum::<u64>(), 4);
}

#[test]
fn join_cost_parent_metadata_batch_rows_and_strategy_exclusions_are_explicit() {
    let session = Session::start();
    let parent = node("full", 2, false, true);
    let parent_scope = parent.scope(Phase::BuildInput);
    let child = node("left_anti", 1, true, false);
    let child_scope = child.scope(Phase::BuildState);
    child.input(true, 31);
    child.input(true, 0);
    child.input(false, 7);
    output(5, 2, 3);
    drop(child_scope);
    for phase in [
        Phase::OrderedExcluded,
        Phase::AsOfExcluded,
        Phase::CyclicExcluded,
    ] {
        let excluded = scope(phase);
        drop(scope(Phase::Payload));
        output(999, 3, 3);
        drop(excluded);
    }
    drop(parent_scope);
    let report = session.finish();
    let child = &report.nodes[1];
    assert_eq!(report.node_count, 2);
    assert_eq!(child.parent, Some(0));
    assert_eq!(child.kind, "left_anti");
    assert_eq!(child.key_columns, 1);
    assert!(child.membership_only && !child.conditional);
    assert_eq!((child.build_batches, child.build_rows), (2, 31));
    assert_eq!((child.probe_batches, child.probe_rows), (1, 7));
    assert_eq!((child.output_batches, child.output_rows), (1, 5));
    assert_eq!((child.output_cells, child.validation_only_cells), (10, 15));
    assert_eq!(report.nodes[0].output_rows, 0);
    assert_eq!(report.excluded_calls.iter().sum::<u64>(), 3);
    assert_eq!(report.nodes[0].calls[Phase::Payload as usize], 0);
}

#[test]
fn join_cost_payload_work_excludes_sources_keys_and_consumers() {
    direct_numeric_gather(999, 9);
    let session = Session::start();
    let owner = node("inner", 1, false, false);
    for phase in [
        Phase::BuildInput,
        Phase::BuildState,
        Phase::ProbeMatch,
        Phase::Downstream,
    ] {
        let scope = owner.scope(phase);
        gathered_coordinates(999, 9);
        direct_numeric_gather(999, 9);
        selected_numeric_copy(999);
        leaf_copy(999, true);
        dictionary_copy(999, 9);
        drop(scope);
    }
    let scope = owner.scope(Phase::Payload);
    gathered_coordinates(10, 3);
    direct_numeric_gather(10, 1);
    direct_numeric_gather(7, 3);
    selected_numeric_copy(11);
    leaf_copy(13, true);
    leaf_copy(17, false);
    dictionary_copy(19, 2);
    drop(scope);
    let report = session.finish();
    let work = report.payload_work[0];
    assert_eq!(
        (
            work.coordinate_batches,
            work.coordinate_rows,
            work.coordinate_chunks
        ),
        (1, 10, 3)
    );
    assert_eq!(
        (
            work.direct_numeric_batches,
            work.direct_numeric_rows,
            work.direct_numeric_chunks
        ),
        (2, 17, 4)
    );
    assert_eq!(work.direct_numeric_single_chunk_rows, 10);
    assert_eq!(
        (
            work.selected_numeric_copy_batches,
            work.selected_numeric_copy_rows
        ),
        (1, 11)
    );
    assert_eq!((work.variable_copy_rows, work.scalar_copy_rows), (13, 17));
    assert_eq!(
        (work.dictionary_copy_rows, work.dictionary_copy_values),
        (19, 2)
    );
}

#[test]
fn join_cost_fixed_node_capacity_and_thread_isolation_are_enforced() {
    let session = Session::start();
    for _ in 0..MAX_NODES {
        let _ = node("inner", 1, false, false);
    }
    assert!(std::panic::catch_unwind(|| node("inner", 1, false, false)).is_err());
    std::thread::spawn(|| {
        let other = node("inner", 1, false, false);
        drop(other.scope(Phase::BuildState));
    })
    .join()
    .unwrap();
    let report = session.finish();
    assert_eq!(report.node_count, MAX_NODES);
    assert!(
        report
            .nodes
            .iter()
            .all(|node| node.calls == [0; PHASES.len()])
    );
}
