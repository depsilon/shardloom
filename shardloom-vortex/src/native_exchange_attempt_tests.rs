// SPDX-License-Identifier: Apache-2.0
use super::*;

#[test]
#[ignore = "observes the frozen sixty-second attempt deadline under the workload supervisor"]
fn absolute_attempt_deadline_cancels_native_work_and_replay_before_fresh_retry() {
    let (_, registry, binding) = fixture();
    let connection = Connection::new();
    let attempt = registry
        .begin(Principal::Reader, &binding, &connection, false)
        .unwrap();
    let cancellation = attempt.cancellation.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(ATTEMPT_TIME + Duration::from_secs(2), attempt.cancelled())
            .await
            .unwrap();
    });
    assert!(cancellation.check().is_err());
    assert_eq!(attempt.check().unwrap_err().code, "SLX_CANCELLED");
    assert!(attempt.complete().is_err());
    drop(attempt);
    assert_eq!(registry.snapshot().unwrap().0, 0);
    assert_eq!(
        registry
            .begin(Principal::Reader, &binding, &connection, false)
            .err()
            .unwrap()
            .code,
        "SLX_REPLAY"
    );
    let fresh = registry
        .begin(
            Principal::Reader,
            &other_id(&binding, 55),
            &connection,
            false,
        )
        .unwrap();
    fresh.complete().unwrap();
    drop(fresh);
    assert_eq!(registry.snapshot().unwrap().0, 0);
}

fn fixture() -> (Arc<Authority>, Registry, Binding) {
    let authority = Arc::new(Authority::load().unwrap());
    let epoch = Bytes::from_static(&[3; 16]);
    let binding = authority
        .case("dictionary_large")
        .unwrap()
        .binding(
            "reader",
            &epoch,
            Bytes::from_static(&[4; 16]),
            1,
            SUITE_QUERY,
        )
        .unwrap();
    let registry = Registry::new(authority.clone(), epoch).unwrap();
    (authority, registry, binding)
}

fn other_id(binding: &Binding, id: u16) -> Binding {
    let mut next = binding.clone();
    let mut bytes = [0; 16];
    bytes[..2].copy_from_slice(&id.to_le_bytes());
    next.attempt_id = Bytes::copy_from_slice(&bytes);
    next
}

#[test]
fn verified_certificate_lookup_uses_only_distinct_fixed_fingerprints() {
    let fingerprints = [
        hash(b"reader cert"),
        hash(b"reader2 cert"),
        hash(b"denied cert"),
        hash(b"administrator cert"),
    ];
    let auth = Authentication::new(fingerprints.clone()).unwrap();
    for (certificate, principal) in [
        (&b"reader cert"[..], Principal::Reader),
        (&b"reader2 cert"[..], Principal::Reader2),
        (&b"denied cert"[..], Principal::Denied),
        (&b"administrator cert"[..], Principal::Administrator),
    ] {
        assert_eq!(auth.verified_leaf(certificate).unwrap(), principal);
    }
    assert_eq!(
        auth.verified_leaf(b"reader").unwrap_err().code,
        "SLX_UNAUTHENTICATED"
    );
    let mut duplicate = fingerprints.clone();
    duplicate[1] = duplicate[0].clone();
    assert!(Authentication::new(duplicate).is_err());
    let mut truncated = fingerprints;
    truncated[2] = Bytes::new();
    assert!(Authentication::new(truncated).is_err());
}

#[test]
fn every_request_rechecks_principal_source_operation_result_and_cancellation_rights() {
    let (_, registry, binding) = fixture();
    let connection = Connection::new();
    for principal in [
        Principal::Denied,
        Principal::Administrator,
        Principal::Reader2,
    ] {
        assert!(
            registry
                .begin(principal, &binding, &connection, false)
                .is_err()
        );
    }
    let mut changed = binding.clone();
    changed.source_id = "not_authorized".into();
    assert!(
        registry
            .begin(Principal::Reader, &changed, &connection, false)
            .is_err()
    );
    changed = binding.clone();
    changed.query_id = "not_authorized".into();
    assert!(
        registry
            .begin(Principal::Reader, &changed, &connection, false)
            .is_err()
    );
    let active = registry
        .begin(Principal::Reader, &binding, &connection, false)
        .unwrap();
    for principal in [
        Principal::Denied,
        Principal::Administrator,
        Principal::Reader2,
    ] {
        assert!(registry.cancel(principal, &binding).is_err());
        assert!(!active.cancellation.is_cancelled());
    }
    registry.cancel(Principal::Reader, &binding).unwrap();
    assert!(active.check().is_err());
    assert!(active.complete().is_err());
    drop(active);
    assert_eq!(registry.snapshot().unwrap(), (0, 1, 1));
    let ticket = registry.issue_sql(Principal::Reader).unwrap();
    assert!(
        registry
            .begin(Principal::Reader2, &ticket, &connection, true)
            .is_err()
    );
    let mut stolen = ticket.clone();
    stolen.principal = "reader2".into();
    assert!(
        registry
            .begin(Principal::Reader2, &stolen, &connection, true)
            .is_err()
    );
    assert!(registry.cancel(Principal::Reader2, &stolen).is_err());
    let response = registry
        .begin(Principal::Reader, &ticket, &connection, true)
        .unwrap();
    response.complete().unwrap();
    drop(response);
    assert!(
        registry
            .begin(Principal::Reader, &ticket, &connection, true)
            .is_err()
    );
    for principal in [Principal::Denied, Principal::Administrator] {
        assert!(registry.issue_sql(principal).is_err());
    }
    let unused = registry.issue_sql(Principal::Reader2).unwrap();
    registry.cancel(Principal::Reader2, &unused).unwrap();
    assert!(
        registry
            .begin(Principal::Reader2, &unused, &connection, true)
            .is_err()
    );
}

#[test]
fn data_permits_outlive_handler_completion_and_cancellation_until_last_owner() {
    let (_, registry, binding) = fixture();
    let connection = Connection::new();
    let first = registry
        .begin(Principal::Reader, &binding, &connection, false)
        .unwrap();
    let downstream_owner = first.clone();
    first.complete().unwrap();
    drop(first);
    let next = other_id(&binding, 2);
    assert!(
        registry
            .begin(Principal::Reader, &next, &connection, false)
            .is_err()
    );
    let second_connection = Connection::new();
    let second = registry
        .begin(Principal::Reader, &next, &second_connection, false)
        .unwrap();
    let third = other_id(&binding, 3);
    let third_connection = Connection::new();
    assert!(
        registry
            .begin(Principal::Reader, &third, &third_connection, false)
            .is_err()
    );
    registry.cancel(Principal::Reader, &next).unwrap();
    assert_eq!(registry.snapshot().unwrap().0, 2);
    drop(second);
    let third = registry
        .begin(Principal::Reader, &third, &third_connection, false)
        .unwrap();
    drop(downstream_owner);
    assert!(
        registry
            .begin(Principal::Reader, &binding, &connection, false)
            .is_err()
    );
    drop(third);
    assert_eq!(registry.snapshot().unwrap().0, 0);
    let fresh = registry
        .begin(
            Principal::Reader,
            &other_id(&binding, 5),
            &connection,
            false,
        )
        .unwrap();
    assert!(fresh.check().is_ok());
    drop(fresh);
    assert_eq!(registry.snapshot().unwrap().0, 0);
}

#[test]
fn policy_replacement_revokes_active_and_unused_tickets_on_reused_connections() {
    let (authority, registry, binding) = fixture();
    let connection = Connection::new();
    let active = registry
        .begin(Principal::Reader, &binding, &connection, false)
        .unwrap();
    let unused = registry.issue_sql(Principal::Reader2).unwrap();
    let replace = ReplacePolicy {
        server_epoch: binding.server_epoch.clone(),
        expected_version: 1,
        new_version: 2,
    };
    for principal in [Principal::Reader, Principal::Reader2, Principal::Denied] {
        assert!(registry.replace_policy(principal, &replace).is_err());
    }
    let mut wrong = replace.clone();
    wrong.new_version = 3;
    assert!(
        registry
            .replace_policy(Principal::Administrator, &wrong)
            .is_err()
    );
    registry
        .replace_policy(Principal::Administrator, &replace)
        .unwrap();
    assert!(active.cancellation.is_cancelled());
    assert!(active.complete().is_err());
    assert!(
        registry
            .begin(Principal::Reader2, &unused, &Connection::new(), true)
            .is_err()
    );
    drop(active);
    let mut stale = other_id(&binding, 7);
    assert!(
        registry
            .begin(Principal::Reader, &stale, &connection, false)
            .is_err()
    );
    stale.policy_version = 2;
    let current = registry
        .begin(Principal::Reader, &stale, &connection, false)
        .unwrap();
    current.complete().unwrap();
    drop(current);
    assert!(
        registry
            .replace_policy(Principal::Administrator, &replace)
            .is_err()
    );
    let restarted = Registry::new(authority, Bytes::from_static(&[9; 16])).unwrap();
    assert!(
        restarted
            .begin(Principal::Reader, &binding, &Connection::new(), false)
            .is_err()
    );
    assert!(
        restarted
            .begin(Principal::Reader2, &unused, &Connection::new(), true)
            .is_err()
    );
}

#[test]
fn failed_completed_and_issued_ids_are_bounded_without_replay_eviction() {
    let (_, registry, binding) = fixture();
    let connection = Connection::new();
    for id in 1..=u16::try_from(MAX_ATTEMPTS).unwrap() {
        let attempt = registry
            .begin(
                Principal::Reader,
                &other_id(&binding, id),
                &connection,
                false,
            )
            .unwrap();
        if id % 2 == 0 {
            attempt.complete().unwrap();
        }
        drop(attempt);
    }
    assert_eq!(registry.snapshot().unwrap(), (0, MAX_ATTEMPTS, 1));
    for id in [1, 2, 257] {
        assert!(
            registry
                .begin(
                    Principal::Reader,
                    &other_id(&binding, id),
                    &connection,
                    false
                )
                .is_err()
        );
    }
    assert!(registry.issue_sql(Principal::Reader).is_err());
}

#[test]
fn cancellation_wakes_all_registered_waiters_and_is_visible_to_native_work() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Attempt>();
    assert_send_sync::<CancellationToken>();
    let (_, registry, binding) = fixture();
    let active = registry
        .begin(Principal::Reader, &binding, &Connection::new(), false)
        .unwrap();
    let native = active.cancellation.clone();
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            let mut handles = Vec::new();
            for _ in 0..2 {
                let active = active.clone();
                handles.push(tokio::spawn(async move {
                    active.cancelled().await;
                }));
            }
            tokio::task::yield_now().await;
            registry.cancel(Principal::Reader, &binding).unwrap();
            for handle in handles {
                tokio::time::timeout(Duration::from_secs(1), handle)
                    .await
                    .unwrap()
                    .unwrap();
            }
            // Cancellation before registration cannot lose a wakeup either.
            tokio::time::timeout(Duration::from_secs(1), active.cancelled())
                .await
                .unwrap();
        });
    assert!(native.check().is_err());
    drop(active);
    assert_eq!(registry.snapshot().unwrap().0, 0);
}
