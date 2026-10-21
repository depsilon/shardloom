// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "native-encoded-exchange-experiment", unix))]

#[path = "native_exchange_support/mod.rs"]
mod support;

#[test]
fn negotiation_identity_and_strict_native_refusal() {
    support::protocol_suite(
        "negotiation_identity_and_strict_native_refusal",
        &[
            ("native_exchange_protocol::tests::", 7),
            ("native_exchange_protocol::arrow::tests::", 4),
            ("native_exchange_protocol::authority::tests::", 1),
            (
                "native_exchange_protocol::service::tests::faults::actual_tls_rejects_the_complete_open_identity_and_wire_mismatch_matrix",
                1,
            ),
        ],
    );
}

#[test]
fn mutual_tls_per_request_authorization_and_isolation() {
    support::protocol_suite(
        "mutual_tls_per_request_authorization_and_isolation",
        &[
            ("native_exchange_protocol::transport::tests::", 2),
            ("native_exchange_protocol::service::tests::sql::", 1),
        ],
    );
}

#[test]
fn framing_limits_cancellation_replay_and_cleanup() {
    support::protocol_suite(
        "framing_limits_cancellation_replay_and_cleanup",
        &[
            ("native_exchange_protocol::attempts::tests::", 7),
            ("native_exchange_protocol::queue::tests::", 4),
            (
                "native_exchange_protocol::payload::tests::private_receiving_refusals_release_buffers_before_retry_and_never_expose_partial_sources",
                1,
            ),
            (
                "resident_memory_source::exchange_bridge::tests::bridge_grant_denial_and_cloned_parts_retain_allocation_owners",
                1,
            ),
            (
                "native_exchange_protocol::service::tests::actual_data_requests_refuse_bad_open_extra_input_and_replay_then_allow_fresh_retry",
                1,
            ),
            (
                "native_exchange_protocol::service::tests::slow_socket_consumer_is_closed_and_all_native_and_queue_owners_drain",
                1,
            ),
            (
                "native_exchange_protocol::service::tests::faults::active_data_cancellation",
                1,
            ),
            (
                "native_exchange_protocol::service::tests::faults::rpc_deadline",
                1,
            ),
            (
                "native_exchange_protocol::service::tests::faults::real_rpc_sender",
                1,
            ),
            (
                "native_exchange_protocol::service::tests::response_faults::",
                1,
            ),
            ("resident_memory_source::ipc_intake::corruption_tests::", 6),
        ],
    );
    support::failure_workload(
        "framing_limits_cancellation_replay_and_cleanup-process-failures",
        4,
    );
}
