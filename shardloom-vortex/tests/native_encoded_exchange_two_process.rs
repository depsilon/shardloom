// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "native-encoded-exchange-experiment", unix))]

#[path = "native_exchange_support/mod.rs"]
mod support;

#[test]
fn received_native_values_execute_after_sender_exit() {
    support::workload(
        "received_native_values_execute_after_sender_exit",
        "native",
        13,
    );
}

#[test]
fn matched_arrow_flight_and_vortex_ipc_controls() {
    support::workload(
        "matched_arrow_flight_and_vortex_ipc_controls",
        "controls",
        52,
    );
}
