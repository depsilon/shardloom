// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "native-encoded-exchange-experiment", unix))]

#[path = "native_exchange_support/mod.rs"]
mod support;

#[test]
fn ordinary_flight_sql_and_explicit_native_selection() {
    support::workload(
        "ordinary_flight_sql_and_explicit_native_selection",
        "interop",
        2,
    );
}
