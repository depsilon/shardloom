<!-- SPDX-License-Identifier: Apache-2.0 -->

# FSST publication source binding

This additive receipt preserves the final relocated replay of the
[closed FSST packet](../native-fsst-consumer-2026-11-15/README.md), all 15 passing
API-reference tests and passing contribution-governance check. It binds the final
generated reference refresh to the measured source and already-verified docs.
Only the recorded Cargo.lock identity and derived application identity change
in the generated references; the measured runtime and original packet are
byte-identical. No query, build or performance observation is repeated.

`manifest.json` gives exact identities for every file here. The replay receipt
binds the original packet manifest, which binds every packet file. The final
source record compares all captured source files and all native artifacts with
the measured candidate. Existing guards record unchanged sources and drained
process groups. This is local evidence and implies no merge or release.

The first preservation attempt stopped before committing on whitespace in two
immutable captured files: blank-line excerpt labels and a saved failed-replay
log. The original bytes, failure receipt, output and initial manifest remain
preserved. A second preservation stop caught the same whitespace copied into the
saved first-failure log. Both stops are preserved. The final check enumerates
all observations in those three captured files and checks every other staged
file. Captured evidence bytes remain unchanged.
