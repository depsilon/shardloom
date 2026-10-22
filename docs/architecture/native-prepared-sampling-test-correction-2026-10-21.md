<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared sampling: cyclic identity-projection test correction

The complete release native suite at `95fbca52` reports 3,089 passing library
tests, one failure and 31 existing manual/opt-in ignores. It stops before the
example test executions and is not a successful all-targets acceptance run.
The failed guard receipt and complete log are preserved as
`prepared-sampling-integration-861-r3-native-all-a1` in the local evidence
workspace. Both complete frozen sampling comparisons remain unchanged.

The failing test,
`cyclic_candidate_keeps_small_anti_filtered_and_transformed_plans_ordinary`,
labels a full ordered `x -> x, y -> y, id -> id` Project as transformed and
expects zero cyclic-strategy executions. The
[shared identity-projection normalization](native-prepared-sampling-integration-2026-10-21.md)
removes exactly that Project after complete binding and validation. Its input
then satisfies the existing cyclic strategy's pure-memory-scan admission.
Complete result and schema checks pass before the execution-count assertion
reports one execution instead of zero. The separate filtered-scan case passes.

Correct the test contract without changing production source: the exact
identity case must admit the cyclic strategy; a computed `id + 0` and an
explicit field reorder must retain their Project and use ordinary execution.
Keep the small-input, anti-join and filtered-scan refusal cases. Check complete
values and schema for every case, and zero credits after dropping each prepared
owner. The renamed test is
`cyclic_candidate_admits_identity_but_keeps_real_transformations_ordinary`.

This is a test-only correction. Final acceptance must bind the corrected source
to the scored source, verify that every production/compiler input other than
this test module is byte-identical, and pass the corrected test and complete
native suite. Original measured binaries, protocols, samples, scores and failed
observations keep their original identities. No numerical gate, candidate
eligibility rule or result is changed, and no benchmark rerun is justified by
this test correction alone. Remaining public, package, Full43 and format gates
still apply before retention.
