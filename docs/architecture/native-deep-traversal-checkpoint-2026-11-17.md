<!-- SPDX-License-Identifier: Apache-2.0 -->

# Deep-traversal PR checkpoint

## Maintainer decision

The November 17 maintainer request stops further implementation expansion and
asks for a reviewable PR checkpoint. Remaining resource-admitted deep traversal
resumes **after Foundry integration**. The
[current phase checklist](phased-execution-plan.md#current-execution) owns that
order. Item 19, item 20 and CG-1 through CG-23 retain their independent status.

## Value and present limits

This work replaces selected recursive native and Arrow traversals with ordered
continuations, makes independently retained owners safe to release on small
stacks, and preserves original serialization, errors and callback order. It also
adds explicit memory admission to selected native construction and traversal
paths. The tests expose and prevent stack growth, incomplete cleanup and
changed-error regressions in those internal paths.

The public depth-24 payload limit and other existing depth guards remain in
place. The complete import → retain → filter/join → native spill → write → fresh
reopen → export lifecycle at greater depth is unproven. This checkpoint therefore
adds no deeper public-data support, throughput claim or performance comparison.
The broader user benefit must be established through a concrete required
workflow when this item resumes.

## Preserved implementation

The phase-plan record and RFC 0044 retain the detailed contracts and earlier
source-bound checks for native schema/array/scalar ownership, schema and value
serialization, decoding, JSON construction, optimizer traversal, payload copying,
Variant validation, schema classification, Arrow preparation, binary-object
discovery and nested relational keys.

The final Arrow work adds intrinsic schema and array cleanup, ordered schema
comparison/hash/formatting, descriptor cleanup/clone/inspection/validation/slicing,
and typed array-to-descriptor export. Test-only frozen originals cover exact
shallow behavior, including competing faults. Deep tests exercise independent
owners, partial failures and 8,192-level small-stack paths; pure dictionary
chains use 512 levels because their existing boxed schema copies grow quadratically.

Arrow remains an explicit compatibility boundary. The same-version schema,
JSON and row provider patches preserve the existing Arrow/Parquet 59.3.2 graph.
The JSON and row changes accommodate intrinsic schema ownership; they do not
introduce a new execution engine. Spark, DataFusion and external execution
fallback remain prohibited.

## Deferred acceptance

- Typed descriptor-to-array reconstruction and remaining Arrow/native schema
  conversion, field counting and matching.
- Remaining nested native provider take/filter/cast/scalar/canonical routes
  required by the selected complete workflow.
- Actual caller grants for all traversal, retained model, output, error and
  cleanup storage, including detached result owners and refused growth.
- Complete deep import, retention, filter/join, actual native spill, write,
  fresh reopen and export with full values/schema/metadata, cancellation,
  failure cleanup and final refunds.
- A justified public-depth change only after those boundaries pass together.

Raw provider stack tests establish their stated local behavior. They do not
establish caller memory prepayment or full workflow admission. Required source,
provider, package and PR checks for this checkpoint are recorded separately
from those deferred full-item gates. Package publication remains separately
authorized; this checkpoint does not request it.

## Checkpoint validation

The [local validation record](../release/evidence/native-deep-traversal-checkpoint-2026-11-17/checkpoint-validation.json)
retains exact commands, test counts, log/guard identities and a reproducible
source fingerprint. The original failed attempts remain retained separately.

- `cargo fmt --all -- --check` passes.
- `cargo clippy --locked --workspace --all-targets -- -D warnings` passes.
- `cargo test --locked --workspace --all-targets` passes 3,344 tests across
  108 targets, with zero failures or ignores.
- Native CLI/Vortex strict lint with `release-user-surfaces` passes. Four
  immediate Arrow/Parquet consumer tests and two ORC write/reopen tests pass.
- Focused final Arrow provider suites pass: Schema 180, Data 61, Array 788,
  JSON 134 and Row 95. Array retains one existing upstream ignored test;
  the other suites have no ignores. Their strict lint checks pass.
- All 2,668 registered source records across thirty providers match. Twenty-six
  existing lockfiles retain all package/version identities and dependency edges;
  only reviewed same-version source/checksum entries change. Thirty provider and
  legal-bundle tests, the CI gate matrix and inclusion-scope validation pass.

The full local native debug run was stopped with 1,575 completed rows and zero
observed failures. It is **incomplete**, not a passing full-suite result. Full
native coverage remains required in the existing checked optimized `ci-native`
hosted lane, alongside MSRV, feature, platform and installed-package checks.
The initial zero-test consumer filter was corrected; only the four actually
executed immediate-consumer tests count above. No benchmark was rerun and no
test or CI gate was removed.
