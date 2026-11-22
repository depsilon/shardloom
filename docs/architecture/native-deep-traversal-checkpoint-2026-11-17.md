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

## November 21 completion update

This update closes the deferred implementation, end-to-end nested workflow
acceptance, empirical depth-boundary evaluation, and local source/provider/Python
validation recorded above while retaining hosted CI acceptance as an explicit
requirement.

### Completed deferred scope

- **Iterative provider continuations and 128-KiB stack audit**: Completed
  iterative slicing in `vendor/arrow-array/src/array/slice_iter.rs` (wired into
  `StructArray`, `FixedSizeListArray`, and `UnionArray`), depth-gated iterative
  pushdown in `vendor/vortex-array` (`continuation.rs`, `canonical.rs`,
  `probe`, `union`, `erased`, and `builtins`), `vendor/arrow-data` iterative
  `MutableArrayData` and equality, and iterative `vortex-arrow` /
  `shardloom-vortex` conversions and walkers. `deep_traversal_provider_128k_stack_audit`
  verifies nested execution at depths `64`, `512`, and `2,048` on `128-KiB`
  worker stacks.
- **Complete depth-24 end-to-end nested workflow**: Added
  `deep_24_end_to_end_nested_workflow_spill_vortex_arrow_and_cleanup` in
  `shardloom-vortex/src/local_primitive_relational_deep_traversal_tests.rs`,
  exercising depth-`24` mixed `Struct`/`List` intake (`2,150` rows per side
  with nulls and `1,200`-byte payloads in a `128-MiB` `ResidentVortexSession`),
  `Filter` (`id > 39`) and `Inner Join`, forced native spill (`runs_written > 0`,
  `owned_cleanup_completed == true`, empty spill directory), `.vortex` output
  writing (`2,110` rows), fresh reopen and streamed multi-batch
  `for_each_arrow_batch` export, detached batch memory retention
  (`reserved_bytes > 0` while retained and `0` after drop), pre/mid-stream
  cancellation, and `64-KiB` budget refusal (`reserved_bytes == 0`).

### Empirical depth-boundary probe and public ceiling decision

Across a `42`-cell subprocess probe (`pure_struct` and `mixed_struct_list` at
container depths `32`, `48`, and `64` across all seven format boundaries under
default verifier options, without overriding `VORTEX_MAX_LAYOUT_DEPTH = 64` or
FlatBuffers `max_depth = 64`), third-party format and Python boundaries behaved
as follows:

| Boundary | Container depth `23` (leaf `24`) | Container depth `32` | Container depth `48` | Container depth `64` |
| --- | --- | --- | --- | --- |
| `1_vortex_file` | Pass (both shapes) | Fail (`Nested table depth limit reached`) | Fail | Fail |
| `2_vortex_ipc` | Pass (both shapes) | Fail (`Nested table depth limit reached`) | Fail | Fail |
| `3_arrow_ipc` | Pass (both shapes) | Pass (both shapes) | Pass (both shapes) | Fail (`DepthLimitReached`) |
| `4_parquet` | Pass (both shapes) | Pass (both shapes) | Fail (`SIGABRT` stack overflow) | Fail (`SIGABRT` stack overflow) |
| `5_avro` | Pass (both shapes) | `pure_struct` fails (`recursion limit exceeded`); `mixed_struct_list` passes | Fail (`recursion limit exceeded`) | Fail (`recursion limit exceeded`) |
| `6_orc` | Pass (both shapes) | Pass (both shapes) | Pass (both shapes) | Fail (`ORC schema nesting exceeds 64 levels`) |
| `7_json` | Pass (both shapes) | Pass (both shapes) | Pass (both shapes) | Pass (both shapes) |

At the Python boundary, `_input_schema.normalize_schema` accepts depth `24` and
rejects depth `>= 25` (`ValueError`), while `_wire_type`/`encode_cell` and
`_result_schema._dtype` accept up to `64` (`_result_schema` rejects `65`).

**Decision**: Empirically retain ShardLoom's public payload nesting ceiling at
`24` (`depth <= 24`), do not override `VORTEX_MAX_LAYOUT_DEPTH = 64` or
FlatBuffers `max_depth = 64`, and lock that boundary in permanent regression
`deep_24_all_seven_format_boundaries_and_32_limit_probe`.

### Observed November 21 local validation counts

- Provider CI groups (`scripts/ci_provider_tests.py`): `arrays` (`26` commands),
  `io` (`8` commands), and `formats` (`20` commands) passed.
- `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  and `cargo clippy --offline --locked -p shardloom-vortex --features release-user-surfaces --all-targets -- -D warnings`
  passed.
- `cargo test --workspace --all-targets`: `3,352 passed; 0 failed; 0 ignored`
  across `108` targets; `deep_traversal_tests` (`--features release-user-surfaces`):
  `3 passed; 0 failed`.
- `cargo deny check licenses advisories bans sources`,
  `python3 -B scripts/check_dependency_audit.py`, and
  `python3 -B scripts/check_python_license_bundle.py` (`registry_packages=421`,
  `vendored_packages=30`, `license_file_records=840`, `unique_text_files=249`) passed.
- Python `core` shard (`scripts/run_python_test_shard.py --shard core`) with
  rebuilt ordinary `--features python-native` extension and
  `SHARDLOOM_REQUIRE_NATIVE_PYTHON=1`: `status=passed`, `test_count=1356`
  (`1,338` executed unittest cases, `failure_count=0`, `error_count=0`,
  `skipped_count=11`, `module_count=164`).
- Posture unchanged: `fallback_attempted=false`, `external_engine_invoked=false`,
  `claim_gate_status=not_claim_grade`; no performance superiority claim and no
  package publication; hosted CI acceptance remains pending.
