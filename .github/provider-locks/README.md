# Standalone patched-provider validation locks

These eight lockfiles freeze the existing excluded provider test configurations.
`scripts/ci_provider_tests.py` copies the matching lock to each provider before
running its unchanged feature/patch/target configuration with `--locked`.
The three CI groups share compatible Cargo fingerprints within each job; they
do not combine every provider into a maximal-feature graph. The buffer command
still includes integration tests and doctests.

The initial October 25 resolution starts from workspace `Cargo.lock` SHA-256
`2784ccccf7f886593291618ea7cfd8494dc7405757ef0d416b030d160f473bea`, resolves
each standalone manifest with its actual CI arguments, then verifies the result
with `cargo metadata --locked`. Tests and strict Clippy pass for all eight
configurations. Root dependencies, provider source and runtime features are
unchanged. These locks are CI inputs, not replacement runtime lockfiles.

## Provenance review

The standalone graphs previously resolved additional development dependencies
on every CI run. Relative to the prior local locks, the initial frozen graphs
contain these reviewed version changes (some already match the root lock):

| Packages | Versions | Declared licenses |
| --- | --- | --- |
| aws-lc-rs / aws-lc-sys | 1.18.2 / 0.46.0 | ISC, Apache-2.0, MIT, BSD-3-Clause and MIT-0 in the manifests' combined expressions |
| const_format / const_format_proc_macros | 0.2.37 / 0.2.37 | Zlib; optional lock entries outside the selected metadata graph |
| schemars / combine | 1.2.3 / 4.6.9 | MIT |
| prost / prost-derive | 0.14.4 / 0.14.4 | Apache-2.0 |
| rstest_reuse / memmap2 | 0.7.1 / 0.9.12 | MIT OR Apache-2.0 |
| rustls-native-certs / schannel | 0.8.5 / 0.1.30 | Apache-2.0 OR ISC OR MIT / MIT |
| tokio | 1.54.1 | MIT |
| webpki-root-certs | 1.0.11 | CDLA-Permissive-2.0 certificate data |
| js-sys / wasm-bindgen-futures | 0.3.106 / 0.4.79 | MIT OR Apache-2.0 |
| wasm-bindgen and its macro, macro-support and shared crates | 0.2.129 | MIT OR Apache-2.0 |

License evidence comes from Cargo package manifests and the registry metadata
for [const_format](https://crates.io/api/v1/crates/const_format/0.2.37) and
[const_format_proc_macros](https://crates.io/api/v1/crates/const_format_proc_macros/0.2.37).
Registry checksums remain in the lockfiles. No external implementation code is
copied or modified by this change.

`colored 3.1.1` declares MPL-2.0 and is already present through the providers'
development benchmark/test graph. The completed [#1604 native job](https://github.com/depsilon/shardloom/actions/runs/40167963537/job/121092781115)
both resolves and compiles that exact version. It is absent from the workspace
runtime lock and shipped CLI/extension graph. Provider jobs retain reports and
logs, not these development executables. Its source and notices stay intact.
This scoped review preserves existing test coverage and does not widen the
runtime license allowlist. Mozilla documents file-level obligations and
[use/distribution boundaries](https://www.mozilla.org/en-US/MPL/2.0/FAQ/);
shipping these development binaries would require its own distribution review.

Update a lock only with the matching configuration arguments, inspect the
resolved dependency/license changes, and rerun that provider's tests and lint.
Do not remove development tests or add an external execution engine to simplify
resolution.
