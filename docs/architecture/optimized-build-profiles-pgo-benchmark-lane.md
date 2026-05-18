# Optimized Build Profiles And PGO Benchmark Lane

Status: implemented build-profile evidence contract for `GAR-PERF-2H`; PGO profile-use remains
benchmark-only and report-only unless a merged local profile artifact is explicitly supplied.

## Summary

`GAR-PERF-2H` defines the build-profile and benchmark lane for optimized local binaries:

- `release-lto`
- `release-pgo`
- `release-native-benchmark`

The lane exists to make build configuration explicit in benchmark evidence. It does not change the
default release artifact, does not make `target-cpu=native` portable, and does not authorize any
performance or superiority claim.

## Source References

- Cargo profiles: <https://doc.rust-lang.org/cargo/reference/profiles.html>
- rustc profile-guided optimization: <https://doc.rust-lang.org/rustc/profile-guided-optimization.html>
- rustc codegen options: <https://doc.rust-lang.org/rustc/codegen-options/index.html>

Cargo supports custom profiles that inherit from another profile. rustc PGO is a two-build workflow:
build with profiling instrumentation, run representative workloads to produce `.profraw`, merge them
with `llvm-profdata`, then rebuild with `-Cprofile-use`. Host-native codegen, such as
`-Ctarget-cpu=native`, is machine-specific and belongs in benchmark lanes only.

## Current State

The workspace now defines explicit `release-lto`, `release-pgo`, and
`release-native-benchmark` Cargo profiles. The traditional analytics harness accepts those lanes via
`--shardloom-build-profile`, records a build-profile row contract in JSON/Markdown artifacts, and
keeps `release-native-benchmark` host-native and benchmark-only through an explicit
`-Ctarget-cpu=native` build setting. The default `cargo build --release` path remains the portable
baseline.

The checked-in helper `scripts/build_shardloom_pgo.py` documents the PGO workflow and can execute it
with `--run`. Without a `SHARDLOOM_PGO_PROFILE` merged profile artifact, `release-pgo` rows remain
`pgo_status=report_only_missing_profile_use_artifact` and cannot be used as PGO performance proof.

## Goals

- Add explicit optimized build-profile planning without changing default release behavior.
- Keep portable release artifacts portable.
- Keep `target-cpu=native` benchmark-only.
- Make PGO profile generation and use reproducible.
- Require benchmark rows to record the build profile, rustc version, target triple, target CPU
  posture, LTO/PGO/native status, and claim gate.
- Keep performance claims blocked until claim-grade gates pass.

## Non-Goals

- No replacement of the default release build.
- No public performance, superiority, memory-efficiency, or Spark-replacement claim.
- No package publication, release tag, or artifact signing change.
- No hidden `RUSTFLAGS` in release workflows.
- No `target-cpu=native` for portable release artifacts.
- No PGO profile checked in as authoritative performance evidence.

## Implemented Profiles

The profile contract distinguishes manifest profiles from required environment flags.
Cargo profile settings belong in `Cargo.toml`; rustc flags such as PGO and `target-cpu=native` may
need explicit `RUSTFLAGS` or wrapper scripts.

```text
release-lto
  Intended use: portable optimized local artifact lane.
  Cargo profile: inherits release, enables ThinLTO, codegen-units=1.
  Native CPU: prohibited.
  Claim status: not_claim_grade until workload gates pass.

release-pgo
  Intended use: reproducible PGO benchmark experiment.
  Cargo profile: inherits release-lto.
  PGO workflow: profile-generate -> representative benchmark smoke -> llvm-profdata merge ->
    profile-use rebuild.
  Harness profile-use flag: set by SHARDLOOM_PGO_PROFILE when a merged profile is supplied.
  Native CPU: prohibited unless explicitly combined with the native benchmark lane and labeled.
  Claim status: not_claim_grade until workload gates pass.

release-native-benchmark
  Intended use: host-local benchmark exploration only.
  Cargo profile: inherits release-lto.
  Required flag: target-cpu=native, supplied only when this benchmark profile is selected.
  Release status: never a portable release artifact.
  Claim status: benchmark-only, not public performance proof.
```

## Evidence Contract

Benchmark rows now record:

```text
build_profile
build_profile_kind
rustc_version
cargo_version
target_triple
target_cpu_policy
target_cpu_native_enabled
lto_enabled
lto_mode
codegen_units
pgo_status
pgo_profile_generate_status
pgo_profile_use_status
pgo_profile_artifact_ref
pgo_training_workload_ref
pgo_training_workload_digest
build_reproducibility_status
portable_release_artifact
benchmark_only_build
build_profile_correctness_digest
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

`pgo_status` distinguishes `not_configured`, `report_only_missing_profile_use_artifact`,
`profile_use_artifact_configured`, future `instrumented_build`/`profile_merged`/`profile_use_build`
states, and blocked or unsupported states.

## Acceptance Criteria

- `cargo build --profile release-lto` succeeds.
- The default `cargo build --release` behavior remains the portable release baseline.
- `release-native-benchmark` cannot be used as a release/publication artifact.
- PGO smoke is reproducible from documented commands and records training workload refs.
- Benchmark harness output records the selected build profile and native/PGO/LTO status.
- Claims remain blocked until the claim-grade benchmark gate passes.

## Verification Plan

Validation should include:

- `cargo build --profile release-lto`.
- optional PGO smoke:
  - instrumented build with `-Cprofile-generate`.
  - benchmark smoke training run.
  - `llvm-profdata merge`.
  - rebuild with `-Cprofile-use`.
- `python scripts\build_shardloom_pgo.py`.
- benchmark harness row-contract test for build-profile fields.
- release-readiness test that portable artifacts do not use `target-cpu=native`.
- `git diff --check`.

## Claim Boundary

Optimized build-profile evidence may say only which build lane produced a local benchmark binary and
which compiler settings were recorded. It cannot claim that ShardLoom is faster, superior,
production ready, a Spark replacement, or ready for package/public release.
