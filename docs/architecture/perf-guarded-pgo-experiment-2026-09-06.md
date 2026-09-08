# Guarded local PGO experiment

Status: 14 orchestration tests pass and the native compiler/profile compatibility
smoke passed on September 8. Workspace builds, representative training and
independent evaluation remain pending. This covers PERF-13 with PERF-12 evidence
obligations, without changing a runtime path, default build or release.

## Provider contract

The [rustc PGO book](https://doc.rust-lang.org/rustc/profile-guided-optimization.html)
describes instrumentation, native training, profile merge and profile-use compilation.
It specifically recommends an explicit Cargo target, absolute profile paths and the
same other compiler options. The helper independently implements those steps; no
provider implementation was copied and no dependency or tool installation is added.

The selected `llvm-profdata --version` and `rustc -vV` are evidence, not a version-label
compatibility test. A tiny native program must instrument, produce a profile, merge,
recompile with profile use and return the independently computed integer result.
The local rustc LLVM 22.1.8 / Apple LLVM 21 observation alone neither passes nor fails
compatibility. On September 8, rustc 1.98.0 and Apple llvm-profdata 21 completed all
seven executable smoke stages, including actual profile generation, merge, use and
exact-result validation. Evidence is retained at
`/Users/dylan/LocalData/shardloom/perf-all-20260906/pgo-toolchain-smoke-20260908/report.json`.
That small-program result does not establish full-workspace PGO compatibility or
performance. The helper never installs tools.

## Commands and ownership

The wrapper stays at `scripts/build_shardloom_pgo.py`; implementation and tests live in
`scripts/pgo_local_experiment.py` and `scripts/test_build_shardloom_pgo.py`.
Use Python 3.11 or newer for the helper's `hashlib.file_digest` calls. Older
interpreters fail before creating a run directory.
Omitting `--run` performs no subprocess execution or filesystem writes. Unknown host
target is displayed explicitly until execution can inspect rustc. An example plan:

```sh
python3 scripts/build_shardloom_pgo.py \
  --run-dir /Users/dylan/LocalData/shardloom/pgo/reviewed-plan \
  --target aarch64-apple-darwin \
  --llvm-profdata /Library/Developer/CommandLineTools/usr/bin/llvm-profdata
```

Use a different **non-existing** directory for every actual run:

```sh
python3 scripts/build_shardloom_pgo.py --run --smoke-only \
  --run-dir /Users/dylan/LocalData/shardloom/pgo/reviewed-smoke \
  --target aarch64-apple-darwin \
  --llvm-profdata /Library/Developer/CommandLineTools/usr/bin/llvm-profdata
```

After the smoke passes and the source is committed/clean, the root can run the full
experiment by omitting `--smoke-only` and choosing another fresh directory. The full
run repeats the small compatibility gate, resolves Cargo metadata offline, then uses
the returned target directory and Cargo JSON's exact `shardloom` executable. Output
paths must be outside the checkout and macOS cloud-managed destinations. The source
checkout itself can remain in Documents because all generated data is elsewhere.

Default sampled limits are 40 GiB total run data, 1 GiB raw profiles, 256 MiB combined
outer/training logs, 12 GiB free headroom, 3,600 seconds per build/tool stage and 900
seconds for training. Full-run preflight reserves the declared data ceiling before
work starts; the smoke-only reservation is 1 MiB. Cargo output is an explicit fresh
`cargo-target` within that run, preventing instrumented objects from replacing a
normal developer build. These are sampled watchdog limits, not filesystem quotas,
provider-allocation limits or bounds on other workspaces. The root still serializes
all builds and performance runs. Existing directories, old profiles and failed-run
artifacts are never deleted or reused; a crash leaves evidence for inspection.

Each stage has separate capped-on-observation stdout/stderr files, a monotonic clock
and a process group. Any failed build, failed training, timeout or budget denial stops
the sequence before merge/use. Cleanup sends TERM, allows five seconds, then KILLs
the owned group and reaps its leader. The default training harness also handles TERM
and owns/drains its nested native groups. Arbitrary custom commands must not daemonize
or escape that group; this is not a general sandbox or universal descendant cgroup.
Stage clocks include watchdog polling and teardown, so they are build/training
lifecycle evidence rather than native query latency.

## Matched builds and fresh profiles

All three binaries use the same `release-pgo` profile, target, features and base
`CARGO_ENCODED_RUSTFLAGS`. The workspace profile inherits ThinLTO and one codegen unit.
No `target-cpu=native` is admitted by this helper. Control has no PGO option;
instrumentation adds an absolute `-Cprofile-generate`; use adds the absolute
`-Cprofile-use` and missing-function diagnostic. `--target` keeps instrumentation
away from build scripts. This comparison therefore does not credit ThinLTO gains to
PGO. Conflicting inherited Rust flag variables and preexisting PGO/coverage flags
are rejected. Actual base flags are included in the plan/report.

`LLVM_PROFILE_FILE=.../shardloom-%m.profraw` uses the binary-signature merge pool,
so repeated CLI processes update bounded shared profiles instead of producing a
large file per PID. Only 1–16 fresh, nonempty, regular `.profraw` files in this run's
new profile directory are admitted for merge. Smoke profiles are separate. A build
that unexpectedly produces raw profiles before training fails. The helper freezes
and hashes the instrumented binary before a later build can replace Cargo output.
Control, instrumented and use copies, their sizes, flags and build step times remain
in the report; profile/source identities are checked after the final build.

## Training is not evaluation

Default training uses the existing independent Python oracle in
`run_heldout_operator_uat.py`, with 4,096 renamed-schema rows, worker requests 1/4,
one warmup and one sample, and 12 explicitly recorded scalar/group/filter/projection
cases. Both harness binary slots receive the exact frozen instrumented binary:
96 complete query results plus native fixture preparation are validated. Despite
the historical harness filename and comparison fields, this run is **training**.
Its repeated binary slots do not establish speed ratios. Source fixture hash,
generation, full-value completion, selected cases and script hashes are recorded.

Later evaluation must use an untrained case subset or independently renamed
schema/distribution and retain full exact results on the control and profile-use
binaries. A second run of the same training corpus is not heldout evaluation.
ClickBench must not be the only evaluation surface. The final status after builds
is `built_and_trained_requires_independent_evaluation`; no performance claim is
enabled automatically by a merged profile or successful compilation.

A custom `--training-command` is shell-free argv with a standalone literal
`{instrumented_binary}` token, replaced by the exact copy path; the same path is
also supplied as `SHARDLOOM_PGO_TRAIN_BINARY`. For example:

```sh
--training-command 'python3 /absolute/native_trainer.py {instrumented_binary}'
```

This supplies a path but cannot certify an arbitrary program actually uses it or
avoids external engines. Custom mode reports correctness/external-engine policy
as unverified and makes no blanket `external_engine_invoked=false` claim. Nonempty
fresh profiles remain mandatory. Commands are trusted local experiment inputs,
not shell snippets or a public remote execution API.

## Validation to run

```sh
PYTHONPATH=scripts python3 -m unittest test_build_shardloom_pgo
python3 scripts/build_shardloom_pgo.py
git diff --check
```

Tests use fake build stages and tiny process/file fixtures, never Cargo, a compiler,
an engine or LLVM. They cover true print-only behavior, old-artifact preservation,
flag parity, exact executable selection, custom argv, stale/unbounded profiles,
failure short-circuiting, three immutable output copies, and forced group cleanup.
The real compiler/merge/use smoke and performance evaluation remain separate gates.
