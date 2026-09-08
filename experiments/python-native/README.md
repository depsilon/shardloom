# Unpublished native Python experiment

This isolated Cargo package and Python import facade are not workspace members or
released Python package files. They perform real prepared metadata count,
filtered count and bounded projection through `shardloom-vortex`. No extension
is built, installed or loaded automatically. See the [contract](../../docs/architecture/perf-native-python-binding-2026-09-06.md).

The adapter now registers this feature-gated private child module:

```rust
#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "resident_result_json.rs"]
mod result_json;
```

The second additive hook, in `impl PreparedVortexCountWhere`, exposes the retained
footer count without another source open or query execution:

```rust
/// Logical rows in the retained source generation; no scan is performed.
#[must_use]
pub fn source_row_count(&self) -> u64 {
    self.source.file().row_count()
}
```

The prototype uses that value to reject a filtered source above 65,536 rows
before its first scan. Metadata count remains an O(1) operation; bounded
projection retains its existing limit and result-byte contracts. An additional
`validate_file_metadata(&Metadata)` hook on the prepared source and each of the
three prepared operators compares captured admission metadata with the actual
native provider generation before revalidating its descriptor and path. Both
the binding and Rust control use it before exposing the prepared operation.
This closes the preflight-only ABA gap without another provider open or query
execution. The 16 MiB admission applies to a successfully published plan's
retained generation; it does not intercept preparation allocations during a
concurrent file change. These hooks and actual-provider race tests are
implemented in the active source; compilation and runtime validation are pending.

The root owns all dependency resolution, builds and timed execution. After the
[dependency review](../../docs/dependencies/native-python-prototype-intake.md),
example commands from this directory are:

```sh
PYO3_PYTHON=/opt/homebrew/opt/python@3.13/libexec/bin/python3 \
  cargo build --manifest-path Cargo.toml --release --bin native_baseline
PYO3_PYTHON=/opt/homebrew/opt/python@3.13/libexec/bin/python3 \
  PYO3_BUILD_EXTENSION_MODULE=1 \
  cargo build --manifest-path Cargo.toml --release --lib
PYO3_PYTHON=/opt/homebrew/opt/python@3.13/libexec/bin/python3 \
  cargo test --manifest-path Cargo.toml --bin native_baseline native_file_admission_
```

Both commands require an explicitly admitted `CARGO_TARGET_DIR` in LocalData;
use the root's storage policy instead of writing under this source directory.
The first dependency resolution creates an isolated package lockfile which must
be reviewed and retained. Record exact source, Python, dependency graph and
binary hashes. Do not reuse an extension linked for another Python interpreter.
The manual loader accepts the explicit `.dylib`/`.so` path directly without
copying, wheel creation or installation.

```sh
SHARDLOOM_NATIVE_EXPERIMENT=/absolute/LocalData/release/lib_shardloom_native_experiment.dylib \
  /opt/homebrew/opt/python@3.13/libexec/bin/python3 -m unittest test_native
/opt/homebrew/opt/python@3.13/libexec/bin/python3 acceptance.py \
  --extension /absolute/LocalData/release/lib_shardloom_native_experiment.dylib \
  --native-control /absolute/LocalData/release/native_baseline \
  --cli /absolute/LocalData/frozen/shardloom \
  --uat-root /Users/dylan/LocalData/shardloom/clickbench-100m-uat
```

The compatibility facade in `python/shardloom_native_experiment` is available
after explicit `load_native.load(path)` initializes the module, or when a locally
built module is already on `sys.path`. Import failure is an error; it never
switches to the CLI. These commands are proposed verification steps and have
not been run. No package publication, superiority claim or PERF closure follows
from the existence of the prototype.
