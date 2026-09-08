# Native Python binding experiment

This records an isolated, unpublished PERF-02 experiment under RFC 0044. Its
unbuilt prototype is preserved on the local `codex/perf-remaining-work` branch
at `be2a69f3`; `experiments/python-native` is excluded from the retained-runtime
shipping batch. Paths and reproduction steps below refer to that preserved
checkpoint, not to a currently shipped Python extension. It uses the
existing native resident session and prepared operators in `shardloom-vortex`.
Its Cargo package is not a workspace member or selected by the released Python client.
The additive adapter hooks are registered under the native local feature gate;
dependency resolution, compilation and runtime validation remain pending.
Default Python packaging and its persistent CLI worker remain unchanged.

## Feasibility and dependency boundary

The current Python client sends JSON requests to one retained `python-worker`
process, or the same CLI in a fresh process. It has no native in-process module.
The shared Rust adapter already exposes retained metadata count, actual filtered
count and bounded projection consumers, so another execution engine is unnecessary.
The missing boundary is a Python-owned safe wrapper around these existing owners.

The standalone `experiments/python-native/Cargo.toml` proposes exact PyO3 0.29.2
and pyo3-build-config 0.29.2, with local path dependencies on the existing core,
plan and Vortex crates. No extra Vortex, Arrow or query-engine dependency is added.
The proposal does not modify the root lockfile or production feature graph.

PyO3 0.29.2 supports Rust 1.83+ and CPython 3.8+, within this repository's Rust
1.96 and Python 3.10+ minimums. It is MIT OR Apache-2.0. Its own checked-in
compile-pass test combines `#![forbid(unsafe_code)]` with `pyclass`, `pymethods`
and `pymodule`; this experiment keeps that lint rather than adding an exception.
The provider contains the FFI implementation. Exact transitive resolution,
licenses, compiled macro behavior and load tests remain required before intake.
These are validation work, not an inferred permission requirement for the
user-authorized implementation.

Primary provider references:

- [PyO3 0.29.2 metadata](https://docs.rs/crate/pyo3/0.29.2)
- [Exact provider manifest and licenses](https://github.com/PyO3/pyo3/blob/v0.29.2/Cargo.toml)
- [Exact forbid-unsafe compile-pass test](https://github.com/PyO3/pyo3/blob/v0.29.2/tests/ui/forbid_unsafe.rs)
- [Thread-safe classes](https://pyo3.rs/v0.29.2/class/thread-safety.html)
- [Detach Python during native work](https://pyo3.rs/v0.29.2/parallelism.html)
- [Build and distribution contract](https://pyo3.rs/v0.29.2/building-and-distribution.html)

## Admitted operations and ownership

The experiment preflights local regular Vortex files up to 16 MiB, absolute paths,
1–4 GiB requested native memory, 1–8 requested CPU lanes and at most 64 live
prepared handles. These small explicit limits are not a general data-access API.
Provider background-worker counts are reported as observed rather than inferred
from the requested CPU ceiling. Existing source generation checks remain active.
Preparation retains a preflight file handle and its captured metadata. The
prepared source's additive `validate_file_metadata` method compares that expected
device/inode, length, mtime and ctime with the actual generation held by the native
provider, then validates its current descriptor and path under the resident
admission gate. Count, filtered-count and collect expose the same check. The
isolated adapter checks this provider identity before checking its preflight
descriptor and path again, and publishes a Python handle only after both pass.
An ABA path or symlink switch to a second valid file during preparation therefore
fails even when the original preflight metadata is restored before publication.
In-memory sources reject file-metadata admission explicitly.

This adds no payload read, second native provider open or query execution; the
temporary preflight OS handle is separately scoped. It binds the file-size
admission to the generation used by a successfully published plan, but is not an
interceptor for allocations made during preparation of a concurrently changed
file. Executions still validate the provider's retained generation, so mutation
after publication cannot authorize a scan over the changed source. These checks
are implemented; compilation and deterministic race tests remain pending.

- `Session.prepare_count` owns an actual prepared file and returns its metadata
  count on every execution. The preparation counter distinguishes the one open
  from subsequent source-generation validation; no query answer is stored.
- `prepare_count_where_i64` lowers six comparisons on one named field through
  `prepare_count_where_in_session`. Every call executes the retained native scan,
  constructs the real filtered-count report/certificate and validates source
  generation, including predicates which prune the entire input.
  The `source_row_count()` accessor reads only its retained footer count;
  sources above 65,536 logical rows are rejected before the first scan. Physical
  file bytes alone cannot bound the work of compressed logical rows.
- `prepare_projection` accepts 1–64 unique fields and an explicit limit of
  1–65,536 rows. `execute_arrays` returns the existing `OwnedVortexResultBatch`
  without constructing scalar Python rows. The existing 32 MiB retained native
  result bound remains authoritative. `execute_json` uses the existing complete
  collection sink and its native I/O certificate.
- `NativeBatch.to_json` explicitly materializes and copies into a bounded native
  JSON string, then a Python Unicode object. A small native child module,
  `resident_result_json.rs`, calls the existing native sink with the result's own
  retained runtime and allocator. Output growth is charged before allocation;
  the reservation survives until the Python copy finishes. The native result may
  outlive its original session and prepared handle. This is native array-owner
  reuse, not a claim that JSON or Python values are zero-copy.

Frozen Python classes use interior mutexes. Every operation detaches from Python
before waiting for the per-session guard or performing native work. No Python
object, borrowed buffer, callback or reference crosses into a native task.
Session close waits for any admitted operation, invalidates its prepared slots
and drops those native owners. Independently returned arrays remain valid and
retain their own credits. A closed prepared handle fails deterministically.
Dropping the Python session object while a prepared handle exists keeps its
runtime alive; explicit close revokes prepared work. There is no process-global
session, answer cache, automatic subprocess transport or fallback engine.
Prepared collection and independently retained result JSON rendering share the
same native runtime admission gate. The prepared scan releases that gate before
the JSON stage reacquires it, avoiding recursive locking while preserving its
existing generation and certificate checks. This shared sink keeps the existing
public collection field admission; only the experimental Python boundary adds
its narrower 64-field limit.

Errors remain explicit (`SL_NATIVE_CLOSED`, `SL_NATIVE_PLAN_LIMIT`,
`SL_NATIVE_POISONED`, `SL_NATIVE_EXECUTION`, `SL_NATIVE_CERTIFICATE` or Python
argument errors). Native diagnostics remain in the exception text. Interrupts
are checked before and after native calls. This prototype does not yet expose
mid-scan Python cancellation or deadline tokens; a Python interrupt can be
delivered only after the bounded native operation returns. No asynchronous API,
buffer protocol, pandas/NumPy conversion, PyPy, free-threaded interpreter or
subinterpreter certification is claimed.

## Validation and measurement packet

The core seam has independent complete-value tests for a retained result after
source/session drop, output reservation lifetime, failed byte/field admission
and later successful use without rerunning the query. Python tests additionally
exercise all six comparison operators, exact ordered projection, per-operation
close, source replacement/unlink/in-place growth, metadata-pruned stale zero,
plan-cap release and deterministic errors. These tests are authored but unrun.
The concurrency regression pauses prepared collection after its scan, then holds
the shared gate while both its JSON stage and a retained result sink try to run.
Neither may complete until the gate is released. Standalone admission tests
actually prepare native metadata-count, filtered-count and collect operations.
For each route they switch a symlink to a second valid Vortex file during native
preparation, restore the first file, verify that preflight-only validation passes,
and require the held-provider check to reject publication with one source open
and zero executions. Further cases cover stable repeated validation, initial
oversize rejection before a provider open, actual post-prepare replacement,
growth and unlink, and a stable but different provider file. Core tests also
reject an in-memory source and prove mismatched caller metadata does not poison
an otherwise valid retained generation. These tests are authored, not yet run.

`acceptance.py` prepares the existing independent 32-row public fixture with
renamed fields, nullable Unicode and signed exact identifiers above 2^60. It
uses the guarded native CLI solely for fixture preparation. Its oracle comes
from the generated source rows, not from another engine or a native result.
Metadata count, selective filtered count, empty filtered count and complete
projection each run one warmup plus 30 measured samples per surface:

1. A standalone Rust control calls the same prepared operators and returns full
   values for Python validation. Native return, explicit JSON sink and drop
   clocks are separate; exporting the validation copy is outside them.
2. The existing persistent JSON worker encodes a request and waits for complete
   response bytes. JSON parsing and independent comparison occur afterward.
3. The in-process module returns an exact Python integer or retained native
   result. Explicit native JSON plus Python Unicode-copy time is separate from
   native-array return. A continuous complete-Python-return span is also recorded.

Worker/in-process execution order alternates for every sample. Rust controls run
separately and sequentially, so their timings do not independently establish a
paired cross-language speedup. Session and plan setup are recorded separately;
worker plan preparation is in its excluded warmup because the public transport
has no equivalent separate prepare operation. Full raw samples, percentile
method, input/binary hashes, actual open/execution counters and losslessly
compressed worker envelopes remain in the result packet. Exactly 372 complete
records are required, including 12 warmups. Every case checks one source open
and one additional actual execution per call, with no cached answer. No elapsed
value is a CPU-time, controlled cold-cache or process-RSS measurement.

The harness uses the existing 100 GiB workspace/256 MiB log guards, shared
exclusive UAT lock, process timeout and cleanup helpers. It does not delete
retained artifacts, disable storage guards or hydrate a large source. Python
allocations, oracle values and native allocations outside the existing allocator
remain outside reported owned-byte coverage. Runtime/prototype changes are
unmeasured until the root runs the exact build, load and acceptance gates.

## Integration sequence

The adapter hooks now integrated in the active source are the
read-only filtered-count `source_row_count()` accessor and a private
`resident_session` child module under
`cfg(all(feature = "vortex-local-primitives", unix))`, pointing to
`resident_result_json.rs`. Its public method remains on the existing owned
result type. The standalone Cargo package already depends on that feature.
No root Cargo registration, package release or default Python behavior change
is necessary to compile and load the experiment.

The [dependency intake record](../dependencies/native-python-prototype-intake.md)
separates verified provider provenance from pending resolved-graph proof.
Root should resolve the isolated dependency graph and record its lockfile and
license metadata, then build a normal Rust control executable and the extension
separately. Set `PYO3_PYTHON` to the tested CPython interpreter and use
`PYO3_BUILD_EXTENSION_MODULE=1` only for the extension build; do not enable the
extension-module link behavior for Rust executable/tests. The build script uses
the provider's platform link helper. Keep Cargo output in the existing admitted
LocalData build root. The package README records exact build/load selectors.

Distribution remains a later choice: first prove the local exact-interpreter
extension. ABI3 wheels, platform-wheel repair, multiple Python versions and
release packaging require their own actual build/install proof. They are not
inferred from this prototype or from the existing bundled-CLI wheel.
