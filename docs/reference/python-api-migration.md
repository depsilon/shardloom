# Python API migration: context, results and discovery

This reference describes **unpublished development source after v0.6.0**.
The package version remains held. The [published guide](https://shardloom.io/field-guide/start-local-proof/)
continues to describe the verified v0.6.0 artifact. Exact source inputs and
callable signatures are recorded in the generated
[application reference](shardloom-user-surface-index.json).

## What changes

| Existing code or behavior | Development contract | Migration |
| --- | --- | --- |
| `sl.context(...)` implicitly used a CLI process | Linux/macOS native installations select in-process execution; Windows selects the CLI | Set `transport="subprocess"` to retain process isolation. Inspect `ctx.transport`. |
| Context cleanup required `ctx.client.close()` | Contexts support `close()`, `cancel()`, `closed` and `with` | Prefer `with sl.context(...) as ctx:`. Existing client cleanup still works. |
| `collect()` returned `VortexWorkflowExecutionReport` | `collect()` returns `Result`, backed by the same data owner | Use `result.schema`, `result.to_rows()` and `result.details`. The old class name is an identity alias. |
| Execution helpers could return a non-throwing error report by default | `collect()`, `run()`, count, previews, profiling and conversions raise typed execution errors by default | Code deliberately inspecting unsuccessful envelopes must pass `check=False` and inspect `is_error` before using values. |
| Native reuse was spelled `from_arrow_table(result)` | `ctx.from_result(result)` admits ShardLoom-owned data directly | Use `from_result` for results; keep `from_arrow_table` for Arrow inputs. The earlier native-result spelling still works. |
| A session's `reuse=False` closed its native client before executing | It releases prepared plans while preserving the shared owner | Existing calls now execute and remain usable. It does not enable a query-answer cache. |
| Root completion mixed application APIs and maintainer reports | Root `__all__`, `dir()` and type stubs expose the application vocabulary | Import inspection types from `shardloom.diagnostics`, and fixture/rehearsal types from `shardloom.testing`. |
| A worker startup/response failure could retry the request as a one-shot process | A submitted request is never replayed automatically | Inspect any output before retrying. For older CLI binaries, construct `ShardLoomClient(use_persistent_worker=False)` explicitly. |

## Installation and resource selection

Backend selection happens once when the context is constructed. An explicit
`transport` wins. Otherwise `binary`, `repo_root`, `SHARDLOOM_BIN` or
`SHARDLOOM_REPO_ROOT` selects a CLI installation; a native Linux/macOS package
selects `native`, and Windows selects `subprocess`. Other platforms require an
explicit supported build choice. Source builds without the native extension
must select the CLI explicitly. A selected but unavailable native extension
raises `ShardLoomInstallationError` (`SL_NATIVE_UNAVAILABLE`); it never starts
another backend after failure.

The low-level `ShardLoomClient()` retains its CLI default for integrations that
already choose that boundary. `shardloom.native.Session` remains the direct
typed native embedding interface. Neither is needed for ordinary native
performance through a context.

Memory and parallelism remain caller choices. Context construction without a
grant is useful for discovery, but execution requires both a positive memory
allocation and `max_parallelism`, supplied on a client, context/session or call. Invalid or
missing grants fail before source access. Importing the package does not choose
resources, load the extension, start a worker or import Arrow/pandas/NumPy.
Deployment ceilings and explicit spill authorization are unchanged.

## Data and materialization

SQL and DataFrame collection use the shared native plan/provider boundary.
Building a fluent file plan does not execute a producer. Input admission such
as `from_rows` can validate and allocate the source; `schema`/explain operations
retain their documented metadata-access contracts.

Prepared native file bindings preserve source generation identity. If a bound
file changes, existing plans reject the changed generation instead of returning
stale data or retrying silently. Prepare the changed input to a new destination
with `ctx.prepare_vortex(source, new_target)`, then use `ctx.read_vortex(new_target)`.
This preserves an existing prepared artifact; completed results from the earlier
generation remain valid.

`Result.schema` is the ordered exact schema, including empty results.
Native collection retains the actual result owner and resource credits.
`to_rows()` explicitly allocates logical Python values outside the engine's
retained allocation. `to_arrow()`, `to_pandas()` and `to_numpy()` remain optional
conversion boundaries; none reruns the query. Existing `python_objects`,
`result_schema`, `result_rows`, certificates and evidence fields remain
available. `details` is the typed `OutputEnvelope` from this execution.

`from_result` recognizes trusted native results and batches without compulsory
rows, JSON or Arrow export. It preserves the existing admission and receiving
resource-accounting rules; cross-owner admission can copy. Moving a process
result uses an explicit typed Python payload under the process input bounds.
It does not share buffers across processes. The
[executable lifecycle example](../../examples/paid-orders/context-result.py)
returns IDs 1 and 3 after both owning contexts have closed.

Collection is a bounded complete-result operation, not an arbitrary-sized
Python list. The existing small computed-result boundary is 65,536 rows and
8 MiB; native owned columnar inputs/results retain their separately admitted
delivery paths. Use `iter_batches()` or a writer when the plan requires larger
delivery. Native and process batches expose `schema`, `to_rows()` and explicit
optional conversions through their existing provider-specific classes.
Process rows are framed and serialized, not zero-copy.
Process batch pipes still require POSIX. Windows uses bounded collection and
writers; cross-platform batch delivery remains with the existing PERF-10
interface and PERF-11 lifecycle owners.

Use a context manager for batch consumption that may stop early. Yielded
batches are provisional: only full drain and the iterator's final `report`
establish success. Late producer errors remain errors even if earlier batches
were valid. Explicit iterator `close()` stops demand; cancellation raises a
typed cancellation error on the next demand and then terminates the iterator.
Already delivered owners remain valid.

`run()` executes once and returns its transport-level receipt; use `collect()`
for data. `profile()` executes the existing bounded sample and summarizes it
in Python; it is not side-effect-free plan inspection. `explain`, `route` and
capability methods remain the explicitly named inspection/reporting surfaces.
They do not pretend that a query ran.

Writes keep the existing plan and writer contracts. The `Result` write receipt
reports actual publication; it is not collected data and cannot be passed to
`from_result`. Use a new destination. Existing nested-format fidelity,
non-overwrite restrictions and multi-destination failure semantics still apply.
No multi-file atomic publication guarantee is introduced.

## Shared lifetime and errors

`ctx.session()` shares the context's client, resource policy and lifecycle.
Closing either facade revokes future work from both, including plans already
constructed from that owner. Use a new context for an independent lifetime.
`SessionSqlResult` is a compatibility subclass of `Result` that preserves
session reuse fields and delegates to the same completed report; it does not
execute or materialize again. It is not a separate execution engine.
Session `to_python_objects()` now restores logical values consistently with
ordinary result conversion; callers needing the wire representation can use
`result.result_rows` explicitly.

`cancel()` interrupts current work and allows subsequent operations on the
still-open context. Native cancellation is cooperative. Process cancellation
terminates the active process, so staged cleanup and output publication may be
uncertain; the diagnostic says so. `close()` also revokes future work.
Completed retained results stay usable after either action.

Engine, resource, installation, lifecycle, protocol and timeout errors share
`ShardLoomError` and expose `diagnostics`. Existing catches for
`ShardLoomCommandError`, `RuntimeError`, `ImportError`, resource `ValueError`,
`TimeoutError` and `subprocess.TimeoutExpired` remain valid for their respective
error classes. Closed work raises `ShardLoomClosedError` (`SL_CONTEXT_CLOSED`).
Caller-provided producer exceptions keep their original type and traceback;
ordinary Python argument mistakes remain `TypeError`/`ValueError`.

## Discovery and compatibility window

The single lazy export authority is `python/src/shardloom/_exports.py`.
Named imports of previously working root exports continue through a lazy
compatibility lookup; wildcard imports and IDE completion are intentionally
curated. New code should use the documented namespaces. Type stubs precisely
describe those namespaces; legacy root lookup has type `Any` instead of
republishing every maintainer name into application completion.

The legacy result alias and named root lookup remain for the next minor
release cycle. Removal requires a later announced compatibility change and
cannot occur in a patch release. No removal version is scheduled while the
current release hold is active. The repository uses the canonical result name;
compatibility tests explicitly exercise the aliases.

CLI command names and JSON envelopes remain compatible. `shardloom help`
foregrounds application commands. Use `command-metadata --audience application`
or `--audience maintainer` to filter discovery; unfiltered metadata still returns
the complete registry. `command-metadata --completion bash`, `zsh` or `fish`
generates static completion from that authority. Help and completion do not
execute datasets or advertise maintainer fixtures as a faster engine.
