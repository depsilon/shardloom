# Native result composition

Status: implementation and full local acceptance complete; PR acceptance pending
after the merged ownership/preparation unit in PR #1455.
This prerequisite belongs to the existing native runtime completion plan and
PERF-02/07/10/11. It does not complete general joins or public operator parity.

Owned results retain an authoritative native dtype even when no data arrays are
returned. Every batch must match it and the checked total row count. Completed
native and compatibility sinks consume that schema, including empty output.

Vortex-first decision: `use_vortex_native_provider`. Pinned Vortex 0.85 exposes
native arrays, Chunked/Struct layouts, Flat segment serialization and VortexFile
scans. Its production VortexFile reader is constructed from footer/layout/segment
sources; it does not accept injection of an arbitrary owned array reader. Reuse
ShardLoom's existing MemoryFileGeneration and segment builder so downstream
operators keep the same Vortex scan and optimized aggregate lowering boundary.

The bounded owned-batch intake supplements the existing typed memory intake.
It retains typed native columns and chunk references, without row or Arrow conversion,
and normalizes nullable root-Struct validity into logical field validity when building
the tabular nonnullable Struct root. Native column values and their nulls remain
unchanged. Explicit bounds cover rows, fields, batch references, segments,
serialized bytes and metadata. Existing typed-intake and JSON defaults remain.

Native serialization is an explicit copy boundary. Sliced dictionaries and
VarBinView arrays can retain whole backing domains; row count alone does not
bound the serialized volume. Deny serialized amplification before publishing an
immutable generation. Retain input reservations while serialization and assembled
segments overlap, reserve adapter reference vectors before allocation, and keep
untracked upstream serializer/layout internals explicit in the evidence.

Lazy slices and validity expressions are completed through the same native
recursive-canonical provider used by persisted sinks, once per bounded column
leaf. Already serializable encodings remain encoded; a Chunked array remains one
Flat stream item. Construction evidence counts those completion calls and column
rows separately from query execution. These counts do not measure physical
decodes or copies inside upstream providers.

Source-based aggregate preparation must reuse current lowering, physical-key
proofs, exact partitions, weighted reducers and owned finalizers. Bind a stable
opaque memory URI to the immutable generation; do not label it as a filesystem
source. Certificates identify memory segments, construction serialization, native
scan work and zero source-file opens. Repeated calls own fresh aggregate state.

Composed operators borrow one native execution context. Reject foreign sessions
before work; only the outer operation admits, drains and increments completed-call
counters. Producer/provider jobs must join before the next stage starts its own
workers. Generation construction does not count as query execution. Public APIs
admit ordinary calls; borrowing entry points remain internal. A metadata-only
grant cannot be expanded into scanning or construction. Memory evidence IDs are
checked process-local monotonic IDs; they are never resolved as filesystem paths.

Ordinary aggregate scans observe parent cancellation at chunk/stage boundaries,
including exact recount passes. The parent token remains independent of worker
attempt cancellation so a pressure replay cannot poison the whole operation.
Current explicit spill kernels keep their separate policy token; a borrowed
operation token is observed before/after that stage. Extending parent-token
propagation inside spill kernels remains a subsequent cancellation obligation.
Public cancellable admission observes both the spill policy and enclosing operation
tokens while queued. Either owner can cancel admission; neither propagates
cancellation backwards into the other owner. Renewing a spill policy retains its
resource configuration and detaches stale cancellation owners.

Acceptance covers typed empty result/source/aggregate/sink, multiple batches over
65,536 rows, nullable and mixed-width fields, dictionary and sliced-text
amplification, explicit bounds and partial-build cancellation, source lifetime,
foreign contexts, memory provenance, and P1 composition without nested admission.
Full43 is the regression gate for file aggregates and their existing physical
choices. General joins and further prepared/owned/public families follow this
source contract.

## Local acceptance

The implementation at `ef957adf`, with documentation at `534095f7`, passes the native
value and ownership checks. Builds use the resolved local
Cargo target `/Users/dylan/.cache/shardloom/cargo-target`. Command logs and hashed
JSON receipts are retained under
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/`.

| Check | Result and receipt |
| --- | --- |
| Workspace all-target tests, one test thread | 3,425 passed across 102 targets, no failures/ignores; `composition-workspace-tests-1.json`, 341.086564 s |
| CLI/Vortex all-target tests, `release-user-surfaces`, one test thread | 3,435 passed across 86 targets, ten explicitly ignored, no failures; `composition-native-all-targets-1.json`, 354.904390 s |
| Workspace all-target Clippy with `-D warnings` | Passed; `composition-workspace-clippy-1.json`, 39.236725 s |
| CLI/Vortex native all-target Clippy with `-D warnings` | Passed; `composition-native-clippy-final.json`, 28.421312 s |
| Lean no-default `vortex-local-primitives` check | Passed; `composition-lean-check-1.json`, 9.280233 s |
| Formatting, diff whitespace, public-status docs, release architecture tracker | Passed; architecture tracker used its existing `--allow-blocked` audit mode and does not certify release readiness |

The workspace test preceded the final recovery-policy API addition from PR #1455;
the complete native suite and both final Clippy checks include that addition.
Native acceptance includes 98,304 composed rows across three batches, repeated
complete aggregate values after producer owners are dropped, typed zero-array
results through Vortex and Arrow IPC sinks, nullable root/field validity, mixed
integer widths, dictionary/backing amplification denial, explicit construction
limits and partial-build cancellation. A 65,536-row composed source also executes
real exact DISTINCT spill, validates all results, removes every owned run after
each call and retries using public cancellation renewal. Memory-source certificates
retain zero source-file opens and explicit construction work.

Cancellation coverage includes cancellation during a segment request, a fresh
retry on the same prepared source, cache-pressure replay without poisoning the
parent, both cancellation owners while queued for each admitted spill family,
and provider-worker evidence on cancellable calls after worker admission denial.

Log SHA-256 values, respectively, for the two test suites, two Clippy checks and
lean check above:

```text
397d64caba300d54f19dff454eca7b82dd9baa24e7e730c8407ccf82c3c3243f
ffb4e9e5d37244c411f448154a0b03b443b143cf8a0bc6b85019d2e366863ee8
170b807c53c4eb9b4a41cdcf32193ba06299f89b04a306f32ef242dd27adb2ad
e028075af1c3373266413ba486634b0326740d693ebbdf1177c0ddad1193c481
5b01bc20d1c7a60362bda4d41c6f075643cd2d2d7b5110efb71a74a37b14c4a8
```

No performance gain, total RSS bound, broad public parity, completed join support
or production serving fairness is claimed by this prerequisite.

The explicitly invoked fixed-arrival serving fixture also passes at 1,000 µs
arrival intervals (`composition-serving-load-1.json`, 25.513484 s including build;
log SHA-256 `3095462839c33d8f854d28cce950d379244564e8cf333d96b2899104ea8889fe`).
Serving completed 96/96 requests with exact values, zero rejections/errors and
zero final reservations. Peak ownership was four CPU lanes, four queued calls
(96 bytes of admission metadata), and one I/O request/264,124 bytes. Exclusive
mode completed 47 requests and rejected 49 at the bounded client queue, with no
engine errors and zero final reservations. This debug fixture is lifecycle
acceptance, not a production-scale latency comparison.

## Release UAT

The release binary built from clean `534095f78cc50c910e585c250979f432e2bad7f1`
with `release-user-surfaces` is frozen as `candidate-534095f7` (85,234,544 bytes),
SHA-256 `64a6d56579cbd56bc9acfc0c96c83ac3ac888ef32a783c8445d264d1bbafab4b`.
The subsequent main merge changes history only; the remaining overlay is documentation.

Full43 passes all 129/129 complete results on the retained 99,997,497-row,
18,591,586,804-byte native source. Sum of per-query best-of-three times is
63.459160 s; all raw calls total 197.966844 s. These fresh-process timings include
complete CLI output and exit. The run uses the same 24 GiB/P12 request (effective
ten CPUs), Apple M5/macOS 27 host, uncontrolled OS cache, query definitions and
retained native references as the first-unit evidence. It is an unpaired
observation and does not establish a causal performance gain or an independent oracle.

Receipt: `clickbench-100m-uat/logs/full43_20260920T161041150303Z/summary.json`,
SHA-256 `d01c007614e57b1c401fe15b3b73f6755f165c7f98d957c776d771b19516bb36`.
`composition-full43-2.json` preserves the exact command and log hash. The initial
`composition-full43-1` attempt stopped before executing a query because the system
Python lacks `hashlib.file_digest`; the successful run uses installed Python 3.13.
Storage limits and concurrency guards are unchanged.

The first public-call attempt stopped at the unchanged log-storage guard after
2,107 passing checks; it is retained as incomplete, not a passing full matrix.
`archive_storage_stopped_composition_public.json` preserves its raw output and
the earlier storage-stopped held-out run without changing either summary status.
Completed Full43 logs were also packed into verified raw-member archives before
the complete public-call retry. This cleanup changes storage representation only;
no guard was disabled and no failed acceptance was reclassified as complete.

The complete retry passes 2,232/2,232 checks: 12 deterministic 32-row cases,
three public surfaces, baseline/candidate alternating pairs and 30 measured calls
plus one warmup per case. Candidate p50 ranges across those cases are
6.228–7.078 ms fresh CLI, 0.387–0.935 ms persistent worker and 0.706–1.381 ms Python.
All complete values, retained-source counts and prepared-lowering reuse checks
pass. The baseline is the first unit's final query binary, `candidate-049e33da`;
this fixture does not measure large payloads, total RSS or production concurrency.

Public receipt: `clickbench-100m-uat/logs/resident_call_paths_20260920T162914389048Z/summary.json`,
SHA-256 `f6ad1a2569bcc4d208c0c00e369ec4232f782d52458819bd656e2024eff157da`.
`composition-public-uat-2.json` records the command and 526.904107 s harness time;
the timing ranges above use native/public call boundaries, not total harness time.
Verified raw logs are indexed by `archive_completed_composition_public.json`.
Four completed historical held-out fixtures were independently hash-checked
against their unchanged summaries and packed losslessly before the next run;
`composition-completed-heldout-fixtures-archive.json` preserves that custody.

The held-out matrix passes 760/760 calls: 19 cases on 131,072 rows, P1/2/4/8/12,
baseline/candidate pairs and three measured calls plus warmup. An independent
Python oracle checks 720 complete values; 40 calls verify the expected signed
overflow diagnostic and no-fallback evidence. Native DISTINCT worker execution is
required and verified for both selected worker cases. Nullable fields are declared
in the PyArrow 25.0.1 Parquet fixture, which is an explicit input boundary only.

Held-out receipt: `clickbench-100m-uat/logs/heldout_operators_20260920T163918059577Z/summary.json`,
SHA-256 `7c77bc401592c5b7433ebfa3bbab2a84e7feb4e147102595d1779f7f9a484c0e`.
`composition-heldout-uat-1.json` records the exact command and 188.134442 s harness
time. The runtime, Python and harness sources match the frozen binary's source
revision; the following branch overlay contains documentation only.
