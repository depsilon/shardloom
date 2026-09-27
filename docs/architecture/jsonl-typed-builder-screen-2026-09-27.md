# C7: JSONL typed builder screen

Status: retain the JSONL typed-builder change after complete acceptance. C7's
whole-file JSON construction screen remains open. This is an existing-input
optimization under the performance intake, RFC 0031/0033 and CG-5/CG-6 evidence
obligations; it does not expand input semantics or public capabilities.

The previous public JSONL ingest reader created a map and a named scalar row
for each input line, retained those rows for a complete batch, then traversed
them again to fill typed Arrow columns. The retained change replaces this bridge
with schema-indexed reusable row slots and bounded typed column builders.
Duplicate keys overwrite slots before coercion; a complete object must parse
and all values must coerce before appending. Builder errors invalidate the
batch. Inference, CSV, general JSON, source limits and native commit behavior
remain covered by regression checks. JSONL numeric parsing still tries i64
then finite f64; this is not new exact-u64 JSON support.

## Admission and provider check

The frozen ordinary release binary `f837cf6f...` ingested 524,288 generated
rows / 185,776,788 JSONL bytes through guarded public `prepare dataframe` in
3.200243 seconds with a macOS CPU sample. Source inference took 1,205 ms;
nested source production reported 1,808 ms and encode/write wall 1,032 ms.
These clocks overlap and are not additive. Sampled stacks include inference,
object/string parsing, named row construction and typed batch construction.
This admits a controlled experiment, not a speedup forecast. The fixture uses
two 262,144-row product batches. Source and artifact hashes, generator and
command are preserved in the checked-in C7 evidence packet below.

Vortex-first provider check:

- Subject: explicit JSONL compatibility input construction.
- Decision: `implement_shardloom_kernel` at the existing input adapter. Keep
  ShardLoom's current object grammar, duplicate handling and schema coercion;
  the Vortex source/array APIs do not define those frontend semantics.
- Provider: reuse Arrow 58.3 typed builders and the existing scalar append
  helper in `shardloom-vortex::universal_format_io`. The existing Vortex 0.85
  ArrowSession/ArrayRef import and native writer remain the persistence path.
- Gates: `universal-format-io` and `vortex-write`; existing source admission,
  materialization reports, native output and no-fallback certificates apply.
- Boundary: decoded compatibility input to typed Arrow batches, then native
  Vortex arrays/output. This is not decoding Vortex for query execution.
- Residuals: native parsing/coercion or deterministic error; no external
  executor, query-engine integration or new dependency.
- ShardLoom techniques: avoid repeated construction before scheduling more
  work; preserve existing capillary batch sizing and writer admission. No new
  PulseWeave scheduling or dynamic worker policy is needed for this adapter.
- Evidence: exact semantic fixtures, malformed input/atomicity checks,
  complete native output equivalence, paired complete ingest calls, memory
  and CPU receipts, broad workspace/native gates and Full43 regression UAT.
- Boundaries still open: byte-parser redesign, direct general JSON construction,
  inference elimination and production-scale JSONL serving evidence. No
  Parquet ClickBench ingest speedup or general capability completion claimed.

## Complete comparison and acceptance

The [machine-readable summary](../benchmarks/jsonl-typed-builder-screen-2026-09-27.json)
and [raw evidence](../benchmarks/jsonl-typed-builder-screen-2026-09-27.json.gz)
retain commands, complete envelopes, source/binary identities, recipes, all
samples and per-member hashes. The local host is Apple M5 / arm64, 16 GiB;
both binaries use Rust 1.98.0, Vortex 0.85.0, `release-user-surfaces` and ordinary
portable ThinLTO. No host CPU flags, trained profile or cache eviction was used.
The guarded public calls use 2 GiB policy memory and four workers. OS peak RSS
is observed process memory, not a claim of whole-process reservation enforcement.

| Sample | Control complete ingest | Candidate complete ingest | Control peak RSS | Candidate peak RSS |
| --- | ---: | ---: | ---: | ---: |
| 1 | 2.881499 s | 2.919412 s | 624,721,920 B | 248,692,736 B |
| 2 | 2.767108 s | 2.378445 s | 625,803,264 B | 257,982,464 B |
| 3 | 2.780983 s | 2.414744 s | 628,064,256 B | 263,274,496 B |

Execution order was C1/P1/P2/C2/C3/P3. Best-valid complete ingest improves
**14.05%**, from **2.767108 to 2.378445 seconds**. All observations remain in the
record, including the slower first candidate call. There is no percentage floor.
Native child CPU and page-fault counters are also retained. Candidate RSS is
249–263 MB versus 625–628 MB; initial builders start at at most 1,024 rows and
grow within the existing 262,144-row product batch rather than eagerly sizing
short inputs to a full batch.

All six outputs are byte-identical: 26,252,040 bytes, SHA-256
`7c8eccb2189abbe5ed5dc8100554e14a638893232dfbe80752655726fe5b1b34`.
An executed native export checks all **524,288 rows / 6,291,456 source and
derived cells** against independent fixture formulas and parsed source, including
normalized nested JSON text, nulls, Unicode, Boolean/numeric types and derived
URI length/domain values. The 240,969,583-byte compatibility export and six
duplicate native outputs were removed only after complete verification.

The candidate is frozen at `c0766d1e369575ce673295fc082c319c8645f254`, binary
SHA-256 `0b4ec877243d3fc26ad709dd97dddf9d03811960cccc918e9d0e924fc6592346`.
The control is the accepted C5 ordinary release artifact, binary SHA-256
`f837cf6fab4f144ba0acccef330b4a1baa64fd4e08c6ed4f906d2fb581d6a4ba`.
Later changes in this unit are documentation/evidence only.

- Workspace formatting/clippy/tests pass: 3,426 tests.
- CLI/Vortex native-feature clippy and all targets pass: 3,507 tests, 22 existing
  ignored tests. Five new tests cover the reader/builder contracts; existing
  late-failure, cancellation, publication and cleanup tests also pass.
- Full43: 129/129 complete results, **62.640458 s** best-of-three sum.
- Full-size Parquet ingest: **84.536593 s**, byte-identical retained
  **15,682,956,116-byte** artifact; verified duplicate removed.
- Held-out operators: 456 calls, including 24 expected overflow diagnostics,
  at declared workers 1/4/12. This is not the full default worker matrix.
- Pressure: five checks pass, including exact SQL/DataFrame spill results,
  reservations, disk quota denial and owned cleanup.

Full43 and Parquet timings are unpaired regression observations, not JSONL
speedups. Primary acceptance replay verifies complete saved values and hashes.
Review combines primary semantic review with independent mechanical coverage
inventory; it does not claim an independent semantic reviewer. Two incorrect
coverage-draft findings were withdrawn after source/test verification. An initial
`route` export preflight performed planning only; the corrected `run` command's
complete row verification is the export proof. Both receipts remain preserved.

General JSON accepts a whole object or array through a separate materialized
path. Screen that construction boundary before closing C7; neither this JSONL
result nor the collecting visitor refactor establishes a general-JSON speedup.
