# C7: JSONL typed builder screen

Status: admitted experiment; no retained speedup yet. This closes one candidate
under the existing performance intake, RFC 0031/0033 and CG-5/CG-6 evidence
obligations. It does not expand input semantics or public capabilities.

The public JSONL ingest reader currently creates a map and a named scalar row
for each input line, retains those rows for a complete batch, then traverses
them again to fill typed Arrow columns. The candidate replaces this bridge
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
command are preserved in the local C7 evidence packet pending the decision.

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

Retain useful complete-ingest savings with preserved correctness and resource
behavior; there is no arbitrary percentage floor. Preserve every sample and
compare symmetric best-valid calls. The profiled admission call is not an
unprofiled timing control. Ship/drop results will replace this pending status.
