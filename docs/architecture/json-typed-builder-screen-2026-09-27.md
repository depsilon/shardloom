# C7: whole-file JSON typed construction

Status: admitted experiment; no retained performance claim yet. This completes
the separate whole-file JSON screen in C7 under RFC 0031/0033, the existing
performance intake and CG-5/CG-6 evidence obligations.

The 524,288-row, 186,301,079-byte array fixture preserves every source object in
the accepted JSONL fixture. A guarded public control ingest with CPU sampling
completed in 2.635842 seconds and peaked at 2,586,230,784 bytes RSS. This is
attribution, not an unprofiled comparison. Stacks include object parsing, row
map construction and ordered row copying before typed column construction.

## Experiment contract

Parse through the existing JSON object grammar, resolve each row's duplicate
fields in reusable slots, and append directly to inferred typed columns.
Discover columns in first-seen order; backfill missing earlier values with
nulls. Infer each column from final duplicate values, preserving the existing
whole-JSON stable scalar-type rule and all-null UTF8 default. Validate the
complete document before the native writer begins. Keep schema-hinted JSON
on its existing explicit adapter path.

This candidate retains whole-file text/character parsing and whole typed
columns in chunks at the existing batch sizes, including late-null backfill.
This preserves each Arrow string buffer's existing offset range. It is not a
bounded-memory JSON parser or a whole-process reservation guarantee. Test
first/late/all nulls, changing field order, duplicates, nested text, Unicode,
numeric precision, malformed/trailing input, limits and failure atomicity.
Compare complete output values and paired complete public operations; preserve
every sample and keep useful gains without an arbitrary percentage floor.

## Vortex-first provider check

- Subject: existing explicit JSON compatibility input, with no new grammar.
- Decision: `implement_shardloom_kernel` in the input adapter; reuse the current
  parser and Arrow 58.3 builders through `shardloom-vortex::universal_format_io`.
- Provider: existing scalar dtype/append helpers, Arrow `RecordBatch` ownership,
  Vortex 0.85 `ArrayRef::from_arrow` and the native streaming writer.
- Boundary: decoded compatibility input to typed columns and native Vortex
  output. Vortex input is not decoded to execute this adapter.
- Reports/certificates: existing SourceState, materialization, Native I/O,
  resource admission and no-fallback reports; describe whole typed ownership.
- Gates: `vortex-write` and `universal-format-io`; no dependency or unsafe code.
- Residuals: ShardLoom parsing/type errors; `fallback_attempted=false` and
  `external_engine_invoked=false`. No query-engine integration.
- Reused mechanism: avoid row maps and repeated copies before scheduling work;
  retain the established batch geometry, derived metadata and native commit.
- Still blocked: streaming byte-parser redesign and general capability claims.
