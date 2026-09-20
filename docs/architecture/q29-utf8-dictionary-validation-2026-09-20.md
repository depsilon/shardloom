# Q29 UTF8 dictionary validation and cached hashes

Status: candidate under test; no speedup claimed.

The active performance queue selects Q29 chunk dictionary construction after the
retained Q36 partition reduction. The current runtime is `58f0443597cae4c90266dbe2d4fa4481e8c16495`
(merged in PR #1451). Its saved Q29 best complete call is 10.215333 s; 5.080100 s
is nested dictionary construction inside 7.657883 s accessor setup. The accessor
sees 81,032,736 rows and creates 25,771,910 chunk-local dictionary entries, copying
3,120,823,803 UTF8 payload bytes. These are cumulative work counts, not peak memory.

Three new sampled complete CLI calls passed their full result references.
`core::str::from_utf8` directly under dictionary construction accounts for
714/3438, 841/3665 and 767/3663 main-thread samples (20.8–22.9%). These profiled
calls are attribution evidence only; sampling changes execution and cannot be
used for the retention timing gate. Evidence lives under
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/q29-current-cpu-sampling-receipt.json`
and guarded run `full43_20260920T092141822185Z`.

## Candidate contract

Use a private exact byte-to-ID directory with cached content hashes. On lookup,
compare full bytes against the existing validated `Arc<str>` value. Equality to
that value proves duplicate UTF8 validity. Validate a new distinct byte sequence
before creating its owned string and publishing its first-seen ID. Hash matches
alone never establish equality or validity. Reuse stored hashes during growth.

Keep the existing owned string vector, row IDs, null masks, source classification,
native Dict precedence, weighted consumers and retained MIN/MAX ownership. Null
rows are skipped before byte access. No new dependency, unsafe code, query-specific
dispatch, partitioning, recount elimination or provider execution is introduced.
This differs from the dropped owned-count partial: it changes only construction
of the same chunk dictionary, without rerouting owners or consumers.

The existing accessor has no memory-pool handle. This change must not claim
reservation accounting or an RSS limit. Checked directory allocation and growth
do not make existing Arc values or provider allocations reservation-owned.
The new directory is dropped inside `dictionary_build_nanos`; the former
HashMap was dropped after that nested timer. Complete accessor and process
timings retain the same boundary and govern comparisons.

## Vortex-first provider check

- Subject: adapter-local UTF8 dictionary identity and validity reuse.
- Checked provider: pinned Vortex 0.85.0 `VarBinViewArray`, `bytes_at`, validity,
  native Dict dispatch and the existing scan/canonical execution boundary.
- Decision: `implement_shardloom_kernel` for the private dictionary lookup;
  reuse the existing Vortex native provider for arrays, bytes and validity.
- Evidence surface: existing `aggregate_utf8_chunk_accessor` work counters and
  exact native aggregate result certificates.
- Residual handling and materialization: unchanged; no new decode, Arrow path,
  external engine or fallback. `fallback_attempted=false` remains required.
- Gate: collision/invalid UTF8/ownership/null correctness, then same-artifact
  alternating paired Q29 complete calls against the current merged binary.
  Retain for at least one second lower best valid complete call, or at least 30%
  lower OS peak RSS without complete-time regression. Preserve every sample.
  A retained candidate then requires Full43 complete-value UAT and broad checks.
- Broader CG-5/CG-6 and production memory/serving claims remain unchanged.
