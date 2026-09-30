# R2.b: bounded dictionary preparation

Status: admitted prototype; no performance acceptance yet. This closes the last
candidate in the September 26 intake before its final profiling refresh. It does
not resume the release train or the paused format comparison.

The retained R10 Q29 cohort spends 2.348–2.370 seconds building first-seen UTF8
dictionaries, 2.254–2.266 seconds in the provider, and 1.813–1.830 seconds in
aggregate updates, within 6.743–6.790 second complete calls. These caller spans
motivate one preparation worker alongside the caller, with two outstanding jobs.
They do not establish the candidate's speedup.

Reuse the source-backed dictionary builder, `AggregateChunkJobs`, and the existing
transformed dense-general aggregate consumer. Canonicalization stays on the caller;
only dictionary preparation moves to the worker. Complete accessors are consumed
in source order, retaining their task lease and window through aggregation. This
preserves first-seen IDs, floating updates, HAVING, ties and independent owned-key
promotion. It does not revive the rejected owned-count-partial/recount recipe.

Admission requires the existing single-field, nonnullable UTF8, UrlDomain
dense-general proof, ordered completion, no residual predicate and no spill.
Existing native dictionaries drain older jobs and use the original consumer.
Prepared sessions that already own provider workers keep their current execution.
Upstream Vortex remains the native provider; no engine integration, new public
operator, dependency or Arrow execution boundary is introduced.

Each task pre-admits worst-case dictionary and row-directory capacity for at most
262,144 rows, plus native `nbytes` as an estimate. Native buffer capacity and
provider allocations are not fully exposed by that measure: this bounds retained
chunk count and reserved metadata, not RSS. Global aggregate state remains owned
and accounted under its existing contract. Initial capacity denial drains older
jobs and retires to the same serial builder/consumer. Errors after admission,
source corruption and cancellation fail without replay. A stage-local token links
to the operation token without cancelling successful prepared operations on drop.

Acceptance requires complete ordinary/prepared/owned values, active cancellation,
pressure retirement and refund, source-error behavior and schema rejection tests;
then one guarded paired Q29 screen. Retention requires full local correctness/UAT,
complete Full43, immutable evidence and PR checks. Slower candidates are dropped.

The prototype passes 808 native local-primitive tests (11 existing manual fixtures
ignored), native CLI/Vortex all-target Clippy and formatting. New tests cover
dictionary collision/capacity boundaries, native dictionary interleaving,
nullable/root/P1/metadata-pressure rejection, pre- and post-submission pressure,
prepared provider restoration, active operation cancellation, corruption and
typed source denial without a new worker replay. Existing cache-attempt policy is
unchanged. Initial fixture pressure was insufficient after its older task drained;
the corrected fixture grows the next chunk and proves retirement. A test-only
missing import and semicolon were corrected before freezing the candidate.
