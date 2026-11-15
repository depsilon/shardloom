# Payload correctness-correction evidence

This packet records the payload validation correction and its complete 388-case
cohort. The payload cohort, full10k baseline, and sort-demand baseline are distinct
observations with distinct source commits; their results and timings are never
combined. The original design, frozen inputs and all retained failed/stopped
payload observations are preserved alongside the accepted evidence.

The complete payload answers were independently replayed locally against their
reference results. The portable packet preserves the case and certificate data,
but its verifier does not rerun the full oracle. This is a correctness correction,
not a speedup claim and not a whole-engine coverage claim.

Run `python3 -B verify.py` from this directory. The verifier checks the archived
bytes and source-bound cohort evidence without running queries.
