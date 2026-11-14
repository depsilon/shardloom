# Shared source-demand connection evidence

The Sort and cardinality/expansion cohorts are separate, source-bound observations.
They reuse their exact original 10k baseline cases and execute each candidate once.
Their engine totals must not be combined into a synthetic full10k result.

Run `python3 -B verify.py` from any location. The verifier checks all archived bytes,
exact declaration/order/identity joins, dependency counts, saved engine clocks,
pipeline evidence, source equality across checks/build/install, and cohort totals.
It runs no query and needs only Python's standard library.

Complete results were independently replayed locally against the original reference
ledgers. Raw answer/reference ledgers and large runtime binaries remain local; this
compact verifier does not independently repeat answer correctness checks. Original
failed checks and their logs are preserved, separately from the accepted check set.

Native Vortex controls lack the comparable generic per-source column counter used
for Parquet/IPC. No whole-engine coverage, repeatability or release claim is made.
