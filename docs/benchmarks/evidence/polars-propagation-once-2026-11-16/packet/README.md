# One Polars observation on the saved 11,579 requests

Run `python3 -I -B verify.py` with Python 3.13 or later and every packet file
together. This replays stored values, contracts, transport and timing evidence;
it cannot execute either engine, start another process or use the network.
Allow at least the unchanged 12 GiB free-space floor plus the unpacked archive.

One attempt per original request: 11,489 passed and 90 mismatched. All 90 come
from the benchmark adapter's constant-only projection collapsing input rows.
The failed results, original references, declared plans and raw timings remain
unchanged. There is no accepted complete-set competitive total and no rerun.

The 11,489 passing requests use 101.07273444440216 seconds in Polars collect.
The same saved native IDs use 888.133524476 engine seconds. These are different
clock boundaries; no ratio is claimed. The native data are historical November
15 observations, not the latest runtime, and their original not-admitted work
and performance decision is retained. Polars engine_execution_seconds is null.

The frozen periodic memory guard passed. Sixty-nine OS process high-water
observations exceed its sampled 2.5 GiB threshold, with a maximum of 5,302,255,616
bytes. The packet preserves them and does not claim a hard peak-memory cap.

Included: both complete 11,579-result raw ledgers, frozen jobs and independent
references, original source bytes, input-equivalence/readback proofs, all failures,
preparation and capture receipts, and 1,120 offline corruption controls.
Bulk input files and installed execution libraries are identity-only; portable
verification checks their archived prior proofs and does not reopen those files.
Native Vortex slots have explicit IPC/Parquet counterparts for Polars; they do
not represent execution of Vortex encodings by Polars. Native interfaces and
encoding labels retain their original repeated slots, not additional Polars APIs.
