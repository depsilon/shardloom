<!-- SPDX-License-Identifier: Apache-2.0 -->

# Independent native exchange fixtures

These are prospective input recipes for the bounded native exchange investigation.
No transport candidate or comparison result is supplied by these files.

`producer/` is a separate Cargo workspace pinned to unpatched upstream Vortex
0.85.0. Its executable writes native dtype and array IPC messages into a new
directory. It does not link ShardLoom or a query-engine integration. Its lockfile
starts from the earlier accepted independent producer graph and is separately
resolved without the unused file and Zstd features.

`reference.py` independently specifies every scalar value, logical schema and
projection result. It does not decode the native input. After the producer exits,
it creates three complete compressed JSONL references per fixture and a manifest
binding the original messages, complete references and representation-preserving
Arrow export fields. It refuses existing outputs and verifies compressed readback.

The 13 cases include empty/tiny/nullable plain records, typed all-NULL constants,
nonnullable constants, small/large dictionaries and a different dictionary
domain, run ends, bit packing, wide text, nested records/lists and sliced shared
children. Values include non-ASCII text, embedded NUL, empty text, exact integers
above binary64's integer precision and explicit validity. All three projections
fit the existing 65,536-row / 8-MiB JSON collection boundary so that the real
prepared relational engine clock can be used. One projection is repeated later;
its result is not cached.

Run both programs under the shared sequential local-storage/process guard.
Bulk outputs belong outside the checkout and synced directories. Resolve the
Cargo target directory explicitly; use a fresh output directory for every
attempt. Preserve failed-generation outcomes and changed recipe identities.
Before a candidate is implemented or timed, verify all complete native values
and schemas against these references, exercise every preserving Arrow codec
control, and freeze the actual input, recipe, manifest and control hashes with
the complete protocol and numerical gates. Recipe presence alone is not that
freeze or fixture acceptance.

The producer writes its native messages without console summaries. Fixture counts,
sizes and checksums are recorded by `reference.py` in the generated manifest.
The earlier frozen experiment retains its original recipe and manifest identities.
