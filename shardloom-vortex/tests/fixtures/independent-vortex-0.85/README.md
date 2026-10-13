# Independent Vortex 0.85 fixtures

These twelve files were exported by the original Apache-2.0 test producer in
`producer/`, built and executed outside the ShardLoom workspace. Its empty
workspace, exact Vortex 0.85.0 dependency and retained lockfile resolve the
unpatched crates.io provider. No ShardLoom runtime or query-engine integration
is linked into that producer. `provenance.json` records source hashes, the
toolchain, resolved package identities, fixture sizes/hashes and export timings.
Those single observations are fixture provenance, not comparative benchmarks.

The four groups cover nullable numeric/Boolean/UTF-8 values, Constant/Dict/RunEnd/
BitPacked physical representations, nested nullable list elements and empty
records. Each group has a public native file plus separate native IPC dtype and
array messages. They occupy 467,383 bytes in total.

For reproduction, copy `producer/` to a new directory outside the workspace,
verify its three source hashes, resolve Cargo's target directory to local-only
storage, and build with the retained lockfile. Run the producer with a new,
nonexistent output directory as its sole argument. The producer refuses to
replace existing files. Apply the repository's local storage and serial workload
guards. The exported values and schemas must match; future toolchain or provider
changes must create a separate fixture identity instead of rewriting provenance.
