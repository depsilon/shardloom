# ShardLoom Arrow data slice correction

This is the Apache-2.0 `arrow-data` 59.3.2 registry source. The exact archive,
every original source member, and unchanged license and notice files are recorded
in `upstream-provenance.json`. The root lockfile owns runtime resolution;
`publish = false` prevents publishing this provider.

A struct slice applies the complete parent offset to its children and resets
the parent descriptor offset to zero. The original code both sliced children
and retained the parent offset, causing typed Arrow construction to apply it
again. A sliced fixed-size list containing a struct exposed this as a bounds
panic at the Python C stream boundary, before ShardLoom tensor validation.
Independent child offsets, parent validity, payload buffers and row counts are
preserved. No buffer copy or unsafe block is added.

The original regression checks nested structs with independent parent and child
offsets, successive slices, visible values and validity. The variable-tensor
lifecycle also exercises Python C stream import of sliced structs, variable
lists and fixed-size lists. This correction retains the 59.3.2 validation fixes
and does not change the Arrow version or admit an execution fallback.
One unchanged upstream error assertion now uses `to_string()` instead of a
literal-only `format!` so the full test target passes strict current Clippy.

The November 17 checkpoint adds intrinsic child cleanup and ordered cloning,
iterative descriptor validation, memory inspection, alignment and slicing.
Subsequent deep-traversal completion also replaces recursive nested equality in
`src/equal/{mod,structure,list,list_view,fixed_list,dictionary,union,run}.rs`
with an explicit heap worklist (`EqualFrame`) tested on a 128-KiB stack in
`src/equal/equal_tests.rs`, and makes `MutableArrayData` construction,
`try_extend`, `try_extend_nulls`, `into_builder`/`freeze`, and `Drop` iterative in
`src/transform/{mod,boolean,fixed_binary,fixed_size_list,list,list_view,null,primitive,run,structure,union,variable_size}.rs`
tested on 128-KiB stacks at depth 512 and depth 2,048 (`src/transform/mod.rs`).
Frozen shallow references verify the original values, errors and competing-fault
order; deep tests cover retained descriptors, logical equality, and failure
cleanup on small stacks. Buffers remain shared. This provider work adds no unsafe
blocks and preserves public depth boundaries until cross-boundary checks complete.
