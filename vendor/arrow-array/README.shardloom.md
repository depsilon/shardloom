# ShardLoom Arrow array provider corrections

This is the `arrow-array` 59.3.2 registry source, licensed Apache-2.0 AND MIT.
`upstream-provenance.json` records the exact archive, original source members,
and preserved Apache license, MIT license and notice files. The root lockfile
owns runtime resolution; `publish = false` prevents publishing this provider.

All three fixed-size-list builder finish modes pass their already-known row
count to the existing checked array constructor. Inferring length from an empty
child array loses every present row when list width is zero and no null bitmap
is needed. Rank-zero variable tensors expose this in their empty shape field.
Nullability, payload values and nonzero-width construction are unchanged.

An original regression covers finish, cloned finish and value-preserving finish
with all-present, mixed-null and all-null zero-width lists. ShardLoom's typed
row and persistence tests exercise the same boundary through tensor values.
There is no new array layout, dependency version, unsafe code or execution
provider.
The standalone strict Clippy check also requires the upstream union builder's
values-only map iteration to use `values_mut()`; iteration and mutation are
unchanged.

The C stream importer preserves extension declarations carried by dictionary
values. Arrow Rust represents these declarations on the containing field;
the upstream schema conversion loses dictionary-value metadata. The original
`ffi_stream/schema.rs` adapter retains the parsed physical shape, field flags,
unrelated field metadata and nested child declarations. Contradictory extension
identities or metadata on indices and values fail explicitly. Unannotated
binary remains unannotated, and incomplete or unknown extension declarations
remain visible to the consumer's admission policy.

Original provider regressions cover dictionary chains, nested lists, structs,
maps, unions and run-end values, matching and conflicting declarations and
unannotated/incomplete cases. Public ShardLoom UUID tests exercise actual C
stream data and file persistence. This changes schema import only; payload
buffers and their release callbacks retain the existing provider ownership.
The existing Apache-2.0/MIT provider and version remain unchanged.

The November 17 checkpoint adds intrinsic release of built-in nested arrays
through typed final-owner hooks, preserving independently retained children and
declaration-order cleanup, including unwind. Typed array-to-descriptor export
uses ordered continuations while preserving payload buffers, offsets, validity,
metadata and custom-array callback order. Test-only frozen export bodies check
the original shallow behavior.

Follow-up completion adds ordered iterative descriptor-to-array reconstruction
(`src/array/data_import.rs` and `data_import_tests.rs`) across `make_array` and
concrete `From<ArrayData>` / `TryFrom<ArrayData>` entrypoints for built-in
nested arrays (`Struct`, `List`, `LargeList`, `ListView`, `LargeListView`,
`FixedSizeList`, `Map`, `Union`, `Dictionary`, and `Run`). Pending frames unwind
innermost-first on partial or invalid nested descriptors without unsafe code,
preserving constructor checks, slices, metadata, panic/error order, and public
generic bounds. Original regressions cover shallow descriptor parity,
out-of-order union type ids, all dictionary/run-end widths, invalid descriptors,
and 8,192-level (512-level dictionary) 128-KiB small-stack round trips and
unwind cleanup.

Step 3C completion adds ordered iterative slicing (`src/array/slice_iter.rs`) for
`StructArray`, `FixedSizeListArray`, and sparse `UnionArray`, replacing recursive
`child.slice(offset, len)` descent across mixed nested Arrow hierarchies and
`RecordBatch::slice`. Original regressions cover shallow sparse/dense union
parity, 128-level public `UnionArray::try_new` slicing, and 2,048-level pure
`Struct` and mixed `Struct`/`FixedSizeList`/sparse `Union` `RecordBatch::slice`
on 128-KiB stacks.
