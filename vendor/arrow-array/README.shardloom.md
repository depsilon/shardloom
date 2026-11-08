# ShardLoom zero-width Arrow list builder correction

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
