<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native temporal calendar provider review

Status: implementation and acceptance in progress under item 20. This review
records the prospective dependency and representation boundaries; it is not a
release, complete temporal-support or performance claim.

## Dependency decision

The existing lockfile already contains Jiff 0.2.35, jiff-core 0.1.0 and
jiff-tzdb 0.1.6 through the pinned Vortex dependency graph. The temporal work
adds exact direct workspace declarations for Jiff and its database, both
`Unlicense OR MIT`, selecting the MIT-compatible permission. They are optional
dependencies of `shardloom-core`, behind `temporal-calendar`, activated by the
`shardloom-vortex` `native-temporal` feature used by native reads/execution and
writes. The default workspace remains free of the calendar provider.

Jiff is a civil-calendar/timezone library, not an execution engine. ShardLoom
calls its checked calendar/offset conversion APIs from admitted native kernels.
No query-engine integration, external executor, host timezone inference,
filesystem lookup or network access is introduced. The pinned database embeds
IANA **2026a** rules. `BoundTimeZone` reads the database's TZif bytes directly,
binds immutable rules once per expression and exposes a conservative workspace
reservation before parsing. Its parsed state must fit that reservation.
The native binder remains responsible for holding the corresponding credit.
The vendored Vortex array provider also pins `jiff-tzdb` 0.1.6 directly for
timestamp scalar validation and diagnostic display. Named-zone scalar admission
only checks the immutable name inventory; display resolves embedded TZif bytes.
Neither path uses Jiff's host-database lookup.

Native timestamps retain their full signed-64-bit tick range. Calendar operations
use their narrower declared range and reject invalid conversions. Unit scaling
uses signed-128-bit integer intermediates, with exact divisibility and final
range checks. No floating-point timestamp conversion is admitted.

## Pinned-provider correction

The focused calendar tests exposed a Jiff 0.2.35/jiff-core 0.1.0 boundary:
`Timestamp::from_nanosecond` checks whether whole seconds fit i64, then reaches
`new_unchecked` without checking the narrower timestamp calendar bounds. An
i64-second endpoint therefore hits a debug assertion. ShardLoom's wrapper uses
`Timestamp::new(seconds, signed_subsecond)` instead. The original failing
observation is retained; the corrected tests cover both storage endpoints and
values immediately outside the calendar endpoints. No dependency version was
silently changed to avoid the failure.

The existing vendored Vortex 0.90.0 timestamp patch is expanded from naive
microseconds to all four fixed units and valid timezone metadata. Native scalar
construction and statistics preserve raw ticks independently of calendar range.
Display uses checked constructors and falls back to exact ticks plus metadata
outside that range. Date display also uses checked calendar arithmetic, and
time-of-day scalar admission validates its half-open daily domain before
constructing any bounded provider value. These are documented local changes to
the Apache-2.0 provider, not copied code from another engine.

## Duration representation

Vortex 0.90.0 supplies Date, Time and Timestamp extensions but no fixed-duration
extension. `shardloom.duration.v1` is a first-party extension registered through
Vortex's dtype registry. It stores signed i64 ticks and exactly one metadata byte:
`0=s`, `1=ms`, `2=us`, `3=ns`. Unknown tags, extra bytes and non-i64 storage fail.
Calendar months and civil days are not encoded as fixed durations. Native
extension recognition checks the typed vtable metadata and physical type.
The native-temporal feature registers and explicitly enables the first-party
`shardloomtemporal2026.10.0` edition for that dtype alone. It is a draft until
accepted in a named ShardLoom release; it does not extend Vortex's frozen core
membership or enable optional arrays, layouts, aggregate functions or codecs.
Readers without this extension must reject duration files explicitly. The
initial persistence test caught the missing edition admission and is retained
with the other unsuccessful observations.
Arrow duration import/export needs an explicit translation in the existing
compatibility boundary; unsupported paths remain blocked until that is tested.

## Evidence and remaining acceptance

The first public multi-format temporal workflow exposed two additional boundary
defects. The prepared-file identity reader now registers the same duration
extension and edition as the writer. Parquet source admission now retains its
embedded Arrow schema: ignoring that footer metadata discarded duration and
timezone identity. Footer parsing remains under source metadata admission and
does not read row pages.

The pinned Parquet provider also needs a narrow original correction. PyArrow
25.0.1 writes second timestamps as millisecond Parquet columns while retaining
second-unit Arrow metadata. When those units differ, the reader now preserves
the stored Parquet unit and the declared presentation timezone instead of
dropping the zone. It does not reinterpret raw ticks as the old unit. INT96
keeps its existing explicit conversion rule. The public workflow supplies
independent expected milliseconds and timezone values; the provider unit test
covers all stored/hinted unit pairs and the INT96 distinction. No dependency
version or external execution engine is introduced.

The full-domain nested Arrow-file readback exposed an independent Vortex
DateTimeParts defect: its 0.90.0 compressor narrows the timestamp day count to
i32 before compression. Second and millisecond endpoints are silently truncated.
The new, separately recorded `vortex-datetime-parts` path patch retains i64 days
in splitting, scalar access and encoded comparisons. Its existing canonical
decoder and serialized child types already admit that width, and downstream
compression still narrows observed values. Seconds/subseconds remain i32.
The transient uncompressed day buffer is four bytes per row larger. This uses
the existing native provider with no layout/extension ID change; it does not
restore the older comparison patch, which 0.90.0 already incorporates.
The [provider note](../../vendor/vortex-datetime-parts/README.shardloom.md)
and archive/member provenance distinguish this new correction from that retired
patch. Full signed storage remains distinct from bounded calendar operations.

The initial core suite passes eight focused tests after the checked-constructor
correction. It covers canonical metadata, exact conversions, full timestamp
storage endpoints, Gregorian/month-end behavior, leap-second rejection, New
York gaps/folds and 23/25-hour days, Lord Howe's half-hour fold, fixed offsets,
unknown zones and calendar range limits. Native array/provider, input/output,
five-interface, package and broad regression acceptance remains in progress.
The parent [implementation contract](../architecture/native-analytical-completion-2026-10-27.md#temporal-fidelity-implementation-contract)
owns those obligations.

Sources: pinned registry Cargo manifests and license files; pinned Jiff and
jiff-core constructor sources; pinned jiff-tzdb `lib.rs`, `tzname.rs` and `COPYING`;
vendored Vortex `extension/datetime` and `dtype/session.rs` sources;
[Jiff's timezone and ambiguity API](https://docs.rs/jiff/0.2.35/jiff/tz/struct.TimeZone.html).
