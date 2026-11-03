# SPDX-License-Identifier: Apache-2.0
"""Independent bounded replay of the pinned Vortex 0.90 footer/range evidence."""

from collections import Counter
import struct


def verify_ranges(tail: bytes, report: dict, original_prepared: dict, inputs: dict) -> None:
    """Verify footer structure and the captured range-accounting models without file I/O."""

    def need(ok, message):
        if not ok:
            raise ValueError(message)

    def identity(doc, path):
        return next(x for x in doc["files"] if x["path"] == path)

    case = report
    prepared = next(
        x
        for x in original_prepared["profiles"]["scale"]["inputs"]
        if x["path"].endswith("/fact-00000.vortex")
    )
    parquet_path = inputs["profiles"]["scale"]["sources"]["parquet"]["fact"][0]
    parquet = identity(inputs, parquet_path)
    need(case["vortex"] == prepared and case["parquet"] == parquet, "input identities differ")
    need(case["status"] == "verified" and case["query_executions"] == 0, "invalid report status")
    size = prepared["generation"]["bytes"]
    ranges = case["footer_segments"]
    start = min(x["offset"] for x in ranges.values())
    need(len(tail) == size - start, "tail length does not match frozen file extent")

    class Tables:
        def __init__(self, raw):
            self.raw = raw
            self.root = self.read("I", 0)

        def read(self, kind, pos):
            n = struct.calcsize("<" + kind)
            need(isinstance(pos, int) and 0 <= pos <= len(self.raw) - n, "table pointer out of bounds")
            return struct.unpack_from("<" + kind, self.raw, pos)[0]

        def slot(self, table, n):
            need(0 <= table <= len(self.raw) - 4, "table out of bounds")
            vt = table - self.read("i", table)
            need(0 <= vt <= len(self.raw) - 4, "vtable out of bounds")
            vlen = self.read("H", vt)
            pos = 4 + 2 * n
            need(vlen >= 4 and vt + vlen <= len(self.raw), "invalid vtable extent")
            if pos + 2 > vlen:
                return None
            delta = self.read("H", vt + pos)
            if not delta:
                return None
            need(table + delta < len(self.raw), "slot pointer out of bounds")
            return table + delta

        def scalar(self, table, n, kind, default=0):
            p = self.slot(table, n)
            return default if p is None else self.read(kind, p)

        def ref(self, p):
            d = self.read("I", p)
            q = p + d
            need(0 < d and q < len(self.raw), "relative pointer out of bounds")
            return q

        def child(self, table, n):
            p = self.slot(table, n)
            return None if p is None else self.ref(p)

        def vector(self, table, n, width, limit=2000000):
            p = self.child(table, n)
            if p is None:
                return ()
            count = self.read("I", p)
            need(count <= limit and p + 4 + count * width <= len(self.raw), "vector extent/size invalid")
            return range(p + 4, p + 4 + count * width, width)

        def string(self, p):
            count = self.read("I", p)
            need(count <= 512 and p + 5 + count <= len(self.raw), "string extent invalid")
            need(self.raw[p + 4 + count] == 0, "string terminator invalid")
            return self.raw[p + 4 : p + 4 + count].decode("utf8")

    # The packed optional user metadata precedes the standard Vortex footer.
    need(8 <= len(tail) <= 8 << 20, "unexpected footer tail size")
    trailer = tail[-8:]
    version, ps_len, magic = struct.unpack("<HH4s", trailer)
    need(magic == b"VTXF" and 0 < ps_len <= 65527, "bad Vortex postscript")
    ps_start = len(tail) - 8 - ps_len
    ps = Tables(tail[ps_start : ps_start + ps_len])

    def segment(i, name):
        d = ps.child(ps.root, i)
        need(d is not None, "missing postscript segment")
        off = ps.scalar(d, 0, "Q")
        length = ps.scalar(d, 1, "I")
        need(0 < length <= 16 << 20 and off + length <= size - 8 - ps_len, "segment extent invalid")
        need(
            ps.child(d, 3) is None and ps.child(d, 4) is None,
            "encoded metadata is outside this evidence format",
        )
        need(ranges[name] == {"offset": off, "bytes": length}, "footer segment report mismatch")
        local = off - start
        need(0 <= local and local + length <= len(tail), "segment not contained in supplied tail")
        return Tables(tail[local : local + length])

    dtype = segment(0, "dtype")
    layout = segment(1, "layout")
    segment(2, "statistics")
    footer = segment(3, "footer")
    need(version == case["version"], "version mismatch")
    # Cross-check user metadata table and its location.
    metas = {}
    for p in ps.vector(ps.root, 4, 4, 1024):
        m = ps.ref(p)
        name = ps.string(ps.child(m, 0))
        spec = ps.child(m, 1)
        metas["metadata/" + name] = {"offset": ps.scalar(spec, 0, "Q"), "bytes": ps.scalar(spec, 1, "I")}
    need(
        metas == {k: v for k, v in ranges.items() if k.startswith("metadata/")},
        "user metadata report mismatch",
    )
    need(dtype.scalar(dtype.root, 0, "B") == 7, "unsupported dtype root")
    typ = dtype.child(dtype.root, 1)
    need(typ is not None and dtype.scalar(typ, 2, "B") == 0, "unexpected dtype schema")
    names = [dtype.string(dtype.ref(p)) for p in dtype.vector(typ, 0, 4, 128)]
    need(
        len(names) == 40 and len(set(names)) == 40 and set(names) == set(case["per_field"]),
        "schema field set invalid",
    )
    enc = [footer.string(footer.child(footer.ref(p), 0)) for p in footer.vector(footer.root, 1, 4, 200000)]
    ext = [(footer.read("Q", p), footer.read("I", p + 8)) for p in footer.vector(footer.root, 2, 16, 200000)]
    need(
        len(ext) == 50713 and all(0 < n <= 16 << 20 and p + n <= size for p, n in ext),
        "layout extents invalid",
    )
    ordered = sorted(ext)
    need(
        all(a + n <= b for (a, n), (b, _) in zip(ordered, ordered[1:])) and sum(ordered[-1]) == start,
        "physical segments overlap or extend into metadata",
    )
    visits = 0

    def describe(node):
        nonlocal visits
        visits += 1
        need(visits <= 150000, "layout exceeds the pinned node bound")
        ei = layout.scalar(node, 0, "H")
        need(ei < len(enc), "encoding index invalid")
        kids = [layout.ref(p) for p in layout.vector(node, 3, 4, 1024)]
        ids = [layout.read("I", p) for p in layout.vector(node, 4, 4, 256)]
        need(all(i < len(ext) for i in ids), "segment id out of range")
        return enc[ei], layout.scalar(node, 1, "Q"), kids, ids

    kind, rows, chunks, ids = describe(layout.root)
    need((kind, rows, len(chunks), ids) == ("vortex.chunked", 5000000, 611, []), "unexpected root layout")
    own = {n: set() for n in names}
    selected_chunks = []
    chosen_names = {"group_key", "category", "signed_value"}
    need(set(case["projected_group_fields"]) == chosen_names, "selected fields mismatch")
    for chunk in chunks:
        ck, _, children, cids = describe(chunk)
        need(ck == "vortex.struct" and not cids and len(children) == len(names), "unexpected chunk schema")
        selected = set()
        for name, child in zip(names, children):
            stack = [child]
            found = set()
            steps = 0
            while stack:
                steps += 1
                need(steps <= 10000, "layout nesting/node limit exceeded")
                _, _, more, sids = describe(stack.pop())
                found.update(sids)
                stack.extend(more)
            own[name].update(found)
            if name in chosen_names:
                selected.update(found)
        selected_chunks.append(selected)
    owners = Counter(i for group in own.values() for i in group)
    need(
        set(owners) == set(range(len(ext))) and set(owners.values()) == {1}, "segment ownership is not exact"
    )
    fields = {n: {"segments": len(v), "segment_bytes": sum(ext[i][1] for i in v)} for n, v in own.items()}
    for n, row in fields.items():
        expected = case["per_field"][n]
        need(
            row["segments"] == expected["segments"] and row["segment_bytes"] == expected["segment_bytes"],
            "per-field counts/bytes differ",
        )
        pc = expected["parquet_compressed_chunk_bytes"]
        need(
            pc >= 0 and expected["vortex_segment_minus_parquet_chunk_bytes"] == row["segment_bytes"] - pc,
            "captured Parquet reconciliation invalid",
        )
    selected = set().union(*(own[n] for n in chosen_names))
    selected_bytes = sum(ext[i][1] for i in selected)
    need(
        len(selected) == case["projected_subtree_segments"]
        and selected_bytes == case["projected_subtree_bytes"],
        "selected subtree totals differ",
    )

    def merge(ids, gap, span):
        out = []
        for p, n in sorted(ext[i] for i in ids if ext[i][1]):
            end = p + n
            if out and p - out[-1][1] <= gap and end - out[-1][0] <= span:
                out[-1][1] = max(out[-1][1], end)
            else:
                out.append([p, end])
        return {"ranges": len(out), "bytes": sum(b - a for a, b in out)}

    gap = case["model_gap"]
    span = case["model_max_span"]
    need((gap, span) == (1 << 20, 4 << 20), "coalescing model policy differs")
    whole = merge(selected, gap, span)
    need(whole == case["all_selected_ranges_sorted_together"], "sorted-all coalescing differs")
    per = [merge(v, gap, span) for v in selected_chunks]
    need(
        {"ranges": sum(x["ranges"] for x in per), "bytes": sum(x["bytes"] for x in per)}
        == case["sort_ranges_separately_per_root_chunk"],
        "per-chunk coalescing differs",
    )
    sens = {str(d): merge(selected, d, span) for d in (0, 1024, 4096, 16384, 65536, 262144, gap)}
    need(sens == case["sorted_pending_set_gap_sensitivity"], "gap sensitivity differs")
    # Reported prefix estimate is tied to the observed file size and footer offsets.
    initial = case["initial_footer_read"]
    need(initial == 65535, "unexpected initial read size")
    minimum = min(ranges[k]["offset"] for k in ("dtype", "layout", "statistics", "footer"))
    missing = size - initial - minimum
    need(
        missing == case["missing_footer_prefix"]
        and missing + 256
        == case["missing_prefix_plus_allocator_alignment"]
        == case["sampled_read_allocation_bytes"],
        "footer-prefix arithmetic differs",
    )
    need(
        case["layout_segments"] == len(ext)
        and case["maximum_layout_segment_bytes"] == max(n for _, n in ext),
        "layout summary differs",
    )
    voutside = size - sum(v["segment_bytes"] for v in fields.values())
    poutside = parquet["generation"]["bytes"] - sum(
        x["parquet_compressed_chunk_bytes"] for x in case["per_field"].values()
    )
    change = (
        sum(x["vortex_segment_minus_parquet_chunk_bytes"] for x in case["per_field"].values())
        + voutside
        - poutside
    )
    need(
        voutside == case["vortex_bytes_outside_layout_segments"]
        and poutside == case["parquet_bytes_outside_compressed_chunks"],
        "outside-byte reconciliation differs",
    )
    need(
        change == case["vortex_minus_parquet_file_bytes"] == size - parquet["generation"]["bytes"],
        "file-size delta reconciliation differs",
    )
