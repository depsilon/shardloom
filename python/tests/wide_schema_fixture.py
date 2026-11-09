"""Fixed wide-schema inputs and independent ordinal-formula expectations."""

from __future__ import annotations

import hashlib
import json

KINDS = ("siblings", "nodes", "metadata")
METADATA = {"model_id": "wide-schema", "space_id": "Ω", "version": 1}


def schema(kind):
    from shardloom import types as t
    if kind == "siblings":
        payload = t.struct({f"f{field:04}": t.int64() for field in range(1025)})
    elif kind == "nodes":
        payload = t.struct({f"b{branch:03}": t.list_(t.struct(
            {f"f{field}": t.int64() for field in range(8)})) for branch in range(512)})
    elif kind == "metadata":
        payload = t.struct({f"b{branch:02}": t.struct(
            {f"f{field:03}": t.int64() for field in range(850)}) for branch in range(10)})
    else:
        raise ValueError(kind)
    return {"id": t.int64(nullable=False), "payload": payload,
            "extra": t.struct({"embedding": t.embedding(t.float32(), 2, metadata=METADATA)})}


def arrow_schema(pa, kind, *, joined=False):
    # This expected Arrow schema is built independently of ShardLoom's adapters.
    if kind == "siblings":
        payload = pa.struct([pa.field(f"f{field:04}", pa.int64()) for field in range(1025)])
    elif kind == "nodes":
        payload = pa.struct([pa.field(f"b{branch:03}", pa.list_(pa.struct(
            [pa.field(f"f{field}", pa.int64()) for field in range(8)]))) for branch in range(512)])
    elif kind == "metadata":
        payload = pa.struct([pa.field(f"b{branch:02}", pa.struct(
            [pa.field(f"f{field:03}", pa.int64()) for field in range(850)])) for branch in range(10)])
    else:
        raise ValueError(kind)
    embedding = pa.field("embedding", pa.list_(pa.field("item", pa.float32()), 2), metadata={
        b"ARROW:extension:name": b"shardloom.embedding.v1",
        b"ARROW:extension:metadata": json.dumps(METADATA, ensure_ascii=False, sort_keys=True,
                                               separators=(",", ":")).encode()})
    fields = [pa.field("id", pa.int64(), nullable=False), pa.field("payload", payload),
              pa.field("extra", pa.struct([embedding]))]
    if joined:
        fields.append(pa.field("tag", pa.int64()))
    return pa.schema(fields)


def row(kind, ordinal):
    def leaf(branch, field, position=0):
        return (None if (ordinal + branch + field + position) % 17 == 0
                else ordinal * 1_000_000 + branch * 10_000 + position * 1000 + field)

    if ordinal % 5 == 0:
        payload = None
    elif kind == "siblings":
        payload = {f"f{field:04}": leaf(0, field) for field in range(1025)}
    elif kind == "nodes":
        payload = {}
        for branch in range(512):
            payload[f"b{branch:03}"] = (None if (ordinal + branch) % 11 == 0 else
                [] if (ordinal + branch) % 7 == 0 else
                [{f"f{field}": leaf(branch, field, 0) for field in range(8)}, None,
                 {f"f{field}": leaf(branch, field, 1) for field in range(8)}])
    elif kind == "metadata":
        payload = {f"b{branch:02}": None if (ordinal + branch) % 13 == 0 else
                   {f"f{field:03}": leaf(branch, field) for field in range(850)}
                   for branch in range(10)}
    else:
        raise ValueError(kind)
    extra = None if ordinal % 4 == 0 else {"embedding": None if ordinal % 3 == 0 else
        [float(ordinal), None if ordinal % 2 else -float(ordinal)]}
    return {"id": ordinal, "payload": payload, "extra": extra}


def tags(ordinal):
    return [2, 1] if ordinal % 3 == 0 else [1] if ordinal % 2 == 0 else [None]


def joined_rows(kind, count=8):
    return [{**row(kind, ordinal), "tag": tag}
            for ordinal in range(count, 1, -1) for tag in tags(ordinal)]


def digest(values):
    return hashlib.sha256(json.dumps(values, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()
