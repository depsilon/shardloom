<!-- SPDX-License-Identifier: Apache-2.0 -->

# Vector expressions

These functions are available in matching native source builds after 1.1.0;
they are not included in the published 1.1.0 wheels. SQL and
`shardloom.functions` use the same native execution and admission rules.

Use Float16, Float32 or Float64 lists, fixed-size lists or dense embeddings for
vector arithmetic. Integer lists, tensors, sparse vectors, quantized vectors
and collections of vectors are not implicitly converted into dense vectors.
Sparse and quantized vectors support dimension inspection; tensors support
shape inspection. Unsupported operations fail explicitly without an external
query engine.

## Compute and select components

```python
import shardloom as sl
from shardloom import functions as f, types as t

with sl.context(transport="native", memory_bytes=128 << 20,
                max_parallelism=1) as context:
    vectors = context.from_rows(
        [{"id": 1, "v": [3.0, 4.0]}, {"id": 2, "v": [0.0, 0.0]}],
        schema={"id": t.int64(), "v": t.fixed_size_list(t.float32(), 2)},
    )
    result = vectors.select(
        "id", f.vector_norm("v").alias("length"),
        f.vector_normalize("v").alias("unit"),
        f.vector_element("v", -1).alias("last"),
    ).collect(check=True)
    assert result.to_rows() == (
        {"id": 1, "length": 5.0, "unit": [0.6, 0.8], "last": 4.0},
        {"id": 2, "length": 0.0, "unit": None, "last": 0.0},
    )
```

Strings in column positions name columns. Use floating literals such as
`f.array([3.0, 4.0])` for an ordinary constant query vector. Integer-valued
arrays do not become floating vectors implicitly. Empty or all-null literal
arrays receive a floating component type only in dense-vector argument
positions. Constant floating query vectors can exceed 128 components within
the existing declaration and memory limits. SQL `ARRAY[...]` accepts literals
only. Nonliteral list constructors in the typed native plan API retain their
128-element limit.

| SQL function | Python function | Result |
| --- | --- | --- |
| `VECTOR_DIMENSION(v)` | `f.vector_dimension(v)` | UInt64 dimension, or null for a null vector |
| `VECTOR_VALUES(v)` | `f.vector_values(v)` | Original dense list storage; embedding identity removed |
| `VECTOR_ELEMENT(v, i)` | `f.vector_element(v, i)` | One component with its original floating type |
| `VECTOR_SLICE(v, start[, end])` | `f.vector_slice(v, start[, end])` | Ordinary variable list with the original component type |
| `VECTOR_DOT(a, b)` | `f.vector_dot(a, b)` | Float64 dot product |
| `VECTOR_L2_DISTANCE(a, b)` | `f.vector_l2_distance(a, b)` | Float64 Euclidean distance |
| `VECTOR_COSINE_SIMILARITY(a, b)` | `f.vector_cosine_similarity(a, b)` | Float64 cosine similarity |
| `VECTOR_NORM(v)` | `f.vector_norm(v)` | Float64 Euclidean norm |
| `VECTOR_NORMALIZE(v)` | `f.vector_normalize(v)` | Nullable list or fixed-size list of nonnull Float64 components |
| `TENSOR_SHAPE(t[, layout])` | `f.tensor_shape(t, layout="logical")` | Fixed-size list of nonnull UInt64 dimensions |

Element indices start at zero; negative indices count from the end. An
out-of-range or null index returns null. Slices include `start` and exclude
`end`, clamp bounds to the vector, and accept negative bounds. An omitted end
means the vector length; an explicit null end returns null. An end before the
start yields an empty list. Boolean controls are not integer indices.

Selection retains component nulls, signed zeros, floating widths and original
NaN payload bits under either float policy. `VECTOR_NORMALIZE` retains fixed
versus variable list shape but returns ordinary Float64 storage. Slices always
return ordinary variable lists. Neither result claims an embedding identity.
Native result delivery preserves explicitly selected values through aliases,
filters, limits, ordering and spill. Other operators retain their existing
payload validation, and arithmetic or sort keys still observe numeric values
under the selected policy. Use `to_arrow()` when original floating bits matter.

## Filtered retrieval and embedding identity

For a source with `id`, `eligible` and `v` columns, filter candidates using the
ordinary eligibility column, score each selected vector, and declare a
secondary order for score ties:

```sql
SELECT id,
       VECTOR_COSINE_SIMILARITY(v, ARRAY[3.0, 4.0]) AS score,
       VECTOR_L2_DISTANCE(v, ARRAY[3.0, 4.0]) AS distance
FROM candidates
WHERE eligible
ORDER BY score DESC NULLS LAST, id ASC
LIMIT 10
```

With an active Python context, bind that source as `context.sql(statement,
relations={"candidates": candidate_frame})`. This is exact candidate scoring through
the native engine, with no ANN index. Floating scores can tie after rounding;
the declared secondary order resolves those ties deterministically.

Binary arithmetic between embeddings requires equal dimensions, equal known
`space_id`, and matching `model_id`, `model_version`, `representation_version`
and `preprocessing_id`. Missing or null secondary fields match each other only
after the known space identifier matches. `normalization` and
`source_provenance` are not coordinate-space identity fields.

Comparing an embedding with a bare query list requires an explicit decision to
drop the embedding identity:

```python
score = f.vector_cosine_similarity(
    f.vector_values("embedding"), f.array([3.0, 4.0])
)
```

`VECTOR_VALUES` removes identity; it does not establish that the query belongs
to the same space. Supply compatible coordinates yourself. It does not densify
sparse vectors, dequantize codes, flatten tensors or merge vector collections.

## Nulls, dimensions and floating policy

A null vector parent produces null. For two present vectors, unequal lengths
fail before component-null handling. Any null component then makes an
arithmetic result null, without treating that component as zero or skipping it.
Known fixed-dimension and embedding-identity conflicts fail during binding,
even for an empty input. A null parent does not hide errors while evaluating
another argument expression.

Present empty vectors have dimension zero and dot product, norm and distance
zero. Cosine is null when either finite norm is zero; normalization of a finite
zero vector is null. Normalization otherwise preserves the sign of zero
components. Exact-zero dot products and cosine results are positive zero.

The default strict policy rejects observed nonfinite components and nonfinite
arithmetic results. Selection and inspection do not turn carried components
into arithmetic observations. Extended policy admits infinities and NaNs:
cosine with any nonfinite component is NaN; a NaN norm produces NaN normalized
components; an infinite norm produces signed zero for finite components and
NaN for infinite components. Newly computed NaNs use canonical Float64 bits.

Finite dot products, norms and distances round the exact real result once to
nearest-even Float64, including subnormal and overflow boundaries. Cosine and
normalization use bounded relative error with a final subnormal allowance;
they avoid false overflow from large intermediate products. The
[numerical and semantic contract](../architecture/native-vector-expressions-2026-11-08.md)
specifies the exact bounds and nonfinite cases. No infinite-precision ranking
or performance claim is implied.

## Inspect tensor shapes and other vector representations

`TENSOR_SHAPE(t)` returns logical dimensions. `TENSOR_SHAPE(t, 'physical')`
returns physical storage dimensions. In Python, use `f.tensor_shape("t")` or
`f.tensor_shape("t", "physical")`. A physical shape `[2, 3]` with permutation
`[1, 0]` returns `[3, 2]` by default. A null tensor produces null, rank zero
produces an empty shape list, and zero dimensions remain zero. Shape inspection
also supports admitted integer and Boolean tensor components; it does not
perform tensor arithmetic or read component values.

For sparse vectors, `VECTOR_DIMENSION` returns the declared size, not the
number of stored coordinates. For quantized vectors, it returns the code-list
length. It validates declared uniform dimensions without reconstructing
coordinates or floating values. Existing input validation still applies.
Inspection alone does not establish a file-I/O saving.

See [rich-column declarations and persistence](rich-columns.md) and
[variable-shape tensors](variable-shape-tensors.md) for source schemas and
format boundaries. Computed vector results can use the existing native writes,
streaming, and explicit native spill routes; Vortex remains the native output.
For PyArrow downstream readers, use Vortex or Arrow IPC when fixed-size-list
parents may be null: the pinned PyArrow 25.0.1 Parquet reader cannot read that
case. ShardLoom's native Parquet readback retains the complete nullable results.
