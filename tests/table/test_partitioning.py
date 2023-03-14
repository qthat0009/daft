from __future__ import annotations

import itertools

import numpy as np
import pytest

from daft.datatype import DataType
from daft.expressions2 import col
from daft.table import Table

daft_int_types = [
    DataType.int8(),
    DataType.int16(),
    DataType.int32(),
    DataType.int64(),
    DataType.uint8(),
    DataType.uint16(),
    DataType.uint32(),
    DataType.uint64(),
]

daft_numeric_types = daft_int_types + [DataType.float32(), DataType.float64()]
daft_string_types = [DataType.string()]


@pytest.mark.parametrize(
    "size, k, dtype", itertools.product([0, 1, 10, 33, 100], [1, 2, 3, 10, 40], daft_numeric_types + daft_string_types)
)
def test_table_partition_by_hash_single_col(size, k, dtype) -> None:
    table = Table.from_pydict(
        {"x": [i % k for i in range(size)], "x_ind": [i for i in range(size)]}
    ).eval_expression_list([col("x").cast(dtype), col("x_ind")])
    split_tables = table.partition_by_hash([col("x")], k)
    seen_so_far = set()
    for st in split_tables:
        unique_to_table = set()
        for x, x_ind in zip(st.get_column("x").to_pylist(), st.get_column("x_ind").to_pylist()):
            assert (x_ind % k) == int(x)
            unique_to_table.add(x)
        for v in unique_to_table:
            assert v not in seen_so_far
            seen_so_far.add(v)


@pytest.mark.parametrize(
    "size, k, dtype", itertools.product([0, 1, 10, 33, 100], [1, 2, 3, 10, 40], daft_numeric_types + daft_string_types)
)
def test_table_partition_by_hash_two_col(size, k, dtype) -> None:
    table = Table.from_pydict({"x": [i for i in range(size)], "x_ind": [i for i in range(size)]}).eval_expression_list(
        [(col("x") % k).cast(dtype), (col("x") % (k + 1)).alias("y"), col("x_ind")]
    )
    split_tables = table.partition_by_hash([col("x"), col("y")], k)
    seen_so_far = set()
    for st in split_tables:
        unique_to_table = set()
        for x, y, x_ind in zip(
            st.get_column("x").to_pylist(), st.get_column("y").to_pylist(), st.get_column("x_ind").to_pylist()
        ):
            assert (x_ind % k) == int(x)
            unique_to_table.add((x, y))
        for v in unique_to_table:
            assert v not in seen_so_far
            seen_so_far.add(v)


@pytest.mark.parametrize("size, k", itertools.product([0, 1, 10, 33, 100], [1, 2, 3, 10, 40]))
def test_table_partition_by_random(size, k) -> None:
    table = Table.from_pydict({"x": [i for i in range(size)]})
    split_tables = table.partition_by_random(k, 0)
    seen_so_far = set()

    total_split_len = sum([len(t) for t in split_tables])
    assert total_split_len == size

    for st in split_tables:
        for x in st.get_column("x").to_pylist():
            assert x not in seen_so_far
            seen_so_far.add(x)

    # ensure deterministic
    re_split_tables = table.partition_by_random(k, 0)
    assert all([lt.to_pydict() == rt.to_pydict() for lt, rt in zip(split_tables, re_split_tables)])

    if k > 1 and size > 1:
        diff_split_tables = table.partition_by_random(k, 1)

        assert [t.to_pydict() for t in split_tables] != [t.to_pydict() for t in diff_split_tables]


def test_table_partition_by_hash_bad_input() -> None:
    # negative sample

    table = Table.from_pydict({"x": [1, 2, 3], "b": [0, 1, 2]})

    with pytest.raises(ValueError, match="negative number"):
        table.partition_by_hash([col("x")], -1)

    with pytest.raises(ValueError, match="0 partitions"):
        table.partition_by_hash([col("x")], 0)


def test_table_partition_by_random_bad_input() -> None:
    # negative sample

    table = Table.from_pydict({"x": [1, 2, 3], "b": [0, 1, 2]})

    with pytest.raises(ValueError, match="negative number"):
        table.partition_by_random(10, -1)

    with pytest.raises(ValueError, match="0 partitions"):
        table.partition_by_random(0, 10)

    with pytest.raises(ValueError, match="negative number"):
        table.partition_by_random(-1, 10)


@pytest.mark.parametrize("size, k, desc", itertools.product([0, 1, 10, 33, 100], [1, 2, 3, 10, 40], [False, True]))
def test_table_partition_by_range_single_column(size, k, desc) -> None:
    table = Table.from_pydict({"x": np.arange(size, dtype=np.float64()), "x_ind": list(range(size))})

    original_boundaries = np.linspace(0, size, k)

    input_boundaries = original_boundaries[1:]

    if desc:
        input_boundaries = input_boundaries[::-1]

    boundaries = Table.from_pydict({"x": input_boundaries}).eval_expression_list(
        [col("x").cast(table.get_column("x").datatype())]
    )

    split_tables = table.partition_by_range([col("x")], boundaries, [desc])
    if desc:
        split_tables = split_tables[::-1]

    total_split_len = sum([len(t) for t in split_tables])
    assert total_split_len == size

    seen_idx = set()

    for i, st in enumerate(split_tables):
        for x, x_ind in zip(st.get_column("x").to_pylist(), st.get_column("x_ind").to_pylist()):
            assert original_boundaries[i] <= x
            if i < (k - 1):
                assert x <= original_boundaries[i + 1]
            assert x_ind not in seen_idx
            seen_idx.add(x_ind)


def test_table_partition_by_range_input() -> None:
    # negative sample

    table = Table.from_pydict({"x": [1, 2, 3], "b": [0, 1, 2]})

    with pytest.raises(ValueError, match="Schema Mismatch"):
        table.partition_by_range([col("x")], table, [False])

    with pytest.raises(ValueError, match="Mismatch in number of arguments for `descending`"):
        table.partition_by_range([col("x")], table.eval_expression_list([col("x")]), [False, False])

    with pytest.raises(ValueError, match="Schema Mismatch"):
        table.partition_by_range([col("x")], table.eval_expression_list([col("x").alias("y")]), [False])
