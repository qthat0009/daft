from __future__ import annotations

import csv
import io

import pytest

import daft
from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.table import Table, schema_inference, table_io


def _csv_write_helper(header: list[str] | None, data: list[list[str | None]]):
    f = io.StringIO()
    writer = csv.writer(f, delimiter=",")
    if header is not None:
        writer.writerow(header)
    writer.writerows(data)
    f.seek(0)
    return io.BytesIO(f.getvalue().encode("utf-8"))


@pytest.mark.parametrize(
    ["data", "expected_dtype"],
    [
        ("1", DataType.int64()),
        ("foo", DataType.string()),
        ("1.5", DataType.float64()),
        ("True", DataType.bool()),
    ],
)
def test_csv_infer_schema(data, expected_dtype):
    f = _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", data],
            ["2", data],
            ["3", None],
        ],
    )

    schema = schema_inference.from_csv(f)
    assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_dtype)])


@pytest.mark.parametrize(
    ["data", "expected_data_series"],
    [
        ["1", daft.Series.from_pylist([1, 1, None])],
        # NOTE: Empty gets parsed as "" instead of None for string fields
        ["foo", daft.Series.from_pylist(["foo", "foo", ""])],
        ["1.5", daft.Series.from_pylist([1.5, 1.5, None])],
        ("True", daft.Series.from_pylist([True, True, None])),
    ],
)
def test_csv_read_data(data, expected_data_series):
    f = _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", data],
            ["2", data],
            ["3", None],
        ],
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_data_series.datatype())])
    expected = Table.from_pydict(
        {
            "id": [1, 2, 3],
            "data": expected_data_series,
        }
    )
    table = table_io.read_csv(f, schema)
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"
