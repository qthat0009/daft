from __future__ import annotations

import pyarrow as pa
import pytest
from pyarrow import dataset as pads

import daft
from tests.conftest import assert_df_equals
from tests.cookbook.assets import COOKBOOK_DATA_CSV

PYARROW_GE_7_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (7, 0, 0)


def test_parquet_write(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_parquet(tmp_path)
    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/*.parquet").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(pd_df) == 1
    assert len(pd_df._preview.preview_partition) == 1


def test_parquet_write_with_partitioning(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_parquet(tmp_path, partition_cols=["Borough"])

    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/**/*.parquet").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(pd_df) == 5
    assert len(pd_df._preview.preview_partition) == 5


def test_empty_parquet_write_without_partitioning(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)
    df = df.where(daft.lit(False))
    output_files = df.write_parquet(tmp_path)
    assert len(output_files) == 1
    assert len(output_files._preview.preview_partition) == 1


def test_empty_parquet_write_with_partitioning(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)
    df = df.where(daft.lit(False))
    output_files = df.write_parquet(tmp_path, partition_cols=["Borough"])
    assert len(output_files) == 0
    assert len(output_files._preview.preview_partition) == 0


def test_parquet_write_with_partitioning_readback_values(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    output_files = df.write_parquet(tmp_path, partition_cols=["Borough"])
    output_dict = output_files.to_pydict()
    boroughs = {"QUEENS", "MANHATTAN", "STATEN ISLAND", "BROOKLYN", "BRONX"}
    assert set(output_dict["Borough"]) == boroughs

    for path, bor in zip(output_dict["path"], output_dict["Borough"]):
        assert f"Borough={bor}" in path
        read_back = daft.read_parquet(path).to_pydict()
        assert all(b == bor for b in read_back["Borough"])

    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/**/*.parquet").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(output_files) == 5
    assert len(output_files._preview.preview_partition) == 5


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="We only use pyarrow datasets 7 for this test",
)
def test_parquet_write_with_null_values(tmp_path):
    df = daft.from_pydict({"x": [1, 2, 3, None]})
    df.write_parquet(tmp_path, partition_cols=[df["x"].alias("y")])
    ds = pads.dataset(tmp_path, format="parquet", partitioning=pads.HivePartitioning(pa.schema([("y", pa.int64())])))
    readback = ds.to_table()
    assert readback.to_pydict() == {"x": [1, 2, 3, None], "y": [1, 2, 3, None]}


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="We only use pyarrow datasets 7 for this test",
)
def test_parquet_write_multifile(tmp_path):
    daft.set_execution_config(parquet_target_filesize=1024)
    data = {"x": list(range(1_000))}
    df = daft.from_pydict(data)
    df2 = df.write_parquet(tmp_path)
    assert len(df2) > 1
    ds = pads.dataset(tmp_path, format="parquet")
    readback = ds.to_table()
    assert readback.to_pydict() == data


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="We only use pyarrow datasets 7 for this test",
)
def test_parquet_write_multifile_with_partitioning(tmp_path):
    daft.set_execution_config(parquet_target_filesize=1024)
    data = {"x": list(range(1_000))}
    df = daft.from_pydict(data)
    df2 = df.write_parquet(tmp_path, partition_cols=[df["x"].alias("y") % 2])
    assert len(df2) >= 4
    ds = pads.dataset(tmp_path, format="parquet", partitioning=pads.HivePartitioning(pa.schema([("y", pa.int64())])))
    readback = ds.to_table()
    readback = readback.sort_by("x").to_pydict()
    assert readback["x"] == data["x"]
    assert readback["y"] == [y % 2 for y in data["x"]]


def test_csv_write(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_csv(tmp_path)

    read_back_pd_df = daft.read_csv(tmp_path.as_posix() + "/*.csv").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(pd_df) == 1
    assert len(pd_df._preview.preview_partition) == 1


def test_csv_write_with_partitioning(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)
    schema = df.schema()
    names = schema.column_names()
    types = {}
    for n in names:
        types[n] = schema[n].dtype

    pd_df = df.write_csv(tmp_path, partition_cols=["Borough"]).to_pandas()
    read_back_pd_df = daft.read_csv(tmp_path.as_posix() + "/**/*.csv", schema_hints=types).to_pandas()
    assert_df_equals(df.to_pandas().fillna(""), read_back_pd_df.fillna(""))

    assert len(pd_df) == 5
