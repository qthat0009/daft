from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from tests.conftest import assert_df_equals
from tests.cookbook.assets import COOKBOOK_DATA_CSV

PYARROW_GE_11_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)


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


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="We only use pyarrow datasets 11 for this test",
)
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
