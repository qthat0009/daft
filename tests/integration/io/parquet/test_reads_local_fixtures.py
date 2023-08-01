from __future__ import annotations

import uuid

import pytest

from daft.io import IOConfig, S3Config
from daft.table import Table

BUCKETS = ["head-retries-bucket", "get-retries-bucket"]


@pytest.mark.integration()
@pytest.mark.parametrize("status_code", [400, 403, 404])
@pytest.mark.parametrize("bucket", BUCKETS)
def test_non_retryable_errors(status_code: int, bucket: str):
    io_config = IOConfig(s3=S3Config(endpoint_url="http://127.0.0.1:8000"))
    data_path = f"s3://{bucket}/{status_code}/1/{uuid.uuid4()}"

    with pytest.raises(ValueError):
        Table.read_parquet(data_path, io_config=io_config)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "status_code",
    [
        500,
        503,
        504,
        # TODO: We should also retry correctly on these error codes (PyArrow does retry appropriately here)
        # These are marked as retryable error codes, see:
        # https://github.com/aws/aws-sdk-cpp/blob/8a9550f1db04b33b3606602ba181d68377f763df/src/aws-cpp-sdk-core/include/aws/core/http/HttpResponse.h#L113-L131
        # 509,
        # 408,
        # 429,
        # 419,
        # 440,
        # 598,
        # 599,
    ],
)
@pytest.mark.parametrize("bucket", BUCKETS)
def test_retryable_errors(status_code: int, bucket: str):
    io_config = IOConfig(s3=S3Config(endpoint_url="http://127.0.0.1:8000"))

    # By default the SDK retries 3 times, so we should be able to tolerate NUM_ERRORS=2
    NUM_ERRORS = 2
    data_path = f"s3://{bucket}/{status_code}/{NUM_ERRORS}/{uuid.uuid4()}"

    Table.read_parquet(data_path, io_config=io_config)
