from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES
from tests.utils import ANSI_ESCAPE

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
def test_sparse_tensor_roundtrip(dtype):
    np_dtype = dtype.to_pandas_dtype()
    data = [
        np.array([[0, 1, 0, 0], [0, 0, 0, 0]], dtype=np_dtype),
        None,
        np.array([[0, 0, 0, 0], [0, 0, 0, 0]], dtype=np_dtype),
        np.array([[0, 0, 0, 0], [0, 0, 4, 0]], dtype=np_dtype),
    ]
    s = Series.from_pylist(data, pyobj="allow")

    tensor_dtype = DataType.tensor(DataType.from_arrow_type(dtype))

    t = s.cast(tensor_dtype)
    assert t.datatype() == tensor_dtype

    # Test sparse tensor roundtrip.
    sparse_tensor_dtype = DataType.sparse_tensor(dtype=DataType.from_arrow_type(dtype))
    sparse_tensor_series = t.cast(sparse_tensor_dtype)
    assert sparse_tensor_series.datatype() == sparse_tensor_dtype
    back = sparse_tensor_series.cast(tensor_dtype)
    out = back.to_pylist()
    np.testing.assert_equal(out, data)


def test_sparse_tensor_repr():
    arr = np.arange(np.prod((2, 2)), dtype=np.int64).reshape((2, 2))
    arrs = [arr, arr, None]
    s = Series.from_pylist(arrs, pyobj="allow")
    s = s.cast(DataType.sparse_tensor(dtype=DataType.from_arrow_type(pa.int64())))
    out_repr = ANSI_ESCAPE.sub("", repr(s))
    assert (
        out_repr.replace("\r", "")
        == """╭─────────────────────────────╮
│ list_series                 │
│ ---                         │
│ SparseTensor(Int64)         │
╞═════════════════════════════╡
│ <SparseTensor shape=(2, 2)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ <SparseTensor shape=(2, 2)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ None                        │
╰─────────────────────────────╯
"""
    )


@pytest.mark.parametrize("indices_dtype", [np.uint8, np.uint16])
def test_fixed_shape_sparse_minimal_indices_dtype(indices_dtype: np.dtype):
    def get_inner_indices_dtype(fixed_shape_sparse_dtype: DataType) -> pa.DataType:
        arrow_sparse_dtype = fixed_shape_sparse_dtype.to_arrow_dtype()
        indices_field_idx = arrow_sparse_dtype.get_field_index("indices")
        indices_dtype = arrow_sparse_dtype.field(indices_field_idx).type.value_type
        return indices_dtype

    largest_index_possible = np.iinfo(indices_dtype).max
    tensor_shape = (largest_index_possible + 1, 1)
    series = Series.from_pylist([np.zeros(shape=tensor_shape, dtype=np.uint8)]).cast(
        DataType.tensor(DataType.uint8(), shape=tensor_shape)
    )
    sparse_series = series.cast(DataType.sparse_tensor(DataType.uint8(), shape=tensor_shape))

    actual_dtype = get_inner_indices_dtype(sparse_series.datatype())
    assert actual_dtype == pa.from_numpy_dtype(indices_dtype)
