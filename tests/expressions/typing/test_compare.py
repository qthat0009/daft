from __future__ import annotations

import operator as ops

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import Table
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    is_comparable,
)


def comparable_resolvable(lhs: DataType, rhs: DataType) -> bool:
    return is_comparable(lhs) and is_comparable(rhs) and (DataType.supertype(lhs, rhs) is not None)


@pytest.mark.parametrize("op", [ops.eq, ops.ne, ops.lt, ops.le, ops.gt, ops.ge])
def test_comparable(binary_data_fixture, op):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({s.name(): s for s in binary_data_fixture}),
        op(col(lhs.name()), col(rhs.name())),
        lambda tbl: op(tbl.get_column(lhs.name()), tbl.get_column(rhs.name())),
        comparable_resolvable(lhs.datatype(), rhs.datatype()),
    )
