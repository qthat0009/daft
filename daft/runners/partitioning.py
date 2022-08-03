from dataclasses import dataclass
from typing import Any, Dict

from daft.expressions import ColID, Expression

PartID = int


@dataclass(frozen=True)
class PyListTile:
    column_id: ColID
    column_name: str
    partition_id: PartID
    block: Any


@dataclass(frozen=True)
class vPartition:
    columns: Dict[ColID, PyListTile]
    partition_id: PartID

    def eval_expression(self, expr: Expression) -> PyListTile:
        required_cols = expr.required_columns()
        required_blocks = {}
        for c in required_cols:
            col_id = c.get_id()
            assert col_id is not None
            block = self.columns[col_id].block
            name = c.name()
            assert name is not None
            required_blocks[name] = block
        result = expr.eval(**required_blocks)
        expr_col_id = expr.get_id()
        expr_name = expr.name()
        assert expr_col_id is not None
        assert expr_name is not None
        return PyListTile(column_id=expr_col_id, column_name=expr_name, partition_id=self.partition_id, block=result)


@dataclass
class PartitionManager:
    partitions: Dict[PartID, vPartition]
