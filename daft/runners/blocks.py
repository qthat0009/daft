from __future__ import annotations

import collections
from abc import abstractmethod
from dataclasses import dataclass
from functools import partialmethod
from typing import Any, Callable, ClassVar, Generic, List, TypeVar, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pac

ArrType = TypeVar("ArrType", bound=collections.abc.Sequence)
UnaryFuncType = Callable[[ArrType], ArrType]
BinaryFuncType = Callable[[ArrType, ArrType], ArrType]


@dataclass
class FunctionDispatch:
    # UnaryOps
    # Arithmetic

    neg: UnaryFuncType
    pos: UnaryFuncType
    abs: UnaryFuncType

    # Logical
    invert: UnaryFuncType

    # BinaryOps
    # Arithmetic
    add: BinaryFuncType
    sub: BinaryFuncType
    mul: BinaryFuncType
    truediv: BinaryFuncType
    pow: BinaryFuncType

    # Logical
    and_: BinaryFuncType
    or_: BinaryFuncType
    lt: BinaryFuncType
    le: BinaryFuncType
    eq: BinaryFuncType
    ne: BinaryFuncType
    gt: BinaryFuncType
    ge: BinaryFuncType

    # Dataframe ops
    filter: BinaryFuncType
    take: BinaryFuncType


ArrowFunctionDispatch = FunctionDispatch(
    neg=pac.negate,
    pos=lambda x: x,
    abs=pac.abs,
    invert=pac.invert,
    add=pac.add,
    sub=pac.subtract,
    mul=pac.multiply,
    truediv=pac.divide,
    pow=pac.power,
    and_=pac.and_,
    or_=pac.or_,
    lt=pac.less,
    le=pac.less_equal,
    eq=pac.equal,
    ne=pac.not_equal,
    gt=pac.greater,
    ge=pac.greater_equal,
    filter=pac.array_filter,
    take=pac.take,
)


class DataBlock(Generic[ArrType]):
    data: ArrType
    operators: ClassVar[FunctionDispatch]

    def __init__(self, data: ArrType) -> None:
        self.data = data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n{self.data}"

    def __len__(self) -> int:
        return len(self.data)

    @classmethod
    def make_block(cls, data: Any) -> DataBlock:
        # if isinstance(data, list):
        #     return PyListDataBlock(data=data)
        if isinstance(data, pa.ChunkedArray):
            return ArrowDataBlock(data=data)
        elif isinstance(data, np.ndarray):
            return ArrowDataBlock(data=pa.chunked_array([data]))
        else:
            try:
                arrow_type = pa.infer_type([data])
            except pa.lib.ArrowInvalid:
                arrow_type = None
            if arrow_type is None or pa.types.is_nested(arrow_type):
                raise ValueError("Don't know what block {data} should be")
            return ArrowDataBlock(data=pa.scalar(data))

    def _unary_op(self, func) -> DataBlock[ArrType]:
        fn = getattr(self.__class__.operators, func)
        return DataBlock.make_block(data=fn(self.data))

    def _convert_to_block(self, input: Any) -> DataBlock[ArrType]:
        if isinstance(input, DataBlock):
            return input
        else:
            return DataBlock.make_block(input)

    def _binary_op(self, other: Any, func) -> DataBlock[ArrType]:
        other = self._convert_to_block(other)
        fn = getattr(self.__class__.operators, func)
        return DataBlock.make_block(data=fn(self.data, other.data))

    def _reverse_binary_op(self, other: Any, func) -> DataBlock[ArrType]:
        other_block: DataBlock[ArrType] = self._convert_to_block(other)
        return other_block._binary_op(self, func=func)

    # UnaryOps

    # Arithmetic
    __neg__ = partialmethod(_unary_op, func="neg")
    __pos__ = partialmethod(_unary_op, func="pos")
    __abs__ = partialmethod(_unary_op, func="abs")

    # # Logical
    __invert__ = partialmethod(_unary_op, func="invert")

    # # BinaryOps

    # # Arithmetic
    __add__ = partialmethod(_binary_op, func="add")
    __sub__ = partialmethod(_binary_op, func="sub")
    __mul__ = partialmethod(_binary_op, func="mul")
    __truediv__ = partialmethod(_binary_op, func="truediv")
    __pow__ = partialmethod(_binary_op, func="pow")

    # # Reverse Arithmetic
    __radd__ = partialmethod(_reverse_binary_op, func="add")
    __rsub__ = partialmethod(_reverse_binary_op, func="sub")
    __rmul__ = partialmethod(_reverse_binary_op, func="mul")
    __rtruediv__ = partialmethod(_reverse_binary_op, func="truediv")
    __rpow__ = partialmethod(_reverse_binary_op, func="pow")

    # # Logical
    __and__ = partialmethod(_binary_op, func="and_")
    __or__ = partialmethod(_binary_op, func="or_")

    __lt__ = partialmethod(_binary_op, func="lt")
    __le__ = partialmethod(_binary_op, func="le")
    __eq__ = partialmethod(_binary_op, func="eq")  # type: ignore
    __ne__ = partialmethod(_binary_op, func="ne")  # type: ignore
    __gt__ = partialmethod(_binary_op, func="gt")
    __ge__ = partialmethod(_binary_op, func="ge")

    # # Reverse Logical
    __rand__ = partialmethod(_reverse_binary_op, func="and_")
    __ror__ = partialmethod(_reverse_binary_op, func="or_")

    # DataFrame ops
    filter = partialmethod(_binary_op, func="filter")
    take = partialmethod(_binary_op, func="take")

    def head(self, num: int) -> DataBlock[ArrType]:
        return DataBlock.make_block(self.data[:num])

    @abstractmethod
    def sample(self, num: int) -> DataBlock[ArrType]:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def _argsort(blocks: List[DataBlock], desc: bool = False) -> DataBlock:
        raise NotImplementedError()

    @classmethod
    def argsort(cls, blocks: List[DataBlock[ArrType]], desc: bool = False) -> DataBlock[ArrType]:
        assert len(blocks) > 0, "no blocks to sort"
        first_type = type(blocks[0])
        assert all(type(b) == first_type for b in blocks), "all block types must match"
        return first_type._argsort(blocks, desc=desc)

    @abstractmethod
    def partition(self, num: int, targets: DataBlock[ArrType]) -> List[DataBlock[ArrType]]:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def _merge_blocks(blocks: List[DataBlock]) -> DataBlock:
        raise NotImplementedError()

    @classmethod
    def merge_blocks(cls, blocks: List[DataBlock[ArrType]]) -> DataBlock[ArrType]:
        assert len(blocks) > 0, "no blocks"
        first_type = type(blocks[0])
        assert all(type(b) == first_type for b in blocks), "all block types must match"
        return first_type._merge_blocks(blocks)


class PyListDataBlock(DataBlock[List]):
    ...


class ArrowDataBlock(DataBlock[Union[pa.ChunkedArray, pa.Scalar]]):
    operators: ClassVar[FunctionDispatch] = ArrowFunctionDispatch

    @staticmethod
    def _argsort(blocks: List[DataBlock], desc: bool = False) -> DataBlock:
        order = "descending" if desc else "ascending"
        to_convert = {}
        cols = []
        to_convert = {str(i + 1): o.data for i, o in enumerate(blocks)}
        cols = [(str(i + 1), order) for i, _ in enumerate(blocks)]
        table = pa.table(to_convert)
        sort_indices = pac.sort_indices(table, sort_keys=cols)
        return ArrowDataBlock(data=sort_indices)

    @staticmethod
    def _merge_blocks(blocks: List[DataBlock]) -> DataBlock:
        all_chunks = []
        for block in blocks:
            all_chunks.extend(block.data.chunks)
        return ArrowDataBlock(data=pa.chunked_array(all_chunks))

    def partition(self, num: int, targets: DataBlock) -> List[DataBlock]:
        new_partitions: List[DataBlock] = [
            ArrowDataBlock(data=pa.chunked_array([[]], type=self.data.type)) for _ in range(num)
        ]
        # We first argsort the targets to group the same partitions together
        argsort_indices = ArrowDataBlock.argsort([targets])
        # We now perform a gather to make items targeting the same partition together
        reordered = self.take(argsort_indices)
        sorted_targets = targets.take(argsort_indices)

        pivots = np.where(np.diff(sorted_targets.data, prepend=np.nan))[0]

        # We now split in the num partitions
        unmatched_partitions = np.split(reordered.data, pivots)[1:]
        target_partitions = sorted_targets.data.to_numpy()[pivots]
        for i, target_idx in enumerate(target_partitions):
            new_partitions[target_idx] = ArrowDataBlock(data=pa.chunked_array([unmatched_partitions[i]]))
        return new_partitions

    def sample(self, num: int, replace=False) -> ArrowDataBlock:
        sampled = np.random.choice(self.data, num, replace=replace)
        return ArrowDataBlock(data=pa.chunked_array([sampled]))

    def search_sorted(self, pivots: ArrowDataBlock) -> ArrowDataBlock:
        arr = self.data.to_numpy()
        indices = np.searchsorted(pivots.data.to_numpy(), arr)
        return ArrowDataBlock(data=pa.chunked_array([indices]))
