from __future__ import annotations

import functools
import itertools
import operator
from abc import abstractmethod
from functools import partialmethod
from typing import Any, Callable, Dict, List, Optional, Tuple

from daft.internal.treenode import TreeNode


def col(name: str) -> ColumnExpression:
    return ColumnExpression(name)


_COUNTER = itertools.count()


class Expression(TreeNode["Expression"]):
    def __init__(self) -> None:
        super().__init__()
        self._id: Optional[int] = None

    def __repr__(self) -> str:
        if self.has_id():
            return f"{self._display_str()} AS {self.name()}#{self.get_id()}"
        else:
            return self._display_str()

    def _to_expression(self, input: Any) -> Expression:
        if not isinstance(input, Expression):
            return LiteralExpression(input)
        return input

    def _unary_op(self, func: Callable, symbol: Optional[str] = None) -> Expression:
        return CallExpression(func, func_args=(self,), symbol=symbol)

    def _binary_op(self, other: Any, func: Callable, symbol: Optional[str] = None) -> Expression:
        other_expr = self._to_expression(other)
        return CallExpression(func, func_args=(self, other_expr), symbol=symbol)

    def _reverse_binary_op(self, other: Any, func: Callable, symbol: Optional[str] = None) -> Expression:
        other_expr = self._to_expression(other)
        return other_expr._binary_op(self, func, symbol=symbol)

    def name(self) -> Optional[str]:
        for child in self._children():
            name = child.name()
            if name is not None:
                return name
        return None

    def _assign_id(self, strict: bool = True) -> int:
        if not self.has_id():
            self._id = next(_COUNTER)
            return self._id
        else:
            if strict:
                raise ValueError(f"We have already assigned an id, {self.get_id()}")
            else:
                assert self._id is not None
                return self._id

    def get_id(self) -> Optional[int]:
        return self._id

    def has_id(self) -> bool:
        return self.get_id() is not None

    def to_column_expression(self) -> ColumnExpression:
        if not self.has_id():
            raise ValueError("we can only convert expressions with assigned id to ColumnExpressions")
        name = self.name()
        if name is None:
            raise ValueError("we can only convert expressions to ColumnExpressions if they have a name")
        ce = ColumnExpression(name)
        ce.assign_id_from_expression(self)
        return ce

    def required_columns(self, unresolved_only: bool = False) -> List[ColumnExpression]:
        to_rtn: List[ColumnExpression] = []
        for child in self._children():
            to_rtn.extend(child.required_columns(unresolved_only))
        return to_rtn

    def _replace_column_with_expression(self, col_expr: ColumnExpression, new_expr: Expression) -> Expression:
        assert col_expr.is_same(new_expr)
        if isinstance(self, ColumnExpression) and self.is_eq(col_expr):
            return new_expr
        for i in range(len(self._children())):
            self._registered_children[i] = self._registered_children[i]._replace_column_with_expression(
                col_expr, new_expr
            )
        return self

    # UnaryOps

    # Arithmetic
    __neg__ = partialmethod(_unary_op, func=operator.neg, symbol="-")
    __pos__ = partialmethod(_unary_op, func=operator.pos, symbol="+")
    __abs__ = partialmethod(_unary_op, func=operator.abs)

    # Logical
    __invert__ = partialmethod(_unary_op, func=operator.not_, symbol="~")

    # function
    def map(self, func: Callable) -> Expression:
        return self._unary_op(func)

    # BinaryOps

    # Arithmetic
    __add__ = partialmethod(_binary_op, func=operator.add, symbol="+")
    __sub__ = partialmethod(_binary_op, func=operator.sub, symbol="-")
    __mul__ = partialmethod(_binary_op, func=operator.mul, symbol="*")
    __floordiv__ = partialmethod(_binary_op, func=operator.floordiv, symbol="//")
    __truediv__ = partialmethod(_binary_op, func=operator.truediv, symbol="/")
    __pow__ = partialmethod(_binary_op, func=operator.pow, symbol="**")

    # Reverse Arithmetic
    __radd__ = partialmethod(_reverse_binary_op, func=operator.add, symbol="+")
    __rsub__ = partialmethod(_reverse_binary_op, func=operator.sub, symbol="-")
    __rmul__ = partialmethod(_reverse_binary_op, func=operator.mul, symbol="*")
    __rfloordiv__ = partialmethod(_reverse_binary_op, func=operator.floordiv, symbol="//")
    __rtruediv__ = partialmethod(_reverse_binary_op, func=operator.truediv, symbol="/")
    __rpow__ = partialmethod(_reverse_binary_op, func=operator.pow, symbol="**")

    # Logical
    __and__ = partialmethod(_binary_op, func=operator.and_, symbol="&")
    __or__ = partialmethod(_binary_op, func=operator.or_, symbol="|")

    __lt__ = partialmethod(_binary_op, func=operator.lt, symbol="<")
    __le__ = partialmethod(_binary_op, func=operator.le, symbol="<=")
    __eq__ = partialmethod(_binary_op, func=operator.eq, symbol="=")  # type: ignore
    __ne__ = partialmethod(_binary_op, func=operator.ne, symbol="!=")  # type: ignore
    __gt__ = partialmethod(_binary_op, func=operator.gt, symbol=">")
    __ge__ = partialmethod(_binary_op, func=operator.ge, symbol=">=")

    # Reverse Logical
    __rand__ = partialmethod(_reverse_binary_op, func=operator.and_, symbol="&")
    __ror__ = partialmethod(_reverse_binary_op, func=operator.or_, symbol="|")

    @abstractmethod
    def eval(self, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def _display_str(self) -> str:
        raise NotImplementedError()

    def alias(self, name: str) -> Expression:
        return AliasExpression(self, name)

    def has_call(self) -> bool:
        if isinstance(self, CallExpression):
            return True
        if len(self._children()) > 0:
            return any(c.has_call() for c in self._children())
        return False

    def is_same(self, other: Expression) -> bool:
        if self is other:
            return True
        ids_match = self.has_id() and self.get_id() == other.get_id()
        return ids_match

    @abstractmethod
    def _is_eq_local(self, other: Expression) -> bool:
        raise NotImplementedError()

    def is_eq(self, other: Expression) -> bool:
        if self.is_same(other):
            return True

        if not self._is_eq_local(other):
            print(f"Is local not equal: {self} != {other}")
            return False

        if len(self._children()) != len(other._children()):
            return False

        for s, o in zip(self._children(), other._children()):
            if not s.is_eq(o):
                return False

        return True


class LiteralExpression(Expression):
    def __init__(self, value: Any) -> None:
        super().__init__()
        self._value = value

    def _display_str(self) -> str:
        return f"lit({self._value})"

    def eval(self, **kwargs):
        return self._value

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, LiteralExpression) and self._value == other._value


class MultipleReturnSelectExpression(Expression):
    def __init__(self, expr: Expression, n: int) -> None:
        super().__init__()
        self._register_child(expr)
        self._n = n

    @property
    def _expr(self) -> Expression:
        return self._children()[0]

    def _display_str(self) -> str:
        return f"{self._expr}[{self._n}]"

    def eval(self, **kwargs):
        all_values = self._expr.eval(**kwargs)
        assert isinstance(all_values, tuple), f"expected multiple returns from {self._expr}"
        assert len(all_values) > self._n
        value = all_values[self._n]
        return value

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, MultipleReturnSelectExpression) and self._n == other._n


class CallExpression(Expression):
    def __init__(
        self,
        func: Callable,
        func_args: Tuple,
        func_kwargs: Optional[Dict[str, Any]] = None,
        symbol: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._args_ids = tuple(self._register_child(self._to_expression(arg)) for arg in func_args)
        if func_kwargs is None:
            func_kwargs = dict()
        self._kwargs_ids = {k: self._register_child(self._to_expression(v)) for k, v in func_kwargs.items()}
        self._func = func
        self._symbol = symbol

    @property
    def _args(self) -> Tuple[Expression, ...]:
        return tuple(self._children()[i] for i in self._args_ids)

    @property
    def _kwargs(self) -> Dict[str, Expression]:
        return {k: self._children()[i] for k, i in self._kwargs_ids.items()}

    def _display_str(self) -> str:
        symbol = self._func.__name__ if self._symbol is None else self._symbol

        # Handle Binary Case:
        if len(self._kwargs) == 0 and len(self._args) == 2:
            return f"[{self._args[0]._display_str()} {symbol} {self._args[1]._display_str()}]"

        args = ", ".join(a._display_str() for a in self._args)
        if len(self._kwargs) == 0:
            return f"{symbol}({args})"

        kwargs = ", ".join(f"{k}={v._display_str()}" for k, v in self._kwargs.items())
        return f"{symbol}({args}, {kwargs})"

    def eval(self, **kwargs):
        eval_args = tuple(a.eval(**kwargs) for a in self._args)
        eval_kwargs = {k: self.eval(**kwargs) for k, v in self._kwargs.items()}
        return self._func(*eval_args, **eval_kwargs)

    def _is_eq_local(self, other: Expression) -> bool:
        return (
            isinstance(other, CallExpression)
            and self._args_ids == other._args_ids
            and self._kwargs_ids == other._kwargs_ids
            and self._func == other._func
            and self._symbol == other._symbol
        )


def udf(f: Callable | None = None, num_returns: int = 1) -> Callable:
    def udf_decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            if any(isinstance(a, Expression) for a in args) or any(isinstance(a, Expression) for a in kwargs.values()):
                out_expr = CallExpression(func, func_args=args, func_kwargs=kwargs)
                if num_returns == 1:
                    return out_expr
                else:
                    assert num_returns > 1
                    return tuple(MultipleReturnSelectExpression(out_expr, i) for i in range(num_returns))
            else:
                return func(*args, **kwargs)

        return wrapped_func

    if f is None:
        return udf_decorator
    return udf_decorator(f)


class ColumnExpression(Expression):
    def __init__(self, name: str) -> None:
        super().__init__()
        if not isinstance(name, str):
            raise TypeError(f"Expected name to be type str, is {type(name)}")
        self._name = name

    def _display_str(self) -> str:
        if not self.has_id():
            return f"col({self._name})"
        else:
            return f"col({self._name}#{self._id})"

    def __repr__(self) -> str:
        return self._display_str()

    def eval(self, **kwargs):
        if self._name not in kwargs:
            raise ValueError(f"expected column `{self._name}` to be passed into eval")
        return kwargs[self._name]

    def name(self) -> Optional[str]:
        return self._name

    def required_columns(self, unresolved_only: bool = False) -> List[ColumnExpression]:
        if unresolved_only and self.has_id():
            return []
        return [self]

    def assign_id_from_expression(self, other: Expression) -> int:
        assert self.name() == other.name()
        self._id = other.get_id()
        assert self._id is not None
        return self._id

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, ColumnExpression) and self._name == other._name and self.get_id() == other.get_id()


class AliasExpression(Expression):
    def __init__(self, expr: Expression, name: str) -> None:
        super().__init__()
        if not isinstance(name, str):
            raise TypeError(f"Expected name to be type str, is {type(name)}")
        self._register_child(expr)
        self._name = name

    @property
    def _expr(self) -> Expression:
        return self._children()[0]

    def _display_str(self) -> str:
        return f"[{self._expr}].alias({self._name})"

    def name(self) -> Optional[str]:
        return self._name

    def get_id(self) -> Optional[int]:
        return self._expr.get_id()

    def _assign_id(self, strict: bool = True) -> int:
        return self._expr._assign_id(strict)

    def eval(self, **kwargs):
        return self._expr.eval(**kwargs)

    def _is_eq_local(self, other: Expression) -> bool:
        return isinstance(other, AliasExpression) and self._name == other._name
