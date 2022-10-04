from typing import Optional, Set, Type

from loguru import logger

from daft.expressions import ColID
from daft.internal.rule import Rule
from daft.logical.logical_plan import (
    Coalesce,
    Filter,
    GlobalLimit,
    Join,
    LocalAggregate,
    LocalLimit,
    LogicalPlan,
    PartitionScheme,
    Projection,
    Repartition,
    Scan,
    Sort,
    UnaryNode,
)
from daft.logical.schema import ExpressionList


class PushDownPredicates(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self._combine_filters_rule = CombineFilters()
        self.register_fn(Filter, Filter, self._combine_filters_rule._combine_filters)
        self.register_fn(Filter, Projection, self._filter_through_projection)
        for op in self._supported_unary_nodes:
            self.register_fn(Filter, op, self._filter_through_unary_node)
        self.register_fn(Filter, Join, self._filter_through_join)

    def _filter_through_projection(self, parent: Filter, child: Projection) -> Optional[LogicalPlan]:
        filter_predicate = parent._predicate

        grandchild = child._children()[0]
        ids_produced_by_grandchild = grandchild.schema().to_id_set()
        can_push_down = []
        can_not_push_down = []
        for pred in filter_predicate:
            id_set = {e.get_id() for e in pred.required_columns()}
            if id_set.issubset(ids_produced_by_grandchild):
                can_push_down.append(pred)
            else:
                can_not_push_down.append(pred)
        if len(can_push_down) == 0:
            return None
        logger.debug(f"Pushing down Filter predicate {can_push_down} into {child}")
        pushed_down_filter = Projection(
            input=Filter(grandchild, predicate=ExpressionList(can_push_down)), projection=child._projection
        )

        if len(can_not_push_down) == 0:
            return pushed_down_filter
        else:
            return Filter(pushed_down_filter, ExpressionList(can_not_push_down))

    def _filter_through_unary_node(self, parent: Filter, child: UnaryNode) -> Optional[LogicalPlan]:
        assert type(child) in self._supported_unary_nodes
        grandchild = child._children()[0]
        logger.debug(f"Pushing Filter {parent} through {child}")
        return child.copy_with_new_children([Filter(grandchild, parent._predicate)])

    def _filter_through_join(self, parent: Filter, child: Join) -> Optional[LogicalPlan]:
        left = child._children()[0]
        right = child._children()[1]
        left_ids = left.schema().to_id_set()
        right_ids = right.schema().to_id_set()
        filter_predicate = parent._predicate
        can_not_push_down = []
        left_push_down = []
        right_push_down = []
        for pred in filter_predicate:
            id_set = {e.get_id() for e in pred.required_columns()}
            if id_set.issubset(left_ids):
                left_push_down.append(pred)
            elif id_set.issubset(right_ids):
                right_push_down.append(pred)
            else:
                can_not_push_down.append(pred)

        if len(left_push_down) == 0 and len(right_push_down) == 0:
            logger.debug(f"Could not push down Filter predicates into Join")
            return None

        if len(left_push_down) > 0:
            logger.debug(f"Pushing down Filter predicate left side: {left_push_down} into Join")
            left = Filter(left, predicate=ExpressionList(left_push_down))
        if len(right_push_down) > 0:
            logger.debug(f"Pushing down Filter predicate right side: {right_push_down} into Join")
            right = Filter(right, predicate=ExpressionList(right_push_down))

        new_join = child.copy_with_new_children([left, right])
        if len(can_not_push_down) == 0:
            return new_join
        else:
            return Filter(new_join, ExpressionList(can_not_push_down))

    @property
    def _supported_unary_nodes(self) -> Set[Type[UnaryNode]]:
        return {Sort, Repartition, Coalesce}


class PruneColumns(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Projection, Projection, self._projection_projection)
        self.register_fn(Projection, LocalAggregate, self._projection_aggregate)
        self.register_fn(LocalAggregate, LogicalPlan, self._aggregate_logical_plan)
        self.register_fn(Projection, LogicalPlan, self._projection_logical_plan)

    def _projection_projection(self, parent: Projection, child: Projection) -> Optional[Projection]:
        parent_required_set = parent.required_columns().to_id_set()
        child_output_set = child.schema().to_id_set()
        if child_output_set.issubset(parent_required_set):
            return None

        logger.debug(f"Pruning Columns: {child_output_set - parent_required_set} in projection projection")

        new_child_exprs = [e for e in child.schema() if e.get_id() in parent_required_set]
        grandchild = child._children()[0]
        return Projection(
            input=Projection(grandchild, projection=ExpressionList(new_child_exprs)), projection=parent._projection
        )

    def _projection_aggregate(self, parent: Projection, child: LocalAggregate) -> Optional[Projection]:
        parent_required_set = parent.required_columns().to_id_set()
        agg_op_pairs = child._agg
        agg_ids = set([e.get_id() for e, _ in agg_op_pairs])
        if agg_ids.issubset(parent_required_set):
            return None

        new_agg_pairs = [(e, op) for e, op in agg_op_pairs if e.get_id() in parent_required_set]
        grandchild = child._children()[0]
        if len(new_agg_pairs) == 0:
            # TODO(sammy) add an actual distinct operation to avoid the extra col from being optimized out
            return None

        logger.debug(f"Pruning Columns:  {agg_ids - parent_required_set} in projection aggregate")
        return Projection(
            input=LocalAggregate(input=grandchild, agg=new_agg_pairs, group_by=child._group_by),
            projection=parent._projection,
        )

    def _aggregate_logical_plan(self, parent: LocalAggregate, child: LogicalPlan) -> Optional[LocalAggregate]:
        parent_required_set = parent.required_columns().to_id_set()
        child_output_set = child.schema().to_id_set()
        if child_output_set.issubset(parent_required_set):
            return None

        logger.debug(f"Pruning Columns: {child_output_set - parent_required_set} in aggregate logical plan")

        return LocalAggregate(
            self._create_pruning_child(child, parent_required_set), agg=parent._agg, group_by=parent._group_by
        )

    def _projection_logical_plan(self, parent: Projection, child: LogicalPlan) -> Optional[LogicalPlan]:
        if isinstance(child, Projection) or isinstance(child, LocalAggregate):
            return None
        if len(child._children()) == 0:
            return None
        required_set = parent.required_columns().to_id_set().union(child.required_columns().to_id_set())
        child_output_set = child.schema().to_id_set()
        if child_output_set.issubset(required_set):
            return None

        logger.debug(f"Pruning Columns: {child_output_set - required_set} in projection logical plan")
        new_grandchildren = [self._create_pruning_child(gc, required_set) for gc in child._children()]
        return parent.copy_with_new_children([child.copy_with_new_children(new_grandchildren)])

    def _create_pruning_child(self, child: LogicalPlan, parent_id_set: Set[ColID]) -> LogicalPlan:
        child_ids = child.schema().to_id_set()
        if child_ids.issubset(parent_id_set):
            return child
        return Projection(child, projection=ExpressionList([e for e in child.schema() if e.get_id() in parent_id_set]))


class CombineFilters(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Filter, Filter, self._combine_filters)

    def _combine_filters(self, parent: Filter, child: Filter) -> Filter:
        logger.debug(f"combining {parent} into {child}")
        new_predicate = parent._predicate.union(child._predicate, rename_dup="right.")
        grand_child = child._children()[0]
        return Filter(grand_child, new_predicate)


class DropRepartition(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Repartition, LogicalPlan, self._drop_repartition_if_same_spec)
        self.register_fn(Repartition, Repartition, self._drop_double_repartition)

    def _drop_repartition_if_same_spec(self, parent: Repartition, child: LogicalPlan) -> Optional[LogicalPlan]:
        if (
            parent.partition_spec() == child.partition_spec()
            and parent.partition_spec().scheme != PartitionScheme.RANGE
        ) or ((parent.num_partitions() == 1 and child.num_partitions() == 1)):
            logger.debug(f"Dropping: {parent}")
            return child
        return None

    def _drop_double_repartition(self, parent: Repartition, child: Repartition) -> Repartition:
        grandchild = child._children()[0]
        logger.debug(f"Dropping: {child}")
        return Repartition(
            grandchild,
            partition_by=parent._partition_by,
            num_partitions=parent.num_partitions(),
            scheme=parent.partition_spec().scheme,
        )


class PushDownClausesIntoScan(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        # self.register_fn(Filter, Scan, self._push_down_predicates_into_scan)
        self.register_fn(Projection, Scan, self._push_down_projections_into_scan)

    # def _push_down_predicates_into_scan(self, parent: Filter, child: Scan) -> Scan:
    #     new_predicate = parent._predicate.union(child._predicate, rename_dup="right.")
    #     child_schema = child.schema()
    #     assert new_predicate.required_columns().to_id_set().issubset(child_schema.to_id_set())
    #     return Scan(
    #         schema=child._schema, predicate=new_predicate, columns=child_schema.names, source_info=child._source_info
    #     )

    def _push_down_projections_into_scan(self, parent: Projection, child: Scan) -> Optional[LogicalPlan]:
        required_columns = parent.schema().required_columns()
        scan_columns = child.schema()
        if required_columns.to_id_set() == scan_columns.to_id_set():
            return None

        new_scan = Scan(
            schema=child._schema,
            predicate=child._predicate,
            columns=required_columns.names,
            source_info=child._source_info,
        )

        return Projection(new_scan, parent._projection)


class FoldProjections(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        self.register_fn(Projection, Projection, self._push_down_predicates_into_scan)

    def _push_down_predicates_into_scan(self, parent: Projection, child: Projection) -> Optional[Projection]:
        required_columns = parent._projection.required_columns()
        grandchild = child._children()[0]
        grandchild_output = grandchild.schema()
        grandchild_ids = grandchild_output.to_id_set()

        can_skip_child = required_columns.to_id_set().issubset(grandchild_ids)

        if can_skip_child:
            logger.debug(f"Folding: {parent} into {child}")
            return Projection(grandchild, parent._projection)
        else:
            return None


class PushDownLimit(Rule[LogicalPlan]):
    def __init__(self) -> None:
        super().__init__()
        for op in self._supported_unary_nodes:
            self.register_fn(LocalLimit, op, self._push_down_local_limit_into_unary_node)
            self.register_fn(GlobalLimit, op, self._push_down_global_limit_into_unary_node)

    def _push_down_local_limit_into_unary_node(self, parent: LocalLimit, child: UnaryNode) -> Optional[LogicalPlan]:
        logger.debug(f"pushing {parent} into {child}")
        grandchild = child._children()[0]
        return child.copy_with_new_children([LocalLimit(grandchild, num=parent._num)])

    def _push_down_global_limit_into_unary_node(self, parent: GlobalLimit, child: UnaryNode) -> Optional[LogicalPlan]:
        logger.debug(f"pushing {parent} into {child}")
        grandchild = child._children()[0]
        return child.copy_with_new_children([GlobalLimit(grandchild, num=parent._num)])

    @property
    def _supported_unary_nodes(self) -> Set[Type[LogicalPlan]]:
        return {Repartition, Coalesce, Projection}
