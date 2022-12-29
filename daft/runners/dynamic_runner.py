from __future__ import annotations

from daft.execution.dynamic_construction import Construction
from daft.execution.dynamic_schedule import DynamicSchedule, ScheduleMaterialize
from daft.execution.dynamic_schedule_factory import DynamicScheduleFactory
from daft.expressions import ColumnExpression
from daft.filesystem import glob_path
from daft.internal.rule_runner import FixedPointPolicy, Once, RuleBatch, RuleRunner
from daft.logical import logical_plan
from daft.logical.optimizer import (
    DropProjections,
    DropRepartition,
    FoldProjections,
    PruneColumns,
    PushDownClausesIntoScan,
    PushDownLimit,
    PushDownPredicates,
)
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import PartitionCacheEntry, vPartition
from daft.runners.pyrunner import LocalPartitionSet
from daft.runners.runner import Runner
from daft.types import ExpressionType


class DynamicRunner(Runner):
    """A dynamic version of PyRunner that uses DynamicSchedule to determine execution steps."""

    def __init__(self) -> None:
        super().__init__()
        # From PyRunner
        self._optimizer = RuleRunner(
            [
                RuleBatch(
                    "SinglePassPushDowns",
                    Once,
                    [
                        DropRepartition(),
                        PushDownPredicates(),
                        PruneColumns(),
                        FoldProjections(),
                        PushDownClausesIntoScan(),
                    ],
                ),
                RuleBatch(
                    "PushDownLimitsAndRepartitions",
                    FixedPointPolicy(3),
                    [PushDownLimit(), DropRepartition(), DropProjections()],
                ),
            ]
        )

    def optimize(self, plan: logical_plan.LogicalPlan) -> logical_plan.LogicalPlan:
        # From PyRunner
        return self._optimizer.optimize(plan)

    def glob_filepaths(
        self,
        source_path: str,
        filepath_column_name: str = "filepaths",
    ) -> logical_plan.InMemoryScan:
        filepaths = glob_path(source_path)
        schema = ExpressionList([ColumnExpression(filepath_column_name, ExpressionType.string())]).resolve()
        pset = LocalPartitionSet(
            {
                i: vPartition.from_pydict(data={filepath_column_name: [filepaths[i]]}, schema=schema, partition_id=i)
                for i in range(len(filepaths))  # Hardcoded to 1 path per partition
            },
        )
        cache_entry = self.put_partition_set_into_cache(pset)
        partition_set = cache_entry.value
        assert partition_set is not None

        return logical_plan.InMemoryScan(
            cache_entry=cache_entry,
            schema=schema,
            partition_spec=logical_plan.PartitionSpec(
                logical_plan.PartitionScheme.UNKNOWN, partition_set.num_partitions()
            ),
        )

    def run(self, plan: logical_plan.LogicalPlan) -> PartitionCacheEntry:
        plan = self.optimize(plan)

        schedule_factory = DynamicScheduleFactory[vPartition]()

        schedule: DynamicSchedule[vPartition] = schedule_factory.schedule_logical_node(plan)
        schedule = ScheduleMaterialize[vPartition](schedule)

        for next_construction in schedule:
            assert next_construction is not None, "Got a None construction in singlethreaded mode"
            self._build_partitions(next_construction)

        final_result = schedule.result_partition_set(LocalPartitionSet)
        pset_entry = self.put_partition_set_into_cache(final_result)
        return pset_entry

    def _build_partitions(self, partspec: Construction[vPartition]) -> None:
        construct_fn = partspec.get_runnable()
        results = construct_fn(partspec.inputs)
        partspec.report_completed(results)
