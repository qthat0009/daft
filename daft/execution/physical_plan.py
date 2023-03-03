"""
This file contains physical plan building blocks.
To get a physical plan for a logical plan, see physical_plan_factory.py.

Conceptually, a physical plan decides what steps, and the order of steps, to run to build some target.
Physical plans are closely related to logical plans. A logical plan describes "what you want", and a physical plan figures out "what to do" to get it.
They are not exact analogues, especially due to the ability of a physical plan to dynamically decide what to do next.

Physical plans are implemented here as an iterator of PartitionTask | None.
When a physical plan returns None, it means it cannot tell you what the next step is,
because it is waiting for the result of a previous PartitionTask to can decide what to do next.
"""

from __future__ import annotations

import math
from collections import deque
from typing import Generator, Iterator, List, TypeVar, Union

from daft.execution import execution_step
from daft.execution.execution_step import (
    Instruction,
    PartitionTask,
    PartitionTaskBuilder,
    ReduceInstruction,
    SingleOutputPartitionTask,
)
from daft.logical import logical_plan
from daft.resource_request import ResourceRequest

PartitionT = TypeVar("PartitionT")
T = TypeVar("T")


# A PhysicalPlan that is still being built - may yield both PartitionTaskBuilders and PartitionTasks.
InProgressPhysicalPlan = Iterator[Union[None, PartitionTask[PartitionT], PartitionTaskBuilder[PartitionT]]]

# A PhysicalPlan that is complete and will only yield PartitionTasks.
MaterializedPhysicalPlan = Generator[
    Union[None, PartitionTask[PartitionT]],
    None,
    List[PartitionT],
]


def partition_read(partitions: Iterator[PartitionT]) -> InProgressPhysicalPlan[PartitionT]:
    """Instantiate a (no-op) physical plan from existing partitions."""
    yield from (PartitionTaskBuilder[PartitionT](inputs=[partition]) for partition in partitions)


def file_read(
    child_plan: InProgressPhysicalPlan[PartitionT],
    scan_info: logical_plan.TabularFilesScan,
) -> InProgressPhysicalPlan[PartitionT]:
    """child_plan represents partitions with filenames.

    Yield a plan to read those filenames.
    """

    materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    output_partition_index = 0

    while True:
        # Check if any inputs finished executing.
        while len(materializations) > 0 and materializations[0].done():
            done_task = materializations.popleft()

            vpartition = done_task.vpartition()
            file_sizes_bytes = vpartition.to_pydict()["size"]

            # Emit one partition for each file (NOTE: hardcoded for now).
            for i in range(len(vpartition)):

                file_read_step = PartitionTaskBuilder[PartitionT](inputs=[done_task.partition()]).add_instruction(
                    instruction=execution_step.ReadFile(
                        partition_id=output_partition_index,
                        logplan=scan_info,
                        index=i,
                    ),
                    # Set the filesize as the memory request.
                    # (Note: this is very conservative; file readers empirically use much more peak memory than 1x file size.)
                    resource_request=ResourceRequest(memory_bytes=file_sizes_bytes[i]),
                )
                yield file_read_step
                output_partition_index += 1

        # Materialize a single dependency.
        try:
            child_step = next(child_plan)
            if isinstance(child_step, PartitionTaskBuilder):
                child_step = child_step.finalize_partition_task_single_output()
                materializations.append(child_step)
            yield child_step

        except StopIteration:
            if len(materializations) > 0:
                yield None
            else:
                return


def file_write(
    child_plan: InProgressPhysicalPlan[PartitionT],
    write_info: logical_plan.FileWrite,
) -> InProgressPhysicalPlan[PartitionT]:
    """Write the results of `child_plan` into files described by `write_info`."""

    yield from (
        step.add_instruction(
            execution_step.WriteFile(partition_id=index, logplan=write_info),
        )
        if isinstance(step, PartitionTaskBuilder)
        else step
        for index, step in enumerate_open_executions(child_plan)
    )


def pipeline_instruction(
    child_plan: InProgressPhysicalPlan[PartitionT],
    pipeable_instruction: Instruction,
    resource_request: execution_step.ResourceRequest,
) -> InProgressPhysicalPlan[PartitionT]:
    """Apply an instruction to the results of `child_plan`."""

    yield from (
        step.add_instruction(pipeable_instruction, resource_request) if isinstance(step, PartitionTaskBuilder) else step
        for step in child_plan
    )


def join(
    left_plan: InProgressPhysicalPlan[PartitionT],
    right_plan: InProgressPhysicalPlan[PartitionT],
    join: logical_plan.Join,
) -> InProgressPhysicalPlan[PartitionT]:
    """Pairwise join the partitions from `left_child_plan` and `right_child_plan` together."""

    # Materialize the steps from the left and right sources to get partitions.
    # As the materializations complete, emit new steps to join each left and right partition.
    left_requests: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    right_requests: deque[SingleOutputPartitionTask[PartitionT]] = deque()

    while True:
        # Emit new join steps if we have left and right partitions ready.
        while (
            len(left_requests) > 0 and len(right_requests) > 0 and left_requests[0].done() and right_requests[0].done()
        ):
            next_left = left_requests.popleft()
            next_right = right_requests.popleft()

            join_step = PartitionTaskBuilder[PartitionT](
                inputs=[next_left.partition(), next_right.partition()],
                resource_request=ResourceRequest(
                    memory_bytes=next_left.partition_metadata().size_bytes + next_right.partition_metadata().size_bytes
                ),
            ).add_instruction(instruction=execution_step.Join(join))
            yield join_step

        # Exhausted all ready inputs; execute a single child step to get more join inputs.
        # Choose whether to execute from left child or right child (whichever one is more behind),
        if len(left_requests) <= len(right_requests):
            next_plan, next_requests = left_plan, left_requests
        else:
            next_plan, next_requests = right_plan, right_requests

        try:
            step = next(next_plan)
            if isinstance(step, PartitionTaskBuilder):
                step = step.finalize_partition_task_single_output()
                next_requests.append(step)
            yield step

        except StopIteration:
            # Left and right child plans have completed.
            # Are we still waiting for materializations to complete? (We will emit more joins from them).
            if len(left_requests) + len(right_requests) > 0:
                yield None

            # Otherwise, we are entirely done.
            else:
                return


def local_limit(
    child_plan: InProgressPhysicalPlan[PartitionT],
    limit: int,
) -> Generator[None | PartitionTask[PartitionT] | PartitionTaskBuilder[PartitionT], int, None]:
    """Apply a limit instruction to each partition in the child_plan.

    limit:
        The value of the limit to apply to each partition.

    Yields: PartitionTask with the limit applied.
    Send back: A new value to the limit (optional). This allows you to update the limit after each partition if desired.
    """
    for step in child_plan:
        if not isinstance(step, PartitionTaskBuilder):
            yield step
        else:
            maybe_new_limit = yield step.add_instruction(
                execution_step.LocalLimit(limit),
            )
            if maybe_new_limit is not None:
                limit = maybe_new_limit


def global_limit(
    child_plan: InProgressPhysicalPlan[PartitionT],
    global_limit: logical_plan.GlobalLimit,
) -> InProgressPhysicalPlan[PartitionT]:
    """Return the first n rows from the `child_plan`."""

    remaining_rows = global_limit._num
    assert remaining_rows >= 0, f"Invalid value for limit: {remaining_rows}"
    remaining_partitions = global_limit.num_partitions()

    materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()

    # To dynamically schedule the global limit, we need to apply an appropriate limit to each child partition.
    # We don't know their exact sizes since they are pending execution, so we will have to iteratively execute them,
    # count their rows, and then apply and update the remaining limit.

    # As an optimization, push down a limit into each partition to reduce what gets materialized,
    # since we will never take more than the remaining limit anyway.
    child_plan = local_limit(child_plan=child_plan, limit=remaining_rows)
    started = False

    while True:
        # Check if any inputs finished executing.
        # Apply and deduct the rolling global limit.
        while len(materializations) > 0 and materializations[0].done():
            done_task = materializations.popleft()

            limit = remaining_rows and min(remaining_rows, done_task.partition_metadata().num_rows)

            global_limit_step = PartitionTaskBuilder[PartitionT](
                inputs=[done_task.partition()],
                resource_request=ResourceRequest(memory_bytes=done_task.partition_metadata().size_bytes),
            ).add_instruction(
                instruction=execution_step.LocalLimit(limit),
            )
            yield global_limit_step
            remaining_partitions -= 1
            remaining_rows -= limit

            if remaining_rows == 0:
                # We only need to return empty partitions now.
                # Instead of computing new ones and applying limit(0),
                # we can just reuse an existing computed partition.

                # Cancel all remaining results; we won't need them.
                while len(materializations) > 0:
                    materializations.pop().cancel()

                yield from (
                    PartitionTaskBuilder[PartitionT](
                        inputs=[done_task.partition()],
                        resource_request=ResourceRequest(memory_bytes=done_task.partition_metadata().size_bytes),
                    ).add_instruction(
                        instruction=execution_step.LocalLimit(0),
                    )
                    for _ in range(remaining_partitions)
                )
                return

        # (Optimization. If we are doing limit(0) and already have a partition executing to use for it, just wait.)
        if remaining_rows == 0 and len(materializations) > 0:
            yield None
            continue

        # Execute a single child partition.
        try:
            child_step = child_plan.send(remaining_rows) if started else next(child_plan)
            started = True
            if isinstance(child_step, PartitionTaskBuilder):
                child_step = child_step.finalize_partition_task_single_output()
                materializations.append(child_step)
            yield child_step

        except StopIteration:
            if len(materializations) > 0:
                yield None
            else:
                return


def coalesce(
    child_plan: InProgressPhysicalPlan[PartitionT],
    coalesce: logical_plan.Coalesce,
) -> InProgressPhysicalPlan[PartitionT]:
    """Coalesce the results of the child_plan into fewer partitions.

    The current implementation only does partition merging, no rebalancing.
    """

    coalesce_from = coalesce._children()[0].num_partitions()
    coalesce_to = coalesce.num_partitions()
    assert coalesce_to <= coalesce_from, f"Cannot coalesce upwards from {coalesce_from} to {coalesce_to} partitions."

    starts = [math.ceil((coalesce_from / coalesce_to) * i) for i in range(coalesce_to)]
    stops = [math.ceil((coalesce_from / coalesce_to) * i) for i in range(1, coalesce_to + 1)]
    # For each output partition, the number of input partitions to merge in.
    merges_per_result = deque([stop - start for start, stop in zip(starts, stops)])

    materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()

    while True:

        # See if we can emit a coalesced partition.
        num_partitions_to_merge = merges_per_result[0]
        ready_to_coalesce = [task for task in list(materializations)[:num_partitions_to_merge] if task.done()]
        if len(ready_to_coalesce) == num_partitions_to_merge:
            # Coalesce the partition and emit it.
            merge_step = PartitionTaskBuilder[PartitionT](
                inputs=[_.partition() for _ in ready_to_coalesce],
                resource_request=ResourceRequest(
                    memory_bytes=sum(_.partition_metadata().size_bytes for _ in ready_to_coalesce),
                ),
            ).add_instruction(
                instruction=execution_step.ReduceMerge(),
            )
            [materializations.popleft() for _ in range(num_partitions_to_merge)]
            merges_per_result.popleft()
            yield merge_step

        # Cannot emit a coalesced partition.
        # Materialize a single dependency.
        try:
            child_step = next(child_plan)
            if isinstance(child_step, PartitionTaskBuilder):
                child_step = child_step.finalize_partition_task_single_output()
                materializations.append(child_step)
            yield child_step

        except StopIteration:
            if len(materializations) > 0:
                yield None
            else:
                return


def reduce(
    fanout_plan: InProgressPhysicalPlan[PartitionT],
    num_partitions: int,
    reduce_instruction: ReduceInstruction,
) -> InProgressPhysicalPlan[PartitionT]:
    """Reduce the result of fanout_plan.

    The child plan fanout_plan must produce a 2d list of partitions,
    by producing a single list in each step.

    Then, the reduce instruction is applied to each `i`th slice across the child lists.
    """

    materializations = list()

    # Dispatch all fanouts.
    for step in fanout_plan:
        if isinstance(step, PartitionTaskBuilder):
            step = step.finalize_partition_task_multi_output(num_partitions)
            materializations.append(step)
        yield step

    # All fanouts dispatched. Wait for all of them to materialize
    # (since we need all of them to emit even a single reduce).
    while any(not _.done() for _ in materializations):
        yield None

    inputs_to_reduce = [deque(_.partitions()) for _ in materializations]
    metadatas = [deque(_.partition_metadatas()) for _ in materializations]
    del materializations

    # Yield all the reduces in order.
    while len(inputs_to_reduce[0]) > 0:
        partition_batch = [_.popleft() for _ in inputs_to_reduce]
        metadata_batch = [_.popleft() for _ in metadatas]
        yield PartitionTaskBuilder[PartitionT](
            inputs=partition_batch,
            instructions=[reduce_instruction],
            resource_request=ResourceRequest(
                memory_bytes=sum(metadata.size_bytes for metadata in metadata_batch),
            ),
        )


def sort(
    child_plan: InProgressPhysicalPlan[PartitionT],
    sort_info: logical_plan.Sort,
) -> InProgressPhysicalPlan[PartitionT]:
    """Sort the result of `child_plan` according to `sort_info`."""

    # First, materialize the child plan.
    source_materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    for step in child_plan:
        if isinstance(step, PartitionTaskBuilder):
            step = step.finalize_partition_task_single_output()
            source_materializations.append(step)
        yield step

    # Sample all partitions (to be used for calculating sort boundaries).
    sample_materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    for source in source_materializations:
        while not source.done():
            yield None
        sample = (
            PartitionTaskBuilder[PartitionT](inputs=[source.partition()])
            .add_instruction(
                instruction=execution_step.Sample(sort_by=sort_info._sort_by),
            )
            .finalize_partition_task_single_output()
        )
        sample_materializations.append(sample)
        yield sample

    # Wait for samples to materialize.
    while any(not _.done() for _ in sample_materializations):
        yield None

    # Reduce the samples to get sort boundaries.
    boundaries = (
        PartitionTaskBuilder[PartitionT](
            inputs=[sample.partition() for sample in consume_deque(sample_materializations)]
        )
        .add_instruction(
            execution_step.ReduceToQuantiles(
                num_quantiles=sort_info.num_partitions(),
                sort_by=sort_info._sort_by,
                descending=sort_info._descending,
            ),
        )
        .finalize_partition_task_single_output()
    )
    yield boundaries

    # Wait for boundaries to materialize.
    while not boundaries.done():
        yield None

    # Create a range fanout plan.
    range_fanout_plan = (
        PartitionTaskBuilder[PartitionT](
            inputs=[boundaries.partition(), source.partition()],
            resource_request=ResourceRequest(
                memory_bytes=source.partition_metadata().size_bytes,
            ),
        ).add_instruction(
            instruction=execution_step.FanoutRange[PartitionT](
                num_outputs=sort_info.num_partitions(),
                sort_by=sort_info._sort_by,
                descending=sort_info._descending,
            ),
        )
        for source in consume_deque(source_materializations)
    )

    # Execute a sorting reduce on it.
    yield from reduce(
        fanout_plan=range_fanout_plan,
        num_partitions=sort_info.num_partitions(),
        reduce_instruction=execution_step.ReduceMergeAndSort(
            sort_by=sort_info._sort_by,
            descending=sort_info._descending,
        ),
    )


def fanout_random(child_plan: InProgressPhysicalPlan[PartitionT], node: logical_plan.Repartition):
    """Splits the results of `child_plan` randomly into a list of `node.num_partitions()` number of partitions"""
    seed = 0
    for step in child_plan:
        if isinstance(step, PartitionTaskBuilder):
            instruction = execution_step.FanoutRandom(node.num_partitions(), seed)
            step = step.add_instruction(instruction)
        yield step
        seed += 1


def materialize(
    child_plan: InProgressPhysicalPlan[PartitionT],
) -> MaterializedPhysicalPlan:
    """Materialize the child plan.

    Returns (via generator return): the completed plan's result partitions.
    """

    materializations = list()

    for step in child_plan:
        if isinstance(step, PartitionTaskBuilder):
            step = step.finalize_partition_task_single_output()
            materializations.append(step)
        assert isinstance(step, (PartitionTask, type(None)))

        yield step

    while any(not _.done() for _ in materializations):
        yield None

    return [_.partition() for _ in materializations]


def enumerate_open_executions(
    schedule: InProgressPhysicalPlan[PartitionT],
) -> Iterator[tuple[int, None | PartitionTask[PartitionT] | PartitionTaskBuilder[PartitionT]]]:
    """Helper. Like enumerate() on an iterator, but only counts up if the result is an PartitionTaskBuilder.

    Intended for counting the number of PartitionTaskBuilders returned by the iterator.
    """
    index = 0
    for item in schedule:
        if isinstance(item, PartitionTaskBuilder):
            yield index, item
            index += 1
        else:
            yield index, item


def consume_deque(dq: deque[T]) -> Iterator[T]:
    while len(dq) > 0:
        yield dq.popleft()
