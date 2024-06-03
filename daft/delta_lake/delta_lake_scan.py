from __future__ import annotations

import logging
import os
from collections.abc import Iterator

from deltalake.table import DeltaTable

import daft
from daft.daft import (
    FileFormatConfig,
    ParquetSourceConfig,
    Pushdowns,
    ScanTask,
    StorageConfig,
)
from daft.io.object_store_options import io_config_to_storage_options
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.schema import Schema

logger = logging.getLogger(__name__)


class DeltaLakeScanOperator(ScanOperator):
    def __init__(self, table_uri: str, storage_config: StorageConfig) -> None:
        super().__init__()
        storage_options = io_config_to_storage_options(storage_config.config.io_config, table_uri)
        self._table = DeltaTable(table_uri, storage_options=storage_options)
        self._storage_config = storage_config
        self._schema = Schema.from_pyarrow_schema(self._table.schema().to_pyarrow())
        partition_columns = set(self._table.metadata().partition_columns)
        self._partition_keys = [
            PartitionField(field._field) for field in self._schema if field.name in partition_columns
        ]

    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"DeltaLakeScanOperator({self._table.metadata().name})"

    def partitioning_keys(self) -> list[PartitionField]:
        return self._partition_keys

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partitioning keys = {self.partitioning_keys()}",
            # TODO(Clark): Improve repr of storage config here.
            f"Storage config = {self._storage_config}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        import pyarrow as pa

        # TODO(Clark): Push limit and filter expressions into deltalake action fetch, to prune the files returned.
        # Issue: https://github.com/Eventual-Inc/Daft/issues/1953
        add_actions: pa.RecordBatch = self._table.get_add_actions()

        if len(self.partitioning_keys()) > 0 and pushdowns.partition_filters is None:
            logging.warning(
                "%s has partitioning keys = %s, but no partition filter was specified. This will result in a full table scan.",
                self.display_name(),
                self.partitioning_keys(),
            )

        # TODO(Clark): Add support for deletion vectors.
        # Issue: https://github.com/Eventual-Inc/Daft/issues/1954
        if "deletionVector" in add_actions.schema.names:
            raise NotImplementedError(
                "Delta Lake deletion vectors are not yet supported; please let the Daft team know if you'd like to see this feature!\n"
                "Deletion records can be dropped from this table to allow it to be read with Daft: https://docs.delta.io/latest/delta-drop-feature.html"
            )

        # TODO(Clark): Add support for column mappings.
        # Issue: https://github.com/Eventual-Inc/Daft/issues/1955

        limit_files = pushdowns.limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None
        rows_left = pushdowns.limit if pushdowns.limit is not None else 0
        scan_tasks = []
        is_partitioned = (
            "partition_values" in add_actions.schema.names
            and add_actions.schema.field("partition_values").type.num_fields > 0
        )
        for task_idx in range(add_actions.num_rows):
            if limit_files and rows_left <= 0:
                break

            # NOTE: The paths in the transaction log consist of the post-table-uri suffix.
            path = os.path.join(self._table.table_uri, add_actions["path"][task_idx].as_py())
            record_count = add_actions["num_records"][task_idx].as_py()
            try:
                size_bytes = add_actions["size_bytes"][task_idx].as_py()
            except KeyError:
                size_bytes = None
            file_format_config = FileFormatConfig.from_parquet_config(ParquetSourceConfig())

            if is_partitioned:
                dtype = add_actions.schema.field("partition_values").type
                part_values = add_actions["partition_values"][task_idx]
                arrays = {}
                for field_idx in range(dtype.num_fields):
                    field_name = dtype.field(field_idx).name
                    try:
                        arrow_arr = pa.array([part_values[field_name]], type=dtype.field(field_idx).type)
                    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                        # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                        arrow_arr = pa.array([part_values[field_name].as_py()], type=dtype.field(field_idx).type)
                    arrays[field_name] = daft.Series.from_arrow(arrow_arr, field_name)
                partition_values = daft.table.Table.from_pydict(arrays)._table
            else:
                partition_values = None

            # Populate scan task with column-wise stats.
            dtype = add_actions.schema.field("min").type
            min_values = add_actions["min"][task_idx]
            max_values = add_actions["max"][task_idx]
            # TODO(Clark): Add support for tracking null counts in column stats.
            # null_counts = add_actions["null_count"][task_idx]
            arrays = {}
            for field_idx in range(dtype.num_fields):
                field_name = dtype.field(field_idx).name
                try:
                    arrow_arr = pa.array(
                        [min_values[field_name], max_values[field_name]], type=dtype.field(field_idx).type
                    )
                except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                    # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                    arrow_arr = pa.array(
                        [min_values[field_name].as_py(), max_values[field_name].as_py()],
                        type=dtype.field(field_idx).type,
                    )
                arrays[field_name] = daft.Series.from_arrow(arrow_arr, field_name)
            stats = daft.table.Table.from_pydict(arrays)

            st = ScanTask.catalog_scan_task(
                file=path,
                file_format=file_format_config,
                schema=self._schema._schema,
                num_rows=record_count,
                storage_config=self._storage_config,
                size_bytes=size_bytes,
                pushdowns=pushdowns,
                partition_values=partition_values,
                stats=stats._table,
            )
            if st is None:
                continue
            rows_left -= record_count
            scan_tasks.append(st)
        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True
