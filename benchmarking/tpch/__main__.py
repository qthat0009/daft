from __future__ import annotations

import argparse
import contextlib
import csv
import math
import os
import platform
import socket
import subprocess
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from typing import Any, Callable

import daft
import fsspec
import ray
from benchmarking.tpch import answers, data_generation
from daft import DataFrame
from daft.context import get_context
from loguru import logger
from ray.util.placement_group import placement_group, placement_group_table, remove_placement_group

ALL_TABLES = [
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
    "nation",
    "region",
]


class MetricsBuilder:

    NUM_TPCH_QUESTIONS = 10

    HEADERS = [
        "started_at",
        "runner",
        "commit_hash",
        "commit_time",
        "release_tag",
        "env",
        "python_version",
        "github_runner_os",
        "github_runner_arch",
        "github_workflow",
        "github_run_id",
        "github_run_attempt",
        "github_ref",
        *[f"tpch_q{i}" for i in range(1, NUM_TPCH_QUESTIONS + 1)],
        "worker_count",
        "worker_instance_name",
    ]

    def __init__(self, runner: str):
        self._runner = runner
        self._env = "github_actions" if os.getenv("GITHUB_ACTIONS") else socket.gethostname()
        self._commit_hash = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("utf-8").strip()
        self._commit_time = (
            subprocess.check_output(["git", "show", "-s", "--format=%cI", "HEAD"]).decode("utf-8").strip()
        )
        self._release_tag = subprocess.check_output(["git", "tag", "--points-at", "HEAD"]).decode("utf-8").strip()

        self._metrics: dict[str, Any] = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "runner": runner,
            "commit_hash": self._commit_hash,
            "commit_time": self._commit_time,
            "release_tag": self._release_tag,
            "env": self._env,
            "python_version": ".".join(platform.python_version_tuple()),
            "github_runner_os": os.getenv("RUNNER_OS"),
            "github_runner_arch": os.getenv("RUNNER_ARCH"),
            "github_workflow": os.getenv("GITHUB_WORKFLOW"),
            "github_run_id": os.getenv("GITHUB_RUN_ID"),
            "github_run_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
            "github_ref": os.getenv("GITHUB_REF"),
            "worker_count": os.getenv("WORKER_COUNT"),
            "worker_instance_name": os.getenv("WORKER_INSTANCE_NAME"),
        }

    @contextlib.contextmanager
    def collect_metrics(self, qnum: int):
        logger.info(f"Running benchmarks for TPC-H q{qnum}")
        start = time.time()
        yield
        walltime_s = time.time() - start
        logger.info(f"Finished benchmarks for q{qnum}: {walltime_s}s")
        self._metrics[f"tpch_q{qnum}"] = walltime_s

    def dump_csv(self, csv_output_location: str):
        if len(self._metrics) == 0:
            logger.warning("No metrics to upload!")

        with open(csv_output_location, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            writer.writerow(MetricsBuilder.HEADERS)
            writer.writerow([self._metrics.get(header, "") for header in MetricsBuilder.HEADERS])


def get_df_with_parquet_folder(parquet_folder: str) -> Callable[[str], DataFrame]:
    def _get_df(table_name: str) -> DataFrame:
        return DataFrame.read_parquet(os.path.join(parquet_folder, table_name, "*.parquet"))

    return _get_df


def run_all_benchmarks(parquet_folder: str, skip_questions: set[int], csv_output_location: str | None):
    get_df = get_df_with_parquet_folder(parquet_folder)

    daft_context = get_context()
    metrics_builder = MetricsBuilder(daft_context.runner_config.name)

    for i in range(1, 11):

        if i in skip_questions:
            logger.warning(f"Skipping TPC-H q{i}")
            continue

        answer = getattr(answers, f"q{i}")
        daft_df = answer(get_df)

        with metrics_builder.collect_metrics(i):
            daft_df.collect()

    if csv_output_location:
        logger.info(f"Writing CSV to: {csv_output_location}")
        metrics_builder.dump_csv(csv_output_location)
    else:
        logger.info(f"No CSV location specified, skipping CSV write")


def generate_parquet_data(tpch_gen_folder: str, scale_factor: float, num_parts: int) -> str:
    """Generates Parquet data and returns the path to the folder

    Args:
        tpch_gen_folder (str): Path to the folder containing the TPCH dbgen tool and generated data
        scale_factor (float): Scale factor to run on in GB

    Returns:
        str: Path to folder containing Parquet files
    """
    csv_folder = data_generation.gen_csv_files(basedir=tpch_gen_folder, scale_factor=scale_factor, num_parts=num_parts)
    return data_generation.gen_parquet(csv_folder)


def warmup_environment(requirements: str | None, parquet_folder: str):
    """Performs necessary setup of Daft on the current benchmarking environment"""
    ctx = daft.context.get_context()

    if ctx.runner_config.name == "ray":

        if ctx.runner_config.address is not None and daft_wheel_location is None:
            raise RuntimeError("Running Ray remotely requires a built Daft wheel to provide to Ray cluster")

        runtime_env = {"py_modules": [daft]}
        if requirements:
            runtime_env.update({"pip": requirements})
        if runtime_env:
            runtime_env.update({"eager_install": True})

        ray.init(
            address=ctx.runner_config.address,
            runtime_env=runtime_env,
        )

        logger.info("Warming up Ray cluster with a function...")
        # NOTE: installation of runtime_env is supposed to be eager but it seems to be happening async.
        # Here we farm out some work on Ray to warm up all the workers by downloading data
        # (See: https://discuss.ray.io/t/how-to-run-a-function-exactly-once-on-each-node/2178)
        @ray.remote(num_cpus=1)
        class WarmUpFunction:
            def ready(self, parquet_folder):
                import daft
                from daft.filesystem import get_filesystem_from_path

                # Download all files in the provided parquet_folder by reading a single byte from each of them
                def head(parquet_folder, filepath):
                    fs = get_filesystem_from_path(parquet_folder)
                    fs.head(filepath, size=1)

                fs = get_filesystem_from_path(parquet_folder)
                all_files = fs.find(parquet_folder)
                futures = []
                with ThreadPoolExecutor() as executor:
                    for f in all_files:
                        futures.append(executor.submit(head, parquet_folder, f))
                wait(futures)
                cache_location = get_context().cache_location
                print(
                    f"Daft cache at {cache_location} warmed up with size: {sum(f.stat().st_size for f in cache_location.glob('**/*') if f.is_file())}"
                )

                return f"{daft.__version__}"

        num_nodes = len([n for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU", 0.0) > 0])
        bundles = [{"CPU": 1} for _ in range(num_nodes)]
        pg = placement_group(bundles=bundles, strategy="STRICT_SPREAD")
        ray.get(pg.ready())
        executors = [WarmUpFunction.options(placement_group=pg).remote() for _ in range(num_nodes)]
        assert ray.get([executor.ready.remote(parquet_folder) for executor in executors]) == [
            f"{daft.__version__}" for _ in range(num_nodes)
        ]
        del executors

        # remove_placement_group is async, so we wait here and assert that it was cleaned up
        # (See: https://docs.ray.io/en/latest/ray-core/placement-group.html?highlight=placement_group#quick-start)
        remove_placement_group(pg)
        time.sleep(1)
        assert placement_group_table(pg)["state"] == "REMOVED"
        logger.info("Ray cluster warmed up")

    elif ctx.runner_config.name == "py":
        get_df = get_df_with_parquet_folder(parquet_folder)
        for table in ALL_TABLES:
            df = get_df(table)
            logger.info(
                f"Warming up local execution environment by loading table {table} and counting rows: {df.count(df.columns[0]).to_pandas()}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpch_gen_folder",
        default="data/tpch-dbgen",
        help="Path to the folder containing the TPCH dbgen tool and generated data",
    )
    parser.add_argument(
        "--parquet_file_cache",
        default=None,
        help="Path to root folder (local or in S3) containing cached Parquet files",
    )
    parser.add_argument("--scale_factor", default=10.0, help="Scale factor to run on in GB", type=float)
    parser.add_argument(
        "--num_parts", default=None, help="Number of parts to generate (defaults to 1 part per GB)", type=int
    )
    parser.add_argument("--skip_questions", type=str, default=None, help="Comma-separated list of questions to skip")
    parser.add_argument("--output_csv", default=None, type=str, help="Location to output CSV file")
    parser.add_argument(
        "--output_csv_headers",
        action="store_true",
        help="DEPRECATED: We always output CSV headers regardless of this flag",
    )
    parser.add_argument(
        "--requirements",
        default=None,
        help="Path to pip-style requirements.txt file to bootstrap environment on remote Ray cluster",
    )
    args = parser.parse_args()

    if args.output_csv_headers:
        warnings.warn("Detected --output_csv_headers flag, but this flag is deprecated - CSVs always output headers")

    num_parts = math.ceil(args.scale_factor) if args.num_parts is None else args.num_parts

    # Generate Parquet data, or skip if data is cached on disk
    parquet_folder: str
    if args.parquet_file_cache is not None:
        parquet_folder = (
            os.path.join(args.parquet_file_cache, str(args.scale_factor).replace(".", "_"), str(num_parts), "parquet")
            + "/"
        )
        fs = fsspec.filesystem("s3" if parquet_folder.startswith("s3://") else "file")
        if not fs.isdir(parquet_folder):
            local_parquet_folder = generate_parquet_data(args.tpch_gen_folder, args.scale_factor, num_parts)
            fs.put(local_parquet_folder, parquet_folder, recursive=True)
    else:
        parquet_folder = generate_parquet_data(args.tpch_gen_folder, args.scale_factor, num_parts)

    warmup_environment(args.requirements, parquet_folder)

    run_all_benchmarks(
        parquet_folder,
        skip_questions={int(s) for s in args.skip_questions.split(",")} if args.skip_questions is not None else set(),
        csv_output_location=args.output_csv,
    )
