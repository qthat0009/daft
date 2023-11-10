from __future__ import annotations

import dataclasses
import pathlib
import sys
import urllib.parse

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal

import logging
from typing import Any

import fsspec
import pyarrow as pa
from fsspec.registry import get_filesystem_class
from pyarrow.fs import (
    FileSystem,
    FSSpecHandler,
    GcsFileSystem,
    LocalFileSystem,
    PyFileSystem,
    S3FileSystem,
)
from pyarrow.fs import _resolve_filesystem_and_path as pafs_resolve_filesystem_and_path

from daft.daft import FileFormat, FileInfos, IOConfig, io_glob
from daft.table import Table

logger = logging.getLogger(__name__)

_CACHED_FSES: dict[tuple[str, IOConfig | None], FileSystem] = {}


def _get_fs_from_cache(protocol: str, io_config: IOConfig | None) -> FileSystem | None:
    """
    Get an instantiated pyarrow filesystem from the cache based on the URI protocol.

    Returns None if no such cache entry exists.
    """
    global _CACHED_FSES

    return _CACHED_FSES.get((protocol, io_config))


def _put_fs_in_cache(protocol: str, fs: FileSystem, io_config: IOConfig | None) -> None:
    """Put pyarrow filesystem in cache under provided protocol."""
    global _CACHED_FSES

    _CACHED_FSES[(protocol, io_config)] = fs


@dataclasses.dataclass(frozen=True)
class ListingInfo:
    path: str
    size: int
    type: Literal["file"] | Literal["directory"]
    rows: int | None = None


def _get_s3fs_kwargs() -> dict[str, Any]:
    """Get keyword arguments to forward to s3fs during construction"""

    try:
        import botocore.session
    except ImportError:
        logger.error(
            "Error when importing botocore. Install getdaft[aws] for the required 3rd party dependencies to interact with AWS S3 (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
        )
        raise

    kwargs = {}

    # If accessing S3 without credentials, use anonymous access: https://github.com/Eventual-Inc/Daft/issues/503
    credentials_available = botocore.session.get_session().get_credentials() is not None
    if not credentials_available:
        logger.warning(
            "AWS credentials not found - using anonymous access to S3 which will fail if the bucket you are accessing is not a public bucket. See boto3 documentation for more details on configuring your AWS credentials: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html"
        )
        kwargs["anon"] = True

    # kwargs["default_fill_cache"] = False

    return kwargs


def get_filesystem(protocol: str, **kwargs) -> fsspec.AbstractFileSystem:
    if protocol == "s3" or protocol == "s3a":
        kwargs = {**kwargs, **_get_s3fs_kwargs()}

    try:
        klass = fsspec.get_filesystem_class(protocol)
    except ImportError:
        logger.error(
            f"Error when importing dependencies for accessing data with: {protocol}. Please ensure that getdaft was installed with the appropriate extra dependencies (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
        )
        raise

    fs = klass(**kwargs)
    return fs


def get_protocol_from_path(path: str) -> str:
    parsed_scheme = urllib.parse.urlparse(path, allow_fragments=False).scheme
    parsed_scheme = parsed_scheme.lower()
    if parsed_scheme == "" or parsed_scheme is None:
        return "file"
    if sys.platform == "win32" and len(parsed_scheme) == 1 and ("a" <= parsed_scheme) and (parsed_scheme <= "z"):
        return "file"
    return parsed_scheme


_CANONICAL_PROTOCOLS = {
    "gcs": "gs",
    "https": "http",
    "s3a": "s3",
    "ssh": "sftp",
    "arrow_hdfs": "hdfs",
    "az": "abfs",
    "blockcache": "cached",
    "jlab": "jupyter",
}


def canonicalize_protocol(protocol: str) -> str:
    """
    Return the canonical protocol from the provided protocol, such that there's a 1:1
    mapping between protocols and pyarrow/fsspec filesystem implementations.
    """
    return _CANONICAL_PROTOCOLS.get(protocol, protocol)


def get_filesystem_from_path(path: str, **kwargs) -> fsspec.AbstractFileSystem:
    protocol = get_protocol_from_path(path)
    fs = get_filesystem(protocol, **kwargs)
    return fs


def _resolve_paths_and_filesystem(
    paths: str | pathlib.Path | list[str],
    io_config: IOConfig | None = None,
) -> tuple[list[str], FileSystem]:
    """
    Resolves and normalizes all provided paths, infers a filesystem from the
    paths, and ensures that all paths use the same filesystem.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        io_config: A Daft IOConfig that should be best-effort applied onto the returned
            FileSystem
    """
    if isinstance(paths, pathlib.Path):
        paths = str(paths)
    if isinstance(paths, str):
        paths = [paths]
    assert isinstance(paths, list), paths
    assert all(isinstance(p, str) for p in paths), paths
    assert len(paths) > 0, paths

    # Ensure that protocols for all paths are consistent, i.e. that they would map to the
    # same filesystem.
    protocols = {get_protocol_from_path(path) for path in paths}
    canonicalized_protocols = {canonicalize_protocol(protocol) for protocol in protocols}
    if len(canonicalized_protocols) > 1:
        raise ValueError(
            "All paths must have the same canonical protocol to ensure that they are all "
            f"hitting the same storage backend, but got protocols {protocols} with canonical "
            f"protocols - {canonicalized_protocols} and full paths - {paths}"
        )

    # Canonical protocol shared by all paths.
    protocol = next(iter(canonicalized_protocols))

    # Try to get filesystem from protocol -> fs cache.
    cached_filesystem = _get_fs_from_cache(protocol, io_config)

    # Resolve path and filesystem for the first path.
    # We use this first resolved filesystem for validation on all other paths.
    resolved_path, resolved_filesystem = _infer_filesystem(paths[0], cached_filesystem)

    if cached_filesystem is None:
        # Put resolved filesystem in cache under these paths' canonical protocol.
        _put_fs_in_cache(protocol, resolved_filesystem, io_config)

    # filesystem should be a non-None pyarrow FileSystem at this point, either
    # user-provided, taken from the cache, or inferred from the first path.
    assert resolved_filesystem is not None and isinstance(resolved_filesystem, FileSystem)

    # Resolve all other paths and validate with the user-provided/cached/inferred filesystem.
    resolved_paths = [resolved_path]
    for path in paths[1:]:
        resolved_path = _validate_filesystem(path, resolved_filesystem, io_config)
        resolved_paths.append(resolved_path)

    return resolved_paths, resolved_filesystem


def _validate_filesystem(path: str, fs: FileSystem, io_config: IOConfig | None) -> str:
    resolved_path, inferred_fs = _infer_filesystem(path, io_config)
    if not isinstance(fs, type(inferred_fs)):
        raise RuntimeError(
            f"Cannot read multiple paths with different inferred PyArrow filesystems. Expected: {fs} but received: {inferred_fs}"
        )
    return resolved_path


def _infer_filesystem(
    path: str,
    io_config: IOConfig | None,
) -> tuple[str, FileSystem]:
    """
    Resolves and normalizes the provided path, infers a filesystem from the
    path, and ensures that the inferred filesystem is compatible with the passed
    filesystem, if provided.

    Args:
        path: A single file/directory path.
        io_config: A Daft IOConfig that should be best-effort applied onto the returned
            FileSystem
        cached_filesystem: If provided, this filesystem will be validated against the
            filesystem inferred from the provided path to ensure compatibility. If None,
            a filesystem will be inferred.
    """
    protocol = get_protocol_from_path(path)
    translated_kwargs: dict[str, Any]

    ###
    # S3
    ###
    if protocol == "s3":
        translated_kwargs = {}
        if io_config is not None and io_config.s3 is not None:
            pass  # TODO: Translate Daft S3Config to pyarrow S3FileSystem kwargs

        resolved_filesystem = S3FileSystem(**translated_kwargs)
        resolved_path = resolved_filesystem.normalize_path(_unwrap_protocol(path))
        return resolved_path, resolved_filesystem

    ###
    # Local
    ###
    elif protocol == "file":
        # NOTE: PyArrow really dislikes the "file://" prefix, so we trim it out if present
        path = path[7:] if path.startswith("file://") else path
        resolved_filesystem = LocalFileSystem()
        resolved_path = resolved_filesystem.normalize_path(_unwrap_protocol(path))
        return resolved_path, resolved_filesystem

    ###
    # GCS
    ###
    elif protocol in {"gs", "gcs"}:
        translated_kwargs = {}
        if io_config is not None and io_config.gcs is not None:
            pass  # TODO: Translate Daft GCSConfig to pyarrow S3FileSystem kwargs

        resolved_filesystem = GcsFileSystem(**translated_kwargs)
        resolved_path = resolved_filesystem.normalize_path(_unwrap_protocol(path))
        return resolved_path, resolved_filesystem

    ###
    # HTTP: Use FSSpec as a fallback
    ###
    elif protocol in {"http", "https"}:
        fsspec_fs_cls = get_filesystem_class(protocol)
        fsspec_fs = fsspec_fs_cls()
        resolved_filesystem, resolved_path = pafs_resolve_filesystem_and_path(path, fsspec_fs)
        resolved_path = resolved_filesystem.normalize_path(resolved_path)
        return resolved_path, resolved_filesystem

    ###
    # Azure: Unsupported! Users should use our native IO layer if possible.
    ###
    elif protocol in {"az", "abfs"}:
        raise NotImplementedError(
            "PyArrow filesystem does not support Azure blob storage. Please Daft's native IO layers instead or make "
            "an issue to let us know which operation isn't yet supported!"
        )

    else:
        raise NotImplementedError(f"Cannot infer PyArrow filesystem for protocol {protocol}: please file an issue!")


def _is_http_fs(fs: FileSystem) -> bool:
    """Returns whether the provided pyarrow filesystem is an HTTP filesystem."""
    from fsspec.implementations.http import HTTPFileSystem

    return (
        isinstance(fs, PyFileSystem)
        and isinstance(fs.handler, FSSpecHandler)
        and isinstance(fs.handler.fs, HTTPFileSystem)
    )


def _unwrap_protocol(path):
    """
    Slice off any protocol prefixes on path.
    """
    parsed = urllib.parse.urlparse(path, allow_fragments=False)  # support '#' in path
    query = "?" + parsed.query if parsed.query else ""  # support '?' in path
    return parsed.netloc + parsed.path + query


###
# File globbing
###


def _ensure_path_protocol(protocol: str, returned_path: str) -> str:
    """This function adds the protocol that fsspec strips from returned results"""
    if protocol == "file":
        return returned_path
    parsed_scheme = urllib.parse.urlparse(returned_path).scheme
    if parsed_scheme == "" or parsed_scheme is None:
        return f"{protocol}://{returned_path}"
    return returned_path


def _path_is_glob(path: str) -> bool:
    # fsspec glob supports *, ? and [..] patterns
    # See: : https://filesystem-spec.readthedocs.io/en/latest/api.html
    return any([char in path for char in ["*", "?", "["]])


def glob_path_with_stats(
    path: str,
    file_format: FileFormat | None,
    io_config: IOConfig | None,
) -> FileInfos:
    """Glob a path, returning a list ListingInfo."""
    files = io_glob(path, io_config=io_config)
    filepaths_to_infos = {f["path"]: {"size": f["size"], "type": f["type"]} for f in files}

    # Set number of rows if available.
    if file_format is not None and file_format == FileFormat.Parquet:
        parquet_statistics = Table.read_parquet_statistics(
            list(filepaths_to_infos.keys()),
            io_config=io_config,
        ).to_pydict()
        for path, num_rows in zip(parquet_statistics["uris"], parquet_statistics["row_count"]):
            filepaths_to_infos[path]["rows"] = num_rows

    file_paths = []
    file_sizes = []
    num_rows = []
    for path, infos in filepaths_to_infos.items():
        file_paths.append(path)
        file_sizes.append(infos.get("size"))
        num_rows.append(infos.get("rows"))

    return FileInfos.from_infos(file_paths=file_paths, file_sizes=file_sizes, num_rows=num_rows)


def _get_parquet_metadata_single(path: str) -> pa.parquet.FileMetadata:
    """Get the Parquet metadata for a given Parquet file."""
    fs = get_filesystem_from_path(path)
    with fs.open(path) as f:
        return pa.parquet.ParquetFile(f).metadata
