from __future__ import annotations

import json
import logging
import os
import shutil
import tracemalloc
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from time import time

import psutil
from azure.monitor.opentelemetry import configure_azure_monitor

LOGGER_NAME = "benchmark_runner"


def configure_logging(connection_string: str | None) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if connection_string:
        configure_azure_monitor(
            connection_string=connection_string,
            logger_name=LOGGER_NAME,
        )
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    if not tracemalloc.is_tracing():
        tracemalloc.start()
    return logger


def emit_event(logger: logging.Logger, event: str, **fields: object) -> None:
    payload = {
        "event": event,
        "ts": time(),
        **fields,
    }
    logger.info(json.dumps(payload, default=_json_default, sort_keys=True))


def _json_default(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "asdict"):
        return value.asdict()
    if hasattr(value, "__dict__"):
        return value.__dict__
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


@dataclass(frozen=True)
class ResourceSample:
    rss_bytes: int
    vms_bytes: int
    python_current_bytes: int
    python_peak_bytes: int
    cgroup_memory_current_bytes: int | None
    cgroup_memory_max_bytes: int | None
    work_dir_bytes: int
    temp_dir_bytes: int
    filesystem_total_bytes: int
    filesystem_used_bytes: int
    filesystem_free_bytes: int

    def asdict(self) -> dict[str, int | None]:
        return asdict(self)


class ResourceSampler:
    def __init__(self, work_root: Path) -> None:
        self._process = psutil.Process(os.getpid())
        self._work_root = work_root
        self._size_cache: dict[Path, tuple[float, int]] = {}
        self._size_cache_ttl_seconds = 5.0

    def sample(self) -> ResourceSample:
        mem = self._process.memory_info()
        current, peak = tracemalloc.get_traced_memory()
        total, used, free = shutil.disk_usage(self._work_root)
        cgroup_current, cgroup_max = _read_cgroup_memory()
        return ResourceSample(
            rss_bytes=mem.rss,
            vms_bytes=mem.vms,
            python_current_bytes=current,
            python_peak_bytes=peak,
            cgroup_memory_current_bytes=cgroup_current,
            cgroup_memory_max_bytes=cgroup_max,
            work_dir_bytes=self._directory_size(self._work_root),
            temp_dir_bytes=self._directory_size(Path("/tmp")),
            filesystem_total_bytes=total,
            filesystem_used_bytes=used,
            filesystem_free_bytes=free,
        )

    def _directory_size(self, path: Path) -> int:
        now = time()
        cached = self._size_cache.get(path)
        if cached and now - cached[0] < self._size_cache_ttl_seconds:
            return cached[1]
        size = _directory_size(path)
        self._size_cache[path] = (now, size)
        return size


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            file_path = Path(root, name)
            try:
                total += file_path.stat().st_size
            except FileNotFoundError:
                continue
    return total


def _read_cgroup_memory() -> tuple[int | None, int | None]:
    current = _read_first_existing((
        Path("/sys/fs/cgroup/memory.current"),
        Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
    ))
    maximum = _read_first_existing((
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ))
    return current, maximum


def _read_first_existing(paths: tuple[Path, ...]) -> int | None:
    for path in paths:
        if path.exists():
            raw = path.read_text(encoding="utf-8").strip()
            if raw == "max":
                return None
            return int(raw)
    return None


def serialize_mapping(mapping: Mapping[str, object]) -> dict[str, object]:
    return {
        key: (_json_default(value) if isinstance(value, Path) else value)
        for key, value in mapping.items()
    }
