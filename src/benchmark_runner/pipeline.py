from __future__ import annotations

import json
import re
import shutil
import threading
import time
import uuid
from collections.abc import Generator
from dataclasses import asdict, dataclass
from typing import Any

import dlt
import pyarrow as pa
import snowflake.connector
from dlt.destinations import snowflake as snowflake_destination
from snowflake.connector import DictCursor

from .settings import Settings
from .telemetry import ResourceSampler, emit_event

IDENTIFIER_RE = re.compile(r"[^A-Za-z0-9_]+")


def _safe_identifier(value: str) -> str:
    normalized = IDENTIFIER_RE.sub("_", value)
    return normalized.strip("_") or "default"


@dataclass
class StageResult:
    dataset: str
    source_row_count: int | None
    source_bytes: int | None
    extracted_batches: int
    extracted_rows: int
    loaded_rows: int | None
    duration_seconds: float
    status: str
    error: str | None


@dataclass
class RunState:
    run_id: str
    status: str
    started_at: float
    finished_at: float | None
    requested_by: str
    stages: list[StageResult]
    error: str | None


class BenchmarkService:
    def __init__(self, settings: Settings, logger: Any) -> None:
        self._settings = settings
        self._logger = logger
        self._sampler = ResourceSampler(settings.work_root)
        self._lock = threading.Lock()
        self._state = RunState(
            run_id="",
            status="idle",
            started_at=0.0,
            finished_at=None,
            requested_by="",
            stages=[],
            error=None,
        )
        self._state_path = self._settings.work_root / "latest-run.json"
        self._settings.work_root.mkdir(parents=True, exist_ok=True)

    def current_state(self) -> RunState:
        with self._lock:
            return RunState(
                run_id=self._state.run_id,
                status=self._state.status,
                started_at=self._state.started_at,
                finished_at=self._state.finished_at,
                requested_by=self._state.requested_by,
                stages=list(self._state.stages),
                error=self._state.error,
            )

    def start_run(self, requested_by: str) -> str:
        with self._lock:
            if self._state.status == "running":
                raise RuntimeError("A benchmark run is already in progress.")
            run_id = uuid.uuid4().hex
            self._state = RunState(
                run_id=run_id,
                status="running",
                started_at=time.time(),
                finished_at=None,
                requested_by=requested_by,
                stages=[],
                error=None,
            )
            self._persist_state()
        emit_event(
            self._logger,
            "run_started",
            run_id=run_id,
            requested_by=requested_by,
            datasets=self._settings.datasets,
            resources=self._sampler.sample().asdict(),
        )
        return run_id

    def execute_run(self, run_id: str) -> None:
        try:
            for dataset in self._settings.datasets:
                stage_result = self._run_stage(run_id, dataset)
                with self._lock:
                    self._state.stages.append(stage_result)
                    if stage_result.status != "success":
                        self._state.status = "failed"
                        self._state.finished_at = time.time()
                        self._state.error = stage_result.error
                        self._persist_state()
                        return
                    self._persist_state()
            with self._lock:
                self._state.status = "completed"
                self._state.finished_at = time.time()
                self._persist_state()
            emit_event(
                self._logger,
                "run_completed",
                run_id=run_id,
                resources=self._sampler.sample().asdict(),
                stages=[asdict(stage) for stage in self.current_state().stages],
            )
        except Exception as exc:
            with self._lock:
                self._state.status = "failed"
                self._state.finished_at = time.time()
                self._state.error = str(exc)
                self._persist_state()
            emit_event(
                self._logger,
                "run_failed",
                run_id=run_id,
                error=str(exc),
                resources=self._sampler.sample().asdict(),
            )

    def _run_stage(self, run_id: str, dataset: str) -> StageResult:
        stage_started = time.perf_counter()
        source_row_count, source_bytes = self._get_source_metadata(dataset)
        pipeline_root = self._settings.work_root / run_id / dataset
        if pipeline_root.exists():
            shutil.rmtree(pipeline_root)
        pipeline_root.mkdir(parents=True, exist_ok=True)
        destination = snowflake_destination(
            credentials=self._settings.destination.to_connection_string(),
            naming_convention="sql_cs_v1",
            enable_dataset_name_normalization=False,
        )
        counters = {"batches": 0, "rows": 0}
        run_tag = f"dlthubarrow:{run_id}:{dataset}"

        @dlt.resource(name="LINEITEM", write_disposition="replace")
        def source_arrow() -> Generator[pa.Table, None, None]:
            page_size = self._settings.source_chunk_rows
            base_query = (
                f'SELECT * FROM "{self._settings.source_database}"'
                f'."{dataset}"."{self._settings.source_table}"'
            )
            page_query = (
                f"{base_query} "
                "WHERE (%(last_orderkey)s IS NULL) "
                "   OR (L_ORDERKEY > %(last_orderkey)s) "
                "   OR (L_ORDERKEY = %(last_orderkey)s AND L_LINENUMBER > %(last_linenumber)s) "
                "ORDER BY L_ORDERKEY, L_LINENUMBER "
                "LIMIT %(page_size)s"
            )
            last_orderkey: int | None = None
            last_linenumber: int | None = None
            with snowflake.connector.connect(
                **self._settings.source.connect_kwargs(query_tag=f"{run_tag}:extract")
            ) as connection:
                while True:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            page_query,
                            {
                                "last_orderkey": last_orderkey,
                                "last_linenumber": last_linenumber,
                                "page_size": page_size,
                            },
                        )
                        table = _to_arrow_table(cursor.fetch_arrow_all())
                    if table.num_rows == 0:
                        return
                    counters["batches"] += 1
                    counters["rows"] += table.num_rows
                    last_orderkey, last_linenumber = _extract_lineitem_position(table)
                    emit_event(
                        self._logger,
                        "extract_batch",
                        run_id=run_id,
                        dataset=dataset,
                        batch_index=counters["batches"],
                        batch_rows=table.num_rows,
                        batch_bytes=table.nbytes,
                        page_size=page_size,
                        last_orderkey=last_orderkey,
                        last_linenumber=last_linenumber,
                        resources=self._sampler.sample().asdict(),
                    )
                    yield table

        pipeline = dlt.pipeline(
            pipeline_name=f"lineitem_{_safe_identifier(dataset.lower())}_{run_id[:8]}",
            destination=destination,
            dataset_name=dataset,
            pipelines_dir=str(pipeline_root / "pipelines"),
        )

        emit_event(
            self._logger,
            "stage_started",
            run_id=run_id,
            dataset=dataset,
            source_row_count=source_row_count,
            source_bytes=source_bytes,
            resources=self._sampler.sample().asdict(),
        )

        try:
            load_info = pipeline.run(source_arrow(), loader_file_format="parquet")
            duration = time.perf_counter() - stage_started
            loaded_rows = _extract_loaded_rows(load_info, default_rows=counters["rows"])
            result = StageResult(
                dataset=dataset,
                source_row_count=source_row_count,
                source_bytes=source_bytes,
                extracted_batches=counters["batches"],
                extracted_rows=counters["rows"],
                loaded_rows=loaded_rows,
                duration_seconds=duration,
                status="success",
                error=None,
            )
            emit_event(
                self._logger,
                "stage_completed",
                run_id=run_id,
                dataset=dataset,
                duration_seconds=duration,
                extracted_batches=counters["batches"],
                extracted_rows=counters["rows"],
                loaded_rows=loaded_rows,
                rows_per_second=(counters["rows"] / duration) if duration else None,
                megabytes_per_second=((source_bytes / (1024 * 1024)) / duration)
                if source_bytes and duration
                else None,
                resources=self._sampler.sample().asdict(),
                load_info_summary=_summarize_load_info(load_info),
            )
            return result
        except Exception as exc:
            duration = time.perf_counter() - stage_started
            emit_event(
                self._logger,
                "stage_failed",
                run_id=run_id,
                dataset=dataset,
                duration_seconds=duration,
                extracted_batches=counters["batches"],
                extracted_rows=counters["rows"],
                error=str(exc),
                resources=self._sampler.sample().asdict(),
            )
            return StageResult(
                dataset=dataset,
                source_row_count=source_row_count,
                source_bytes=source_bytes,
                extracted_batches=counters["batches"],
                extracted_rows=counters["rows"],
                loaded_rows=None,
                duration_seconds=duration,
                status="failed",
                error=str(exc),
            )

    def _get_source_metadata(self, dataset: str) -> tuple[int | None, int | None]:
        query = """
        SELECT row_count, bytes
        FROM information_schema.tables
        WHERE table_catalog = %s
          AND table_schema = %s
          AND table_name = %s
        """
        with snowflake.connector.connect(
            **self._settings.source.connect_kwargs(query_tag=f"dlthubarrow:metadata:{dataset}")
        ) as connection:
            with connection.cursor(DictCursor) as cursor:
                cursor.execute(
                    query,
                    (
                        self._settings.source_database,
                        dataset,
                        self._settings.source_table,
                    ),
                )
                row = cursor.fetchone()
                if not row:
                    return None, None
                return row.get("ROW_COUNT"), row.get("BYTES")

    def _persist_state(self) -> None:
        payload = {
            "run_id": self._state.run_id,
            "status": self._state.status,
            "started_at": self._state.started_at,
            "finished_at": self._state.finished_at,
            "requested_by": self._state.requested_by,
            "stages": [asdict(stage) for stage in self._state.stages],
            "error": self._state.error,
        }
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _to_arrow_table(batch: Any) -> pa.Table:
    if batch is None:
        return pa.table({})
    if isinstance(batch, pa.Table):
        return batch
    if isinstance(batch, pa.RecordBatch):
        return pa.Table.from_batches([batch])
    if hasattr(batch, "to_arrow"):
        converted = batch.to_arrow()
        return _to_arrow_table(converted)
    raise TypeError(f"Unsupported Arrow batch type: {type(batch)!r}")


def _extract_lineitem_position(table: pa.Table) -> tuple[int, int]:
    if "L_ORDERKEY" not in table.column_names or "L_LINENUMBER" not in table.column_names:
        raise KeyError("Expected LINEITEM paging columns L_ORDERKEY and L_LINENUMBER.")
    row_index = table.num_rows - 1
    return (
        table.column("L_ORDERKEY")[row_index].as_py(),
        table.column("L_LINENUMBER")[row_index].as_py(),
    )


def _extract_loaded_rows(load_info: Any, *, default_rows: int) -> int:
    metrics = json.dumps(_summarize_load_info(load_info))
    match = re.search(r'"items_count"\s*:\s*(\d+)', metrics)
    if match:
        return int(match.group(1))
    return default_rows


def _summarize_load_info(load_info: Any) -> dict[str, Any]:
    summary = {
        "destination_name": getattr(load_info, "destination_name", None),
        "destination_type": getattr(load_info, "destination_type", None),
        "dataset_name": getattr(load_info, "dataset_name", None),
        "loads_ids": list(getattr(load_info, "loads_ids", []) or []),
        "has_failed_jobs": getattr(load_info, "has_failed_jobs", None),
        "first_run": getattr(load_info, "first_run", None),
        "started_at": _coerce_to_jsonable(getattr(load_info, "started_at", None)),
        "finished_at": _coerce_to_jsonable(getattr(load_info, "finished_at", None)),
    }
    metrics = getattr(load_info, "metrics", None)
    if metrics is not None:
        summary["metrics"] = _coerce_to_jsonable(metrics)
    return summary


def _coerce_to_jsonable(value: Any, *, _seen: set[int] | None = None, _depth: int = 0) -> Any:
    if _depth > 16:
        return f"<max_depth:{type(value).__name__}>"
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if _seen is None:
        _seen = set()
    object_id = id(value)
    if object_id in _seen:
        return f"<recursive_ref:{type(value).__name__}>"
    if isinstance(value, dict):
        _seen.add(object_id)
        return {
            str(key): _coerce_to_jsonable(item, _seen=_seen, _depth=_depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        _seen.add(object_id)
        return [_coerce_to_jsonable(item, _seen=_seen, _depth=_depth + 1) for item in value]
    if hasattr(value, "_asdict"):
        _seen.add(object_id)
        return _coerce_to_jsonable(value._asdict(), _seen=_seen, _depth=_depth + 1)
    if hasattr(value, "asdict"):
        _seen.add(object_id)
        try:
            return _coerce_to_jsonable(value.asdict(), _seen=_seen, _depth=_depth + 1)
        except RecursionError:
            return str(value)
    if hasattr(value, "__dict__"):
        _seen.add(object_id)
        return _coerce_to_jsonable(vars(value), _seen=_seen, _depth=_depth + 1)
    return str(value)
