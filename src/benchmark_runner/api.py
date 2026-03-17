from __future__ import annotations

import asyncio
from dataclasses import asdict

from fastapi import FastAPI, Header, HTTPException

from .pipeline import BenchmarkService
from .settings import Settings
from .telemetry import configure_logging

settings = Settings.from_env()
logger = configure_logging(settings.appinsights_connection_string)
service = BenchmarkService(settings, logger)
app = FastAPI(title="dlthubarrow benchmark runner", version="0.1.0")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/latest")
async def latest() -> dict[str, object]:
    return asdict(service.current_state())


@app.post("/run")
async def run_benchmark(x_run_key: str = Header(..., alias="X-Run-Key")) -> dict[str, str]:
    if x_run_key != settings.run_api_key:
        raise HTTPException(status_code=401, detail="Invalid run key.")
    try:
        run_id = service.start_run(requested_by="api")
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    asyncio.create_task(asyncio.to_thread(service.execute_run, run_id))
    return {"status": "accepted", "run_id": run_id}
