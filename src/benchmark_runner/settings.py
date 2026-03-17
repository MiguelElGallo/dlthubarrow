from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Self
from urllib.parse import quote


def _split_csv(value: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
    if not value:
        return default
    parts = [part.strip() for part in value.split(",")]
    return tuple(part for part in parts if part)


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    role: str
    database: str

    def connect_kwargs(self, *, query_tag: str | None = None) -> dict[str, object]:
        kwargs: dict[str, object] = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "client_session_keep_alive": False,
            "application": "dlthubarrow",
        }
        if self.role:
            kwargs["role"] = self.role
        if query_tag:
            kwargs["session_parameters"] = {"QUERY_TAG": query_tag}
        return kwargs

    def to_connection_string(self) -> str:
        encoded_password = quote(self.password, safe="")
        query_items = [("warehouse", self.warehouse)]
        if self.role:
            query_items.append(("role", self.role))
        query = "&".join(f"{key}={quote(value, safe='')}" for key, value in query_items)
        return (
            f"snowflake://{quote(self.user, safe='')}:{encoded_password}"
            f"@{quote(self.account, safe='')}/{quote(self.database, safe='')}?{query}"
        )


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    run_api_key: str
    source: SnowflakeConfig
    destination: SnowflakeConfig
    source_database: str
    source_table: str
    source_chunk_rows: int
    datasets: tuple[str, ...]
    work_root: Path
    appinsights_connection_string: str | None

    @classmethod
    def from_env(cls) -> Self:
        work_root = Path(os.getenv("BENCHMARK_WORK_ROOT", "/tmp/dlthubarrow")).resolve()
        return cls(
            host=os.getenv("HOST", "0.0.0.0"),
            port=int(os.getenv("PORT", "8080")),
            run_api_key=os.environ["RUN_API_KEY"],
            source=SnowflakeConfig(
                account=os.environ["SOURCE_SNOWFLAKE_ACCOUNT"],
                user=os.environ["SOURCE_SNOWFLAKE_USER"],
                password=os.environ["SOURCE_SNOWFLAKE_PASSWORD"],
                warehouse=os.getenv("SOURCE_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                role=os.getenv("SOURCE_SNOWFLAKE_ROLE", ""),
                database=os.getenv("SOURCE_SNOWFLAKE_DATABASE", "SNOWFLAKE_SAMPLE_DATA"),
            ),
            destination=SnowflakeConfig(
                account=os.environ["DESTINATION_SNOWFLAKE_ACCOUNT"],
                user=os.environ["DESTINATION_SNOWFLAKE_USER"],
                password=os.environ["DESTINATION_SNOWFLAKE_PASSWORD"],
                warehouse=os.getenv("DESTINATION_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                role=os.getenv("DESTINATION_SNOWFLAKE_ROLE", ""),
                database=os.getenv("DESTINATION_SNOWFLAKE_DATABASE", "dummy"),
            ),
            source_database=os.getenv("SOURCE_SNOWFLAKE_DATABASE", "SNOWFLAKE_SAMPLE_DATA"),
            source_table=os.getenv("BENCHMARK_SOURCE_TABLE", "LINEITEM"),
            source_chunk_rows=int(os.getenv("BENCHMARK_SOURCE_CHUNK_ROWS", "50000")),
            datasets=_split_csv(
                os.getenv("BENCHMARK_DATASETS"),
                ("TPCH_SF1", "TPCH_SF10", "TPCH_SF100", "TPCH_SF1000"),
            ),
            work_root=work_root,
            appinsights_connection_string=os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING"),
        )
