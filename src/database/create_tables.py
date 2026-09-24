from __future__ import annotations

import logging
import re
from pathlib import Path

from configs.settings import settings
from src.database.clickhouse_connect import ClickHouseConnect


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"
CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([^\s(]+)",
    re.IGNORECASE,
)


def _load_sql_statements(schema_path: Path) -> tuple[str, ...]:
    if not schema_path.is_file():
        raise FileNotFoundError(f"SQL schema file not found: {schema_path}")

    sql_text = schema_path.read_text(encoding="utf-8")
    sql_without_comments = "\n".join(
        line
        for line in sql_text.splitlines()
        if not line.lstrip().startswith("--")
    )
    statements = tuple(
        statement.strip()
        for statement in sql_without_comments.split(";")
        if statement.strip()
    )

    if not statements:
        raise ValueError(f"SQL schema file is empty: {schema_path}")

    return statements


def create_tables(schema_path: Path = SCHEMA_PATH) -> None:
    statements = _load_sql_statements(schema_path)

    logger.info(
        "Connecting to ClickHouse at %s:%d/%s",
        settings.CLICKHOUSE_HOST,
        settings.CLICKHOUSE_HTTP_PORT,
        settings.CLICKHOUSE_DATABASE,
    )

    with ClickHouseConnect(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_HTTP_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DATABASE,
    ) as client:
        for statement in statements:
            client.command(statement)
            match = CREATE_TABLE_PATTERN.search(statement)
            table_name = match.group(1) if match else "unknown"
            logger.info("Created or verified ClickHouse table: %s", table_name)

    logger.info("ClickHouse schema initialized successfully.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    create_tables()


if __name__ == "__main__":
    main()
