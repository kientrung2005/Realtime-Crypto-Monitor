from __future__ import annotations

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


class ClickHouseConnect:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._client: Client | None = None

    def connect(self) -> Client:
        if self._client is None:
            self._client = get_client(
                host=self._host,
                port=self._port,
                username=self._username,
                password=self._password,
                database=self._database,
            )
            self._client.ping()

        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> Client:
        return self.connect()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
