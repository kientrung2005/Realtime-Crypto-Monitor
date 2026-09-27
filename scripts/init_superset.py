import os
import subprocess
from importlib.metadata import PackageNotFoundError, version

import psycopg2


STATE_TABLE = "realtime_crypto_bootstrap_state"
STATE_KEY = "superset_init"


def _database_connection():
    return psycopg2.connect(
        host=os.environ["SUPERSET_METADATA_HOST"],
        port=int(os.environ.get("SUPERSET_METADATA_PORT", "5432")),
        dbname=os.environ["SUPERSET_METADATA_DATABASE"],
        user=os.environ["SUPERSET_METADATA_USER"],
        password=os.environ["SUPERSET_METADATA_PASSWORD"],
        connect_timeout=10,
    )


def _expected_state() -> str:
    try:
        superset_version = version("apache-superset")
    except PackageNotFoundError:
        superset_version = "unknown"

    revision = os.environ.get("SUPERSET_INIT_REVISION", "1")
    return f"{superset_version}:{revision}"


def _ensure_state_table(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
                state_key VARCHAR(100) PRIMARY KEY,
                state_value VARCHAR(100) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    connection.commit()


def _read_state(connection) -> str | None:
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT state_value FROM {STATE_TABLE} WHERE state_key = %s",
            (STATE_KEY,),
        )
        row = cursor.fetchone()
    return row[0] if row else None


def _admin_exists(connection) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('public.ab_user')")
        if cursor.fetchone()[0] is None:
            return False

        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM ab_user WHERE username = %s)",
            (os.environ["SUPERSET_ADMIN_USERNAME"],),
        )
        return bool(cursor.fetchone()[0])


def _write_state(connection, state_value: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {STATE_TABLE} (state_key, state_value)
            VALUES (%s, %s)
            ON CONFLICT (state_key) DO UPDATE
            SET state_value = EXCLUDED.state_value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (STATE_KEY, state_value),
        )
    connection.commit()


def _run(command: list[str]) -> None:
    print(f"Running: {' '.join(command[:3])}", flush=True)
    subprocess.run(command, check=True)


def main() -> None:
    expected_state = _expected_state()
    force_init = os.environ.get("SUPERSET_FORCE_INIT", "false").lower() == "true"

    with _database_connection() as connection:
        _ensure_state_table(connection)
        current_state = _read_state(connection)

        if current_state == expected_state and not force_init:
            print(
                f"Superset {expected_state} is already initialized; skipping bootstrap.",
                flush=True,
            )
            return

        # Add the marker to metadata databases initialized before this script
        # existed, avoiding one unnecessary full bootstrap after the upgrade.
        if current_state is None and _admin_exists(connection) and not force_init:
            _write_state(connection, expected_state)
            print(
                f"Existing Superset installation detected; recorded {expected_state}.",
                flush=True,
            )
            return

    _run(["superset", "db", "upgrade"])

    with _database_connection() as connection:
        if not _admin_exists(connection):
            _run(
                [
                    "superset",
                    "fab",
                    "create-admin",
                    "--username",
                    os.environ["SUPERSET_ADMIN_USERNAME"],
                    "--firstname",
                    os.environ["SUPERSET_ADMIN_FIRSTNAME"],
                    "--lastname",
                    os.environ["SUPERSET_ADMIN_LASTNAME"],
                    "--email",
                    os.environ["SUPERSET_ADMIN_EMAIL"],
                    "--password",
                    os.environ["SUPERSET_ADMIN_PASSWORD"],
                ]
            )

    _run(["superset", "init"])

    with _database_connection() as connection:
        _ensure_state_table(connection)
        _write_state(connection, expected_state)

    print(f"Superset bootstrap completed for {expected_state}.", flush=True)


if __name__ == "__main__":
    main()
