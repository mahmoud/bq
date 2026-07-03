"""Shared helpers for acceptance tests: polling, time-backdating, chaos."""

import time
import typing
from multiprocessing import Process

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine


def stop_process(proc: Process, timeout: float = 3.0) -> None:
    """Stop a worker child process, preserving its coverage data.

    SIGTERM first so coverage.py's ``sigterm`` handler can flush the child's
    data file; SIGKILL only as a last resort (loses that child's coverage).
    Use only in post-assertion teardown — crash tests that SIGKILL on purpose
    must keep calling ``proc.kill()`` directly.
    """
    proc.terminate()
    proc.join(timeout)
    if proc.is_alive():
        proc.kill()
        proc.join(timeout)


def wait_until(
    condition: typing.Callable[[], typing.Any],
    timeout: float = 10.0,
    interval: float = 0.05,
    message: str = "",
):
    """Poll `condition` until it returns truthy; raise TimeoutError otherwise."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    raise TimeoutError(message or f"condition not met within {timeout}s")


def wait_for_task_state(
    db_url: str,
    state,
    count: int,
    timeout: float = 30.0,
    table: str = "bq_tasks",
) -> int:
    """Poll until at least `count` tasks reach `state`; return the count seen."""
    state_value = getattr(state, "value", state)
    engine = create_engine(db_url)
    n = 0
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with engine.connect() as conn:
                n = conn.execute(
                    text(f"SELECT count(*) FROM {table} WHERE state = :s"),
                    {"s": state_value},
                ).scalar()
            if n >= count:
                return n
            time.sleep(0.3)
    finally:
        engine.dispose()
    raise TimeoutError(f"Only {n}/{count} tasks reached {state_value} within {timeout}s")


def backdate(
    engine_or_url,
    table: str,
    column: str,
    seconds: int,
    where: str,
    params: dict,
) -> int:
    """Shift `table.column` back `seconds` for rows matching `where`; return rowcount.

    Time-backdating (pg-boss technique) replaces waiting out timeouts:
    UPDATE {table} SET {column} = now() - make_interval(secs => :secs) WHERE {where}.
    """
    if isinstance(engine_or_url, Engine):
        engine, owns = engine_or_url, False
    else:
        engine, owns = create_engine(engine_or_url), True
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"UPDATE {table} SET {column} = "
                    f"now() - make_interval(secs => :secs) WHERE {where}"
                ),
                {"secs": seconds, **params},
            )
            return result.rowcount
    finally:
        if owns:
            engine.dispose()


def backdate_worker_heartbeat(db_url: str, worker_id, seconds: int = 300) -> None:
    """Make a worker look dead by pushing its last_heartbeat into the past."""
    backdate(
        db_url,
        "bq_workers",
        "last_heartbeat",
        seconds,
        "id = :worker_id",
        {"worker_id": str(worker_id)},
    )


def terminate_backends(db_url: str, query_like: str) -> list[int]:
    """Kill PG backends of the current database whose query matches `query_like`.

    Returns the list of terminated pids (may be empty).
    """
    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            rows = conn.execute(
                text(
                    "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity"
                    " WHERE datname = current_database()"
                    " AND pid != pg_backend_pid()"
                    " AND query LIKE :pat"
                ),
                {"pat": query_like},
            ).all()
            return [row[0] for row in rows]
    finally:
        engine.dispose()
