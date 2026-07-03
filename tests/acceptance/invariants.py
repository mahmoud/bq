"""Queue-level consistency invariants, checked via raw SQL sweeps.

Used by the torture test, crash-recovery tests, and scripts/soak.py after
the queue has quiesced (no live workers mutating state).
"""

import typing

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _rows(conn, sql: str, **params) -> list:
    return conn.execute(text(sql), params).all()


def assert_queue_consistent(
    engine: Engine, created_count: typing.Optional[int] = None
) -> None:
    """Assert queue-wide invariants; AssertionError carries the offending rows.

    1. No orphaned claims: every PROCESSING task belongs to a RUNNING worker.
    2. Conservation: no lost/phantom task rows (when `created_count` given).
    3. Execution mutual exclusion: two executions of the same task never
       overlap in time — even when re-execution (at-least-once) is legal.
    4. Completion tally: every DONE task that ran record_execution has at
       least one finished execution.

    Checks 3-4 need the test_executions tally table (executions_table
    fixture or scripts/soak.py) and are skipped when it does not exist.
    """
    with engine.connect() as conn:
        orphans = _rows(
            conn,
            """
            SELECT t.id, t.state, t.worker_id, w.state AS worker_state
            FROM bq_tasks t LEFT JOIN bq_workers w ON t.worker_id = w.id
            WHERE t.state = 'PROCESSING' AND (w.id IS NULL OR w.state != 'RUNNING')
            """,
        )
        assert not orphans, f"Orphaned PROCESSING claims: {orphans}"

        if created_count is not None:
            total = conn.execute(text("SELECT count(*) FROM bq_tasks")).scalar()
            assert total == created_count, (
                f"Task conservation violated: {total} rows, expected {created_count}"
            )

        has_tally = (
            conn.execute(text("SELECT to_regclass('test_executions')")).scalar()
            is not None
        )
        if not has_tally:
            return

        overlaps = _rows(
            conn,
            """
            SELECT e1.task_id,
                   e1.id AS exec_a, e1.started_at AS a_start, e1.finished_at AS a_end,
                   e2.id AS exec_b, e2.started_at AS b_start, e2.finished_at AS b_end
            FROM test_executions e1
            JOIN test_executions e2 ON e1.task_id = e2.task_id AND e1.id < e2.id
              AND e1.started_at < COALESCE(e2.finished_at, clock_timestamp())
              AND e2.started_at < COALESCE(e1.finished_at, clock_timestamp())
            """,
        )
        assert not overlaps, f"Overlapping executions of the same task: {overlaps}"

        unfinished = _rows(
            conn,
            """
            SELECT t.id FROM bq_tasks t
            WHERE t.state = 'DONE' AND t.func_name = 'record_execution'
              AND NOT EXISTS (
                SELECT 1 FROM test_executions e
                WHERE e.task_id = t.id AND e.finished_at IS NOT NULL)
            """,
        )
        assert not unfinished, (
            f"DONE record_execution tasks without a finished execution: {unfinished}"
        )
