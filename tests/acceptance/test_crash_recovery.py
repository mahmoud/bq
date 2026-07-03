"""Crash-recovery and connection-chaos tests.

These codify bq's delivery contract: at-least-once execution with
heartbeat-based dead-worker rescue, and LISTEN-connection resilience.
Real faults are injected: SIGKILL on subprocess workers (coverage data of
the killed child is lost by design) and pg_terminate_backend on the
dedicated LISTEN connection.

test_pg_restart_recovery is marked ``chaos`` (excluded by default via
addopts) and restarts the compose PG container; it auto-skips when no
docker CLI/compose service is reachable (e.g. inside the test container).
Run it from the host:

    uv run pytest -m chaos tests/acceptance/test_crash_recovery.py
"""
import json
import pathlib
import shutil
import subprocess
from multiprocessing import Process

import pytest
from sqlalchemy import text

from bq import models
from bq.app import BeanQueue
from bq.config import Config

from .fixtures.thread_processors import app
from .fixtures.thread_processors import concurrent_task
from .fixtures.thread_processors import gated_task
from .fixtures.thread_processors import open_gate
from .fixtures.thread_processors import record_execution
from .helpers import backdate_worker_heartbeat
from .helpers import terminate_backends
from .helpers import wait_for_task_state
from .helpers import wait_until


def run_crash_worker(db_url: str):
    """Subprocess worker target (module-level: works under fork and spawn)."""
    app.config = Config(
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
        DATABASE_URL=db_url,
        MAX_WORKER_THREADS=2,
        BATCH_SIZE=2,
        POLL_TIMEOUT=1,
        WORKER_HEARTBEAT_PERIOD=1,
        WORKER_HEARTBEAT_TIMEOUT=5,
        METRICS_HTTP_SERVER_ENABLED=False,
    )
    app.process_tasks(channels=("thread-tests",))


def _make_app(db_url, **overrides):
    defaults = dict(
        DATABASE_URL=db_url,
        MAX_WORKER_THREADS=2,
        POLL_TIMEOUT=1,
        BATCH_SIZE=2,
        WORKER_HEARTBEAT_PERIOD=1,
        WORKER_HEARTBEAT_TIMEOUT=100,
        METRICS_HTTP_SERVER_ENABLED=False,
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
    )
    defaults.update(overrides)
    return BeanQueue(config=Config(**defaults))


def _notify(engine, channel: str = "thread-tests"):
    with engine.connect() as conn:
        conn.execute(text(f'NOTIFY "{channel}"'))
        conn.commit()


def test_sigkill_worker_task_rescheduled_and_reexecuted(
    db, db_url, run_worker, executions_table
):
    """At-least-once codified: SIGKILL mid-task -> peer rescues -> re-execution.

    The first execution sleeps 60s so the kill lands mid-flight; after the
    kill the task row's kwargs are rewritten to sleep_time=0 (raw SQL on the
    mutable JSONB column) so the re-execution finishes instantly instead of
    stalling the rescuer.
    """
    engine = executions_table
    proc = Process(target=run_crash_worker, args=(db_url,))
    proc.start()

    task = record_execution.run(value=1, sleep_time=60)
    db.add(task)
    db.commit()
    task_id = task.id

    # Wait until the execution has visibly started (autocommit marker row)
    wait_until(
        lambda: engine.connect()
        .execute(
            text("SELECT count(*) FROM test_executions WHERE task_id = :tid"),
            {"tid": str(task_id)},
        )
        .scalar()
        >= 1,
        timeout=30,
        message="first execution never started",
    )

    db.expire_all()
    dead_worker_id = db.get(models.Task, task_id).worker_id
    assert dead_worker_id is not None

    proc.kill()  # SIGKILL is the behavior under test; child coverage lost by design
    proc.join(5)
    assert not proc.is_alive()

    # pg-boss technique: backdate the heartbeat instead of waiting out the timeout
    backdate_worker_heartbeat(db_url, dead_worker_id, seconds=300)
    # Make the re-execution instant
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE bq_tasks SET kwargs = :kwargs WHERE id = :tid"),
            {"kwargs": json.dumps({"value": 1, "sleep_time": 0}), "tid": str(task_id)},
        )

    # Rescuer detects the dead peer, reschedules, and re-executes
    rescuer = _make_app(
        db_url, WORKER_HEARTBEAT_TIMEOUT=2, WORKER_HEARTBEAT_PERIOD=1
    )
    run_worker(rescuer, ("thread-tests",))

    def task_done():
        with engine.connect() as conn:
            state = conn.execute(
                text("SELECT state FROM bq_tasks WHERE id = :tid"),
                {"tid": str(task_id)},
            ).scalar()
        return state == "DONE"

    wait_until(task_done, timeout=30, message="task never re-executed to DONE")

    with engine.connect() as conn:
        executions = conn.execute(
            text(
                "SELECT finished_at, thread_name FROM test_executions"
                " WHERE task_id = :tid ORDER BY id"
            ),
            {"tid": str(task_id)},
        ).all()
        # Forensics on failure: who did what, in event order
        events = conn.execute(
            text(
                "SELECT type, created_at, error_message FROM bq_events"
                " WHERE task_id = :tid ORDER BY created_at"
            ),
            {"tid": str(task_id)},
        ).all()
        workers = conn.execute(
            text("SELECT id, state, last_heartbeat FROM bq_workers")
        ).all()
    forensics = f"executions={executions} events={events} workers={workers}"
    assert len(executions) == 2, f"expected 2 executions; {forensics}"
    assert executions[0][0] is None, "killed execution must stay unfinished"
    assert executions[1][0] is not None, "re-execution must have finished"

    db.expire_all()
    assert db.get(models.Worker, dead_worker_id).state == models.WorkerState.NO_HEARTBEAT


def test_listen_connection_killed_reconnects_when_idle(db, db_url, engine, run_worker):
    """Regression: the dedicated LISTEN connection reconnects after backend death.

    POLL_TIMEOUT=60 so a pass cannot be periodic polling in disguise: the
    task only completes quickly if NOTIFY arrives on the *new* connection.
    """
    worker_app = _make_app(db_url, POLL_TIMEOUT=60)
    run_worker(worker_app, ("thread-tests",))

    def listener_pids():
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT pid FROM pg_stat_activity"
                    " WHERE datname = current_database()"
                    " AND query LIKE 'LISTEN %'"
                )
            ).all()
        return {row[0] for row in rows}

    wait_until(listener_pids, timeout=15, message="LISTEN backend never appeared")

    killed = terminate_backends(db_url, "LISTEN %")
    assert len(killed) >= 1

    # Reconnect proof: a fresh LISTEN backend with a new pid appears
    wait_until(
        lambda: listener_pids() - set(killed),
        timeout=15,
        message="no fresh LISTEN backend after kill",
    )

    task = concurrent_task.run(value=7)
    db.add(task)
    db.commit()
    _notify(engine)

    wait_for_task_state(db_url, models.TaskState.DONE, 1, timeout=10)


def test_listen_connection_killed_while_busy(db, db_url, engine, run_worker):
    """Busy-path variant: LISTEN backend dies while both threads hold tasks.

    _drain_notifications swallows errors without reconnecting; recovery
    happens at the next idle poll. With POLL_TIMEOUT=60, completing well
    under that proves the reconnect (not the poll fallback) delivered.
    """
    worker_app = _make_app(db_url, POLL_TIMEOUT=60)
    run_worker(worker_app, ("thread-tests",))

    for _ in range(2):
        db.add(gated_task.run(gate="listen-busy"))
    db.commit()
    _notify(engine)
    wait_for_task_state(db_url, models.TaskState.PROCESSING, 2, timeout=15)

    killed = terminate_backends(db_url, "LISTEN %")
    assert len(killed) >= 1

    open_gate("listen-busy")
    wait_for_task_state(db_url, models.TaskState.DONE, 2, timeout=15)

    # New task after the drain: must complete fast via the reconnected path
    task = concurrent_task.run(value=13)
    db.add(task)
    db.commit()

    wait_for_task_state(db_url, models.TaskState.DONE, 3, timeout=15)


def test_heartbeat_connection_killed_worker_survives(db, db_url, engine, run_worker):
    """Heartbeat session death is tolerated: the pool reconnects, beats resume."""
    worker_app = _make_app(db_url, WORKER_HEARTBEAT_PERIOD=1)
    run_worker(worker_app, ("thread-tests",))

    def worker_row():
        with engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT id, last_heartbeat FROM bq_workers"
                    " WHERE state = 'RUNNING' ORDER BY created_at DESC LIMIT 1"
                )
            ).one_or_none()

    wait_until(worker_row, timeout=15, message="worker row never appeared")

    def heartbeat_pids():
        """The heartbeat session parks 'idle in transaction' after the
        dead-worker sweep; its last statement mentions bq_workers."""
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT pid FROM pg_stat_activity"
                    " WHERE datname = current_database()"
                    " AND pid != pg_backend_pid()"
                    " AND state IN ('idle', 'idle in transaction')"
                    " AND query LIKE '%bq_workers%'"
                    " AND query NOT LIKE '%pg_stat_activity%'"
                )
            ).all()
        return {row[0] for row in rows}

    pids = wait_until(
        heartbeat_pids, timeout=5, message="heartbeat backend not identifiable"
    )

    before = worker_row()
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        for pid in pids:
            conn.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})

    # Beats resume on a fresh pooled connection (consecutive_failures < 3)
    wait_until(
        lambda: worker_row()[1] > before[1],
        timeout=10,
        message="heartbeat did not advance after backend kill",
    )

    task = concurrent_task.run(value=21)
    db.add(task)
    db.commit()
    _notify(engine)
    wait_for_task_state(db_url, models.TaskState.DONE, 1, timeout=15)

    db.expire_all()
    assert db.get(models.Worker, before[0]).state == models.WorkerState.RUNNING


@pytest.mark.chaos
@pytest.mark.xfail(
    reason=(
        "PG-restart recovery gap: the dispatch path in _process_tasks_threaded"
        " has no connection retry — the worker loop dies on the first dispatch"
        " against a dead pooled connection after a server restart"
    ),
    strict=False,
)
def test_pg_restart_recovery(db, db_url, engine, run_worker):
    """Full PG restart: worker survives, reconnects LISTEN, resumes work.

    Currently an executable bug report (xfail): recovery requires
    retry/backoff around the dispatch loop, which is a product change out
    of scope for the test-hardening work.

    Host-run only — the test container has no docker CLI/socket, so this
    skips there. Run: uv run pytest -m chaos tests/acceptance/test_crash_recovery.py
    """
    repo_root = pathlib.Path(__file__).parents[2]
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not available")
    probe = subprocess.run(
        ["docker", "compose", "ps", "psql"],
        cwd=repo_root,
        capture_output=True,
    )
    if probe.returncode != 0:
        pytest.skip("compose service psql not reachable")

    worker_app = _make_app(
        db_url, POLL_TIMEOUT=5, WORKER_HEARTBEAT_PERIOD=2, WORKER_HEARTBEAT_TIMEOUT=100
    )
    thread = run_worker(worker_app, ("thread-tests",))

    # Pre-restart sanity: the worker processes a task
    db.add(concurrent_task.run(value=1))
    db.commit()
    wait_for_task_state(db_url, models.TaskState.DONE, 1, timeout=15)

    subprocess.run(
        ["docker", "compose", "restart", "psql"],
        cwd=repo_root,
        check=True,
        capture_output=True,
    )

    # Clear the test's own stale pool, then wait for PG to accept connections
    engine.dispose()

    def pg_ready():
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    wait_until(pg_ready, timeout=60, interval=1, message="PostgreSQL never came back")

    db.rollback()
    db.add(concurrent_task.run(value=2))
    db.commit()
    _notify(engine)

    wait_for_task_state(db_url, models.TaskState.DONE, 2, timeout=60)
    assert thread.is_alive(), "worker died during PG restart"
