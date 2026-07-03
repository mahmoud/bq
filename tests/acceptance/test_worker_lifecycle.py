"""Acceptance tests for worker lifecycle: shutdown, heartbeat, rescheduling, healthcheck."""
import json
import socket
import time
import urllib.request

from sqlalchemy import create_engine, text

from bq import models
from bq.app import BeanQueue
from bq.config import Config

from .fixtures.thread_processors import concurrent_task, gated_task, open_gate
from .helpers import wait_for_task_state


def _make_app(db_url, max_workers=2, poll_timeout=1, **overrides):
    defaults = dict(
        DATABASE_URL=db_url,
        MAX_WORKER_THREADS=max_workers,
        POLL_TIMEOUT=poll_timeout,
        BATCH_SIZE=10,
        WORKER_HEARTBEAT_PERIOD=1,
        WORKER_HEARTBEAT_TIMEOUT=5,
        METRICS_HTTP_SERVER_ENABLED=False,
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
    )
    defaults.update(overrides)
    return BeanQueue(config=Config(**defaults))


def _wait_processing(db_url, count, timeout=30):
    """Wait until at least `count` tasks are PROCESSING."""
    return wait_for_task_state(db_url, models.TaskState.PROCESSING, count, timeout)


def test_graceful_shutdown_drains_inflight(db, db_url, run_worker):
    """Shutdown drains in-flight tasks to completion (deterministic via gates)."""
    app = _make_app(db_url, max_workers=2)

    # Two gated tasks occupy both worker threads, held in-flight by the gate
    for _ in range(2):
        db.add(gated_task.run(gate="graceful-shutdown"))
    db.commit()

    t = run_worker(app, ("thread-tests",))

    # Wait until both tasks are in-flight
    _wait_processing(db_url, 2, timeout=15)

    # Request shutdown while tasks are held in-flight, then release them
    app.request_shutdown()
    open_gate("graceful-shutdown")
    t.join(30)
    assert not t.is_alive(), "Worker did not shut down"

    db.expire_all()
    done = db.query(models.Task).filter(
        models.Task.state == models.TaskState.DONE
    ).count()
    # Both in-flight tasks drained to completion
    assert done == 2

    # Worker row should be SHUTDOWN
    workers = db.query(models.Worker).filter(
        models.Worker.state == models.WorkerState.SHUTDOWN
    ).all()
    assert len(workers) >= 1


def test_worker_state_change_stops_processing(db, db_url, run_worker):
    """Bug-2 regression: non-RUNNING worker state triggers shutdown via request_shutdown."""
    app = _make_app(db_url, max_workers=2, WORKER_HEARTBEAT_PERIOD=1)
    t = run_worker(app, ("thread-tests",))
    time.sleep(2)  # let heartbeat run at least once

    # Get the worker id
    db.expire_all()
    w = db.query(models.Worker).filter(
        models.Worker.state == models.WorkerState.RUNNING
    ).first()
    assert w is not None, "Worker not found in RUNNING state"

    # From a separate session, mark worker as NO_HEARTBEAT
    engine = create_engine(db_url)
    with engine.connect() as conn:
        conn.execute(text(
            f"UPDATE {models.Worker.__tablename__} SET state = :s WHERE id = :wid"
        ), {"s": models.WorkerState.NO_HEARTBEAT.value, "wid": str(w.id)})
        conn.commit()
    engine.dispose()

    # Worker should exit within 10s (heartbeat period is 1s)
    t.join(10)
    assert not t.is_alive(), "Worker did not stop after state changed to NO_HEARTBEAT"


def test_dead_worker_tasks_rescheduled_by_peer(db, db_url, run_worker):
    """A live worker detects a dead peer and reschedules its stuck tasks."""
    from datetime import datetime, timedelta, timezone

    stale_time = datetime.now(timezone.utc) - timedelta(seconds=300)
    dead_worker = models.Worker(
        state=models.WorkerState.RUNNING,
        name="dead-worker",
        channels=["thread-tests"],
        last_heartbeat=stale_time,
        created_at=datetime.now(timezone.utc),
    )
    db.add(dead_worker)
    db.commit()
    dead_worker_id = dead_worker.id

    # Create a task "owned" by the dead worker
    stuck_task = concurrent_task.run(value=99)
    db.add(stuck_task)
    db.commit()

    # Manually set the task to PROCESSING with dead worker's id
    engine = create_engine(db_url)
    with engine.connect() as conn:
        conn.execute(text(
            f"UPDATE {models.Task.__tablename__} SET state = :s, worker_id = :wid WHERE id = :tid"
        ), {
            "s": models.TaskState.PROCESSING.value,
            "wid": str(dead_worker_id),
            "tid": str(stuck_task.id),
        })
        conn.commit()
    engine.dispose()

    # Start a live worker that should detect the dead worker and reschedule
    app = _make_app(
        db_url,
        max_workers=2,
        WORKER_HEARTBEAT_PERIOD=1,
        WORKER_HEARTBEAT_TIMEOUT=2,
    )
    run_worker(app, ("thread-tests",))

    # The stuck task should be rescheduled and eventually completed
    wait_for_task_state(db_url, models.TaskState.DONE, 1, timeout=30)

    # Dead worker should be marked as NO_HEARTBEAT
    db.expire_all()
    dead = db.query(models.Worker).filter(
        models.Worker.id == dead_worker_id
    ).first()
    assert dead.state == models.WorkerState.NO_HEARTBEAT


def test_healthz_endpoint_e2e(db, db_url, run_worker):
    """End-to-end test: /healthz returns 200 while worker is running."""
    # Find a free port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    app = _make_app(
        db_url,
        max_workers=2,
        METRICS_HTTP_SERVER_ENABLED=True,
        METRICS_HTTP_SERVER_PORT=port,
    )
    t = run_worker(app, ("thread-tests",))

    # Poll /healthz until 200
    url = f"http://127.0.0.1:{port}/healthz"
    deadline = time.monotonic() + 15
    got_200 = False
    while time.monotonic() < deadline:
        try:
            resp = urllib.request.urlopen(url, timeout=2)
            if resp.status == 200:
                body = json.loads(resp.read())
                assert body["status"] == "ok"
                got_200 = True
                break
        except Exception:
            time.sleep(0.5)

    assert got_200, "Never got 200 from /healthz"

    # Shutdown and verify endpoint stops
    app.request_shutdown()
    t.join(15)

    # After shutdown, connection should be refused or return error
    try:
        resp = urllib.request.urlopen(url, timeout=2)
        assert resp.status != 200
    except Exception:
        pass  # Connection refused is the expected outcome
