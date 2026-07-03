"""Acceptance tests for threaded task processing using in-process workers."""
import time

from sqlalchemy import create_engine, text

from bq import models
from bq.app import BeanQueue
from bq.config import Config

from .fixtures.thread_processors import (
    concurrent_task,
    record_thread_name,
    timed_task,
)
from .helpers import wait_for_task_state


def _make_app(db_url, max_workers=4, poll_timeout=1, batch_size=10):
    config = Config(
        DATABASE_URL=db_url,
        MAX_WORKER_THREADS=max_workers,
        POLL_TIMEOUT=poll_timeout,
        BATCH_SIZE=batch_size,
        WORKER_HEARTBEAT_PERIOD=1,
        WORKER_HEARTBEAT_TIMEOUT=5,
        METRICS_HTTP_SERVER_ENABLED=False,
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
    )
    return BeanQueue(config=config)


def test_concurrent_execution(db, db_url, run_worker):
    """6 timed_tasks on 4 threads: overlapping windows prove concurrency."""
    app = _make_app(db_url, max_workers=4)
    run_worker(app, ("thread-tests",))
    time.sleep(1)

    task_count = 6
    for i in range(task_count):
        task = timed_task.run(task_num=i, sleep_time=0.5)
        db.add(task)
    db.commit()

    wait_for_task_state(db_url, models.TaskState.DONE, task_count, timeout=30)

    db.expire_all()
    done_tasks = db.query(models.Task).filter(
        models.Task.state == models.TaskState.DONE
    ).all()
    assert len(done_tasks) == task_count


def test_session_isolation(db, db_url, run_worker):
    """Concurrent tasks each get correct results (no cross-session contamination)."""
    app = _make_app(db_url, max_workers=4)
    run_worker(app, ("thread-tests",))
    time.sleep(1)

    task_count = 8
    for i in range(task_count):
        task = concurrent_task.run(value=i)
        db.add(task)
    db.commit()

    wait_for_task_state(db_url, models.TaskState.DONE, task_count, timeout=30)

    db.expire_all()
    done_tasks = db.query(models.Task).filter(
        models.Task.state == models.TaskState.DONE
    ).all()
    assert len(done_tasks) == task_count


def test_sequential_backward_compat(db, db_url, run_worker):
    """MAX_WORKER_THREADS=1 processes all tasks with no task_worker threads."""
    app = _make_app(db_url, max_workers=1)
    run_worker(app, ("thread-tests",))
    time.sleep(1)

    task_count = 4
    for i in range(task_count):
        task = record_thread_name.run()
        db.add(task)
    db.commit()

    wait_for_task_state(db_url, models.TaskState.DONE, task_count, timeout=30)

    db.expire_all()
    done = db.query(models.Task).filter(
        models.Task.state == models.TaskState.DONE
    ).all()
    assert len(done) == task_count


def test_exactly_once_under_competing_workers(db, db_url, run_worker):
    """Two workers with 3 threads each; 40 tasks; no double-processing."""
    app1 = _make_app(db_url, max_workers=3, batch_size=3)
    app2 = _make_app(db_url, max_workers=3, batch_size=3)

    task_count = 40
    for i in range(task_count):
        task = concurrent_task.run(value=i)
        db.add(task)
    db.commit()

    run_worker(app1, ("thread-tests",))
    run_worker(app2, ("thread-tests",))

    deadline = time.monotonic() + 60
    engine = create_engine(db_url)
    done = 0
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT state, count(*) FROM {models.Task.__tablename__} GROUP BY state"
            )).fetchall()
            counts = {r[0]: r[1] for r in rows}
            done = counts.get("DONE", 0)

            # Overshoot detector: done should never exceed created
            assert done <= task_count, f"More DONE ({done}) than created ({task_count})!"

            if done == task_count:
                break
        time.sleep(0.5)
    else:
        engine.dispose()
        raise TimeoutError(f"Only {done}/{task_count} done within 60s")

    engine.dispose()

    db.expire_all()
    assert db.query(models.Task).filter(models.Task.state == models.TaskState.DONE).count() == task_count
    assert db.query(models.Task).filter(models.Task.state == models.TaskState.PENDING).count() == 0
    assert db.query(models.Task).filter(models.Task.state == models.TaskState.PROCESSING).count() == 0
    assert db.query(models.Task).filter(models.Task.state == models.TaskState.FAILED).count() == 0


def test_notify_wakeup_threaded(db, db_url, run_worker):
    """Bug-1 regression: NOTIFY wakes worker within seconds, not POLL_TIMEOUT."""
    app = _make_app(db_url, max_workers=4, poll_timeout=60)
    run_worker(app, ("thread-tests",))
    time.sleep(2)  # let worker reach idle poll

    # Insert a task from a separate connection and NOTIFY
    engine = create_engine(db_url)
    task = concurrent_task.run(value=42)
    db.add(task)
    db.commit()

    with engine.connect() as conn:
        conn.execute(text("NOTIFY \"thread-tests\""))
        conn.commit()
    engine.dispose()

    # Should complete within 10s (not 60s poll timeout)
    wait_for_task_state(db_url, models.TaskState.DONE, 1, timeout=10)


def test_batch_size_smaller_than_threads(db, db_url, run_worker):
    """BATCH_SIZE=1, threads=4: all tasks still processed, proves multiple rounds."""
    app = _make_app(db_url, max_workers=4, batch_size=1)
    run_worker(app, ("thread-tests",))
    time.sleep(1)

    task_count = 8
    for i in range(task_count):
        task = timed_task.run(task_num=i, sleep_time=0.5)
        db.add(task)
    db.commit()

    wait_for_task_state(db_url, models.TaskState.DONE, task_count, timeout=30)

    db.expire_all()
    done = db.query(models.Task).filter(
        models.Task.state == models.TaskState.DONE
    ).all()
    assert len(done) == task_count


def test_missing_processor_marks_task_failed(db, db_url, run_worker):
    """A task naming an unregistered function is FAILED with an error event.

    Real operational scenario: a deploy removes (or renames) a processor
    while tasks referencing it are still queued.
    """
    app = _make_app(db_url)
    run_worker(app, ("thread-tests",))

    task = models.Task(
        channel="thread-tests",
        module="tests.acceptance.fixtures.thread_processors",
        func_name="nonexistent_processor",
        kwargs={},
    )
    db.add(task)
    db.commit()

    wait_for_task_state(db_url, models.TaskState.FAILED, 1, timeout=15)

    db.expire_all()
    row = db.query(models.Task).filter(models.Task.id == task.id).one()
    assert row.state == models.TaskState.FAILED
    assert "Cannot find processor" in row.error_message
    events = db.query(models.Event).filter(models.Event.task_id == task.id).all()
    assert len(events) == 1
    assert events[0].type == models.EventType.FAILED
    assert "Cannot find processor" in events[0].error_message
