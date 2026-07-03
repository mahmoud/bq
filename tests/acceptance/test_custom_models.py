"""Acceptance tests for custom model config and inline registry processing (tape parity)."""
import time
import threading

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from bq import models
from bq.app import BeanQueue
from bq.config import Config
from bq.db.base import Base
from bq.processors.registry import collect

from .fixtures.custom_models import custom_processor
from .helpers import wait_for_task_state


def _make_custom_app(db_url, max_workers=3):
    """Create a BeanQueue with dotted-path model config (tape pattern)."""
    config = Config(
        DATABASE_URL=db_url,
        MAX_WORKER_THREADS=max_workers,
        POLL_TIMEOUT=1,
        BATCH_SIZE=5,
        WORKER_HEARTBEAT_PERIOD=1,
        WORKER_HEARTBEAT_TIMEOUT=5,
        METRICS_HTTP_SERVER_ENABLED=False,
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.custom_models"],
        TASK_MODEL="bq.Task",
        WORKER_MODEL="bq.Worker",
        EVENT_MODEL="bq.Event",
    )
    return BeanQueue(config=config)


def test_custom_models_threaded_e2e(db, db_url):
    """Model config via dotted paths works correctly under threading with NOTIFY."""
    app = _make_custom_app(db_url, max_workers=3)

    t = threading.Thread(
        target=app.process_tasks,
        kwargs=dict(channels=("custom",)),
        daemon=True,
    )
    t.start()
    time.sleep(1)

    # Create tasks using the processor helper
    task_count = 10
    for i in range(task_count):
        task = custom_processor.run(value=i)
        db.add(task)
    db.commit()

    # Wait for all tasks to complete
    wait_for_task_state(db_url, models.TaskState.DONE, task_count, timeout=30)

    # Verify all tasks are DONE and events created
    db.expire_all()
    done_tasks = db.query(models.Task).filter(
        models.Task.state == models.TaskState.DONE
    ).all()
    assert len(done_tasks) == task_count

    events = db.query(models.Event).filter(
        models.Event.type == models.EventType.COMPLETE
    ).all()
    assert len(events) == task_count

    # Explicit shutdown before db fixture teardown
    app.request_shutdown()
    t.join(10)
    if app._engine is not None:
        app._engine.dispose()


def test_inline_registry_process(db, db_url):
    """Downstream harness parity: registry.process() works inline without a worker."""
    import importlib

    pkgs = [importlib.import_module("tests.acceptance.fixtures.custom_models")]
    registry = collect(pkgs)

    # Create a task inline
    task = custom_processor.run(value=5)
    db.add(task)
    db.commit()

    # Process inline via registry
    task_in_session = db.query(models.Task).filter(
        models.Task.id == task.id
    ).one()
    registry.process(task_in_session, event_cls=models.Event)
    db.commit()

    # Verify task is DONE
    db.expire_all()
    processed = db.query(models.Task).filter(
        models.Task.id == task.id
    ).one()
    assert processed.state == models.TaskState.DONE

    # Verify COMPLETE event was created
    events = db.query(models.Event).filter(
        models.Event.task_id == task.id
    ).all()
    assert len(events) >= 1
    assert any(e.type == models.EventType.COMPLETE for e in events)
