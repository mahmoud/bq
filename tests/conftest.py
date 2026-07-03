import os
import threading
import typing

import pytest
from pytest_factoryboy import register
from sqlalchemy import text
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine

from .factories import EventFactory
from .factories import TaskFactory
from .factories import WorkerFactory
from bq.db.base import Base
from bq.db.session import Session

register(TaskFactory)
register(WorkerFactory)
register(EventFactory)


@pytest.fixture
def db_url() -> str:
    return os.environ.get("TEST_DB_URL", "postgresql://bq:@localhost/bq_test")


@pytest.fixture
def engine(db_url: str) -> Engine:
    return create_engine(db_url)


@pytest.fixture
def db(engine: Engine) -> typing.Generator[Session, None, None]:
    Session.configure(bind=engine)
    # Drop first: leftover state from aborted runs or scripts/soak.py on the
    # shared test DB must never leak into a test.
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    try:
        yield Session
    finally:
        Session.remove()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def run_worker():
    """Factory: run app.process_tasks(channels) in a daemon thread; graceful teardown."""
    started = []

    def start(app, channels):
        t = threading.Thread(
            target=app.process_tasks,
            kwargs=dict(channels=channels),
            daemon=True,
        )
        t.start()
        started.append((app, t))
        return t

    yield start

    for app, t in started:
        app.request_shutdown()
        t.join(10)
        if t.is_alive():
            # Worker stuck — dispose engine to break any DB-level hangs,
            # then re-join briefly.
            if app._engine is not None:
                app._engine.dispose()
            t.join(5)
        else:
            # Clean case: dispose after join to release pooled connections.
            if app._engine is not None:
                app._engine.dispose()
        assert not t.is_alive(), "worker failed to shut down gracefully"


@pytest.fixture(autouse=True)
def _reset_gates():
    yield
    from .acceptance.fixtures.thread_processors import reset_gates

    reset_gates()


@pytest.fixture
def executions_table(engine: Engine) -> typing.Generator[Engine, None, None]:
    """Side table tallying every task execution (see record_execution).

    Dropped and recreated so leftover rows (e.g. from scripts/soak.py on the
    shared test DB) never leak into a test.
    """
    ddl = """
    CREATE TABLE test_executions (
        id BIGSERIAL PRIMARY KEY,
        task_id UUID NOT NULL,
        thread_name TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        finished_at TIMESTAMPTZ
    )
    """
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test_executions"))
        conn.execute(text(ddl))
    yield engine
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test_executions"))
