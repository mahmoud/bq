import os
import threading
import typing

import pytest
from pytest_factoryboy import register
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
