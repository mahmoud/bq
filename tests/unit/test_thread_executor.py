"""Unit tests for thread executor configuration and pool management."""
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.pool import SingletonThreadPool

import bq
from bq.config import Config


def test_default_pool_is_singleton(db_url: str):
    """Test that default configuration uses SingletonThreadPool (backward compatibility)."""
    app = bq.BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_WORKER_THREADS=1,  # Default value
        )
    )

    engine = app.engine
    assert isinstance(engine, Engine)
    assert isinstance(engine.pool, SingletonThreadPool)


def test_thread_pool_uses_queue_pool(db_url: str):
    """Test that thread pool executor configuration uses QueuePool."""
    app = bq.BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_WORKER_THREADS=4,
        )
    )

    engine = app.engine
    assert isinstance(engine, Engine)
    assert isinstance(engine.pool, QueuePool)


def test_queue_pool_size_configuration(db_url: str):
    """Test that pool size is configured based on MAX_WORKER_THREADS."""
    max_workers = 8
    app = bq.BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_WORKER_THREADS=max_workers,
        )
    )

    engine = app.engine
    pool = engine.pool
    assert isinstance(pool, QueuePool)

    # Pool size should be max_workers + 5 (for main thread and worker update thread)
    expected_pool_size = max_workers + 5
    assert pool.size() == expected_pool_size
    assert pool._max_overflow == 10


def test_zero_max_workers_uses_queue_pool(db_url: str):
    """Test that MAX_WORKER_THREADS=0 uses QueuePool with default sizing."""
    app = bq.BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_WORKER_THREADS=0,  # Use default (num_cpus * 5)
        )
    )

    engine = app.engine
    assert isinstance(engine.pool, QueuePool)

    # When MAX_WORKER_THREADS=0, pool size should be 10 + 5 = 15
    pool = engine.pool
    assert pool.size() == 15


@pytest.mark.parametrize(
    "max_workers,expected_pool_class,expected_pool_size",
    [
        (1, SingletonThreadPool, None),  # Sequential mode
        (2, QueuePool, 7),  # 2 + 5
        (4, QueuePool, 9),  # 4 + 5
        (8, QueuePool, 13),  # 8 + 5
        (16, QueuePool, 21),  # 16 + 5
        (0, QueuePool, 15),  # Default: 10 + 5
    ],
)
def test_pool_configuration_matrix(
    db_url: str, max_workers: int, expected_pool_class: type, expected_pool_size: int
):
    """Test various MAX_WORKER_THREADS configurations."""
    app = bq.BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_WORKER_THREADS=max_workers,
        )
    )

    engine = app.engine
    pool = engine.pool

    assert isinstance(pool, expected_pool_class)

    if expected_pool_size is not None:
        assert pool.size() == expected_pool_size


def test_config_max_worker_threads_default():
    """Test that MAX_WORKER_THREADS defaults to 1."""
    config = Config()
    assert config.MAX_WORKER_THREADS == 1


def test_config_max_worker_threads_from_env(monkeypatch):
    """Test that MAX_WORKER_THREADS can be set via environment variable."""
    monkeypatch.setenv("BQ_MAX_WORKER_THREADS", "8")
    config = Config()
    assert config.MAX_WORKER_THREADS == 8


def test_config_batch_size_with_threads():
    """Test that BATCH_SIZE can be configured independently of MAX_WORKER_THREADS."""
    config = Config(
        MAX_WORKER_THREADS=8,
        BATCH_SIZE=20,
    )
    assert config.MAX_WORKER_THREADS == 8
    assert config.BATCH_SIZE == 20
