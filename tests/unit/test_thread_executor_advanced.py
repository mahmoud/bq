"""Advanced unit tests for thread executor functionality."""
import pytest
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.pool import SingletonThreadPool

import bq
from bq.config import Config


def test_engine_recreation_with_different_config(db_url: str):
    """Test that engine is recreated when config changes."""
    # First config: sequential mode
    config1 = Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=1)
    app1 = bq.BeanQueue(config=config1)
    engine1 = app1.engine
    assert isinstance(engine1.pool, SingletonThreadPool)

    # Second config: threaded mode
    config2 = Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=4)
    app2 = bq.BeanQueue(config=config2)
    engine2 = app2.engine
    assert isinstance(engine2.pool, QueuePool)

    # Engines should be different
    assert engine1 is not engine2


def test_batch_size_independent_of_threads(db_url: str):
    """Test that BATCH_SIZE can be configured independently."""
    config = Config(
        DATABASE_URL=db_url,
        MAX_WORKER_THREADS=4,
        BATCH_SIZE=20,
    )
    app = bq.BeanQueue(config=config)

    assert app.config.MAX_WORKER_THREADS == 4
    assert app.config.BATCH_SIZE == 20


def test_zero_max_workers_pool_size_calculation(db_url: str):
    """Test pool size when MAX_WORKER_THREADS=0 (auto)."""
    config = Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=0)
    app = bq.BeanQueue(config=config)

    # Should use QueuePool
    assert isinstance(app.engine.pool, QueuePool)

    # Pool size should be 15 (10 default workers + 5 extra)
    assert app.engine.pool.size() == 15


def test_large_thread_count_pool_size(db_url: str):
    """Test pool size with large thread count."""
    config = Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=32)
    app = bq.BeanQueue(config=config)

    # Pool size should be 32 + 5 = 37
    assert app.engine.pool.size() == 37
    assert app.engine.pool._max_overflow == 10


def test_session_factory_uses_engine_pool(db_url: str):
    """Test that session factory uses the configured engine pool."""
    config = Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=8)
    app = bq.BeanQueue(config=config)

    # Create multiple sessions
    session1 = app.make_session()
    session2 = app.make_session()
    session3 = app.make_session()

    # All should use the same engine
    assert session1.bind is app.engine
    assert session2.bind is app.engine
    assert session3.bind is app.engine

    # Clean up
    session1.close()
    session2.close()
    session3.close()


def test_config_validation_max_worker_threads():
    """Test that MAX_WORKER_THREADS accepts valid values."""
    # Valid values
    assert Config(MAX_WORKER_THREADS=0).MAX_WORKER_THREADS == 0
    assert Config(MAX_WORKER_THREADS=1).MAX_WORKER_THREADS == 1
    assert Config(MAX_WORKER_THREADS=100).MAX_WORKER_THREADS == 100


def test_thread_pool_mode_detection(db_url: str):
    """Test detection of thread pool vs sequential mode."""
    # Sequential mode
    app_seq = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=1))
    assert isinstance(app_seq.engine.pool, SingletonThreadPool)

    # Thread pool modes
    app_thread2 = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=2))
    app_thread4 = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=4))
    app_thread0 = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=0))

    assert isinstance(app_thread2.engine.pool, QueuePool)
    assert isinstance(app_thread4.engine.pool, QueuePool)
    assert isinstance(app_thread0.engine.pool, QueuePool)


def test_pool_size_overflow_configuration(db_url: str):
    """Test that pool overflow is consistently configured."""
    for max_workers in [2, 4, 8, 16]:
        config = Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=max_workers)
        app = bq.BeanQueue(config=config)

        if max_workers > 1:
            pool = app.engine.pool
            assert isinstance(pool, QueuePool)
            # All pools should have max_overflow of 10
            assert pool._max_overflow == 10
            # Pool size should be max_workers + 5
            assert pool.size() == max_workers + 5


@pytest.mark.parametrize(
    "max_workers,batch_size,expected_relationship",
    [
        (1, 1, "equal"),  # Sequential: batch = threads
        (4, 4, "equal"),  # Optimal: batch = threads
        (4, 8, "batch_larger"),  # Batch > threads
        (4, 2, "batch_smaller"),  # Batch < threads
        (8, 16, "batch_larger"),  # Batch > threads
        (0, 10, "default_auto"),  # Auto-detect threads
    ],
)
def test_batch_size_thread_combinations(
    db_url: str, max_workers: int, batch_size: int, expected_relationship: str
):
    """Test various combinations of BATCH_SIZE and MAX_WORKER_THREADS."""
    config = Config(
        DATABASE_URL=db_url,
        MAX_WORKER_THREADS=max_workers,
        BATCH_SIZE=batch_size,
    )
    app = bq.BeanQueue(config=config)

    assert app.config.MAX_WORKER_THREADS == max_workers
    assert app.config.BATCH_SIZE == batch_size

    # Verify configuration is consistent
    if expected_relationship == "equal":
        assert app.config.BATCH_SIZE == app.config.MAX_WORKER_THREADS
    elif expected_relationship == "batch_larger":
        assert app.config.BATCH_SIZE > app.config.MAX_WORKER_THREADS
    elif expected_relationship == "batch_smaller":
        assert app.config.BATCH_SIZE < app.config.MAX_WORKER_THREADS


def test_multiple_app_instances_independent_pools(db_url: str):
    """Test that multiple BeanQueue instances have independent pools."""
    app1 = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=2))
    app2 = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=4))
    app3 = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=1))

    # Each should have its own engine
    assert app1.engine is not app2.engine
    assert app2.engine is not app3.engine
    assert app1.engine is not app3.engine

    # Each should have appropriate pool
    assert isinstance(app1.engine.pool, QueuePool)
    assert isinstance(app2.engine.pool, QueuePool)
    assert isinstance(app3.engine.pool, SingletonThreadPool)

    # Pool sizes should differ
    assert app1.engine.pool.size() == 7  # 2 + 5
    assert app2.engine.pool.size() == 9  # 4 + 5
    # SingletonThreadPool doesn't have size() method


def test_engine_caching(db_url: str):
    """Test that engine is cached and reused."""
    app = bq.BeanQueue(config=Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=4))

    # Get engine multiple times
    engine1 = app.engine
    engine2 = app.engine
    engine3 = app.engine

    # Should be the same instance
    assert engine1 is engine2
    assert engine2 is engine3


def test_custom_engine_override(db_url: str):
    """Test that custom engine can be provided."""
    # Create custom engine
    custom_engine = create_engine(db_url, poolclass=QueuePool, pool_size=20)

    app = bq.BeanQueue(
        config=Config(DATABASE_URL=db_url, MAX_WORKER_THREADS=1),
        engine=custom_engine,
    )

    # Should use the custom engine
    assert app.engine is custom_engine
    assert isinstance(app.engine.pool, QueuePool)
    assert app.engine.pool.size() == 20
