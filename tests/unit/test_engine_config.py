"""Unit tests for engine configuration, pool selection, and worker thread resolution."""
import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from sqlalchemy.pool import QueuePool

from bq.app import BeanQueue
from bq.config import Config


@pytest.fixture
def bq():
    return BeanQueue(config=Config(DATABASE_URL="postgresql://test@localhost/test"))


class TestResolveMaxWorkers:
    """Tests for _resolve_max_workers single-source resolution."""

    def test_explicit_value_returned(self, bq):
        bq.config = Config(DATABASE_URL="postgresql://test@localhost/test", MAX_WORKER_THREADS=4)
        assert bq._resolve_max_workers() == 4

    def test_sequential_returns_one(self, bq):
        bq.config = Config(DATABASE_URL="postgresql://test@localhost/test", MAX_WORKER_THREADS=1)
        assert bq._resolve_max_workers() == 1

    def test_zero_auto_resolves(self, bq):
        bq.config = Config(DATABASE_URL="postgresql://test@localhost/test", MAX_WORKER_THREADS=0)
        expected = min(32, (os.cpu_count() or 1) + 4)
        assert bq._resolve_max_workers() == expected

    @patch("os.cpu_count", return_value=2)
    def test_zero_with_known_cpu_count(self, mock_cpu, bq):
        bq.config = Config(DATABASE_URL="postgresql://test@localhost/test", MAX_WORKER_THREADS=0)
        assert bq._resolve_max_workers() == 6  # min(32, 2+4)

    @patch("os.cpu_count", return_value=None)
    def test_zero_with_unknown_cpu_count(self, mock_cpu, bq):
        bq.config = Config(DATABASE_URL="postgresql://test@localhost/test", MAX_WORKER_THREADS=0)
        assert bq._resolve_max_workers() == 5  # min(32, 1+4)

    @patch("os.cpu_count", return_value=100)
    def test_zero_caps_at_32(self, mock_cpu, bq):
        bq.config = Config(DATABASE_URL="postgresql://test@localhost/test", MAX_WORKER_THREADS=0)
        assert bq._resolve_max_workers() == 32


class TestCreateDefaultEngine:
    """Tests for QueuePool-only engine creation."""

    def test_always_uses_queue_pool(self, bq):
        engine = bq.create_default_engine()
        assert isinstance(engine.pool, QueuePool)

    def test_sequential_uses_queue_pool(self):
        app = BeanQueue(config=Config(
            DATABASE_URL="postgresql://test@localhost/test",
            MAX_WORKER_THREADS=1,
        ))
        engine = app.create_default_engine()
        assert isinstance(engine.pool, QueuePool)

    def test_threaded_pool_size_scales_with_workers(self):
        app = BeanQueue(config=Config(
            DATABASE_URL="postgresql://test@localhost/test",
            MAX_WORKER_THREADS=8,
        ))
        engine = app.create_default_engine()
        assert engine.pool.size() == 8 + 5  # workers + 5

    def test_engine_override_bypasses_default(self):
        """When engine= is passed, create_default_engine is never called."""
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool
        custom = create_engine("postgresql://test@localhost/test", poolclass=NullPool)
        app = BeanQueue(
            config=Config(DATABASE_URL="postgresql://test@localhost/test"),
            engine=custom,
        )
        assert app.engine is custom

    def test_engine_cached(self, bq):
        e1 = bq.engine
        e2 = bq.engine
        assert e1 is e2


class TestConfigValidation:
    """Tests for pydantic Field constraints."""

    def test_negative_max_worker_threads_rejected(self):
        with pytest.raises(ValidationError):
            Config(DATABASE_URL="postgresql://test@localhost/test", MAX_WORKER_THREADS=-1)

    def test_zero_batch_size_rejected(self):
        with pytest.raises(ValidationError):
            Config(DATABASE_URL="postgresql://test@localhost/test", BATCH_SIZE=0)

    def test_negative_batch_size_rejected(self):
        with pytest.raises(ValidationError):
            Config(DATABASE_URL="postgresql://test@localhost/test", BATCH_SIZE=-1)

    def test_valid_defaults(self):
        config = Config(DATABASE_URL="postgresql://test@localhost/test")
        assert config.MAX_WORKER_THREADS == 1
        assert config.BATCH_SIZE == 1


class TestEnvParsing:
    """Tests for environment variable parsing of config."""

    def test_max_worker_threads_from_env(self):
        with patch.dict(os.environ, {"BQ_MAX_WORKER_THREADS": "8", "BQ_DATABASE_URL": "postgresql://test@localhost/test"}):
            config = Config()
            assert config.MAX_WORKER_THREADS == 8

    def test_batch_size_from_env(self):
        with patch.dict(os.environ, {"BQ_BATCH_SIZE": "5", "BQ_DATABASE_URL": "postgresql://test@localhost/test"}):
            config = Config()
            assert config.BATCH_SIZE == 5
