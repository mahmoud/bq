import json
from unittest.mock import MagicMock, patch

import pytest

from bq.app import BeanQueue


def _make_environ(path: str) -> dict:
    """Build a minimal WSGI environ dict."""
    return {"PATH_INFO": path, "REQUEST_METHOD": "GET"}


@pytest.fixture
def bq():
    """Create a BeanQueue with stubbed config (no real DB needed)."""
    with patch("bq.app.Config") as MockConfig:
        config = MockConfig.return_value
        config.DATABASE_URL = "postgresql://test@localhost/test"
        config.METRICS_HTTP_SERVER_INTERFACE = "127.0.0.1"
        config.METRICS_HTTP_SERVER_PORT = 0
        config.METRICS_HTTP_SERVER_ENABLED = False
        config.METRICS_HTTP_SERVER_LOG_LEVEL = "WARNING"
        config.WORKER_HEARTBEAT_PERIOD = 30
        config.WORKER_HEARTBEAT_TIMEOUT = 60
        config.MAX_WORKER_THREADS = 1
        instance = BeanQueue(config=config)
        yield instance


class TestHealthzEndpoint:
    """Tests for the /healthz HTTP handler."""

    def test_healthz_returns_200_when_healthy(self, bq):
        bq._health_ok = True
        bq._health_info = {"state": "RUNNING"}

        start_response = MagicMock()
        result = bq._serve_http_request("42", _make_environ("/healthz"), start_response)

        start_response.assert_called_once_with(
            "200 OK", [("Content-Type", "application/json")]
        )
        body = json.loads(result[0])
        assert body["status"] == "ok"
        assert body["worker_id"] == "42"
        assert body["state"] == "RUNNING"

    def test_healthz_returns_500_when_unhealthy(self, bq):
        bq._health_ok = False
        bq._health_info = {"state": "SHUTDOWN"}

        start_response = MagicMock()
        result = bq._serve_http_request("42", _make_environ("/healthz"), start_response)

        start_response.assert_called_once_with(
            "500 Internal Server Error",
            [("Content-Type", "application/json")],
        )
        body = json.loads(result[0])
        assert body["status"] == "error"
        assert body["worker_id"] == "42"
        assert body["state"] == "SHUTDOWN"

    def test_healthz_returns_500_before_worker_initialized(self, bq):
        """Before process_tasks runs, _health_ok is False and _health_info is empty."""
        start_response = MagicMock()
        result = bq._serve_http_request("1", _make_environ("/healthz"), start_response)

        start_response.assert_called_once_with(
            "500 Internal Server Error",
            [("Content-Type", "application/json")],
        )
        body = json.loads(result[0])
        assert body["status"] == "error"
        assert body["worker_id"] == "1"

    def test_healthz_does_not_create_db_session(self, bq):
        """The critical fix: /healthz must never touch the DB."""
        bq._health_ok = True
        bq._health_info = {"state": "RUNNING"}

        bq.make_session = MagicMock()
        start_response = MagicMock()
        bq._serve_http_request("42", _make_environ("/healthz"), start_response)

        bq.make_session.assert_not_called()

    def test_unknown_path_returns_404(self, bq):
        start_response = MagicMock()
        result = bq._serve_http_request("42", _make_environ("/unknown"), start_response)

        start_response.assert_called_once_with(
            "404 NOT FOUND", [("Content-Type", "application/json")]
        )
        body = json.loads(result[0])
        assert body["status"] == "not found"

    def test_404_does_not_create_db_session(self, bq):
        bq.make_session = MagicMock()
        start_response = MagicMock()
        bq._serve_http_request("42", _make_environ("/anything"), start_response)

        bq.make_session.assert_not_called()


class TestHealthStateInitialization:
    """Tests that _health_ok defaults correctly."""

    def test_defaults_to_unhealthy(self, bq):
        assert bq._health_ok is False
        assert bq._health_info == {}
