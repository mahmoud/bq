"""End-to-end acceptance test for the bq CLI.

Drives the real console script (`bq create_tables` / `bq submit` /
`bq process`) via subprocess — the layer every other acceptance test
bypasses by calling app.process_tasks() in-process. Child coverage is
captured through coverage.py's subprocess patch when run under the
dockerized entrypoint.
"""
import os
import pathlib
import signal
import subprocess

from bq import models

from .helpers import wait_for_task_state

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
PROCESSOR_MODULE = "tests.acceptance.fixtures.thread_processors"


def _cli_env(db_url: str) -> dict:
    return {
        **os.environ,
        "BQ_DATABASE_URL": db_url,
        "BQ_PROCESSOR_PACKAGES": f'["{PROCESSOR_MODULE}"]',
        "BQ_METRICS_HTTP_SERVER_ENABLED": "false",
        "BQ_POLL_TIMEOUT": "1",
        "BQ_MAX_WORKER_THREADS": "2",
        # The processor fixtures live in the repo, not the installed package
        "PYTHONPATH": str(REPO_ROOT),
    }


def _bq(args: list, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bq", *args],
        env=env,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_cli_end_to_end(db, db_url):
    """create_tables -> submit -> process drives a task to DONE via the CLI."""
    env = _cli_env(db_url)

    result = _bq(["create_tables"], env)
    assert result.returncode == 0, result.stderr

    result = _bq(
        [
            "submit",
            "thread-tests",
            PROCESSOR_MODULE,
            "concurrent_task",
            "-k",
            '{"value": 6}',
        ],
        env,
    )
    assert result.returncode == 0, result.stderr

    proc = subprocess.Popen(["bq", "process", "thread-tests"], env=env, cwd=REPO_ROOT)
    try:
        wait_for_task_state(db_url, models.TaskState.DONE, 1, timeout=30)
    finally:
        # Graceful drain (contract: SIGINT finishes in-flight work and exits)
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(5)

    assert proc.returncode in (0, -signal.SIGINT), (
        f"worker did not exit gracefully on SIGINT: {proc.returncode}"
    )

    db.expire_all()
    task = db.query(models.Task).one()
    assert task.state == models.TaskState.DONE
    assert task.result == 36  # concurrent_task returns value ** 2
