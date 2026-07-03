import threading
import time
import datetime

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import object_session

import bq
from bq.processors.retry_policies import DelayRetry

app = bq.BeanQueue()


@app.processor(channel="thread-tests")
def slow_task(task: bq.Task, task_num: int, sleep_time: float):
    """A slow task that simulates I/O-bound work."""
    time.sleep(sleep_time)
    return task_num * 2


@app.processor(channel="thread-tests")
def timed_task(task: bq.Task, task_num: int, sleep_time: float):
    """A task that records its start and end times to prove concurrent execution."""
    start_time = time.time()
    time.sleep(sleep_time)
    end_time = time.time()
    return {"task_num": task_num, "start": start_time, "end": end_time}


@app.processor(channel="thread-tests")
def concurrent_task(task: bq.Task, value: int):
    """A task to test concurrent execution and session isolation."""
    # Simulate some work
    time.sleep(0.1)
    result = value ** 2
    return result


@app.processor(channel="thread-tests")
def failing_task(task: bq.Task, task_num: int, should_fail: bool):
    """A task that fails conditionally to test error handling."""
    time.sleep(0.1)
    if should_fail:
        raise ValueError(f"Intentional failure for task {task_num}")
    return task_num * 2


# Retry policy: retry once after 0.5 seconds
retry_policy = DelayRetry(delay=datetime.timedelta(seconds=0.5))


@app.processor(channel="thread-tests", retry_policy=retry_policy)
def retry_task(task: bq.Task, task_num: int, max_attempts: int):
    """A task that fails initially but succeeds on retry."""
    time.sleep(0.1)

    # Count attempts by checking if this is first attempt (no scheduled_at)
    # or a retry (has scheduled_at set)
    if task.scheduled_at is None:
        # First attempt - always fail
        raise ValueError(f"First attempt fails for task {task_num}")
    else:
        # Retry attempt - succeed
        return task_num * 3


@app.processor(channel="thread-tests")
def record_thread_name() -> str:
    """Records the current thread name for debugging and testing."""
    thread_name = threading.current_thread().name
    return thread_name


# --- Deterministic gates: hold tasks in-flight until the test opens the gate ---

GATES: dict[str, threading.Event] = {}


def open_gate(name: str) -> None:
    GATES.setdefault(name, threading.Event()).set()


def reset_gates() -> None:
    GATES.clear()


@app.processor(channel="thread-tests")
def gated_task(task: bq.Task, gate: str):
    """Blocks in-flight until the test calls open_gate(gate); 30s safety valve."""
    GATES.setdefault(gate, threading.Event()).wait(timeout=30)


# --- Execution tally: record every execution in a side table (autocommit) ---

_side_engines: dict[str, object] = {}
_side_engines_lock = threading.Lock()


def _get_side_engine(url: str):
    """Lazily-created autocommit engine per bind URL, shared across threads.

    Autocommit is load-bearing: the started marker must be visible to other
    sessions *before* the task's own transaction commits.
    """
    with _side_engines_lock:
        engine = _side_engines.get(url)
        if engine is None:
            engine = create_engine(url, isolation_level="AUTOCOMMIT", pool_size=2)
            _side_engines[url] = engine
        return engine


@app.processor(channel="thread-tests")
def record_execution(task: bq.Task, value: int = 0, sleep_time: float = 0.0):
    """Probe for exactly-once/overlap assertions: tallies into test_executions."""
    session = object_session(task)
    url = session.get_bind().url.render_as_string(hide_password=False)
    engine = _get_side_engine(url)
    with engine.connect() as conn:
        row_id = conn.execute(
            text(
                "INSERT INTO test_executions (task_id, thread_name)"
                " VALUES (:tid, :tn) RETURNING id"
            ),
            {"tid": str(task.id), "tn": threading.current_thread().name},
        ).scalar()
    if sleep_time:
        time.sleep(sleep_time)
    with engine.connect() as conn:
        conn.execute(
            text(
                "UPDATE test_executions SET finished_at = clock_timestamp()"
                " WHERE id = :id"
            ),
            {"id": row_id},
        )
    return value
