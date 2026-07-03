#!/usr/bin/env python
"""Soak + latency harness for bq. Not pytest — run nightly or manually.

Enqueues N record_execution tasks, processes them with real `bq process`
worker subprocesses, then drains gracefully (SIGINT) and sweeps the
queue-consistency invariants. Prints throughput and NOTIFY-wakeup latency
percentiles; exits 1 on any invariant violation.

Canonical dockerized invocation:

    docker compose run --rm --entrypoint "" test uv run python scripts/soak.py \
        --db-url postgresql://bq:@psql/bq_test --tasks 5000 --workers 2

Host invocation (compose PG on localhost):

    uv run python scripts/soak.py --tasks 2000
"""
import argparse
import os
import pathlib
import random
import signal
import subprocess
import sys
import time

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy import text  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from bq.db.base import Base  # noqa: E402
from tests.acceptance.fixtures.thread_processors import record_execution  # noqa: E402
from tests.acceptance.invariants import assert_queue_consistent  # noqa: E402

EXECUTIONS_DDL = """
CREATE TABLE IF NOT EXISTS test_executions (
    id BIGSERIAL PRIMARY KEY,
    task_id UUID NOT NULL,
    thread_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ
)
"""

ENQUEUE_BATCH = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-url",
        default=os.environ.get("TEST_DB_URL", "postgresql://bq:@localhost/bq_test"),
    )
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--tasks", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--timeout", type=int, default=600)
    return parser.parse_args()


def spawn_workers(args: argparse.Namespace) -> list:
    env = {
        **os.environ,
        "BQ_PROCESSOR_PACKAGES": '["tests.acceptance.fixtures.thread_processors"]',
        "BQ_MAX_WORKER_THREADS": str(args.threads),
        "BQ_BATCH_SIZE": "10",
        "BQ_DATABASE_URL": args.db_url,
        "BQ_METRICS_HTTP_SERVER_ENABLED": "false",
        "BQ_POLL_TIMEOUT": "5",
        "PYTHONPATH": str(REPO_ROOT),
    }
    return [
        subprocess.Popen(
            ["uv", "run", "bq", "process", "thread-tests"],
            env=env,
            cwd=REPO_ROOT,
        )
        for _ in range(args.workers)
    ]


def main() -> int:
    args = parse_args()
    engine = create_engine(args.db_url)

    # Clean slate: the conservation invariant needs an empty queue
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test_executions"))
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        conn.execute(text(EXECUTIONS_DDL))

    procs = spawn_workers(args)
    rng = random.Random(args.seed)
    print(
        f"soak: tasks={args.tasks} workers={args.workers}"
        f" threads={args.threads} seed={args.seed}"
    )

    start = time.monotonic()
    with Session(bind=engine) as session:
        pending = 0
        for i in range(args.tasks):
            # A sprinkle of short sleeps varies thread interleavings
            sleep_time = 0.01 if rng.random() < 0.05 else 0.0
            session.add(record_execution.run(value=i, sleep_time=sleep_time))
            pending += 1
            if pending >= ENQUEUE_BATCH:
                session.commit()
                pending = 0
        session.commit()
    enqueue_done = time.monotonic()

    deadline = time.monotonic() + args.timeout
    while True:
        with engine.connect() as conn:
            active = conn.execute(
                text(
                    "SELECT count(*) FROM bq_tasks"
                    " WHERE state IN ('PENDING', 'PROCESSING')"
                )
            ).scalar()
        if active == 0:
            break
        if time.monotonic() > deadline:
            print(f"FAIL: {active} tasks still active after {args.timeout}s")
            for proc in procs:
                proc.kill()
            return 1
        time.sleep(1)
    wall = time.monotonic() - start

    # Graceful drain: SIGINT, wait for clean exit
    for proc in procs:
        proc.send_signal(signal.SIGINT)
    worker_exit_ok = True
    for proc in procs:
        try:
            code = proc.wait(timeout=30)
            if code not in (0, -signal.SIGINT):
                worker_exit_ok = False
                print(f"WARN: worker exited with code {code}")
        except subprocess.TimeoutExpired:
            worker_exit_ok = False
            print("WARN: worker did not exit after SIGINT, killing")
            proc.kill()

    with engine.connect() as conn:
        p50, p95, p100 = conn.execute(
            text(
                """
                SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY lat),
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY lat),
                       max(lat)
                FROM (SELECT extract(epoch FROM e.started_at - t.created_at) AS lat
                      FROM test_executions e
                      JOIN bq_tasks t ON t.id = e.task_id) latencies
                """
            )
        ).one()
        not_done = conn.execute(
            text("SELECT count(*) FROM bq_tasks WHERE state != 'DONE'")
        ).scalar()
        bad_tally = conn.execute(
            text(
                """
                SELECT count(*) FROM (
                    SELECT task_id FROM test_executions
                    WHERE finished_at IS NOT NULL
                    GROUP BY task_id HAVING count(*) != 1
                ) violations
                """
            )
        ).scalar()

    print("--- soak report ---")
    print(f"wall time:        {wall:.1f}s (enqueue {enqueue_done - start:.1f}s)")
    print(f"throughput:       {args.tasks / wall:.1f} tasks/s")
    print(f"wakeup latency:   p50={p50:.3f}s p95={p95:.3f}s max={p100:.3f}s")
    print(f"tasks not DONE:   {not_done}")
    print(f"tally violations: {bad_tally}")

    violations = 0
    try:
        assert_queue_consistent(engine, created_count=args.tasks)
    except AssertionError as error:
        violations += 1
        print(f"INVARIANT VIOLATION: {error}")
    if not_done:
        violations += 1
        print(f"INVARIANT VIOLATION: {not_done} tasks not DONE")
    if bad_tally:
        violations += 1
        print(f"INVARIANT VIOLATION: {bad_tally} tasks without exactly 1 execution")
    if not worker_exit_ok:
        violations += 1

    print("RESULT:", "OK" if violations == 0 else f"FAIL ({violations} violations)")
    return 0 if violations == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
