"""Seeded torture test: mixed success/fail/retry load under competing workers.

Replay a failure by re-running with the printed seed:

    BQ_TORTURE_SEED=<seed> uv run pytest tests/acceptance/test_torture.py

The seed only controls the task mix; thread interleavings remain
nondeterministic — the invariants (no double execution overlap, no lost
tasks, no orphaned claims) must hold for every interleaving.
"""
import os
import random

from sqlalchemy import text

from bq.app import BeanQueue
from bq.config import Config

from .fixtures.thread_processors import failing_task, record_execution, retry_task
from .helpers import wait_until
from .invariants import assert_queue_consistent

TASK_COUNT = 200


def _make_app(db_url):
    return BeanQueue(
        config=Config(
            DATABASE_URL=db_url,
            MAX_WORKER_THREADS=3,
            BATCH_SIZE=3,
            POLL_TIMEOUT=1,
            METRICS_HTTP_SERVER_ENABLED=False,
            PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
        )
    )


def test_mixed_load_invariants(db, db_url, run_worker, executions_table):
    seed = int(os.environ.get("BQ_TORTURE_SEED", "0"))
    print(f"torture seed: {seed} (replay: BQ_TORTURE_SEED={seed})")
    rng = random.Random(seed)
    engine = executions_table

    # Mixed load: ~60% tallied successes, ~20% failures, ~20% retry-then-succeed
    record_count = fail_count = retry_count = 0
    for i in range(TASK_COUNT):
        r = rng.random()
        if r < 0.6:
            db.add(record_execution.run(value=i))
            record_count += 1
        elif r < 0.8:
            db.add(failing_task.run(task_num=i, should_fail=True))
            fail_count += 1
        else:
            db.add(retry_task.run(task_num=i, max_attempts=2))
            retry_count += 1
    db.commit()
    assert record_count + fail_count + retry_count == TASK_COUNT

    # 3 competing in-process workers
    for _ in range(3):
        run_worker(_make_app(db_url), ("thread-tests",))

    def quiesced():
        with engine.connect() as conn:
            active = conn.execute(
                text(
                    "SELECT count(*) FROM bq_tasks"
                    " WHERE state IN ('PENDING', 'PROCESSING')"
                )
            ).scalar()
            # Overshoot detectors: fail fast instead of waiting out the deadline
            done = conn.execute(
                text("SELECT count(*) FROM bq_tasks WHERE state = 'DONE'")
            ).scalar()
            assert done <= TASK_COUNT, f"overshoot: {done} DONE > {TASK_COUNT}"
            max_exec = conn.execute(
                text(
                    "SELECT COALESCE(max(cnt), 0) FROM"
                    " (SELECT count(*) AS cnt FROM test_executions"
                    "  GROUP BY task_id) counts"
                )
            ).scalar()
            assert max_exec <= 2, f"a task executed {max_exec} times (no crash injected)"
            return active == 0

    wait_until(quiesced, timeout=90, interval=0.5, message="queue did not quiesce")

    assert_queue_consistent(engine, created_count=TASK_COUNT)

    with engine.connect() as conn:
        # Every record_execution task is DONE with exactly one finished execution
        bad_tallies = conn.execute(
            text(
                """
                SELECT t.id, t.state, count(e.id) AS finished
                FROM bq_tasks t
                LEFT JOIN test_executions e
                  ON e.task_id = t.id AND e.finished_at IS NOT NULL
                WHERE t.func_name = 'record_execution'
                GROUP BY t.id, t.state
                HAVING t.state != 'DONE' OR count(e.id) != 1
                """
            )
        ).all()
        assert not bad_tallies, f"record_execution tally violations: {bad_tallies}"

        # Every retry task is DONE after exactly one retry:
        # one FAILED_RETRY_SCHEDULED event + one COMPLETE event
        bad_retries = conn.execute(
            text(
                """
                SELECT t.id, t.state, count(ev.id) AS events
                FROM bq_tasks t
                LEFT JOIN bq_events ev ON ev.task_id = t.id
                WHERE t.func_name = 'retry_task'
                GROUP BY t.id, t.state
                HAVING t.state != 'DONE' OR count(ev.id) != 2
                """
            )
        ).all()
        assert not bad_retries, f"retry_task violations: {bad_retries}"

        # Every failing task is FAILED with a FAILED event
        bad_failures = conn.execute(
            text(
                """
                SELECT t.id, t.state
                FROM bq_tasks t
                WHERE t.func_name = 'failing_task'
                  AND (t.state != 'FAILED' OR NOT EXISTS (
                    SELECT 1 FROM bq_events ev
                    WHERE ev.task_id = t.id AND ev.type = 'FAILED'))
                """
            )
        ).all()
        assert not bad_failures, f"failing_task violations: {bad_failures}"

        counts = dict(
            conn.execute(
                text("SELECT state, count(*) FROM bq_tasks GROUP BY state")
            ).all()
        )
    assert counts.get("DONE", 0) == record_count + retry_count
    assert counts.get("FAILED", 0) == fail_count
