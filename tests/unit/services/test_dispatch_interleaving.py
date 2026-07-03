"""Deterministic claim-interleaving tests for DispatchService.

Two raw ORM sessions on *distinct* connections step through dispatch with
held-open transactions (GoodJob semaphore-test style). No threads, no
sleeps — every interleaving is explicit. ``lock_timeout=5s`` on both
connections makes a wrong implementation (e.g. missing SKIP LOCKED) fail
fast instead of hanging the suite.
"""
import datetime

import pytest
from sqlalchemy import func
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session as RawSession

from bq import models
from bq.services.dispatch import DispatchService
from bq.services.worker import WorkerService

CHANNEL = "interleave"


@pytest.fixture
def session_pair(db, engine: Engine):
    """Two ORM sessions on two distinct raw connections, lock_timeout=5s."""
    conn_a = engine.connect()
    conn_b = engine.connect()
    assert conn_a is not conn_b
    sessions = []
    try:
        for conn in (conn_a, conn_b):
            conn.execute(text("SET SESSION lock_timeout = '5s'"))
            conn.commit()
            sessions.append(RawSession(bind=conn))
        yield sessions[0], sessions[1]
    finally:
        for session in sessions:
            session.rollback()
            session.close()
        conn_a.close()
        conn_b.close()


def test_uncommitted_claim_blocks_second_dispatcher(
    session_pair, db, task_factory, worker_factory
):
    """An uncommitted claim is invisible to SKIP LOCKED peers, before and after commit."""
    sess_a, sess_b = session_pair
    worker_a = worker_factory()
    worker_b = worker_factory()
    task = task_factory(channel=CHANNEL)

    svc_a = DispatchService(sess_a)
    svc_b = DispatchService(sess_b)

    claimed_a = list(svc_a.dispatch([CHANNEL], worker_id=worker_a.id))
    assert [t.id for t in claimed_a] == [task.id]
    # A's transaction stays open: the row is locked and already PROCESSING

    claimed_b = list(svc_b.dispatch([CHANNEL], worker_id=worker_b.id))
    assert claimed_b == []
    sess_b.rollback()

    sess_a.commit()

    # After A commits, the row is PROCESSING — still nothing for B
    claimed_b = list(svc_b.dispatch([CHANNEL], worker_id=worker_b.id))
    assert claimed_b == []
    sess_b.rollback()

    db.expire_all()
    row = db.get(models.Task, task.id)
    assert row.state == models.TaskState.PROCESSING
    assert row.worker_id == worker_a.id


def test_rolled_back_claim_is_reclaimable(
    session_pair, db, task_factory, worker_factory
):
    """Crash-before-commit loses nothing: a rolled-back claim returns to PENDING."""
    sess_a, sess_b = session_pair
    worker_a = worker_factory()
    worker_b = worker_factory()
    task = task_factory(channel=CHANNEL)

    svc_a = DispatchService(sess_a)
    svc_b = DispatchService(sess_b)

    claimed_a = list(svc_a.dispatch([CHANNEL], worker_id=worker_a.id))
    assert [t.id for t in claimed_a] == [task.id]
    sess_a.rollback()  # simulated crash before commit: claim undone

    db.expire_all()
    assert db.get(models.Task, task.id).state == models.TaskState.PENDING

    claimed_b = list(svc_b.dispatch([CHANNEL], worker_id=worker_b.id))
    assert [t.id for t in claimed_b] == [task.id]
    sess_b.commit()

    db.expire_all()
    row = db.get(models.Task, task.id)
    assert row.state == models.TaskState.PROCESSING
    assert row.worker_id == worker_b.id


def test_two_dispatchers_disjoint_batches(
    session_pair, db, task_factory, worker_factory
):
    """Concurrent batch dispatchers claim disjoint sets covering all tasks."""
    sess_a, sess_b = session_pair
    worker_a = worker_factory()
    worker_b = worker_factory()
    tasks = [task_factory(channel=CHANNEL) for _ in range(4)]
    all_ids = {t.id for t in tasks}

    svc_a = DispatchService(sess_a)
    svc_b = DispatchService(sess_b)

    claimed_a = {
        t.id for t in svc_a.dispatch([CHANNEL], worker_id=worker_a.id, limit=2)
    }
    assert len(claimed_a) == 2
    # A stays uncommitted, holding locks on its two rows

    claimed_b = {
        t.id for t in svc_b.dispatch([CHANNEL], worker_id=worker_b.id, limit=4)
    }
    assert len(claimed_b) == 2

    assert claimed_a | claimed_b == all_ids
    assert claimed_a & claimed_b == set()
    sess_a.commit()
    sess_b.commit()


def test_scheduled_at_boundary_uses_now_param(
    session_pair, db, task_factory, worker_factory
):
    """dispatch(now=...) controls time: nothing 1s early, claimed exactly at T0."""
    sess_a, _ = session_pair
    worker_a = worker_factory()
    t0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    task = task_factory(channel=CHANNEL, scheduled_at=t0)

    svc_a = DispatchService(sess_a)

    before = list(
        svc_a.dispatch(
            [CHANNEL],
            worker_id=worker_a.id,
            now=t0 - datetime.timedelta(seconds=1),
        )
    )
    assert before == []
    sess_a.rollback()

    at_boundary = list(svc_a.dispatch([CHANNEL], worker_id=worker_a.id, now=t0))
    assert [t.id for t in at_boundary] == [task.id]
    sess_a.commit()


def test_reschedule_dead_tasks_vs_live_dispatch_no_deadlock(
    session_pair, db, task_factory, worker_factory
):
    """Dead-worker rescue holding worker-row locks never blocks live dispatch."""
    sess_a, sess_b = session_pair
    now = db.scalar(func.now())
    dead_worker = worker_factory(
        last_heartbeat=now - datetime.timedelta(seconds=300)
    )
    live_worker = worker_factory(last_heartbeat=now)
    stuck = task_factory(
        channel=CHANNEL, state=models.TaskState.PROCESSING, worker=dead_worker
    )
    fresh = task_factory(channel="interleave-live")

    worker_svc_a = WorkerService(sess_a)
    dispatch_svc_b = DispatchService(sess_b)

    # A: mark the stale worker dead (FOR UPDATE SKIP LOCKED on its row),
    # transaction held open
    dead = worker_svc_a.fetch_dead_workers(timeout=60).all()
    assert [w.id for w in dead] == [dead_worker.id]

    # B: live dispatch on another channel must complete without blocking
    sess_b.execute(text("SET statement_timeout = '5s'"))
    claimed = list(
        dispatch_svc_b.dispatch(["interleave-live"], worker_id=live_worker.id)
    )
    assert [t.id for t in claimed] == [fresh.id]
    sess_b.commit()

    # A: rescheduling the dead worker's tasks proceeds
    count = worker_svc_a.reschedule_dead_tasks(
        sess_a.query(models.Worker.id).filter(models.Worker.id == dead_worker.id)
    )
    assert count == 1
    sess_a.commit()

    db.expire_all()
    row = db.get(models.Task, stuck.id)
    assert row.state == models.TaskState.PENDING
    assert row.worker_id is None
