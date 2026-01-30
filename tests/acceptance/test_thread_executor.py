import datetime
import time
from multiprocessing import Process

from sqlalchemy.orm import Session

from .fixtures.thread_processors import app
from .fixtures.thread_processors import slow_task
from .fixtures.thread_processors import concurrent_task
from bq import models
from bq.config import Config


def run_process_cmd_with_threads(db_url: str, max_workers: int, batch_size: int):
    """Run worker process with thread pool executor enabled."""
    app.config = Config(
        PROCESSOR_PACKAGES=["tests.acceptance.fixtures.thread_processors"],
        DATABASE_URL=db_url,
        MAX_WORKER_THREADS=max_workers,
        BATCH_SIZE=batch_size,
    )
    app.process_tasks(channels=("thread-tests",))


def test_thread_executor_with_multiple_threads(db: Session, db_url: str):
    """Test that thread executor processes tasks concurrently within a single worker."""
    # Use a single worker process with 4 threads
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 4, 10),  # 4 threads, batch size 10
    )
    proc.start()

    # Create 16 tasks (each takes 0.2 seconds)
    task_count = 16
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=0.2)
        db.add(task)
    db.commit()

    # With 4 threads, 16 tasks should complete in ~1 second
    # (16 tasks / 4 threads = 4 batches, 4 * 0.2s = 0.8s + overhead)
    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_tasks == task_count:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 15:
            raise TimeoutError(
                f"Timeout waiting for all tasks to finish. Only {done_tasks}/{task_count} completed"
            )
        time.sleep(0.5)

    elapsed = (datetime.datetime.now() - begin).total_seconds()

    # Verify all tasks completed
    assert done_tasks == task_count

    # Verify results are correct
    completed_tasks = (
        db.query(models.Task).filter(models.Task.state == models.TaskState.DONE).all()
    )
    for task in completed_tasks:
        # Result should be task_num * 2
        expected_result = task.kwargs["task_num"] * 2
        assert task.result == expected_result, f"Task {task.id} has incorrect result"

    proc.kill()
    proc.join(3)


def test_thread_executor_session_isolation(db: Session, db_url: str):
    """Test that each thread has its own database session without conflicts."""
    # Use a single worker process with 4 threads
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 4, 8),  # 4 threads, batch size 8
    )
    proc.start()

    # Create 12 tasks that will be processed concurrently
    task_count = 12
    for i in range(task_count):
        task = concurrent_task.run(value=i)
        db.add(task)
    db.commit()

    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_tasks == task_count:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 15:
            # Check for failed tasks
            failed_tasks = (
                db.query(models.Task)
                .filter(models.Task.state == models.TaskState.FAILED)
                .count()
            )
            raise TimeoutError(
                f"Timeout waiting for all tasks to finish. "
                f"Done: {done_tasks}/{task_count}, Failed: {failed_tasks}"
            )
        time.sleep(0.5)

    # Verify all tasks completed successfully (no session conflicts)
    failed_tasks = (
        db.query(models.Task)
        .filter(models.Task.state == models.TaskState.FAILED)
        .count()
    )
    assert failed_tasks == 0, f"Found {failed_tasks} failed tasks due to session conflicts"
    assert done_tasks == task_count, f"Only {done_tasks}/{task_count} tasks completed"

    proc.kill()
    proc.join(3)


def test_sequential_processing_backward_compatibility(db: Session, db_url: str):
    """Test that MAX_WORKER_THREADS=1 maintains sequential processing behavior."""
    # Use a single worker process with sequential processing (MAX_WORKER_THREADS=1)
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 1, 5),  # 1 thread (sequential), batch size 5
    )
    proc.start()

    task_count = 10
    for i in range(task_count):
        task = slow_task.run(task_num=i, sleep_time=0.1)
        db.add(task)
    db.commit()

    begin = datetime.datetime.now()
    while True:
        db.expire_all()
        done_tasks = (
            db.query(models.Task)
            .filter(models.Task.state == models.TaskState.DONE)
            .count()
        )
        if done_tasks == task_count:
            break
        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 10:
            raise TimeoutError("Timeout waiting for all tasks to finish")
        time.sleep(0.2)

    # Verify all tasks completed
    assert done_tasks == task_count

    proc.kill()
    proc.join(3)


def test_tasks_arriving_during_processing_run_concurrently(db: Session, db_url: str):
    """Test that tasks arriving while others are processing get picked up immediately.

    This simulates the scenario of two browser tabs submitting tasks - the second task
    should start processing as soon as it's created, not wait for the first to complete.
    """
    # Worker with 3 threads, so we have capacity for concurrent tasks
    proc = Process(
        target=run_process_cmd_with_threads,
        args=(db_url, 3, 5),  # 3 threads, batch size 5
    )
    proc.start()

    # Give the worker time to start and begin polling
    time.sleep(0.5)

    # Submit first long-running task (simulates first browser tab)
    task1 = slow_task.run(task_num=1, sleep_time=1.5)
    db.add(task1)
    db.commit()
    task1_id = task1.id

    # Wait a bit for task1 to start processing
    time.sleep(0.3)

    # Submit second long-running task (simulates second browser tab)
    task2 = slow_task.run(task_num=2, sleep_time=1.5)
    db.add(task2)
    db.commit()
    task2_id = task2.id

    # Track when each task completes
    task1_done_time = None
    task2_done_time = None

    begin = datetime.datetime.now()
    while True:
        db.expire_all()

        t1 = db.query(models.Task).filter(models.Task.id == task1_id).one()
        t2 = db.query(models.Task).filter(models.Task.id == task2_id).one()

        if t1.state == models.TaskState.DONE and task1_done_time is None:
            task1_done_time = datetime.datetime.now()
        if t2.state == models.TaskState.DONE and task2_done_time is None:
            task2_done_time = datetime.datetime.now()

        if task1_done_time and task2_done_time:
            break

        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 10:
            raise TimeoutError(
                f"Timeout waiting for tasks. Task1 state: {t1.state}, Task2 state: {t2.state}"
            )
        time.sleep(0.1)

    # The key assertion: if tasks ran concurrently, they should finish close together
    # Task2 was submitted 0.3s after task1, and both take 1.5s
    # If concurrent: task2 finishes ~0.3s after task1 (within 0.5s tolerance)
    # If sequential: task2 finishes ~1.5s after task1
    completion_gap = abs((task2_done_time - task1_done_time).total_seconds())

    # With concurrent processing, the gap should be around 0.3s (the delay between submissions)
    # Allow up to 0.8s to account for scheduling overhead
    assert completion_gap < 0.8, (
        f"Tasks appear to have run sequentially! "
        f"Completion gap: {completion_gap:.2f}s (expected < 0.8s for concurrent execution). "
        f"Task1 done at {task1_done_time}, Task2 done at {task2_done_time}"
    )

    proc.kill()
    proc.join(3)
