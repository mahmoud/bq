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
