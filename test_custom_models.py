#!/usr/bin/env python3
"""
Test thread executor with customized Task and Event models.

This test verifies that the thread executor works correctly with:
- Custom task models with additional fields
- Custom event models with additional fields
- Custom processors that access custom fields
"""

import bq
import time
import datetime
from multiprocessing import Process
from sqlalchemy import Column, String, Integer, DateTime, create_engine
from sqlalchemy.orm import Session


# Define custom models with additional fields
class CustomTask(bq.Task):
    """Custom task model with additional tracking fields."""
    # Use same table as bq.Task (single table inheritance)
    # SQLAlchemy will add these columns to the bq_tasks table

    # Additional custom fields
    priority = Column(Integer, default=0)
    category = Column(String(50))
    user_id = Column(String(50))
    metadata_json = Column(String(1000))


class CustomEvent(bq.Event):
    """Custom event model with additional tracking fields."""
    # Use same table as bq.Event (single table inheritance)

    # Additional custom fields
    severity = Column(String(20))
    source = Column(String(50))


# Create app with custom models via config
app = bq.BeanQueue(
    config=bq.Config(
        PROCESSOR_PACKAGES=["__main__"],
        DATABASE_URL="postgresql://bq:@localhost/bq_test_custom",
        MAX_WORKER_THREADS=4,
        BATCH_SIZE=8,
        TASK_MODEL="__main__.CustomTask",
        EVENT_MODEL="__main__.CustomEvent",
    ),
)


# Processor that uses custom task fields
@app.processor(channel="custom-processing")
def process_with_custom_fields(task: CustomTask, data: dict):
    """Process task using custom fields."""
    time.sleep(0.1)  # Simulate work

    # Access custom fields from task
    priority = task.priority
    category = task.category
    user_id = task.user_id

    # Process based on custom fields
    result = {
        "data": data,
        "priority": priority,
        "category": category,
        "user_id": user_id,
        "processed_at": datetime.datetime.now().isoformat(),
        "processing_thread": f"thread-{id(task) % 1000}",
    }

    return result


# High priority processor
@app.processor(channel="priority-processing")
def high_priority_task(task: CustomTask, operation: str):
    """Process high priority tasks."""
    time.sleep(0.05)

    return {
        "operation": operation,
        "priority": task.priority,
        "status": "completed",
        "processed_at": datetime.datetime.now().isoformat(),
    }


def run_worker_process(db_url: str):
    """Run worker process with custom models and thread pool."""
    from bq.config import Config

    # Create app with same custom models
    worker_app = bq.BeanQueue(
        config=Config(
            PROCESSOR_PACKAGES=["__main__"],
            DATABASE_URL=db_url,
            MAX_WORKER_THREADS=4,
            BATCH_SIZE=8,
            TASK_MODEL="__main__.CustomTask",
            EVENT_MODEL="__main__.CustomEvent",
        ),
    )
    worker_app.process_tasks(channels=("custom-processing", "priority-processing"))


def run_custom_model_test():
    """Run integration test with custom models."""

    print("=" * 80)
    print("THREAD EXECUTOR TEST - CUSTOM MODELS")
    print("=" * 80)

    # Setup database
    engine = create_engine("postgresql://bq:@localhost/bq_test_custom")

    # Drop and recreate tables
    from bq.db.base import Base
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    db = Session(bind=engine)

    print("\n📊 Creating tasks with custom fields...")

    # Create tasks with custom fields
    tasks_created = []

    # High priority tasks
    for i in range(10):
        task = high_priority_task.run(operation=f"priority-op-{i}")
        # Update custom fields
        task.priority = 10
        task.category = "critical"
        task.user_id = f"admin-{i % 3}"
        task.metadata_json = f'{{"request_id": "REQ-{i:04d}"}}'
        db.add(task)
        db.commit()
        tasks_created.append(task.id)

    # Normal priority tasks with custom processing
    for i in range(15):
        task = process_with_custom_fields.run(data={"item_id": i, "value": i * 10})
        # Update custom fields
        task.priority = 5
        task.category = "standard"
        task.user_id = f"user-{i % 5}"
        task.metadata_json = f'{{"batch": "BATCH-{i // 5}"}}'
        db.add(task)
        db.commit()
        tasks_created.append(task.id)

    print(f"✓ Created {len(tasks_created)} tasks with custom fields")
    print(f"  - 10 high priority tasks (priority=10)")
    print(f"  - 15 standard priority tasks (priority=5)")

    # Verify custom fields are set
    sample_task = db.query(CustomTask).filter(CustomTask.priority == 10).first()
    print(f"\n📋 Sample task with custom fields:")
    print(f"  - ID: {sample_task.id}")
    print(f"  - Priority: {sample_task.priority}")
    print(f"  - Category: {sample_task.category}")
    print(f"  - User ID: {sample_task.user_id}")
    print(f"  - Metadata: {sample_task.metadata_json}")

    print(f"\n🚀 Starting worker with 4 threads...")

    # Start worker process
    proc = Process(
        target=run_worker_process,
        args=("postgresql://bq:@localhost/bq_test_custom",),
    )
    proc.start()

    # Wait for tasks to complete
    start_time = time.time()
    begin = datetime.datetime.now()

    while True:
        db.expire_all()
        completed = (
            db.query(CustomTask)
            .filter(CustomTask.state == bq.TaskState.DONE)
            .count()
        )

        if completed == len(tasks_created):
            break

        delta = datetime.datetime.now() - begin
        if delta.total_seconds() > 30:
            proc.kill()
            proc.join(3)
            raise TimeoutError(
                f"Timeout waiting for tasks. Only {completed}/{len(tasks_created)} completed"
            )
        time.sleep(0.5)

    elapsed = time.time() - start_time

    # Stop worker
    proc.kill()
    proc.join(3)

    # Check results
    print(f"\n📊 Verifying results...")

    completed_tasks = db.query(CustomTask).filter(
        CustomTask.state == bq.TaskState.DONE
    ).all()

    failed_tasks = db.query(CustomTask).filter(
        CustomTask.state == bq.TaskState.FAILED
    ).all()

    processing_tasks = db.query(CustomTask).filter(
        CustomTask.state == bq.TaskState.PROCESSING
    ).all()

    # Check custom events
    events = db.query(CustomEvent).all()

    print("\n" + "=" * 80)
    print("📊 TEST RESULTS")
    print("=" * 80)
    print(f"\n✓ Total Tasks: {len(tasks_created)}")
    print(f"✓ Completed: {len(completed_tasks)} ({len(completed_tasks)/len(tasks_created)*100:.1f}%)")
    print(f"✓ Failed: {len(failed_tasks)}")
    print(f"✓ Time Elapsed: {elapsed:.2f} seconds")
    print(f"✓ Throughput: {len(completed_tasks)/elapsed:.2f} tasks/second")

    print(f"\n📝 Events Generated: {len(events)}")

    # Verify results contain custom field data
    print(f"\n🔍 Verifying custom field data in results...")

    tasks_with_results = [t for t in completed_tasks if t.result]
    high_priority_completed = [
        t for t in completed_tasks
        if t.priority == 10
    ]
    standard_priority_completed = [
        t for t in completed_tasks
        if t.priority == 5
    ]

    print(f"  ✓ Tasks with results: {len(tasks_with_results)}/{len(completed_tasks)}")
    print(f"  ✓ High priority tasks completed: {len(high_priority_completed)}/10")
    print(f"  ✓ Standard priority tasks completed: {len(standard_priority_completed)}/15")

    # Sample some results
    if tasks_with_results:
        print(f"\n📋 Sample Results:")
        for i, task in enumerate(tasks_with_results[:3], 1):
            print(f"\n  Task {i}:")
            print(f"    Priority: {task.priority}")
            print(f"    Category: {task.category}")
            print(f"    User ID: {task.user_id}")
            print(f"    Result: {task.result}")

    # Data integrity checks
    print(f"\n🔍 Data Integrity Check:")
    if processing_tasks:
        print(f"  ⚠️  Warning: {len(processing_tasks)} tasks stuck in PROCESSING state")
    else:
        print(f"  ✓ No tasks stuck in PROCESSING state")

    # Verify custom fields preserved
    corrupted_tasks = [
        t for t in completed_tasks
        if t.priority is None or t.category is None
    ]
    if corrupted_tasks:
        print(f"  ⚠️  Warning: {len(corrupted_tasks)} tasks lost custom field data")
    else:
        print(f"  ✓ All custom fields preserved correctly")

    # Cleanup
    print(f"\n🧹 Cleaning up...")
    db.query(CustomEvent).delete()
    db.query(CustomTask).delete()
    db.commit()
    db.close()
    Base.metadata.drop_all(bind=engine)

    print("\n" + "=" * 80)

    # Determine success
    success = (
        len(completed_tasks) == len(tasks_created) and
        len(failed_tasks) == 0 and
        len(processing_tasks) == 0 and
        len(corrupted_tasks) == 0 and
        len(tasks_with_results) == len(completed_tasks)
    )

    if success:
        print("✅ TEST PASSED")
        print("=" * 80)
        print("\n🎉 SUCCESS: Thread executor works correctly with custom models!")
        return True
    else:
        print("❌ TEST FAILED")
        print("=" * 80)
        return False


if __name__ == "__main__":
    try:
        result = run_custom_model_test()
        exit(0 if result else 1)
    except Exception as e:
        print(f"\n💥 ERROR: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
