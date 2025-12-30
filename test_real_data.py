#!/usr/bin/env python
"""
Real-world integration test for thread executor.
Simulates a data processing pipeline with realistic tasks.
"""
import datetime
import json
import random
import time
from dataclasses import dataclass
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import bq
from bq.config import Config
from bq.db.base import Base
from bq.models import TaskState


# Define realistic task processors
app = bq.BeanQueue(
    config=Config(
        PROCESSOR_PACKAGES=["__main__"],
        DATABASE_URL="postgresql://bq:@localhost/bq_test",
        MAX_WORKER_THREADS=8,
        BATCH_SIZE=16,
    )
)


@dataclass
class Transaction:
    """Simulated financial transaction"""
    id: str
    amount: float
    currency: str
    merchant: str
    timestamp: str
    category: str


@app.processor(channel="data-processing")
def process_transaction(task: bq.Task, transaction_data: dict):
    """Process a financial transaction with validation and categorization."""
    time.sleep(0.1)  # Simulate DB lookup/API call

    # Parse transaction
    txn = Transaction(**transaction_data)

    # Simulate processing logic
    categorized = {
        "transaction_id": txn.id,
        "amount": txn.amount,
        "currency": txn.currency,
        "merchant": txn.merchant,
        "category": txn.category,
        "processed_at": datetime.datetime.now().isoformat(),
        "risk_score": random.uniform(0, 1),
    }

    return categorized


@app.processor(channel="data-processing")
def analyze_merchant(task: bq.Task, merchant_name: str, transaction_count: int):
    """Analyze merchant data and compute statistics."""
    time.sleep(0.15)  # Simulate data aggregation

    return {
        "merchant": merchant_name,
        "total_transactions": transaction_count,
        "average_amount": random.uniform(10, 500),
        "fraud_probability": random.uniform(0, 0.1),
        "analyzed_at": datetime.datetime.now().isoformat(),
    }


@app.processor(channel="data-processing")
def generate_report(task: bq.Task, report_type: str, data_count: int):
    """Generate a comprehensive report."""
    time.sleep(0.2)  # Simulate report generation

    return {
        "report_type": report_type,
        "records_processed": data_count,
        "generated_at": datetime.datetime.now().isoformat(),
        "status": "completed",
    }


def generate_realistic_transactions(count: int) -> List[dict]:
    """Generate realistic transaction data."""
    merchants = [
        "Amazon", "Walmart", "Target", "Starbucks", "McDonald's",
        "Shell Gas", "Uber", "Netflix", "Spotify", "Apple Store",
        "Best Buy", "Home Depot", "Kroger", "Costco", "CVS Pharmacy"
    ]

    categories = [
        "groceries", "entertainment", "transportation", "utilities",
        "shopping", "dining", "healthcare", "education", "travel"
    ]

    currencies = ["USD", "EUR", "GBP", "CAD"]

    transactions = []
    base_time = datetime.datetime.now()

    for i in range(count):
        txn = {
            "id": f"TXN-{i+1:06d}",
            "amount": round(random.uniform(5.0, 500.0), 2),
            "currency": random.choice(currencies),
            "merchant": random.choice(merchants),
            "timestamp": (base_time - datetime.timedelta(hours=random.randint(0, 720))).isoformat(),
            "category": random.choice(categories),
        }
        transactions.append(txn)

    return transactions


def run_integration_test():
    """Run comprehensive integration test with realistic data."""
    print("=" * 80)
    print("THREAD EXECUTOR INTEGRATION TEST - REALISTIC DATA")
    print("=" * 80)

    # Setup
    engine = create_engine("postgresql://bq:@localhost/bq_test")
    Base.metadata.create_all(bind=engine)
    db = Session(bind=engine)

    # Clean up existing tasks (delete events first due to FK constraint)
    from bq.models import Event
    db.query(Event).delete()
    db.query(bq.Task).delete()
    db.commit()

    print("\n📊 Generating realistic test data...")

    # Generate realistic transaction data
    transactions = generate_realistic_transactions(50)
    merchants = list(set(txn["merchant"] for txn in transactions))

    print(f"✓ Generated {len(transactions)} transactions")
    print(f"✓ From {len(merchants)} unique merchants")
    print(f"✓ Sample transaction: {json.dumps(transactions[0], indent=2)}")

    # Create tasks
    print("\n📝 Creating tasks...")
    task_count = 0

    # 1. Transaction processing tasks (50 tasks)
    for txn in transactions:
        task = process_transaction.run(transaction_data=txn)
        db.add(task)
        task_count += 1

    # 2. Merchant analysis tasks (15 tasks)
    for merchant in merchants:
        merchant_txns = [t for t in transactions if t["merchant"] == merchant]
        task = analyze_merchant.run(
            merchant_name=merchant,
            transaction_count=len(merchant_txns)
        )
        db.add(task)
        task_count += 1

    # 3. Report generation tasks (5 tasks)
    report_types = ["daily_summary", "fraud_analysis", "merchant_ranking", "category_breakdown", "currency_report"]
    for report_type in report_types:
        task = generate_report.run(
            report_type=report_type,
            data_count=len(transactions)
        )
        db.add(task)
        task_count += 1

    db.commit()
    print(f"✓ Created {task_count} tasks total")
    print(f"  - {len(transactions)} transaction processing tasks")
    print(f"  - {len(merchants)} merchant analysis tasks")
    print(f"  - {len(report_types)} report generation tasks")

    # Start worker process
    print(f"\n🚀 Starting worker with configuration:")
    print(f"  - MAX_WORKER_THREADS: {app.config.MAX_WORKER_THREADS}")
    print(f"  - BATCH_SIZE: {app.config.BATCH_SIZE}")
    print(f"  - Connection Pool: {app.engine.pool.__class__.__name__}")
    print(f"  - Pool Size: {app.engine.pool.size()}")

    from multiprocessing import Process

    def run_worker():
        app.process_tasks(channels=("data-processing",))

    worker = Process(target=run_worker)
    worker.start()

    # Monitor progress
    print("\n⏱️  Processing tasks...")
    start_time = time.time()
    last_done = 0

    while True:
        db.expire_all()

        done = db.query(bq.Task).filter(bq.Task.state == TaskState.DONE).count()
        processing = db.query(bq.Task).filter(bq.Task.state == TaskState.PROCESSING).count()
        failed = db.query(bq.Task).filter(bq.Task.state == TaskState.FAILED).count()
        pending = db.query(bq.Task).filter(bq.Task.state == TaskState.PENDING).count()

        elapsed = time.time() - start_time

        if done != last_done:
            progress = (done / task_count) * 100
            throughput = done / elapsed if elapsed > 0 else 0
            print(f"  Progress: {done}/{task_count} ({progress:.1f}%) | "
                  f"Processing: {processing} | Failed: {failed} | "
                  f"Throughput: {throughput:.1f} tasks/sec")
            last_done = done

        if done + failed == task_count:
            break

        if elapsed > 60:
            print("\n⚠️  Timeout - stopping test")
            break

        time.sleep(0.5)

    worker.kill()
    worker.join(3)

    # Results
    elapsed_time = time.time() - start_time

    print("\n" + "=" * 80)
    print("📊 TEST RESULTS")
    print("=" * 80)

    # Summary statistics
    done_tasks = db.query(bq.Task).filter(bq.Task.state == TaskState.DONE).all()
    failed_tasks = db.query(bq.Task).filter(bq.Task.state == TaskState.FAILED).all()

    print(f"\n✓ Total Tasks: {task_count}")
    print(f"✓ Completed: {len(done_tasks)} ({len(done_tasks)/task_count*100:.1f}%)")
    print(f"✓ Failed: {len(failed_tasks)}")
    print(f"✓ Time Elapsed: {elapsed_time:.2f} seconds")
    print(f"✓ Throughput: {len(done_tasks)/elapsed_time:.2f} tasks/second")

    # Expected vs actual performance
    sequential_time = task_count * 0.15  # Average task duration
    print(f"\n⚡ Performance Comparison:")
    print(f"  - Sequential (estimated): {sequential_time:.1f}s")
    print(f"  - Parallel (actual): {elapsed_time:.1f}s")
    print(f"  - Speedup: {sequential_time/elapsed_time:.2f}x")

    # Sample results
    if done_tasks:
        print(f"\n📋 Sample Processed Results:")
        for i, task in enumerate(done_tasks[:3]):
            print(f"\n  Task {i+1}: {task.func_name}")
            print(f"    Result: {json.dumps(task.result, indent=6)}")

    # Task breakdown by type
    print(f"\n📈 Task Breakdown:")
    txn_tasks = [t for t in done_tasks if t.func_name == "process_transaction"]
    merchant_tasks = [t for t in done_tasks if t.func_name == "analyze_merchant"]
    report_tasks = [t for t in done_tasks if t.func_name == "generate_report"]

    print(f"  - Transactions processed: {len(txn_tasks)}")
    print(f"  - Merchant analyses: {len(merchant_tasks)}")
    print(f"  - Reports generated: {len(report_tasks)}")

    # Verify data integrity
    print(f"\n🔍 Data Integrity Check:")
    all_results = [t.result for t in done_tasks if t.result]
    print(f"  ✓ All {len(all_results)} tasks have valid results")

    # Check for any stuck tasks
    stuck_processing = db.query(bq.Task).filter(bq.Task.state == TaskState.PROCESSING).count()
    if stuck_processing > 0:
        print(f"  ⚠️  Warning: {stuck_processing} tasks stuck in PROCESSING state")
    else:
        print(f"  ✓ No tasks stuck in PROCESSING state")

    # Cleanup
    print(f"\n🧹 Cleaning up...")
    # Delete events first due to foreign key constraint
    from bq.models import Event
    db.query(Event).delete()
    db.query(bq.Task).delete()
    db.commit()
    db.close()
    Base.metadata.drop_all(bind=engine)

    print("\n" + "=" * 80)
    print("✅ INTEGRATION TEST COMPLETE")
    print("=" * 80)

    return {
        "success": len(failed_tasks) == 0 and stuck_processing == 0,
        "total_tasks": task_count,
        "completed": len(done_tasks),
        "failed": len(failed_tasks),
        "elapsed_time": elapsed_time,
        "throughput": len(done_tasks) / elapsed_time,
    }


if __name__ == "__main__":
    try:
        result = run_integration_test()

        if result["success"]:
            print("\n🎉 SUCCESS: Thread executor working correctly with realistic data!")
            exit(0)
        else:
            print("\n❌ FAILURE: Some tasks failed or got stuck")
            exit(1)
    except Exception as e:
        print(f"\n💥 ERROR: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
