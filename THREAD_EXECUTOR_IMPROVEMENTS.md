# Thread Executor Improvements - Learning from Celery

## Current Implementation Analysis

### Architecture
- **Pattern**: Batch dispatch → Commit → Submit all → Wait for all
- **BATCH_SIZE**: Number of tasks fetched per iteration
- **MAX_WORKER_THREADS**: Concurrent processing threads
- **Issue**: If BATCH_SIZE > MAX_WORKER_THREADS, tasks wait unnecessarily

### Comparison with Celery

| Feature | Current BeanQueue | Celery Thread Pool | Should Implement? |
|---------|-------------------|-------------------|-------------------|
| Task feeding | Batch-and-wait | Continuous feeding | ✅ Yes |
| Prefetch multiplier | No (fixed BATCH_SIZE) | Yes (configurable) | ✅ Yes |
| Result tracking | DB only | In-memory futures | 🤔 Maybe |
| Pool stats | No | Yes (_get_info) | ✅ Yes |
| Acknowledgement | Implicit (PROCESSING) | Explicit (early/late) | ✅ Yes |
| Dynamic resize | No | Yes (autoscale) | ⏸️ Future |

## Improvement Proposals

### 1. **Continuous Task Feeding** (High Priority)

**Problem**: Current implementation fetches batch of tasks, waits for all to complete, then fetches next batch.

**Current Code**:
```python
while True:
    tasks = dispatch(..., limit=BATCH_SIZE).all()  # Fetch 10
    if executor:
        db.commit()
        futures = []
        for task in tasks:  # Submit all 10
            futures.append(executor.submit(...))
        for f in futures:  # Wait for all 10
            f.result()
    if not tasks:
        break
```

**Proposed Improvement**:
```python
# Keep executor queue full continuously
from collections import deque

running_futures = deque()
max_queued = MAX_WORKER_THREADS * 2  # Allow some queueing

while True:
    # Remove completed futures
    while running_futures and running_futures[0].done():
        try:
            running_futures.popleft().result()
        except Exception as e:
            logger.error("Task failed: %s", e)

    # Keep pool fed
    while len(running_futures) < max_queued:
        tasks = dispatch(..., limit=1).all()  # Fetch one at a time
        if not tasks:
            break
        db.commit()
        task = tasks[0]
        future = executor.submit(process_task, task.id)
        running_futures.append(future)

    # Wait briefly if queue is full
    if len(running_futures) >= max_queued:
        time.sleep(0.01)  # Prevent busy waiting
```

**Benefits**:
- Threads never starve
- Better resource utilization
- Faster overall throughput

### 2. **Add Prefetch Multiplier** (Medium Priority)

**Purpose**: Optimize for different task durations

**Configuration**:
```python
class Config(BaseSettings):
    MAX_WORKER_THREADS: int = 1
    BATCH_SIZE: int = 1  # Deprecated, use PREFETCH_MULTIPLIER

    # New config
    WORKER_PREFETCH_MULTIPLIER: int = 1

    @property
    def max_prefetch(self):
        """Maximum tasks to reserve at once"""
        if self.MAX_WORKER_THREADS == 1:
            return self.BATCH_SIZE  # Backward compat
        return self.MAX_WORKER_THREADS * self.WORKER_PREFETCH_MULTIPLIER
```

**Usage Guide**:
```python
# For long-running tasks (5-60 seconds each)
WORKER_PREFETCH_MULTIPLIER = 1  # Only reserve as many as can process
# With 4 threads: reserves max 4 tasks

# For short tasks (< 1 second each)
WORKER_PREFETCH_MULTIPLIER = 4  # Reserve more to reduce latency
# With 4 threads: reserves up to 16 tasks
```

### 3. **Add Late Acknowledgement Mode** (Low Priority)

**Current Behavior**: Tasks marked PROCESSING immediately on dispatch (early ACK)

**Problem**: If worker crashes after dispatch but before processing, tasks are stuck in PROCESSING state

**Proposed**: Add configuration for late acknowledgement

```python
class Config(BaseSettings):
    # When True: commit PROCESSING state only after task completes successfully
    # When False: commit PROCESSING state immediately (current behavior)
    TASK_ACKS_LATE: bool = False
```

**Implementation**:
```python
def _process_task_in_thread(self, task_id, registry):
    db = self.make_session()
    try:
        task = db.query(self.task_model).filter(...).one()

        if self.config.TASK_ACKS_LATE:
            # Task still in PENDING, set to PROCESSING now
            task.state = TaskState.PROCESSING
            db.commit()

        registry.process(task, event_cls=self.event_model)
        db.commit()
    except:
        if self.config.TASK_ACKS_LATE:
            # Reset to PENDING so another worker can try
            task.state = TaskState.PENDING
            db.commit()
        raise
```

**Tradeoff**:
- ✅ Better failure recovery
- ❌ Same task might be processed twice if crash happens mid-processing
- ❌ More database transactions

### 4. **Pool Statistics** (Low Priority)

**Celery Pattern**:
```python
def _get_info(self):
    return {
        'max-concurrency': self.limit,
        'threads': len(self.executor._threads),
    }
```

**BeanQueue Implementation**:
```python
def get_pool_stats(self):
    """Get thread pool statistics"""
    if not hasattr(self, '_executor') or self._executor is None:
        return {
            'mode': 'sequential',
            'max_workers': 1,
            'active_threads': 0,
        }

    return {
        'mode': 'threaded',
        'max_workers': self._executor._max_workers,
        'active_threads': len(self._executor._threads),
        'queue_size': self._executor._work_queue.qsize(),
    }
```

**Usage**: Expose via metrics endpoint or logging

### 5. **Optimize BATCH_SIZE Default** (High Priority)

**Current**: `BATCH_SIZE = 1` (safe but slow)

**Proposed**:
```python
class Config(BaseSettings):
    MAX_WORKER_THREADS: int = 1

    # Auto-calculate optimal batch size
    BATCH_SIZE: int = None  # None = auto

    @field_validator("BATCH_SIZE", mode="after")
    def set_batch_size_default(cls, v, info):
        if v is not None:
            return v
        # Default: match thread count for thread mode
        max_workers = info.data.get("MAX_WORKER_THREADS", 1)
        if max_workers > 1:
            return max_workers  # Fetch as many as can process
        return 1  # Sequential mode
```

## Implementation Priority

1. **Critical** (Do Now):
   - ✅ Already fixed: Commit after dispatch
   - 🔄 Optimize BATCH_SIZE default

2. **High** (Next):
   - Continuous task feeding
   - Prefetch multiplier

3. **Medium** (Soon):
   - Pool statistics/monitoring

4. **Low** (Future):
   - Late acknowledgement mode
   - Dynamic pool resizing (like Celery autoscale)

## Testing Strategy

### Benchmark Tests
```python
def test_throughput_short_tasks():
    """Test throughput with 1000 tasks of 10ms each"""
    # Compare: batch-and-wait vs continuous feeding

def test_resource_utilization():
    """Verify threads don't starve"""
    # Monitor: executor._threads length over time
    # Should stay at max_workers, not fluctuate

def test_latency_distribution():
    """Measure p50, p95, p99 latency"""
    # Lower is better
```

## Migration Path

1. **Phase 1** (Current): Basic thread pool ✅
2. **Phase 2**: Optimize BATCH_SIZE defaults
3. **Phase 3**: Continuous feeding pattern
4. **Phase 4**: Prefetch multiplier support
5. **Phase 5**: Advanced features (late ack, autoscale)

## Conclusion

Celery's thread pool is mature and battle-tested. Key learnings:
- **Continuous feeding** > Batch-and-wait
- **Prefetch multiplier** allows optimization
- **Explicit acknowledgement** gives control
- **Pool monitoring** enables observability

Our current implementation is **solid for v1** but can improve throughput with these patterns.
