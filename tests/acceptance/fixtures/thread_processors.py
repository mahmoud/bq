import time

import bq

app = bq.BeanQueue()


@app.processor(channel="thread-tests")
def slow_task(task: bq.Task, task_num: int, sleep_time: float):
    """A slow task that simulates I/O-bound work."""
    time.sleep(sleep_time)
    return task_num * 2


@app.processor(channel="thread-tests")
def concurrent_task(task: bq.Task, value: int):
    """A task to test concurrent execution and session isolation."""
    # Simulate some work
    time.sleep(0.1)
    result = value ** 2
    return result
