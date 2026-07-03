"""Custom model processor definitions for testing model configuration under threading.

Uses the default Task/Worker/Event models (as tape does via dotted-path config),
exercising the TASK_MODEL/WORKER_MODEL/EVENT_MODEL config paths and proving
NOTIFY-on-insert fires from worker-thread sessions.
"""
import bq

app = bq.BeanQueue()


@app.processor(channel="custom")
def custom_processor(task, value: int):
    """Normal processor for custom models."""
    return value * 10


@app.processor(channel="custom")
def custom_child_processor(task, value: int):
    """Processor that demonstrates child task creation."""
    return value * 10
