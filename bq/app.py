import functools
import importlib
import logging
import os
import platform
import threading
import time
import typing
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as futures_wait
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version


import venusian
from sqlalchemy import func
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session as DBSession
from sqlalchemy.pool import QueuePool

from . import constants
from . import events
from . import models
from .config import Config
from .db.session import SessionMaker
from .metrics import MetricsServer
from .processors.processor import Processor
from .processors.processor import ProcessorHelper
from .processors.registry import collect
from .services.dispatch import DispatchService
from .services.worker import WorkerService
from .utils import load_module_var

logger = logging.getLogger(__name__)



class BeanQueue:
    def __init__(
        self,
        config: Config | None = None,
        session_cls: DBSession = SessionMaker,
        worker_service_cls: typing.Type[WorkerService] = WorkerService,
        dispatch_service_cls: typing.Type[DispatchService] = DispatchService,
        engine: Engine | None = None,
    ):
        self.config = config if config is not None else Config()
        self.session_cls = session_cls
        self.worker_service_cls = worker_service_cls
        self.dispatch_service_cls = dispatch_service_cls
        self._engine = engine
        self._worker_update_shutdown_event: threading.Event = threading.Event()
        self._metrics_server: MetricsServer | None = None
        self._channels: tuple[str, ...] = ()

    def _resolve_max_workers(self) -> int:
        """Resolve the effective max worker thread count."""
        if self.config.MAX_WORKER_THREADS > 0:
            return self.config.MAX_WORKER_THREADS
        return min(32, (os.cpu_count() or 1) + 4)

    def create_default_engine(self):
        pool_size = self._resolve_max_workers() + 5
        return create_engine(
            str(self.config.DATABASE_URL),
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=10,
        )

    def request_shutdown(self) -> None:
        """Ask a running process_tasks() loop to shut down gracefully."""
        self._worker_update_shutdown_event.set()
        # Wake the worker from a blocking poll() by sending a NOTIFY on a
        # listened channel. Uses a fresh connection to avoid thread-safety issues.
        channels = self._channels
        if channels:
            try:
                conn = self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
                try:
                    identifier_preparer = conn.dialect.identifier_preparer
                    quoted = identifier_preparer.quote_identifier(channels[0])
                    conn.exec_driver_sql(f"NOTIFY {quoted}")
                finally:
                    conn.close()
            except Exception:
                pass  # Best-effort; the event flag will be checked on next loop

    def make_session(self) -> DBSession:
        return self.session_cls(bind=self.engine)

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = self.create_default_engine()
        return self._engine

    # GoodTurn: https://goodturn.ai/p/gtp_01kwmpmvstfk69acm3pbvftpsv
    @property
    def _conn_error_types(self) -> tuple:
        """Errors signaling a dead notification connection. dispatch.poll()
        touches the raw driver connection, so its failures surface as DBAPI
        errors (e.g. psycopg2.OperationalError), never SQLAlchemy-wrapped."""
        dbapi = self.engine.dialect.dbapi
        if dbapi is None:
            return (OperationalError,)
        return (OperationalError, dbapi.OperationalError, dbapi.InterfaceError)

    @property
    def task_model(self) -> typing.Type[models.Task]:
        return load_module_var(self.config.TASK_MODEL)

    @property
    def worker_model(self) -> typing.Type[models.Worker]:
        return load_module_var(self.config.WORKER_MODEL)

    @property
    def event_model(self) -> typing.Type[models.Event] | None:
        if self.config.EVENT_MODEL is None:
            return
        return load_module_var(self.config.EVENT_MODEL)

    def _make_worker_service(self, session: DBSession):
        return self.worker_service_cls(
            session=session, task_model=self.task_model, worker_model=self.worker_model
        )

    def _make_dispatch_service(self, session: DBSession):
        return self.dispatch_service_cls(session=session, task_model=self.task_model)

    def processor(
        self,
        channel: str = constants.DEFAULT_CHANNEL,
        auto_complete: bool = True,
        retry_policy: typing.Callable | None = None,
        retry_exceptions: typing.Type | typing.Tuple[typing.Type, ...] | None = None,
        task_model: typing.Type | None = None,
    ) -> typing.Callable:
        def decorator(wrapped: typing.Callable):
            processor = Processor(
                module=wrapped.__module__,
                name=wrapped.__name__,
                channel=channel,
                func=wrapped,
                auto_complete=auto_complete,
                retry_policy=retry_policy,
                retry_exceptions=retry_exceptions,
            )
            helper_obj = ProcessorHelper(
                processor,
                task_cls=task_model if task_model is not None else self.task_model,
            )

            def callback(scanner: venusian.Scanner, name: str, ob: typing.Callable):
                if processor.name != name:
                    raise ValueError("Name is not the same")
                scanner.registry.add(processor)

            venusian.attach(
                helper_obj, callback, category=constants.BQ_PROCESSOR_CATEGORY
            )
            return helper_obj

        return decorator

    def update_workers(
        self,
        worker_id: typing.Any,
    ):
        db = self.make_session()

        worker_service = self._make_worker_service(db)
        dispatch_service = self._make_dispatch_service(db)

        try:
            current_worker = worker_service.get_worker(worker_id)
            logger.info(
                "Updating worker %s with heartbeat_period=%s, heartbeat_timeout=%s",
                current_worker.id,
                self.config.WORKER_HEARTBEAT_PERIOD,
                self.config.WORKER_HEARTBEAT_TIMEOUT,
            )
            consecutive_failures = 0
            while True:
                try:
                    dead_workers = worker_service.fetch_dead_workers(
                        timeout=self.config.WORKER_HEARTBEAT_TIMEOUT
                    )
                    task_count = worker_service.reschedule_dead_tasks(
                        # TODO: a better way to abstract this?
                        dead_workers.with_entities(current_worker.__class__.id)
                    )
                    found_dead_worker = False
                    for dead_worker in dead_workers:
                        found_dead_worker = True
                        logger.info(
                            "Found dead worker %s (name=%s), reschedule %s dead tasks in channels %s",
                            dead_worker.id,
                            dead_worker.name,
                            task_count,
                            dead_worker.channels,
                        )
                        dispatch_service.notify(dead_worker.channels)
                    if found_dead_worker:
                        db.commit()

                    if current_worker.state != models.WorkerState.RUNNING:
                        logger.warning(
                            "Current worker %s state is %s instead of running, quit processing",
                            current_worker.id,
                            current_worker.state,
                        )
                        self.request_shutdown()
                        return

                    consecutive_failures = 0
                except Exception:
                    logger.exception(
                        "Error in heartbeat loop for worker %s", worker_id
                    )
                    db.rollback()
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        logger.error(
                            "Heartbeat failed %d consecutive times, shutting down",
                            consecutive_failures,
                        )
                        self.request_shutdown()
                        return

                do_shutdown = self._worker_update_shutdown_event.wait(
                    self.config.WORKER_HEARTBEAT_PERIOD
                )
                if do_shutdown:
                    return

                try:
                    current_worker.last_heartbeat = func.now()
                    db.add(current_worker)
                    db.commit()
                except Exception:
                    logger.exception(
                        "Error updating heartbeat for worker %s", worker_id
                    )
                    db.rollback()
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        logger.error(
                            "Heartbeat failed %d consecutive times, shutting down",
                            consecutive_failures,
                        )
                        self.request_shutdown()
                        return
        finally:
            # GoodTurn: https://goodturn.ai/p/gtp_01kwkn1qf1espsjgrq5rbwsnjm
            db.close()

    def _open_notification_conn(self, dispatch_service, channels):
        # GoodTurn: https://goodturn.ai/p/gtp_01kwkhy0e6ezrscakp0j6p552j
        """Open a dedicated AUTOCOMMIT connection for LISTEN/NOTIFY."""
        conn = self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        dispatch_service.listen(channels, connection=conn)
        return conn

    def _drain_notifications(self, notification_conn) -> bool:
        """Non-blocking check for pending notifications. Returns True if any found."""
        try:
            driver_conn = notification_conn.connection.driver_connection
            driver_conn.poll()
            had = bool(driver_conn.notifies)
            driver_conn.notifies.clear()
            return had
        except Exception:
            logger.warning("Error draining notifications, will reconnect", exc_info=True)
            return False
    def _process_task_in_thread(
        self,
        task_id: typing.Any,
        registry: typing.Any,
    ):
        """Process a single task in a thread-safe manner with its own database session."""
        db = self.make_session()
        try:
            from sqlalchemy.orm.exc import NoResultFound
            try:
                task = db.query(self.task_model).filter(self.task_model.id == task_id).one()
            except NoResultFound:
                logger.warning("Task %s not found (deleted?), skipping", task_id)
                return

            logger.info(
                "Processing task %s, channel=%s, module=%s, func=%s",
                task.id,
                task.channel,
                task.module,
                task.func_name,
            )
            registry.process(task, event_cls=self.event_model)
            db.commit()
        except Exception as e:
            logger.exception("Error processing task %s: %s", task_id, e)
            db.rollback()
            # Attempt to reset task to PENDING so it can be retried
            try:
                from sqlalchemy import update
                from .models import TaskState
                reset_session = self.make_session()
                try:
                    reset_session.execute(
                        update(self.task_model)
                        .where(
                            self.task_model.id == task_id,
                            self.task_model.state == TaskState.PROCESSING,
                        )
                        .values(state=TaskState.PENDING, worker_id=None)
                    )
                    reset_session.commit()
                    logger.info("Reset stuck task %s to PENDING", task_id)
                except Exception:
                    logger.exception("Failed to reset task %s", task_id)
                    reset_session.rollback()
                finally:
                    reset_session.close()
            except Exception:
                logger.exception("Failed to create reset session for task %s", task_id)
            raise
        finally:
            db.close()

    def _process_tasks_sequential(
        self,
        db: DBSession,
        dispatch_service: DispatchService,
        registry: typing.Any,
        channels: tuple[str, ...],
        worker_id: typing.Any,
        notification_conn=None,
    ):
        """Process tasks sequentially (original behavior for MAX_WORKER_THREADS=1)."""
        while True:
            if self._worker_update_shutdown_event.is_set():
                raise SystemExit

            while True:
                tasks = dispatch_service.dispatch(
                    channels,
                    worker_id=worker_id,
                    limit=self.config.BATCH_SIZE,
                ).all()

                for task in tasks:
                    logger.info(
                        "Processing task %s, channel=%s, module=%s, func=%s",
                        task.id,
                        task.channel,
                        task.module,
                        task.func_name,
                    )
                    registry.process(task, event_cls=self.event_model)
                if tasks:
                    db.commit()

                if not tasks:
                    break

            db.close()
            try:
                for notification in dispatch_service.poll(
                    timeout=self.config.POLL_TIMEOUT,
                    connection=notification_conn,
                ):
                    logger.debug("Receive notification %s", notification)
            except TimeoutError:
                logger.debug("Poll timeout, try again")
                continue
            except self._conn_error_types:
                logger.warning("Notification connection lost, reconnecting", exc_info=True)
                try:
                    notification_conn.close()
                except Exception:
                    pass
                notification_conn = self._open_notification_conn(dispatch_service, channels)
                continue

    def _process_tasks_threaded(
        self,
        db: DBSession,
        executor: ThreadPoolExecutor,
        dispatch_service: DispatchService,
        registry: typing.Any,
        channels: tuple[str, ...],
        worker_id: typing.Any,
        notification_conn=None,
        max_workers: int = 4,
    ):
        """Process tasks using thread pool with notification-driven dispatch.

        Dispatches new tasks only when: a thread finishes, a NOTIFY arrives,
        the pool goes idle, or POLL_TIMEOUT elapses (scheduled_at resync).
        """
        running: set = set()
        freed = True  # force initial dispatch

        while True:
            if self._worker_update_shutdown_event.is_set():
                raise SystemExit

            if running:
                done, running = futures_wait(running, timeout=0.1, return_when=FIRST_COMPLETED)
                for f in done:
                    try:
                        f.result()
                    except Exception:
                        logger.exception("Task processing failed")
                freed = freed or bool(done)

            # Non-blocking notification check
            notified = self._drain_notifications(notification_conn)

            capacity = max_workers - len(running)
            if capacity > 0 and (freed or notified or not running
                                 or time.monotonic() - getattr(self, '_last_dispatch', 0) >= self.config.POLL_TIMEOUT):
                task_ids = [t.id for t in dispatch_service.dispatch(
                    channels, worker_id=worker_id,
                    limit=min(capacity, self.config.BATCH_SIZE),
                ).all()]
                db.commit()
                self._last_dispatch = time.monotonic()
                freed = False
                for tid in task_ids:
                    running.add(executor.submit(self._process_task_in_thread, tid, registry))
                if task_ids:
                    freed = True
                    continue  # fill remaining capacity immediately

            if not running:
                db.close()
                try:
                    for n in dispatch_service.poll(
                        timeout=self.config.POLL_TIMEOUT,
                        connection=notification_conn,
                    ):
                        logger.debug("Receive notification %s", n)
                    freed = True
                except TimeoutError:
                    freed = True  # periodic re-dispatch for scheduled_at resync
                except self._conn_error_types:
                    logger.warning("Notification connection lost, reconnecting", exc_info=True)
                    try:
                        notification_conn.close()
                    except Exception:
                        pass
                    notification_conn = self._open_notification_conn(dispatch_service, channels)
                    freed = True

    def process_tasks(
        self,
        channels: tuple[str, ...],
    ):
        # Clear shutdown event so a BeanQueue instance can be reused (e.g. tests)
        self._worker_update_shutdown_event.clear()

        try:
            bq_version = version("beanqueue")
        except PackageNotFoundError:
            bq_version = "unknown"

        logger.info(
            "Starting processing tasks, bq_version=%s",
            bq_version,
        )
        db = self.make_session()
        if not channels:
            channels = [constants.DEFAULT_CHANNEL]
        self._channels = tuple(channels)

        if not self.config.PROCESSOR_PACKAGES:
            logger.error("No PROCESSOR_PACKAGES provided")
            raise ValueError("No PROCESSOR_PACKAGES provided")

        logger.info("Scanning packages %s", self.config.PROCESSOR_PACKAGES)
        pkgs = list(map(importlib.import_module, self.config.PROCESSOR_PACKAGES))
        registry = collect(pkgs)
        for channel, module_processors in registry.processors.items():
            logger.info("Collected processors with channel %r", channel)
            for module, func_processors in module_processors.items():
                for processor in func_processors.values():
                    logger.info(
                        "  Processor module=%r, name=%r", module, processor.name
                    )

        dispatch_service = self.dispatch_service_cls(
            session=db, task_model=self.task_model
        )
        work_service = self.worker_service_cls(
            session=db, task_model=self.task_model, worker_model=self.worker_model
        )

        worker = work_service.make_worker(name=platform.node(), channels=channels)
        db.add(worker)
        db.commit()

        # Open dedicated notification connection (fixes LISTEN-lost-with-QueuePool)
        notification_conn = self._open_notification_conn(dispatch_service, channels)

        if self.config.METRICS_HTTP_SERVER_ENABLED:
            self._metrics_server = MetricsServer(self, worker.id)
            self._metrics_server.start()

        logger.info("Created worker %s, name=%s", worker.id, worker.name)
        events.worker_init.send(self, worker=worker)

        logger.info("Processing tasks in channels = %s ...", channels)
        worker_update_thread = threading.Thread(
            target=functools.partial(
                self.update_workers,
                worker_id=worker.id,
            ),
            name="update_workers",
        )
        worker_update_thread.daemon = True
        worker_update_thread.start()

        worker_id = worker.id

        # Resolve thread count via single source
        resolved_workers = self._resolve_max_workers()

        # Create thread pool executor for concurrent task processing
        executor = None
        if resolved_workers != 1:
            executor = ThreadPoolExecutor(
                max_workers=resolved_workers,
                thread_name_prefix="task_worker",
            )
            logger.info("Created thread pool executor with max_workers=%s", resolved_workers)

        try:
            if executor is not None:
                self._process_tasks_threaded(
                    db=db,
                    executor=executor,
                    dispatch_service=dispatch_service,
                    registry=registry,
                    channels=channels,
                    worker_id=worker_id,
                    notification_conn=notification_conn,
                    max_workers=resolved_workers,
                )
            else:
                self._process_tasks_sequential(
                    db=db,
                    dispatch_service=dispatch_service,
                    registry=registry,
                    channels=channels,
                    worker_id=worker_id,
                    notification_conn=notification_conn,
                )
        except (SystemExit, KeyboardInterrupt):
            db.rollback()
            logger.info("Shutting down ...")

            # Shutdown the executor if it was created
            if executor is not None:
                logger.info("Shutting down thread pool executor...")
                executor.shutdown(wait=True, cancel_futures=False)
                logger.info("Thread pool executor shutdown complete")

            self._worker_update_shutdown_event.set()
            worker_update_thread.join(5)
            if self._metrics_server is not None:
                self._metrics_server.shutdown()

        # Best-effort close of notification connection
        try:
            notification_conn.close()
        except Exception:
            pass

        worker.state = models.WorkerState.SHUTDOWN
        db.add(worker)
        task_count = work_service.reschedule_dead_tasks([worker.id])
        logger.info("Reschedule %s tasks", task_count)
        dispatch_service.notify(channels)
        db.commit()

        logger.info("Shutdown gracefully")
