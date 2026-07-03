---
id: 2026-07-03-2aa24c3c0f
title: "bq multithreading hardening + test suite"
status: draft
workstream: ""
repo: bq
created: 2026-07-03
updated: 2026-07-03
tabled_note: ""
---
# bq multithreading hardening + test suite

## Context

The fork (`mahmoud/bq`) already carries a first-cut threaded executor (`MAX_WORKER_THREADS` + `ThreadPoolExecutor` in `bq/app.py`, merged as fork PR #7 and since released upstream as beanqueue 1.2.0), and finfam's tape module runs it in production (`MAX_WORKER_THREADS=3, BATCH_SIZE=3, POLL_TIMEOUT=15`, custom engine, custom models, git-pinned to branch `healthcheck-reliability`). This project productionizes that threading: sync the fork with upstream, merge contributor PR #1 (reviewed below — worth keeping), fix four real concurrency bugs found in review, add a graceful in-process shutdown API, and restructure the test suite around the concurrency contracts downstream actually relies on.

Deliverable: branch `threading-hardening` + a PR in `mahmoud/bq` **based on `healthcheck-reliability`** (per user decision), leaving fsrv's git pin working unchanged.

## Findings (grounded, from this session's review)

### Repo/PR landscape
- Fork `mahmoud/bq` of `LaunchPlatform/bq`. Fork `master` = threaded worker merged.
- **Upstream is 3 commits ahead of fork master** (fork is 0 ahead): `014f7c7` "Remove not needed AI gen stuff" (deletes root-level `THREAD_EXECUTOR_IMPROVEMENTS.md`, `test_custom_fields_summary.md`, `test_custom_models.py`, `test_real_data.py`), `425ad53` "bump ver" (pyproject → **1.2.0**, `requires-python >=3.11,<4`, psycopg2-binary >=2.9.10), `548a88c` "Update pkg ver" (uv.lock). Upstream master otherwise equals fork master — sync is a pure fast-forward.
- `origin/healthcheck-reliability` = fork master + `e86b5d6` = upstream **PR #8** ("Threaded healthcheck server + take db off the critical path"): `ThreadingWSGIServer` (socketserver.ThreadingMixIn, daemon_threads), in-memory `_health_ok`/`_health_info` written by the heartbeat thread, `/healthz` no longer touches the DB, adds `tests/unit/test_healthcheck.py`.
- Fork **PR #1** (Fazel94, head `dev/wsgiref-fix`, base `healthcheck-reliability`, MERGEABLE) — see review below.
- fsrv pins `beanqueue = { git = "https://github.com/mahmoud/bq", branch = "healthcheck-reliability" }`.

### PR #1 critical review (verdict: merge, with two follow-up fixes on our branch)
Correct and valuable:
- **Atomic `_health_state: tuple[bool, dict]`** replacing the two separate attrs: real fix. CPython attribute stores are atomic, but PR #8 reads TWO attributes in the HTTP handler thread while the heartbeat thread writes them — a reader can pair a new `_health_ok` with a stale `_health_info`. Single tuple swap removes the torn read.
- **Unhealthy during graceful shutdown** (`_health_state = (False, {})` in the shutdown handler): correct — orchestrators must stop routing while the worker drains. PR #8 lacked it.
- **Removes PR #8's redundant duplicate `_health_ok = False` write**: correct cleanup.
- **Real `Config` in the test fixture** instead of `mock.patch("bq.app.Config")`: strictly better; the mock patched a path `BeanQueue(config=...)` never uses.
- Stray-file deletion: right call; now redundant (upstream `014f7c7` deleted the same four files) — resolves as both-deleted in the merge, harmless.

Weaknesses — fix on our branch, not blockers:
1. `(False, {})` drops the `state` key, making the `/healthz` error body shape inconsistent (every other writer includes `state`). Fix: `(False, {"state": "SHUTDOWN"})`.
2. Unaddressed (pre-existing from PR #8): if `update_workers` dies from an unhandled exception (transient DB error), health freezes at its last value — possibly healthy-forever — while heartbeats stop, so peers steal tasks from a still-processing worker. Fixed by Step 2 below.

### Bugs in the current threaded implementation (bq/app.py)
1. **LISTEN lost with QueuePool (threaded mode)** — `process_tasks` runs `dispatch_service.listen(channels)` on the main session's connection, then `db.commit()` returns that connection to the QueuePool. Worker-thread sessions cycle connections through the same pool, so when `_process_tasks_threaded` finally calls `dispatch_service.poll()` (only when idle), `session.connection()` may check out a connection that never executed LISTEN (and PG doesn't deliver NOTIFYs to in-transaction listeners regardless). Net: NOTIFY wakeup silently degrades to `POLL_TIMEOUT` polling in threaded mode. Sequential mode survives only because `SingletonThreadPool` pins one connection per thread. This is why tape runs `POLL_TIMEOUT=15`.
2. **Heartbeat thread `sys.exit(0)` is a no-op for the process** — `update_workers` runs in a daemon thread; when the worker row's state is no longer `RUNNING` it calls `sys.exit(0)`, which raises SystemExit *in that thread only*. The main loop keeps dispatching while peers see this worker as dead and resteal its PROCESSING tasks → duplicate execution.
3. **`MAX_WORKER_THREADS=0` (auto) resolved inconsistently** — `create_default_engine` assumes 10, `_process_tasks_threaded` assumes 10, but `process_tasks` passes `max_workers=None` to `ThreadPoolExecutor` → stdlib default `min(32, cpu+4)`. On a 12-core box: 16 threads sharing a pool sized 15 with capacity accounting for 10.
4. **Dispatch churn while busy** — `_process_tasks_threaded` re-runs the dispatch UPDATE…SKIP LOCKED round trip every 50 ms whenever any future is running, even when nothing new exists (~20 useless UPDATEs/sec/worker).

Minor: after `db.commit()` in the threaded loop, each `task.id` access re-SELECTs the expired instance; infra errors in `_process_task_in_thread` (reload/commit failure) leave tasks stuck PROCESSING while this worker lives (only dead-worker rescheduling would recover them).

### Downstream (finfam tape) compat contract — MUST NOT break
Keep public and unchanged: `BeanQueue(config=, engine=)`, `.processor(channel=, retry_policy=, retry_exceptions=, auto_complete=)`, `.process_tasks(channels=<tuple>)` (kw name `channels` is used), `helper.run(**kwargs)`, `helper._processor` (semi-private, used by tape tests), `bq.TaskState/Task/Config`, `bq.models.WorkerState`, all 8 model mixins and their attribute names, **enum string VALUES** (`PENDING/PROCESSING/DONE/FAILED`, `RUNNING/SHUTDOWN`, `COMPLETE/FAILED_RETRY_SCHEDULED` — tape monitoring queries them as string literals), `bq.models.task.listen_events`, `bq.utils.load_module_var`, `bq.events.{task_failure, worker_init}`, `bq.processors.registry.collect` + `Registry.process(task, event_cls=)` (tape's test harness runs processors inline on arbitrary sessions), `bq.processors.retry_policies.{LimitAttempt, ExponentialBackoffRetry, DelayRetry}`, `DispatchService(session=, task_model=)` / `WorkerService(session=, task_model=, worker_model=)` ctor kwargs and method names (`listen, dispatch, poll, notify, make_worker, reschedule_dead_tasks`), `BeanQueue.update_workers(worker_id=)`, `run_metrics_http_server(worker_id)`, attr name `_worker_update_shutdown_event` (referenced by tape's copied loop in `_sentry_core.py`), logger namespace `"bq"`. Tape passes its **own engine** (QueuePool size 5) — `create_default_engine` changes never affect it.

### Test suite today
- `tests/unit/test_thread_executor.py` + `test_thread_executor_advanced.py`: ~20 tests, mostly pool-shape/config plumbing asserts with heavy duplication (LLM-generated; pool class ×4, pool-size arithmetic ×4 across both files).
- `tests/unit/{services,processors}`: solid behavior tests — dispatch (SKIP LOCKED, scheduled_at), worker service (dead workers, reschedule), processor semantics (kwargs injection, auto_complete, savepoint rollback, retry policies), registry. Keep untouched.
- `tests/acceptance/`: 5 files ≈ 21 tests, all spawning `multiprocessing.Process` workers against real PG. ≥7 tests re-prove "threads overlap in time" with sleep-based timing across `test_thread_executor.py` (4 variants), `test_realistic_integration.py` (3 variants), `test_batch_and_wait_issue.py` (same starvation property at 3 parameter points). Genuinely distinct: failure handling, retry-under-threads, threads>tasks, tasks>threads, state transitions, session isolation, sequential back-compat, CLI E2E. Untested entirely: graceful shutdown, NOTIFY wakeup latency, healthcheck E2E, custom models under threads, heartbeat death handling.
- Infra: `tests/conftest.py` (`db` fixture: scoped session, create_all/drop_all per test, `TEST_DB_URL` env, default `postgresql://bq:@localhost/bq_test`), `docker-compose.yaml` PG 16.3 with `bq_test` init, CircleCI runs `uv run python -m pytest ./tests -svvvv` with a PG 16.2 sidecar. `pytest` only present transitively via `pytest-factoryboy`.

## Approach

### Step 0 — Fork sync, PR merges, branch setup (prerequisite; pure git/gh work)
1. `git remote add upstream https://github.com/LaunchPlatform/bq.git && git fetch upstream`.
2. Fast-forward fork master: `git checkout master && git merge --ff-only upstream/master && git push origin master`. (Verified fast-forwardable: fork is 0 ahead.)
3. Refresh the healthcheck branch: `git checkout healthcheck-reliability` (track origin), `git merge master` (brings the 3 upstream commits under `e86b5d6`; only touched files are the 4 deletions + pyproject/uv.lock — no overlap with app.py → clean), push. This also refreshes upstream PR #8.
4. Merge Fazel's PR #1: `gh pr merge 1 -R mahmoud/bq --merge` (base is `healthcheck-reliability`). The four file deletions resolve as both-deleted. If GitHub reports conflicts after step 3, `gh pr checkout 1`, merge `healthcheck-reliability` into it resolving trivially, push, then merge the PR.
5. `git checkout -b threading-hardening healthcheck-reliability`. All following steps land here. Final delivery: push and `gh pr create -R mahmoud/bq --base healthcheck-reliability --head threading-hardening`.

### Step 1 — Dedicated notification connection (fixes bug 1)
In `bq/services/dispatch.py`:
- Add an optional per-call kwarg to the two receive-side methods: `listen(self, channels, connection: Connection | None = None)` and `poll(self, timeout: int = 5, connection: Connection | None = None)`. Both use `connection if connection is not None else self.session.connection()`. `notify()` stays session-bound (senders want transactional NOTIFY). No ctor change → existing unit tests and tape's copied loops keep working.

In `bq/app.py`:
- New helper on `BeanQueue`:
  ```python
  def _open_notification_conn(self, dispatch_service, channels) -> Connection:
      conn = self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
      dispatch_service.listen(channels, connection=conn)
      return conn
  ```
  `process_tasks` calls it once (replacing the current `dispatch_service.listen(channels)` on the main session — delete that line; keep the `db.commit()` that follows worker creation) and passes the connection into both loops; both loops call `dispatch_service.poll(timeout=..., connection=notification_conn)`.
- Reconnect handling: wrap poll/drain call sites in `except OperationalError` (`sqlalchemy.exc.OperationalError` — psycopg2 errors surface wrapped when going through `exec_driver_sql`, but raw via `driver_connection.poll()`, so also catch `psycopg2.OperationalError`; import guardedly or catch `(OperationalError, Exception subclass psycopg2.OperationalError)` via `self.engine.dialect.dbapi.OperationalError` to avoid a hard psycopg2 import) → log warning, best-effort `conn.close()`, reopen via `_open_notification_conn`, continue the loop. No retry cap — a queue worker should keep trying; the dispatch step retries independently on its own session.
- On shutdown (existing `except (SystemExit, KeyboardInterrupt)` handler): best-effort `notification_conn.close()`.
- Sequential loop uses the same dedicated connection. Keep its existing `db.close()` before poll (still avoids idle-in-transaction on the main session).

Edge handling: PG restart while idle → `select.select` wakes, `driver_conn.poll()` raises OperationalError → reconnect path above. Channel-name quoting already handled by `identifier_preparer.quote_identifier` (covered by existing `test_listen_value_quote`).

### Step 2 — Unified shutdown signal + public `request_shutdown()` (fixes bug 2 + PR #1 follow-ups; enables in-process tests)
In `bq/app.py`:
- Reuse the existing `self._worker_update_shutdown_event` as the single shutdown event (attribute name kept — tape's `_sentry_core.py` copy references it). Clear it at the top of `process_tasks` so one BeanQueue can run process_tasks repeatedly (tests).
- Add:
  ```python
  def request_shutdown(self) -> None:
      """Ask a running process_tasks() loop to shut down gracefully."""
      self._worker_update_shutdown_event.set()
  ```
- `update_workers`:
  - Replace `sys.exit(0)` with `self.request_shutdown(); return` (keep the PR #8/#1 `_health_state` unhealthy write before it). Remove the `import sys` if now unused.
  - Wrap the loop body in `try/except Exception`: on failure `logger.exception(...)`, increment a consecutive-failure counter (reset to 0 on success); after **3 consecutive** failures set `self._health_state = (False, {"state": "HEARTBEAT_ERROR"})`, `self.request_shutdown()`, `return`. Between attempts the existing `event.wait(WORKER_HEARTBEAT_PERIOD)` provides backoff. Also `db.rollback()` in the except before continuing (session may hold a failed transaction).
- `_process_tasks_sequential` and `_process_tasks_threaded`: at the top of each outer-loop iteration, `if self._worker_update_shutdown_event.is_set(): raise SystemExit` — lands in the existing graceful-shutdown handler (executor drain → reschedule → `WorkerState.SHUTDOWN` → notify → commit). Worst-case reaction latency = one `POLL_TIMEOUT` (poll blocks); acceptable, documented in README step.
- PR #1 follow-up: in the shutdown handler change `_health_state = (False, {})` → `(False, {"state": "SHUTDOWN"})`.

### Step 3 — Single-source auto thread count (fixes bug 3)
In `bq/app.py`:
- `def _resolve_max_workers(self) -> int:` → `self.config.MAX_WORKER_THREADS` when `> 0`, else `min(32, (os.cpu_count() or 1) + 4)` (mirrors ThreadPoolExecutor's default; add `import os`).
- Use it in `create_default_engine` (pool sizing formula unchanged: resolved + 5, max_overflow=10), in `process_tasks` (`ThreadPoolExecutor(max_workers=resolved, thread_name_prefix="task_worker")` — never pass None), and pass the resolved value into `_process_tasks_threaded` (delete its local `max_workers == 0 → 10` fallback).
- `bq/config.py`: `MAX_WORKER_THREADS: int = Field(default=1, ge=0)`, `BATCH_SIZE: int = Field(default=1, ge=1)`; fix the stale comment "number of CPUs * 5" → "min(32, cpu_count + 4), matching ThreadPoolExecutor".

### Step 4 — Threaded loop rework (fixes bug 4 + minor issues)
Rewrite `_process_tasks_threaded` (keep name; params: `db, executor, dispatch_service, registry, channels, worker_id, notification_conn, max_workers`):
```python
running: set[Future] = set()
last_dispatch = time.monotonic()
freed = True                      # force initial dispatch
while True:
    if self._worker_update_shutdown_event.is_set(): raise SystemExit
    if running:
        done, running = futures_wait(running, timeout=0.1, return_when=FIRST_COMPLETED)
        for f in done:
            try: f.result()
            except Exception: logger.exception("Task processing failed")
        freed = freed or bool(done)
    notified = self._drain_notifications(notification_conn)   # non-blocking socket check, no DB query
    capacity = max_workers - len(running)
    if capacity > 0 and (freed or notified or not running
                         or time.monotonic() - last_dispatch >= self.config.POLL_TIMEOUT):
        task_ids = [t.id for t in dispatch_service.dispatch(
            channels, worker_id=worker_id, limit=min(capacity, self.config.BATCH_SIZE)).all()]
        db.commit()               # ids captured BEFORE commit → no per-task refresh SELECT
        last_dispatch = time.monotonic(); freed = False
        for tid in task_ids:
            running.add(executor.submit(self._process_task_in_thread, tid, registry))
        if task_ids: freed = True; continue   # fill remaining capacity immediately
    if not running:
        db.close()
        try:
            for n in dispatch_service.poll(timeout=self.config.POLL_TIMEOUT, connection=notification_conn):
                logger.debug("Receive notification %s", n)
            freed = True
        except TimeoutError:
            freed = True          # periodic re-dispatch → scheduled_at resync
        except OperationalError: <reconnect per Step 1>
```
- `_drain_notifications(conn) -> bool` helper on `BeanQueue`: `driver = conn.connection.driver_connection; driver.poll(); had = bool(driver.notifies); driver.notifies.clear(); return had`, with the Step 1 reconnect handling. Replaces the every-50 ms dispatch UPDATE with a zero-round-trip socket check; dispatch now runs only when a thread frees, a NOTIFY arrives, the pool went idle, or `POLL_TIMEOUT` elapsed (preserves the documented ≤POLL_TIMEOUT latency for `scheduled_at` tasks).
- Invariant kept from race-fix commit `2e15eca`: dispatch → commit → submit, in that order.
- `_process_task_in_thread`: keep structure; add infra-failure recovery. Wrap the task reload separately: on `NoResultFound` log warning and return (row gone). On any exception from the final `db.commit()` (or session setup), after `db.rollback()`, best-effort in a FRESH session: `session.execute(update(self.task_model).where(id==task_id, state==PROCESSING).values(state=PENDING, worker_id=None)); session.commit()`, log, re-raise. Do NOT reset on exceptions raised inside `registry.process` — `Processor.process` already persists FAILED/retry state internally; anything escaping it is infra, and `registry.process` itself doesn't commit — so concretely: reset only when the exception escapes the reload or the commit, not the in-between call. Structure with two try blocks rather than one.
- `_process_tasks_sequential`: unchanged except the shutdown-event check and `poll(..., connection=notification_conn)`.

### Step 5 — QueuePool everywhere (settled with user; not a breaking change)
Rationale recorded for the PR description: with LISTEN on a dedicated connection, `SingletonThreadPool`'s only functional advantage (connection affinity for LISTEN) is gone. It is documented mainly for SQLite/testing and not recommended for production. A sequential worker opens ≤3 concurrent connections (main session, heartbeat session, notification conn) under either pool — QueuePool opens lazily, so real connection counts don't change. No public API/config change; only users of `create_default_engine` are affected at all (tape passes `engine=`). Upstreamable, not fork-territory.
- `create_default_engine`: single path — `create_engine(url, poolclass=QueuePool, pool_size=self._resolve_max_workers() + 5, max_overflow=10)`. Delete the `SingletonThreadPool` import and branch.
- Update unit tests asserting `SingletonThreadPool` (`tests/unit/test_thread_executor.py::test_default_pool_is_singleton`, `::test_pool_configuration_matrix`) as part of the Step 6 consolidation.

### Step 6 — Test suite restructure
Answer to the user's question, recorded: the overlap IS largely LLM-generated spam (two unit files assert the same pool arithmetic ~8 ways; ≥7 acceptance variants of "prove overlap with sleeps"), but several tests are genuinely distinct — those are all preserved. Moderate consolidation, some overlap kept.

**New shared fixture** in `tests/conftest.py`:
```python
@pytest.fixture
def worker():
    """Factory: run app.process_tasks(channels) in a daemon thread; graceful teardown."""
    started = []
    def start(app, channels):
        t = threading.Thread(target=app.process_tasks, kwargs=dict(channels=channels), daemon=True)
        t.start(); started.append((app, t)); return t
    yield start
    for app, t in started:
        app.request_shutdown(); t.join(15)
        assert not t.is_alive(), "worker failed to shut down gracefully"
```
Apps under test use `Config(POLL_TIMEOUT=1, WORKER_HEARTBEAT_PERIOD=1, METRICS_HTTP_SERVER_ENABLED=False, DATABASE_URL=db_url, PROCESSOR_PACKAGES=[...])` so shutdown reacts within ~1 s. `process_tasks` is signal-free, so running it off the main thread is safe (SystemExit is raised by our own loop check).

**Unit:**
- Merge `tests/unit/test_thread_executor.py` + `test_thread_executor_advanced.py` → single `tests/unit/test_engine_config.py`; delete both old files. Keep ONE test per behavior: pool selection+sizing matrix (now all QueuePool), `engine=` override, engine caching, env parsing of `BQ_MAX_WORKER_THREADS`, validation errors for `MAX_WORKER_THREADS=-1` and `BATCH_SIZE=0` (pydantic `ValidationError`), `_resolve_max_workers` (1→1, 4→4, 0→`min(32, cpu+4)`).
- `tests/unit/test_healthcheck.py` arrives via Step 0 (PR #1 version). Add: `test_shutdown_health_state_includes_state_key` asserting the Step 2 `(False, {"state": "SHUTDOWN"})` shape.
- `tests/unit/services/test_dispatch_service.py` additions: `listen`/`poll` honor explicit `connection=` — open a second `engine.connect()` with AUTOCOMMIT, `listen(["x"], connection=conn2)`, `notify(["x"])` + commit via the session, assert `poll(timeout=1, connection=conn2)` yields the notification; and that `poll(timeout=0)` without the kwarg still behaves as before (TimeoutError when nothing).

**Acceptance:**
- DELETE `test_batch_and_wait_issue.py` (one property × 3 parameter points), `test_realistic_integration.py` (3 serial-vs-parallel re-proofs + a thread-name test), `test_thread_executor.py` (4 overlap proofs; its 3 distinct tests are ported below). KEEP `test_thread_executor_edge_cases.py` (5 distinct subprocess-based tests: failing tasks, threads>tasks, tasks>threads, retry policy, state transitions) and `test_process_cmd.py` (CLI E2E) unchanged. Keep `tests/acceptance/fixtures/` (processors modules; `fixtures/app.py` still serves the kept files — verify imports after deletion, `grep -r "fixtures.app\|fixtures import" tests/acceptance`).
- NEW `tests/acceptance/test_worker_threaded.py` (in-process `worker` fixture):
  - `test_concurrent_execution` — 6 `timed_task`s (sleep 0.5) on 4 threads; assert ≥2 recorded windows overlap and wall time < 0.5×6 (single consolidated concurrency proof, replaces the 7 deleted variants).
  - `test_session_isolation` — port of `test_thread_executor_session_isolation` (concurrent_task × N, all DONE, results correct).
  - `test_sequential_backward_compat` — port: `MAX_WORKER_THREADS=1` processes all tasks, serially ordered timestamps not required, just completion + no executor threads (`task_worker` prefix absent from thread names recorded by `record_thread_name` processor).
  - `test_exactly_once_under_competing_workers` — 2 worker apps (separate BeanQueue instances + separate engines, 3 threads each) + 40 tasks; poll DONE count once/sec with tape's overshoot detector (`assert done <= created` every poll); final: 40 DONE, 0 PENDING/PROCESSING/FAILED.
  - `test_notify_wakeup_threaded` — **bug-1 regression**: worker `MAX_WORKER_THREADS=4, POLL_TIMEOUT=60`; wait ~1 s for idle, insert a task from a separate session/commit; assert DONE within 5 s. Must fail on pre-fix code (60 s poll) and pass post-fix.
  - `test_batch_size_smaller_than_threads` — BATCH_SIZE=1, threads=4, 8×`timed_task`(0.5 s): all DONE and wall time < 3 s (proves >1 in flight despite BATCH_SIZE=1; replaces `test_batch_and_wait_issue`).
- NEW `tests/acceptance/test_worker_lifecycle.py`:
  - `test_graceful_shutdown_drains_inflight` — worker (2 threads); 2 slow tasks (sleep 1.5) + 3 more PENDING; once 2 are PROCESSING, `request_shutdown()` + join; assert: the 2 in-flight DONE, the 3 others PENDING with `worker_id` cleared, worker row `SHUTDOWN`.
  - `test_worker_state_change_stops_processing` — **bug-2 regression**: worker with `WORKER_HEARTBEAT_PERIOD=1`; from another session set its worker row state to `NO_HEARTBEAT`, commit; assert worker thread exits within 10 s (join) — on master this hangs because only the heartbeat thread dies.
  - `test_dead_worker_tasks_rescheduled_by_peer` — insert a fake dead worker (stale `last_heartbeat`, state RUNNING) owning a PROCESSING task; start live worker with `WORKER_HEARTBEAT_TIMEOUT=2, WORKER_HEARTBEAT_PERIOD=1`; assert the task completes and dead worker row becomes `NO_HEARTBEAT`.
  - `test_healthz_endpoint_e2e` — pick a free port (bind 127.0.0.1:0, close), worker with `METRICS_HTTP_SERVER_ENABLED=True, METRICS_HTTP_SERVER_PORT=<port>`; poll GET `/healthz` until 200 + body `status == "ok"`; after `request_shutdown()` + join, GET → connection refused OR 500 (accept either; server thread is daemon).
- NEW `tests/acceptance/test_custom_models.py` (tape parity):
  - Fixture module `tests/acceptance/fixtures/custom_models.py`: `CustomTask/CustomWorker/CustomEvent` on a dedicated `Base` composed from the 8 mixins per the README pattern, PK columns renamed at DB level via `mapped_column("custom_task_id", ..., key="id")` with client-side `default=uuid.uuid4` (mirrors tape's ULID approach), `listen_events(CustomTask)`, module-level `bq.BeanQueue` app with `TASK_MODEL/WORKER_MODEL/EVENT_MODEL` dotted paths and two processors on channel `custom` (one normal, one that creates a child task via `other.run()` + `db.add`).
  - `test_custom_models_threaded_e2e` — create_all the custom Base on the test engine; in-process worker (3 threads); 10 parent tasks; assert all parents + spawned children DONE (proves NOTIFY-on-insert fires from worker-thread sessions and mixin relationships survive threading).
  - `test_inline_registry_process` — downstream-harness parity: `collect([custom_models])`, task from `helper.run()` added to a plain session, `registry.process(task, event_cls=CustomEvent)`, commit → DONE + COMPLETE event; failing processor variant → FAILED + error event.

**Dev deps / CI:** add explicit `pytest>=8,<9` and `pytest-timeout>=2,<3` to `[dependency-groups].dev`; add `timeout = 120` under `[tool.pytest.ini_options]`. No CircleCI change. No xdist (per-test create_all/drop_all shares one schema).

### Step 7 — Docs (final phase, after everything works)
- README: new "## Concurrency" section after "### Configurations": `MAX_WORKER_THREADS` (1 = sequential default; N = thread pool with per-task sessions; 0 = auto `min(32, cpu+4)`), `BATCH_SIZE` interplay (`limit = min(free_threads, BATCH_SIZE)`), processors must be thread-safe w.r.t. their own shared state (each gets its own Session), graceful shutdown via SIGINT/SIGTERM-as-KeyboardInterrupt or `app.request_shutdown()` (reaction ≤ POLL_TIMEOUT), `/healthz` (threaded server, in-memory state, no DB on request path).
- No version bump: upstream owns releases (already 1.2.0 after sync); fsrv consumes the git branch.
- Push branch; `gh pr create -R mahmoud/bq --base healthcheck-reliability --head threading-hardening` with a description covering: PR #1 merged + follow-ups, the 4 bug fixes with the regression test names, QueuePool rationale (from Step 5), test-suite restructure summary.

## Critical files & anchors
- `bq/app.py` — `_process_tasks_threaded` (l.351), `_process_tasks_sequential` (l.309), `update_workers` (l.159; `sys.exit(0)` at l.206), `process_tasks` (l.435), `create_default_engine` (l.74). All Step 1–5 edits land here; line numbers shift after Step 0 merges — re-read first.
- `bq/services/dispatch.py` — `listen`/`poll`/`notify` (l.80–117): Step 1 `connection=` kwarg.
- `bq/config.py` — `MAX_WORKER_THREADS`/`BATCH_SIZE` (l.17–22): Step 3 Field constraints.
- `tests/conftest.py` — add `worker` fixture beside `db`.
- `tests/acceptance/fixtures/thread_processors.py` — reusable processors (`slow_task`, `timed_task`, `concurrent_task`, `failing_task`, `retry_task`, `record_thread_name`) for the new acceptance files.

## Verification
From repo root; DB via `docker compose up -d psql` (init script creates `bq_test`; `TEST_DB_URL` defaults to `postgresql://bq:@localhost/bq_test`).
1. Full suite green: `uv run python -m pytest tests -q`.
2. Bug-1 regression proof: on `healthcheck-reliability` (pre-fix) cherry-pick only the new test file → `uv run python -m pytest tests/acceptance/test_worker_threaded.py::test_notify_wakeup_threaded` FAILS (poll timeout); on `threading-hardening` it passes in <10 s.
3. Bug-2 regression: `uv run python -m pytest tests/acceptance/test_worker_lifecycle.py::test_worker_state_change_stops_processing` passes; same cherry-pick check on the pre-fix branch hangs/fails.
4. Exactly-once: `uv run python -m pytest tests/acceptance/test_worker_threaded.py::test_exactly_once_under_competing_workers` — overshoot assertion never trips.
5. Manual smoke: terminal A `BQ_PROCESSOR_PACKAGES='["tests.acceptance.fixtures.thread_processors"]' BQ_MAX_WORKER_THREADS=4 BQ_DATABASE_URL=postgresql://bq:@localhost/bq_test uv run bq process thread-tests`; terminal B `uv run bq submit thread-tests tests.acceptance.fixtures.thread_processors slow_task -k '{"task_num": 1, "sleep_time": 0.1}'` (same env vars) → DONE row within ~1 s of submit (NOTIFY wakeup, not poll), Ctrl-C in A logs "Shutdown gracefully".
6. `uv run python -m pytest tests -q --durations=10` — confirm no test exceeds the 120 s timeout; expected total well under the old suite's runtime.

## Assumptions & contingencies
- **Settled with user**: land as new branch + PR based on `healthcheck-reliability` in `mahmoud/bq`; merge PR #1 (review verdict: keep) with follow-up fixes on our branch; QueuePool everywhere (non-breaking, rationale in Step 5); moderate test consolidation keeping distinct cases and `test_thread_executor_edge_cases.py` as-is.
- `MAX_WORKER_THREADS` default stays `1` — no behavior change for existing deployments.
- If `gh pr merge 1` conflicts after the master merge (Step 0.4), resolve in the PR branch; if Fazel's fork ref is unavailable, re-apply the reviewed diff manually (atomic tuple in `__init__`/`update_workers`/`_serve_http_request`/`process_tasks`, fixture swap in `tests/unit/test_healthcheck.py`; skip the 4 file deletions — already gone) and close PR #1 crediting him.
- If in-process workers prove flaky for `test_exactly_once_under_competing_workers` (scoped-session interference from `tests/factories.py` registering on the global `Session`), fall back to `multiprocessing.Process` for that one test, mirroring tape's `test_tape.py`.
- If `psycopg2` raw `OperationalError` never surfaces through the SQLAlchemy wrapper in `_drain_notifications` testing, catch via `self.engine.dialect.loaded_dbapi.OperationalError` (SQLAlchemy 2.0 attr) rather than importing psycopg2 directly.
- If upstream pushes new commits mid-work, re-run Step 0.2/0.3 merges; all our edits are additive to `bq/app.py`/`bq/services/dispatch.py` and unlikely to conflict.
