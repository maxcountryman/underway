# Unreleased

- Breaking: Remove `workflow::Context::tx`; step contexts no longer expose a worker transaction directly
- Breaking: Workflow builders now declare activity contracts via `Workflow::builder().declare::<A>()` instead of registering concrete handlers
- Breaking: Activity handlers are now bound on runtime builders via `workflow.runtime().bind::<A>(handler)`
- Breaking: Remove manual activity keys; operation identity is derived from workflow run ID, step index, and per-step operation order
- Add: `workflow::InvokeActivity` helpers (`A::call` / `A::emit`) with compile-time declaration checks
- Add: `RuntimeBuilder` for explicit runtime activity handler binding and validation
- Breaking: Remove `Workflow::run` and `Workflow::start`; use `workflow.runtime().run()` / `workflow.runtime().start()`
- Breaking: Remove `Workflow::worker` and `Workflow::scheduler`; use `workflow.runtime().worker()` / `workflow.runtime().scheduler()`
- Add: `Activity` trait and standard activity error envelope
- Add: Durable typed activity contract + handler split (`activity::Activity` + `activity::ActivityHandler<A>`)
- Add: `waiting` task state to support workflow suspension while activity calls complete
- Breaking: Rename `enqueue_multi` to `enqueue_many` and return task IDs for batch enqueue
- Perf: Optimize `enqueue_many` for uniform-config batches
- Fix: Pending retry delays now respect the last attempt time when re-queuing tasks
- Fix: Reap completed worker processing tasks to avoid unbounded memory growth
- Fix: Fence reclaimed task attempts so stale workers cannot update task state
- Fix: Workflow step configuration now uses the step index from input to avoid cross-run drift
- Add: Workflow steps can configure task timeouts, TTLs, delays, heartbeats, concurrency keys, and priorities
- Add: Workflow batch enqueue helpers (`enqueue_many`, `enqueue_many_using`)
- Breaking: Rename workflow step transition type `To` to `Transition`, including helper constructors `done` -> `complete` and `delay_for` -> `after`

# 0.2.0

- Breaking: Queues are now stored and passed as `Arc<Queue<_>>`;
  `Worker::new` and `Scheduler::new` take `Arc<Queue<_>>` instead of `Queue<_>`. #100
- Breaking: `Workflow::run_worker` and `Workflow::run_scheduler` have been removed. #100
- Breaking: `Workflow::run` and `Workflow::start` now take `&self` rather than consuming `self`. #100
- Fix: Shutdown channels are now unique. #97
- Add: `Workflow::queue`, `Workflow::worker`, and `Workflow::scheduler` helpers for
  zero‑boilerplate executor creation. #100
- Add: `enqueue_multi` et al, allowing batched enqueue. #79

# 0.1.2

- Provide additional worker and scheduler tracing instrumentation

# 0.1.1

- Fix: Workflow cancellation should lock rows to be cancelled #67

# 0.1.0

- Breaking: Worker and scheduler setters have been renamed #42
- Breaking: Migrations have been reworked to compensate for features that will land in `sqlx` 0.9.0 #44
- Breaking: Workflow enqueues now return an `EnqueuedWorkflow` type #46
- Breaking: Task ID is now a newtype #47
- Breaking: Task dequeues are now encapsulated such that they are visible as they're being processed #55, #59, #60, #64, #65
- Breaking: Task schedule "name" column renamed to "task_queue_name" for consistency #58
- Breaking: Scheduler iterator has been refactored to use `Zoned` directly #62
- Task attempts are now recorded in a separate table, providing a complete log of task execution history #50

# 0.0.6

- Breaking: Queue methods now take input by reference
- Breaking: Workflow methods also take input by reference
- Breaking: The scheduler `run_every` method is removed
- Pending tasks are now processed upon change via a notify channel #25
- An `unschedule` method is provided on `Queue` and `Workflow` #40
- Graceful shutdown is now available on `WorkflowHandle` returned from `start` #37

# 0.0.5

- Breaking: Tasks require an associated type Output
- Breaking: Tasks require a transaction as their first execute argument
- Breaking: Database locking methods are now free functions
- Breaking: Workflow interface rewritten for step functions #24

# 0.0.4

- Breaking: Renamed builders to `Builder` #15
- Breaking: Made task deletion routine a free function #13
- Breaking: `Workflow::run` now runs both the worker and scheduler #12
- Ensure scheduler singleton behavior

# 0.0.3

- Added `ToTaskResult` trait for better task result ergonomics #10

# 0.0.2

- Workflows may provide state #9
- Breaking: `queue` must now be defined after `execute`
- Workers may be gracefully shutdown via `graceful_shutdown` #8
- Workflows and queue are provided `enqueue_after` #7

# 0.0.1

- Pre-release: baseline feature completion

# 0.0.0

- Pre-release :tada:
