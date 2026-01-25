# Unreleased

- Breaking: Rename `enqueue_multi` to `enqueue_many` and return task IDs for batch enqueue
- Perf: Optimize `enqueue_many` for uniform-config batches
- Fix: Pending retry delays now respect the last attempt time when re-queuing tasks
- Fix: Reap completed worker processing tasks to avoid unbounded memory growth
- Fix: Fence reclaimed task attempts so stale workers cannot update task state
- Fix: Job step configuration now uses the step index from input to avoid cross-run drift
- Add: Job steps can configure task timeouts, TTLs, delays, heartbeats, concurrency keys, and priorities
- Add: Job batch enqueue helpers (`enqueue_many`, `enqueue_many_using`)

# 0.2.0

- Breaking: Queues are now stored and passed as `Arc<Queue<_>>`;
  `Worker::new` and `Scheduler::new` take `Arc<Queue<_>>` instead of `Queue<_>`. #100
- Breaking: `Job::run_worker` and `Job::run_scheduler` have been removed. #100
- Breaking: `Job::run` and `Job::start` now take `&self` rather than consuming `self`. #100
- Fix: Shutdown channels are now unique. #97
- Add: `Job::queue`, `Job::worker`, and `Job::scheduler` helpers for
  zero‑boilerplate executor creation. #100
- Add: `enqueue_multi` et al, allowing batched enqueue. #79

# 0.1.2

- Provide additional worker and scheduler tracing instrumentation

# 0.1.1

- Fix: Job cancellation should lock rows to be cancelled #67

# 0.1.0

- Breaking: Worker and scheduler setters have been renamed #42
- Breaking: Migrations have been reworked to compensate for features that will land in `sqlx` 0.9.0 #44
- Breaking: Job enqueues now return an `EnqueuedJob` type #46
- Breaking: Task ID is now a newtype #47
- Breaking: Task dequeues are now encapsulated such that they are visible as they're being processed #55, #59, #60, #64, #65
- Breaking: Task schedule "name" column renamed to "task_queue_name" for consistency #58
- Breaking: Scheduler iterator has been refactored to use `Zoned` directly #62
- Task attempts are now recorded in a separate table, providing a complete log of task execution history #50

# 0.0.6

- Breaking: Queue methods now take input by reference
- Breaking: Job methods also take input by reference
- Breaking: The scheduler `run_every` method is removed
- Pending tasks are now processed upon change via a notify channel #25
- An `unschedule` method is provided on `Queue` and `Job` #40
- Graceful shutdown is now available on `JobHandle` returned from `start` #37

# 0.0.5

- Breaking: Tasks require an associated type Output
- Breaking: Tasks require a transaction as their first execute argument
- Breaking: Database locking methods are now free functions
- Breaking: Job interface rewritten for step functions #24

# 0.0.4

- Breaking: Renamed builders to `Builder` #15
- Breaking: Made task deletion routine a free function #13
- Breaking: `Job::run` now runs both the worker and scheduler #12
- Ensure scheduler singleton behavior

# 0.0.3

- Added `ToTaskResult` trait for better task result ergonomics #10

# 0.0.2

- Jobs may provide state #9
- Breaking: `queue` must now be defined after `execute`
- Workers may be gracefully shutdown via `graceful_shutdown` #8
- Jobs and queue are provided `enqueue_after` #7

# 0.0.1

- Pre-release: baseline feature completion

# 0.0.0

- Pre-release :tada:
