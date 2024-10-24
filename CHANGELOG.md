# Unreleased

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
