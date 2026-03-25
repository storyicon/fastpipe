"""Configuration constants for fastpipe execution.

This module centralizes all timeout and configuration values
to make tuning and maintenance easier.
"""

# Worker Queue Timeouts
# ---------------------
# Timeout for workers to wait on queue.get() operations.
# Balances responsiveness (can check self.running) vs CPU usage.
# Why 0.1s: Long enough to avoid CPU spinning, short enough for responsive shutdown.
WORKER_QUEUE_TIMEOUT = 0.1  # seconds

# Async-specific timeout for queue access in async workers.
# Shorter because async workers need more responsive event loop.
# Why 0.01s: Async can handle tighter loops with sleep, keeps event loop responsive.
ASYNC_QUEUE_TIMEOUT = 0.01  # seconds

# Sleep duration when async queue is empty, to avoid tight loop.
# Why 0.01s: Prevents CPU spinning while maintaining low latency (10ms).
ASYNC_SLEEP_DURATION = 0.01  # seconds

# Shutdown Timeouts
# -----------------
# Max time to wait for processes to exit gracefully before terminate().
PROCESS_JOIN_TIMEOUT = 5  # seconds

# Max time to wait for threads to finish.
THREAD_JOIN_TIMEOUT = 2  # seconds

# Max time to wait for feeder thread in iter() mode.
FEEDER_JOIN_TIMEOUT = 5  # seconds

# Event loop shutdown timeout.
LOOP_SHUTDOWN_TIMEOUT = 2  # seconds

# Default Values
# --------------
# Default queue capacity for backpressure control.
DEFAULT_QUEUE_SIZE = 100

# Default timeout for stream.close(drain=True).
STREAM_DRAIN_TIMEOUT = 30  # seconds

# Interval for checking queue emptiness during drain.
STREAM_DRAIN_CHECK_INTERVAL = 0.1  # seconds

# AsyncExecutor Bridge Queue Size
# --------------------------------
# Size of internal asyncio.Queue for sync/async bridging.
# Should be large enough to avoid blocking but not too large.
ASYNC_BRIDGE_QUEUE_SIZE = 100

# Loop Ready Timeout
# ------------------
# Max time to wait for asyncio event loop to start and be ready.
# Why 5s: Generous timeout for loop initialization, failure indicates system issue.
LOOP_READY_TIMEOUT = 5.0  # seconds

# Exception Check Interval
# -------------------------
# Timeout for queue.get() in main loop to periodically check for worker exceptions.
# Why 0.1s: Balance between responsiveness (fail-fast) and CPU usage.
EXCEPTION_CHECK_TIMEOUT = 0.1  # seconds
