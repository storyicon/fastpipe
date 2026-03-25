"""Streaming mode interface."""

import logging
import time
from queue import Empty
from typing import Any, Optional

from ._constants import STREAM_DRAIN_CHECK_INTERVAL, STREAM_DRAIN_TIMEOUT
from ._runtime import create_queue_chain, insert_adapters, select_queue_type

logger = logging.getLogger(__name__)


class PipelineStream:
    """Streaming interface to running pipeline.

    Provides put/get methods for continuous operation.
    """

    def __init__(self, executors: list, inbound_queue, outbound_queue):
        """Initialize stream (use PipelineStream.start() classmethod instead)."""
        self.executors = executors
        self._inbound_queue = inbound_queue
        self._outbound_queue = outbound_queue
        self._running = True
        self._exception = None
        self._exception_raised = False  # Flag for fail-fast

    @classmethod
    def start(cls, stages: list, queue_size: int, executor_factory=None) -> "PipelineStream":
        """Start pipeline in streaming mode.

        Args:
            stages: List of Stage objects
            queue_size: Default queue capacity
            executor_factory: Optional factory for creating executors

        Returns:
            PipelineStream instance
        """
        from ._executors import ExecutorFactory

        # Build pipeline
        stages_with_adapters = insert_adapters(stages)
        queue_type = select_queue_type(stages_with_adapters)
        queues = create_queue_chain(stages_with_adapters, queue_type, queue_size)

        # Create executors using factory
        factory = executor_factory or ExecutorFactory()
        executors = []
        for i, stage in enumerate(stages_with_adapters):
            input_q = queues[i]
            output_q = queues[i + 1]
            executor = factory.create(stage, input_q, output_q, queue_type)
            executors.append(executor)

        # Start all executors
        for executor in executors:
            executor.start()

        return cls(executors, queues[0], queues[-1])

    def put(self, item: Any, block: bool = True, timeout: Optional[float] = None):
        """Put item into pipeline.

        Args:
            item: Item to process
            block: If True, block until space available
            timeout: Max wait time in seconds

        Raises:
            Full: If queue full and block=False
            Exception: Any exception from workers
        """
        self._check_exception()
        self._inbound_queue.put(item, block=block, timeout=timeout)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """Get result from pipeline.

        Args:
            block: If True, block until result available
            timeout: Max wait time in seconds

        Returns:
            Processed result

        Raises:
            Empty: If no result and block=False
            Exception: Any exception from workers
        """
        self._check_exception()
        return self._outbound_queue.get(block=block, timeout=timeout)

    def put_batch(self, items: list[Any]):
        """Put multiple items into pipeline.

        Args:
            items: List of items to process
        """
        for item in items:
            self.put(item)

    def get_batch(self, max_items: int, timeout: Optional[float] = None) -> list[Any]:
        """Get up to max_items results from pipeline.

        Args:
            max_items: Maximum number of results to get
            timeout: Timeout for each get operation

        Returns:
            List of results (may be fewer than max_items)
        """
        results = []
        for _ in range(max_items):
            try:
                result = self.get(block=True, timeout=timeout)
                results.append(result)
            except Empty:
                break
        return results

    def qsize_inbound(self) -> int:
        """Get approximate number of items in inbound queue."""
        return self._inbound_queue.qsize()

    def qsize_outbound(self) -> int:
        """Get approximate number of results in outbound queue."""
        return self._outbound_queue.qsize()

    def is_running(self) -> bool:
        """Check if pipeline is still running."""
        return self._running

    @property
    def inbound_queue(self):
        """Direct access to inbound queue (advanced usage)."""
        return self._inbound_queue

    @property
    def outbound_queue(self):
        """Direct access to outbound queue (advanced usage)."""
        return self._outbound_queue

    def close(self, drain: bool = True, timeout: float = STREAM_DRAIN_TIMEOUT):
        """Close stream and stop pipeline.

        Args:
            drain: If True, process remaining items before stopping
            timeout: Max wait time for drain in seconds
        """
        # Idempotent: return immediately if already closed
        if not self._running:
            return
        self._running = False

        if drain:
            # Wait for queues to empty (with timeout)
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.qsize_inbound() == 0 and self.qsize_outbound() == 0:
                    break
                time.sleep(STREAM_DRAIN_CHECK_INTERVAL)

        # Stop all executors - collect cleanup exceptions
        # Note: check_exceptions=True because stream mode doesn't check in main loop
        cleanup_exceptions = []
        for executor in self.executors:
            try:
                executor.stop(check_exceptions=True)
            except Exception as cleanup_exc:
                logger.error(f"Cleanup error in executor: {cleanup_exc}")
                cleanup_exceptions.append(cleanup_exc)

        # Raise cleanup errors if any occurred
        if cleanup_exceptions:
            if len(cleanup_exceptions) == 1:
                raise RuntimeError("Executor cleanup failed") from cleanup_exceptions[0]
            else:
                msg = f"Multiple executor cleanup failures ({len(cleanup_exceptions)})"
                raise RuntimeError(msg) from cleanup_exceptions[0]

    def _check_exception(self):
        """Check if any worker had exception (fail-fast)."""
        # Fast path: if we already raised an exception, don't check again
        if self._exception_raised:
            raise RuntimeError("Pipeline has failed, cannot continue")

        # Check all executors for exceptions (fail-fast, atomic get)
        # Current: O(N) checks per operation, where N = number of executors.
        # Possible optimization: Use shared exception_queue across all executors to reduce to O(1).
        # However, benchmarking shows <10ms overhead for typical pipelines (5-10 stages),
        # making this optimization low-priority (not worth now).
        for executor in self.executors:
            try:
                exc, tb_str = executor.exception_queue.get_nowait()
                logger.error(f"Worker exception:\n{tb_str}")
                self._exception_raised = True  # Mark as failed
                self._running = False  # Stop pipeline
                raise exc
            except Empty:
                pass  # No exception from this executor

    def __enter__(self) -> "PipelineStream":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - always close with drain."""
        self.close(drain=True)
        return False  # Don't suppress exceptions
