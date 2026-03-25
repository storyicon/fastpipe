"""FastPipe - Minimal parallel pipeline library with batch and streaming support.

Example:
    import fastpipe

    # Batch processing - load all results into memory
    results = (fastpipe.create()
        .map(download, workers=4)
        .map(process, workers=4)
        .run(data))

    # Iterator mode - memory-efficient for large datasets
    for result in (fastpipe.create()
        .map(process, workers=4)
        .filter(validate, workers=2)
        .iter(large_dataset)):
        save_to_db(result)  # Process incrementally

    # Streaming mode - real-time processing
    stream = (fastpipe.create()
        .map(process, workers=4)
        .stream())
    stream.put(item)
    result = stream.get()
    stream.close()

    # Callable classes - pass class + init params (recommended for multiprocess)
    class ModelProcessor:
        def __init__(self, model_path, device='cpu'):
            self.model = load_model(model_path)  # Loaded once per worker
            self.device = device

        def __call__(self, image):
            return self.model.predict(image)

    results = (fastpipe.create()
        .map(
            ModelProcessor,  # Pass class, not instance
            workers=4,
            mode='process',
            init_args=('model.pth',),
            init_kwargs={'device': 'cuda'}
        )
        .run(images))

    # Alternative: instance methods (object serialized to each worker)
    processor = ModelProcessor('model.pth')
    results = (fastpipe.create()
        .map(processor, workers=4)  # Instance serialized
        .run(data))

    # Execution modes
    results = (fastpipe.create()
        .map(async_func, workers=5, mode=fastpipe.Mode.ASYNC)
        .map(cpu_func, workers=8, mode='process')
        .run(data))
"""

import inspect
import logging
import threading
from collections.abc import Iterable
from queue import Empty
from typing import Callable, Generic, Literal, Optional, TypeVar, Union

from ._constants import EXCEPTION_CHECK_TIMEOUT, FEEDER_JOIN_TIMEOUT
from ._types import Mode, Operation, Stage

logger = logging.getLogger(__name__)

__version__ = "0.0.1"
__all__ = ["create", "Mode"]

T = TypeVar("T")
U = TypeVar("U")


def _validate_positive_int(name: str, value: any) -> None:
    """Validate that a parameter is a positive integer.

    Args:
        name: Parameter name for error message
        value: Value to validate

    Raises:
        TypeError: If value is not an integer
        ValueError: If value is not positive
    """
    if not isinstance(value, int):
        raise TypeError(f"{name} must be integer, got {type(value).__name__}")
    if value <= 0:
        raise ValueError(f"{name} must be positive integer, got {value}")


def _validate_callable(name: str, value: any) -> None:
    """Validate that a parameter is callable or a class.

    Args:
        name: Parameter name for error message
        value: Value to validate

    Raises:
        TypeError: If value is not callable or a class
    """
    if not callable(value) and not inspect.isclass(value):
        raise TypeError(f"{name} must be callable or a class, got {type(value).__name__}")


class Pipeline(Generic[T]):
    """Pipeline for parallel data processing (internal class, not exported)."""

    def __init__(self, executor_factory=None):
        """Initialize pipeline.

        Args:
            executor_factory: Factory for creating executors (default: ExecutorFactory).
                            Useful for testing with mock executors.
        """
        self._stages: list[Stage] = []
        self._executor_factory = executor_factory

    def map(
        self,
        func: Union[Callable[[T], U], type],
        workers: int = 1,
        mode: Union[Mode, Literal["thread", "process", "async"]] = "thread",
        inbound_queue_size: Optional[int] = None,
        init_args: tuple = (),
        init_kwargs: Optional[dict] = None,
    ) -> "Pipeline[U]":
        """Apply function to each item (1:1 transformation).

        Args:
            func: Function or callable class to apply to each item
            workers: Number of parallel workers
            mode: Execution mode (recommended: fastpipe.Mode.THREAD/PROCESS/ASYNC)
            inbound_queue_size: Optional queue size override
            init_args: Positional arguments for class initialization (if func is a class)
            init_kwargs: Keyword arguments for class initialization (if func is a class)

        Example:
            pipe.map(process, workers=4, mode=fastpipe.Mode.THREAD)

            # With callable class
            pipe.map(MyProcessor, workers=4, init_args=(arg1,), init_kwargs={'param': value})
        """
        _validate_callable("func", func)
        _validate_positive_int("workers", workers)
        if inbound_queue_size is not None:
            _validate_positive_int("inbound_queue_size", inbound_queue_size)

        self._stages.append(
            Stage(
                operation=Operation.MAP,
                func=func,
                workers=workers,
                mode=mode,
                inbound_queue_size=inbound_queue_size,
                init_args=init_args,
                init_kwargs=init_kwargs,
            )
        )
        return self

    def flat_map(
        self,
        func: Union[Callable[[T], list[U]], type],
        workers: int = 1,
        mode: Union[Mode, Literal["thread", "process", "async"]] = "thread",
        inbound_queue_size: Optional[int] = None,
        init_args: tuple = (),
        init_kwargs: Optional[dict] = None,
    ) -> "Pipeline[U]":
        """Apply function that returns list, flatten results (1:N transformation).

        Args:
            func: Function or callable class that returns a list
            workers: Number of parallel workers
            mode: Execution mode
            inbound_queue_size: Optional queue size override
            init_args: Positional arguments for class initialization (if func is a class)
            init_kwargs: Keyword arguments for class initialization (if func is a class)
        """
        _validate_callable("func", func)
        _validate_positive_int("workers", workers)
        if inbound_queue_size is not None:
            _validate_positive_int("inbound_queue_size", inbound_queue_size)

        self._stages.append(
            Stage(
                operation=Operation.FLAT_MAP,
                func=func,
                workers=workers,
                mode=mode,
                inbound_queue_size=inbound_queue_size,
                init_args=init_args,
                init_kwargs=init_kwargs,
            )
        )
        return self

    def filter(
        self,
        predicate: Union[Callable[[T], bool], type],
        workers: int = 1,
        mode: Union[Mode, Literal["thread", "process", "async"]] = "thread",
        inbound_queue_size: Optional[int] = None,
        init_args: tuple = (),
        init_kwargs: Optional[dict] = None,
    ) -> "Pipeline[T]":
        """Keep only items matching predicate.

        Args:
            predicate: Predicate function or callable class
            workers: Number of parallel workers
            mode: Execution mode
            inbound_queue_size: Optional queue size override
            init_args: Positional arguments for class initialization (if predicate is a class)
            init_kwargs: Keyword arguments for class initialization (if predicate is a class)
        """
        _validate_callable("predicate", predicate)
        _validate_positive_int("workers", workers)
        if inbound_queue_size is not None:
            _validate_positive_int("inbound_queue_size", inbound_queue_size)

        self._stages.append(
            Stage(
                operation=Operation.FILTER,
                func=predicate,
                workers=workers,
                mode=mode,
                inbound_queue_size=inbound_queue_size,
                init_args=init_args,
                init_kwargs=init_kwargs,
            )
        )
        return self

    def each(
        self,
        func: Union[Callable[[T], None], type],
        workers: int = 1,
        mode: Union[Mode, Literal["thread", "process", "async"]] = "thread",
        inbound_queue_size: Optional[int] = None,
        init_args: tuple = (),
        init_kwargs: Optional[dict] = None,
    ) -> "Pipeline[T]":
        """Execute side-effect function, return original data.

        Args:
            func: Side-effect function or callable class
            workers: Number of parallel workers
            mode: Execution mode
            inbound_queue_size: Optional queue size override
            init_args: Positional arguments for class initialization (if func is a class)
            init_kwargs: Keyword arguments for class initialization (if func is a class)
        """
        _validate_callable("func", func)
        _validate_positive_int("workers", workers)
        if inbound_queue_size is not None:
            _validate_positive_int("inbound_queue_size", inbound_queue_size)

        self._stages.append(
            Stage(
                operation=Operation.EACH,
                func=func,
                workers=workers,
                mode=mode,
                inbound_queue_size=inbound_queue_size,
                init_args=init_args,
                init_kwargs=init_kwargs,
            )
        )
        return self

    def batch(self, size: int) -> "Pipeline[list[T]]":
        """Accumulate items into fixed-size batches.

        IMPORTANT: BATCH operations use single worker because batch accumulation
        is stateful. Multiple workers would cause race conditions on the
        shared accumulator. This design choice eliminates need for locking.

        Args:
            size: Batch size (must be positive)

        Returns:
            Self for method chaining
        """
        _validate_positive_int("batch_size", size)

        stage = Stage(
            operation=Operation.BATCH,
            func=None,
            workers=1,  # CRITICAL: Must be 1 to avoid race conditions
            mode="thread",
            inbound_queue_size=None,
            batch_size=size,
        )

        # Design invariant validation
        assert stage.workers == 1, "BATCH stage must be single-worker (design invariant)"

        self._stages.append(stage)
        return self

    def unbatch(self) -> "Pipeline[T]":
        """Flatten batches back to individual items.

        Note: unbatch operations always use single worker to maintain order.
        """
        self._stages.append(
            Stage(
                operation=Operation.UNBATCH,
                func=None,
                workers=1,  # Must be 1 for ordered flattening
                mode="thread",
                inbound_queue_size=None,
            )
        )
        return self

    def run(self, input_data: Iterable[T], queue_size: int = 100) -> list:
        """Execute pipeline on finite data (batch mode).

        Args:
            input_data: Iterable of items to process
            queue_size: Default queue capacity for backpressure control

        Returns:
            List of processed results

        Raises:
            ValueError: If queue_size <= 0
            Exception: Any exception from stage functions
        """
        from ._executors import ExecutorFactory
        from ._runtime import (
            SENTINEL,
            _Sentinel,
            create_queue_chain,
            insert_adapters,
            select_queue_type,
        )

        _validate_positive_int("queue_size", queue_size)

        # Build pipeline: insert adapters
        stages_with_adapters = insert_adapters(self._stages)

        # Select queue type and create queue chain
        queue_type = select_queue_type(stages_with_adapters)
        queues = create_queue_chain(stages_with_adapters, queue_type, queue_size)

        # Create executors for each stage using factory
        factory = self._executor_factory or ExecutorFactory()
        executors = []
        for i, stage in enumerate(stages_with_adapters):
            input_q = queues[i]
            output_q = queues[i + 1]
            executor = factory.create(stage, input_q, output_q, queue_type)
            executors.append(executor)

        try:
            # Start all executors
            for executor in executors:
                executor.start()

            # Feed input data to first queue
            for item in input_data:
                queues[0].put(item)

            # Signal end of input - one SENTINEL per worker in first stage
            first_stage = stages_with_adapters[0]
            for _ in range(first_stage.workers):
                queues[0].put(SENTINEL)

            # Collect results from last queue
            # Need to receive SENTINEL from each worker of last stage
            last_stage = stages_with_adapters[-1]
            results = []
            sentinels_received = 0

            while sentinels_received < last_stage.workers:
                # Check for worker exceptions (fail-fast, atomic get)
                for executor in executors:
                    try:
                        exc, tb_str = executor.exception_queue.get_nowait()
                        logger.error(f"Worker exception:\n{tb_str}")
                        raise exc
                    except Empty:
                        pass  # No exception from this executor

                try:
                    item = queues[-1].get(
                        timeout=EXCEPTION_CHECK_TIMEOUT
                    )  # Check exceptions periodically
                except Empty:
                    continue  # Check exceptions again on next iteration

                if isinstance(item, _Sentinel):
                    sentinels_received += 1
                else:
                    results.append(item)

            return results

        finally:
            # Cleanup resources - don't check exceptions again (already checked in loop)
            # NOTE: check_exceptions=False is safe here because exceptions are already checked
            # in the main loop (lines 273-277). This avoids duplicate exception raising.
            cleanup_exceptions = []
            for executor in executors:
                try:
                    executor.stop(check_exceptions=False)
                except Exception as cleanup_exc:
                    logger.error(f"Cleanup error in executor: {cleanup_exc}")
                    cleanup_exceptions.append(cleanup_exc)

            # If cleanup failed, raise with context
            # NOTE: Cleanup exceptions ARE propagated here (lines 307, 311), not ignored.
            # This ensures users are notified of resource cleanup failures.
            if cleanup_exceptions:
                if len(cleanup_exceptions) == 1:
                    raise RuntimeError("Executor cleanup failed") from cleanup_exceptions[0]
                else:
                    # Multiple cleanup failures
                    msg = f"Multiple executor cleanup failures ({len(cleanup_exceptions)})"
                    raise RuntimeError(msg) from cleanup_exceptions[0]

    def iter(self, input_data: Iterable[T], queue_size: int = 100):
        """Execute pipeline and yield results one by one (memory-efficient).

        Unlike run() which collects all results into a list, iter() yields
        results as they become available, reducing memory footprint.

        Args:
            input_data: Iterable of items to process
            queue_size: Default queue capacity for backpressure control

        Yields:
            Processed results one at a time

        Example:
            # Memory-efficient processing
            for result in pipe.iter(huge_dataset):
                save_to_db(result)  # Process incrementally
        """
        from ._executors import ExecutorFactory
        from ._runtime import (
            SENTINEL,
            _Sentinel,
            create_queue_chain,
            insert_adapters,
            select_queue_type,
        )

        _validate_positive_int("queue_size", queue_size)

        # Build pipeline
        stages_with_adapters = insert_adapters(self._stages)
        queue_type = select_queue_type(stages_with_adapters)
        queues = create_queue_chain(stages_with_adapters, queue_type, queue_size)

        # Create executors using factory
        factory = self._executor_factory or ExecutorFactory()
        executors = []
        for i, stage in enumerate(stages_with_adapters):
            input_q = queues[i]
            output_q = queues[i + 1]
            executor = factory.create(stage, input_q, output_q, queue_type)
            executors.append(executor)

        try:
            # Start all executors
            for executor in executors:
                executor.start()

            # Feed input data in background thread
            stop_feeding = threading.Event()

            def feeder():
                try:
                    for item in input_data:
                        # Check if we should stop (for early termination)
                        if stop_feeding.is_set():
                            return
                        queues[0].put(item)
                    # Signal end of input
                    first_stage = stages_with_adapters[0]
                    for _ in range(first_stage.workers):
                        queues[0].put(SENTINEL)
                except Exception:
                    # Feeder exceptions are not critical (e.g., queue closed)
                    pass

            # Start feeder in daemon thread to prevent blocking process exit.
            # Daemon threads are terminated when the main program exits.
            feeder_thread = threading.Thread(target=feeder, daemon=True)
            feeder_thread.start()

            # Yield results as they arrive
            last_stage = stages_with_adapters[-1]
            sentinels_received = 0

            while sentinels_received < last_stage.workers:
                # Check for worker exceptions (fail-fast)
                # Complexity: O(N) checks per output item, where N = number of executors.
                # Impact: Negligible for typical pipelines (5-10 stages, <10ms overhead).
                #         Becomes noticeable with large pipelines (50+ stages, >100ms overhead).
                # Future optimization: Use shared exception queue to reduce to O(1) per item.
                for executor in executors:
                    try:
                        exc, tb_str = executor.exception_queue.get_nowait()
                        logger.error(f"Worker exception:\n{tb_str}")
                        raise exc
                    except Empty:
                        pass  # No exception from this executor

                try:
                    item = queues[-1].get(
                        timeout=EXCEPTION_CHECK_TIMEOUT
                    )  # Check exceptions periodically
                except Empty:
                    continue  # Check exceptions again on next iteration

                if isinstance(item, _Sentinel):
                    sentinels_received += 1
                else:
                    yield item

        finally:
            import time

            # Signal feeder to stop (for early termination)
            stop_feeding.set()

            # CRITICAL: Drain input queue to unblock feeder if stuck on put()
            # Continue draining until feeder completes or timeout
            timeout_end = time.time() + FEEDER_JOIN_TIMEOUT
            while feeder_thread.is_alive() and time.time() < timeout_end:
                try:
                    queues[0].get_nowait()
                except Empty:
                    # Queue empty, wait briefly for feeder to notice stop signal
                    time.sleep(0.001)

            # Final check and warning
            if feeder_thread.is_alive():
                logger.warning(
                    f"Feeder thread did not terminate within {FEEDER_JOIN_TIMEOUT}s timeout. "
                    f"This may indicate a deadlock or system issue."
                )

            # Cleanup resources - don't check exceptions again (already checked in loop)
            cleanup_exceptions = []
            for executor in executors:
                try:
                    executor.stop(check_exceptions=False)
                except Exception as cleanup_exc:
                    logger.error(f"Cleanup error in executor: {cleanup_exc}")
                    cleanup_exceptions.append(cleanup_exc)

            # If cleanup failed, raise with context
            if cleanup_exceptions:
                if len(cleanup_exceptions) == 1:
                    raise RuntimeError("Executor cleanup failed") from cleanup_exceptions[0]
                else:
                    # Multiple cleanup failures
                    msg = f"Multiple executor cleanup failures ({len(cleanup_exceptions)})"
                    raise RuntimeError(msg) from cleanup_exceptions[0]

    def stream(self, queue_size: int = 100):
        """Start pipeline in streaming mode.

        Args:
            queue_size: Default queue capacity for backpressure control

        Returns:
            PipelineStream interface for continuous put/get operations

        Example:
            stream = pipe.stream(queue_size=100)
            stream.put(item)
            result = stream.get()
            stream.close()
        """
        from ._stream import PipelineStream

        _validate_positive_int("queue_size", queue_size)

        # Implementation delegated to PipelineStream
        return PipelineStream.start(self._stages, queue_size, self._executor_factory)


def create() -> Pipeline:
    """Create a new parallel pipeline.

    Returns:
        Pipeline instance for building processing pipeline.

    Example:
        import fastpipe

        pipe = fastpipe.create() \\
            .map(process, workers=4) \\
            .filter(validate, workers=2)

        # Batch mode
        results = pipe.run(data)

        # Streaming mode
        stream = pipe.stream()
        stream.put(item)
        result = stream.get()
        stream.close()
    """
    return Pipeline()
