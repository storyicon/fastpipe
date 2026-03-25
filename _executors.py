"""Executors for different execution modes.

This module implements the execution engines that run pipeline stages
in parallel using threads, processes, or async coroutines.

Architecture:
    Each executor follows a consistent pattern:
    1. __init__: Store stage config and queues
    2. start(): Launch workers (threads/processes/async tasks)
    3. Workers: Pull from input_queue → process → push to output_queue
    4. stop(): Cleanup workers and queues, propagate exceptions

Worker Lifecycle:
    - Workers run until SENTINEL received
    - Each worker outputs SENTINEL when done
    - Downstream expects N SENTINELs (N = number of workers)

SENTINEL Handling:
    - BATCH operations: Flush partial batch before outputting SENTINEL
    - ADAPTER operations: Fan-in N upstream SENTINELs, fan-out M downstream
    - Normal operations: Each worker outputs 1 SENTINEL

Exception Handling:
    - Workers capture exceptions with full traceback
    - Exceptions propagated via exception_queue (thread-safe)
    - stop() raises first exception if any occurred

Resource Management:
    - Executors cleanup queues if multiprocessing.Queue type
    - queue.close() + queue.join_thread() on shutdown
    - Best-effort cleanup in exception paths

Constants:
    See _constants.py for all timeout and configuration values.
"""

import asyncio
import inspect
import logging
import multiprocessing
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Full, Queue
from typing import Any, Callable

from ._constants import (
    ASYNC_QUEUE_TIMEOUT,
    ASYNC_SLEEP_DURATION,
    LOOP_READY_TIMEOUT,
    LOOP_SHUTDOWN_TIMEOUT,
    PROCESS_JOIN_TIMEOUT,
    WORKER_QUEUE_TIMEOUT,
)
from ._runtime import SENTINEL, _Sentinel
from ._types import Operation, Stage

logger = logging.getLogger(__name__)


class ExecutorFactory:
    """Factory for creating executors based on stage mode.

    Enables dependency injection for testing and flexibility.
    """

    @staticmethod
    def create(stage: Stage, input_queue, output_queue, queue_type: type):
        """Create appropriate executor for the stage mode.

        Args:
            stage: Stage configuration
            input_queue: Input queue
            output_queue: Output queue
            queue_type: Queue class (queue.Queue or multiprocessing.Queue)

        Returns:
            Executor instance (ThreadExecutor, ProcessExecutor, or AsyncExecutor)

        Raises:
            ValueError: If stage.mode is unknown
        """
        if stage.mode == "thread":
            return ThreadExecutor(stage, input_queue, output_queue, queue_type)
        elif stage.mode == "process":
            return ProcessExecutor(stage, input_queue, output_queue, queue_type)
        elif stage.mode == "async":
            return AsyncExecutor(stage, input_queue, output_queue, queue_type)
        else:
            raise ValueError(f"Unknown mode: {stage.mode}")


def _flush_partial_batch(stage: Stage, batch_accumulator: list, output_queue) -> None:
    """Flush partial batch to output queue.

    Centralized batch flushing logic to ensure consistency.

    Args:
        stage: Stage configuration
        batch_accumulator: List of accumulated items
        output_queue: Queue to output the batch

    Raises:
        Exception: Any exception from output_queue.put() (e.g., pickling errors)

    Design Decision:
        Flush failures are now raised to caller (not swallowed). This ensures:
        - No silent data loss
        - Clear error messages (with exception chaining if applicable)
        - User knows exactly what failed
    """
    if stage.operation == Operation.BATCH and batch_accumulator:
        output_queue.put(batch_accumulator[:])  # Let exceptions propagate
        batch_accumulator.clear()


def _handle_adapter_sentinel(
    stage: Stage, sentinels_received: int, output_queue
) -> tuple[bool, int]:
    """Handle SENTINEL for ADAPTER operation.

    Adapters perform fan-in/fan-out:
    - Fan-in: Collect N SENTINELs from upstream workers
    - Fan-out: Output M SENTINELs to downstream workers

    Args:
        stage: Stage with adapter_info set
        sentinels_received: Count of SENTINELs received so far
        output_queue: Queue to output SENTINELs

    Returns:
        (should_exit, new_sentinels_received)
    """
    sentinels_received += 1
    adapter_info = stage.adapter_info

    # Have we received all upstream SENTINELs?
    if sentinels_received >= adapter_info.upstream_workers:
        # Yes - output downstream SENTINELs and exit
        for _ in range(adapter_info.downstream_workers):
            output_queue.put(SENTINEL)
        return (True, sentinels_received)  # Exit worker

    # No - continue collecting
    return (False, sentinels_received)  # Don't exit yet


class ThreadExecutor:
    """Execute stage using thread pool.

    Lifecycle:
      1. __init__(): Create executor, store configuration
      2. start(): Launch worker threads
      3. Workers process items until SENTINEL received
      4. stop(): Shutdown threads, cleanup queues, propagate exceptions

    Ownership:
      - Executor owns: worker threads, exception_queue
      - Caller owns: input_queue, output_queue (created by Pipeline)
      - Executor responsible for: cleaning up queues if multiprocessing type
    """

    def __init__(self, stage: Stage, input_queue, output_queue, queue_type: type):
        """Initialize thread executor.

        Args:
            stage: Stage configuration
            input_queue: Input queue (owned by caller)
            output_queue: Output queue (owned by caller)
            queue_type: Type of queues (queue.Queue or multiprocessing.Queue)
        """
        self.stage = stage
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.queue_type = queue_type  # Explicit type, no hasattr needed
        self.executor = ThreadPoolExecutor(max_workers=stage.workers)
        self.workers = []
        self.exception_queue = Queue()  # Thread-safe exception propagation (unbounded)
        self.running = True
        self._stopped = False  # Track if stop() already called (idempotent shutdown)
        # THREAD-SAFE: No lock needed because workers=1 enforced at __init__.py:188 (batch()).
        # Single worker means no concurrent access to this shared state.
        self._batch_accumulator = []  # For BATCH operation
        # THREAD-SAFE: No lock needed because workers=1 enforced at _runtime.py:117 (_create_adapter()).
        # Single worker means no concurrent access to this shared state.
        self._sentinels_received = 0  # For ADAPTER operation

    def __enter__(self):
        """Context manager entry - start workers."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self.stop()
        return False  # Don't suppress exceptions

    def start(self):
        """Start worker threads."""
        for _ in range(self.stage.workers):
            future = self.executor.submit(self._worker_loop)
            self.workers.append(future)

    def _initialize_func(self):
        """Initialize function or callable class instance.

        Returns:
            Callable instance ready for processing items

        Raises:
            Exception: Any exception from class initialization
        """
        func = self.stage.func
        if inspect.isclass(func):
            try:
                return func(*self.stage.init_args, **self.stage.init_kwargs)
            except Exception as e:
                raise type(e)(f"Failed to initialize callable class {func.__name__}: {e}") from e
        return func

    def _worker_loop(self):
        """Worker loop: get → process → put."""
        # Initialize function/class once per worker
        try:
            func_instance = self._initialize_func()
        except Exception as e:
            # Initialization failure - report and exit
            exc_info = sys.exc_info()
            tb_str = "".join(traceback.format_exception(*exc_info))
            try:
                self.exception_queue.put((e, tb_str), block=False)
            except Full:
                pass
            return

        try:
            while self.running:
                try:
                    item = self.input_queue.get(timeout=WORKER_QUEUE_TIMEOUT)
                except Empty:
                    continue

                if isinstance(item, _Sentinel):
                    # Flush partial batch before exiting
                    _flush_partial_batch(self.stage, self._batch_accumulator, self.output_queue)

                    # ADAPTER operation: fan-in/fan-out SENTINELs
                    if self.stage.operation == Operation.ADAPTER:
                        should_exit, self._sentinels_received = _handle_adapter_sentinel(
                            self.stage, self._sentinels_received, self.output_queue
                        )
                        if should_exit:
                            break
                        else:
                            continue  # Need more SENTINELs

                    # Normal operations: each worker outputs one SENTINEL
                    self.output_queue.put(SENTINEL)
                    break

                try:
                    result = self._process_item(item, func_instance)

                    # Handle operations that produce multiple outputs
                    if result is not None:
                        if self.stage.operation in (Operation.FLAT_MAP, Operation.UNBATCH):
                            # Flatten: put each item individually
                            for r in result:
                                self.output_queue.put(r)
                        else:
                            self.output_queue.put(result)

                except Exception as e:
                    # Capture exception with full traceback
                    exc_info = sys.exc_info()
                    tb_str = "".join(traceback.format_exception(*exc_info))
                    try:
                        self.exception_queue.put((e, tb_str), block=False)
                    except Full:
                        # Queue full, first exception already captured
                        logger.debug("Exception queue full, first exception takes precedence")
                    self.running = False

                    # Flush partial batch before exiting - chain flush exception if it fails
                    try:
                        _flush_partial_batch(self.stage, self._batch_accumulator, self.output_queue)
                    except Exception as flush_err:
                        logger.error(f"Failed to flush partial batch: {flush_err}")
                        raise e from flush_err  # Chain: primary exception, flush as context

                    raise  # Raise primary exception

        except Exception:
            # Exception already in queue
            pass

    def _process_item(self, item: Any, func: Callable) -> Any:
        """Process single item based on operation type.

        Args:
            item: Item to process
            func: Initialized function/class instance to use

        Returns:
            - For most operations: processed item or None
            - For BATCH: returns list when batch is complete, None otherwise
            - For UNBATCH: returns list items (handled by worker loop)
        """
        operation = self.stage.operation

        if operation == Operation.MAP:
            return func(item)

        elif operation == Operation.FLAT_MAP:
            return func(item)  # List returned, flattened by worker loop

        elif operation == Operation.FILTER:
            return item if func(item) else None

        elif operation == Operation.EACH:
            func(item)
            return item

        elif operation == Operation.BATCH:
            # Accumulate items
            self._batch_accumulator.append(item)
            if len(self._batch_accumulator) >= self.stage.batch_size:
                batch = self._batch_accumulator[:]
                self._batch_accumulator.clear()
                return batch  # Return full batch
            return None  # Not ready yet

        elif operation == Operation.UNBATCH:
            # Item should be a list, return it to be flattened
            return item if isinstance(item, list) else [item]

        elif operation == Operation.ADAPTER:
            # Adapter just forwards data
            return item

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def stop(self, check_exceptions=True):
        """Stop executor and cleanup resources.

        Shuts down thread pool, cleans up multiprocessing queues if applicable.

        Args:
            check_exceptions: If True, check and raise worker exceptions.
                            Set to False when exceptions already handled by caller.
        """
        # Idempotent: if already stopped, return immediately
        if self._stopped:
            return
        self._stopped = True

        self.running = False
        self.executor.shutdown(wait=True)

        # Cleanup queues if multiprocessing type (duck typing for robustness)
        cleanup_error = None
        if hasattr(self.input_queue, "close"):
            try:
                self.input_queue.close()
                self.input_queue.join_thread()
                self.output_queue.close()
                self.output_queue.join_thread()
            except Exception as e:
                logger.error(f"Error closing multiprocessing queues: {e}")
                cleanup_error = e

        # Check for worker exceptions if requested (atomic get to avoid race)
        worker_exception = None
        if check_exceptions:
            try:
                exc, tb_str = self.exception_queue.get_nowait()
                logger.error(f"Worker exception:\n{tb_str}")
                worker_exception = exc
            except Empty:
                pass  # No exception

        # Raise exceptions with proper priority and chaining
        if worker_exception:
            if cleanup_error:
                raise worker_exception from cleanup_error
            raise worker_exception
        elif cleanup_error:
            raise RuntimeError("Queue cleanup failed") from cleanup_error


class ProcessExecutor:
    """Execute stage using independent process workers.

    Each worker is a standalone multiprocessing.Process that directly
    communicates with queues (no intermediate threads).

    Lifecycle:
      1. __init__(): Create executor config
      2. start(): Spawn worker processes
      3. Workers process items until SENTINEL
      4. stop(): Join/terminate processes, cleanup queues

    Ownership:
      - Executor owns: worker processes, exception_queue
      - Caller owns: input/output queues
      - Executor responsible for: terminating processes, closing queues
    """

    def __init__(self, stage: Stage, input_queue, output_queue, queue_type: type):
        """Initialize process executor.

        Args:
            stage: Stage configuration
            input_queue: Input queue (must be multiprocessing.Queue)
            output_queue: Output queue (must be multiprocessing.Queue)
            queue_type: Queue type (should be multiprocessing.Queue)
        """
        self.stage = stage
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.queue_type = queue_type
        self.processes = []
        self.exception_queue = multiprocessing.Queue()  # Process-safe (unbounded)
        self._stopped = False  # Track if stop() already called (idempotent shutdown)

    def __enter__(self):
        """Context manager entry - start workers."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self.stop()
        return False  # Don't suppress exceptions

    def start(self):
        """Start independent process workers."""
        for _ in range(self.stage.workers):
            p = multiprocessing.Process(
                target=_process_worker_func,
                args=(self.stage, self.input_queue, self.output_queue, self.exception_queue),
            )
            p.start()
            self.processes.append(p)

    def stop(self, check_exceptions=True):
        """Stop processes and cleanup.

        Args:
            check_exceptions: If True, check and raise worker exceptions.
                            Set to False when exceptions already handled by caller.
        """
        # Idempotent: if already stopped, return immediately
        if self._stopped:
            return
        self._stopped = True

        # Wait for all processes to complete
        for p in self.processes:
            p.join(timeout=PROCESS_JOIN_TIMEOUT)
            if p.is_alive():
                # Process didn't terminate gracefully, send SIGTERM
                p.terminate()
                p.join(timeout=PROCESS_JOIN_TIMEOUT)

                # If still alive after SIGTERM, escalate to SIGKILL
                if p.is_alive():
                    logger.warning(f"Process {p.pid} didn't respond to SIGTERM, sending SIGKILL")
                    p.kill()
                    p.join()  # SIGKILL cannot be ignored, this will complete

        # Cleanup queues
        cleanup_error = None
        try:
            self.input_queue.close()
            self.input_queue.join_thread()
            self.output_queue.close()
            self.output_queue.join_thread()
            self.exception_queue.close()
            self.exception_queue.join_thread()
        except Exception as e:
            logger.error(f"Error during queue cleanup: {e}")
            cleanup_error = e

        # Check for worker exceptions if requested (atomic get to avoid race)
        worker_exception = None
        if check_exceptions:
            try:
                exc, tb_str = self.exception_queue.get_nowait()
                logger.error(f"Worker exception:\n{tb_str}")
                worker_exception = exc
            except Empty:
                pass  # No exception

        # Raise exceptions with proper priority and chaining
        if worker_exception:
            if cleanup_error:
                raise worker_exception from cleanup_error
            raise worker_exception
        elif cleanup_error:
            raise RuntimeError("Queue cleanup failed") from cleanup_error


def _initialize_func_for_process(stage: Stage):
    """Initialize function or callable class for process worker.

    Args:
        stage: Stage configuration

    Returns:
        Callable instance ready for processing

    Raises:
        Exception: Any exception from class initialization
    """
    func = stage.func
    if inspect.isclass(func):
        try:
            return func(*stage.init_args, **stage.init_kwargs)
        except Exception as e:
            raise type(e)(f"Failed to initialize callable class {func.__name__}: {e}") from e
    return func


def _process_worker_func(stage: Stage, input_queue, output_queue, exception_queue):
    """Process worker function (runs in separate process).

    This must be a module-level function for pickling.
    """
    import sys
    import traceback
    from queue import Empty

    # Initialize function/class once per worker process
    try:
        func_instance = _initialize_func_for_process(stage)
    except Exception as e:
        # Initialization failure - report and exit
        exc_info = sys.exc_info()
        tb_str = "".join(traceback.format_exception(*exc_info))
        try:
            exception_queue.put((e, tb_str), block=False)
        except Exception:
            pass
        return

    # Per-process state for BATCH and ADAPTER operations
    # THREAD-SAFE: No synchronization needed - each process has isolated memory space.
    # Multiple processes cannot access each other's local variables.
    batch_accumulator = []  # For BATCH operation
    sentinels_received = 0  # For ADAPTER operation

    try:
        while True:
            try:
                item = input_queue.get(timeout=WORKER_QUEUE_TIMEOUT)
            except Empty:
                continue

            if isinstance(item, _Sentinel):
                # Flush partial batch before exiting (let exceptions propagate)
                if stage.operation == Operation.BATCH and batch_accumulator:
                    output_queue.put(batch_accumulator[:])
                    batch_accumulator.clear()

                # ADAPTER operation: fan-in/fan-out SENTINELs
                if stage.operation == Operation.ADAPTER:
                    sentinels_received += 1
                    adapter_info = stage.adapter_info
                    # Have we received all upstream SENTINELs?
                    if sentinels_received >= adapter_info.upstream_workers:
                        # Yes - output downstream SENTINELs and exit
                        for _ in range(adapter_info.downstream_workers):
                            output_queue.put(SENTINEL)
                        break
                    else:
                        # No - continue collecting
                        continue

                # Normal operations: output SENTINEL and exit
                output_queue.put(SENTINEL)
                break

            try:
                result = _process_item(stage, item, batch_accumulator, func_instance)

                # Handle operations that produce multiple outputs
                if result is not None:
                    if stage.operation in (Operation.FLAT_MAP, Operation.UNBATCH):
                        for r in result:
                            output_queue.put(r)
                    else:
                        output_queue.put(result)

            except Exception:
                # Check if pickle error
                try:
                    import pickle

                    pickle.dumps(item)
                except Exception:
                    e = TypeError(
                        f"Cannot serialize {type(item).__name__} for process mode. "
                        f"Use mode='thread' or make object picklable."
                    )

                # Capture exception with traceback
                exc_info = sys.exc_info()
                tb_str = "".join(traceback.format_exception(*exc_info))
                try:
                    exception_queue.put((e, tb_str), block=False)
                except Exception as e:
                    logger.warning(f"Cleanup error: {e}")  # Queue full

                # Flush partial batch before exiting - if flush fails, chain exception
                try:
                    if stage.operation == Operation.BATCH and batch_accumulator:
                        output_queue.put(batch_accumulator[:])
                        batch_accumulator.clear()
                except Exception as flush_err:
                    # Update exception queue with chained exception
                    chained_msg = (
                        f"{tb_str}\n\nDuring exception handling, flush failed: {flush_err}"
                    )
                    try:
                        exception_queue.put((e, chained_msg), block=False)
                    except Exception:
                        pass  # Original exception already queued

                break  # Exit worker on exception

    except Exception:
        # All exceptions handled in inner try/except blocks
        pass


def _process_item(
    stage: Stage, item: Any, batch_accumulator: list = None, func: Callable = None
) -> Any:
    """Process single item (helper for process worker).

    Args:
        stage: Stage configuration
        item: Item to process
        batch_accumulator: For BATCH operation (optional)
        func: Initialized function/class instance to use

    Returns:
        Processed result or None
    """
    operation = stage.operation

    if operation == Operation.MAP or operation == Operation.FLAT_MAP:
        return func(item)
    elif operation == Operation.FILTER:
        return item if func(item) else None
    elif operation == Operation.EACH:
        func(item)
        return item
    elif operation == Operation.BATCH:
        # Accumulate items
        if batch_accumulator is not None:
            batch_accumulator.append(item)
            if len(batch_accumulator) >= stage.batch_size:
                batch = batch_accumulator[:]
                batch_accumulator.clear()
                return batch  # Return full batch
        return None  # Not ready yet
    elif operation == Operation.UNBATCH:
        # Flatten batches back to individual items
        return item if isinstance(item, list) else [item]
    elif operation == Operation.ADAPTER:
        # Adapter just forwards data unchanged
        return item
    else:
        return item


class AsyncExecutor:
    """Execute stage using asyncio workers.

    Runs event loop in dedicated thread with async worker coroutines.
    Workers access sync queues directly (blocking calls acceptable in async context
    when combined with sleep for responsiveness).

    Lifecycle:
      1. __init__(): Create executor config
      2. start(): Spawn event loop thread with worker tasks
      3. Workers process items asynchronously
      4. stop(): Cancel tasks, stop loop, cleanup

    Ownership:
      - Executor owns: event loop, loop thread, worker tasks, exception_queue
      - Caller owns: input/output queues
    """

    def __init__(self, stage: Stage, input_queue, output_queue, queue_type: type):
        """Initialize async executor.

        Args:
            stage: Stage configuration (func must be coroutine)
            input_queue: Input queue
            output_queue: Output queue
            queue_type: Queue type
        """
        self.stage = stage
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.queue_type = queue_type
        self.running = True
        self.exception_queue = Queue()  # Thread-safe (unbounded)
        self._stopped = False  # Track if stop() already called (idempotent shutdown)
        self.loop = None
        self.loop_thread = None
        self.worker_tasks = []  # Store async tasks for cancellation
        self.loop_ready = threading.Event()  # Signal when loop is ready

    def __enter__(self):
        """Context manager entry - start workers."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self.stop()
        return False  # Don't suppress exceptions

    def start(self):
        """Start event loop in background thread."""
        # Validate function is coroutine (or class with async __call__)
        func = self.stage.func
        if inspect.isclass(func):
            # For classes, check if __call__ is async
            if not inspect.iscoroutinefunction(getattr(func, "__call__", None)):
                raise TypeError(
                    f"Callable class {func.__name__} must have async __call__ method for mode='async'"
                )
        elif not inspect.iscoroutinefunction(func):
            func_name = getattr(func, "__name__", repr(func))
            raise TypeError(
                f"Function must be async (coroutine) for mode='async'. "
                f"Got: {func_name} ({type(func).__name__})"
            )

        # Start event loop in thread
        self.loop_thread = threading.Thread(target=self._run_loop)
        self.loop_thread.daemon = True
        self.loop_thread.start()

        # Wait for loop to be ready (with timeout)
        if not self.loop_ready.wait(timeout=LOOP_READY_TIMEOUT):
            raise RuntimeError(f"Failed to start async event loop within {LOOP_READY_TIMEOUT}s")

    def _run_loop(self):
        """Run event loop forever in thread."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop_ready.set()  # Signal that loop is ready

        try:
            # Start async main coroutine
            self.loop.run_until_complete(self._async_main())
        except Exception as e:
            # Capture exception with traceback
            exc_info = sys.exc_info()
            tb_str = "".join(traceback.format_exception(*exc_info))
            try:
                self.exception_queue.put((e, tb_str), block=False)
            except Exception as e:
                logger.warning(f"Cleanup error: {e}")
        finally:
            self.loop.close()

    def _initialize_func(self):
        """Initialize function or callable class instance.

        Returns:
            Callable instance ready for processing items

        Raises:
            Exception: Any exception from class initialization
        """
        func = self.stage.func
        if inspect.isclass(func):
            try:
                return func(*self.stage.init_args, **self.stage.init_kwargs)
            except Exception as e:
                raise type(e)(f"Failed to initialize callable class {func.__name__}: {e}") from e
        return func

    async def _async_main(self):
        """Main async coroutine."""
        # Create N async worker tasks
        self.worker_tasks = [
            asyncio.create_task(self._async_worker()) for _ in range(self.stage.workers)
        ]

        try:
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            # Clean up cancelled tasks
            for task in self.worker_tasks:
                if not task.done():
                    task.cancel()
            # Wait for cancellations to complete with timeout
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.worker_tasks, return_exceptions=True),
                    timeout=LOOP_SHUTDOWN_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning(f"Task cancellation exceeded {LOOP_SHUTDOWN_TIMEOUT}s timeout")

    async def _async_worker(self):
        """Async worker coroutine - reads from sync queue via thread."""
        # Initialize function/class once per worker
        try:
            func_instance = self._initialize_func()
        except Exception as e:
            # Initialization failure - report and exit
            exc_info = sys.exc_info()
            tb_str = "".join(traceback.format_exception(*exc_info))
            try:
                self.exception_queue.put((e, tb_str), block=False)
            except Full:
                pass
            return

        try:
            while True:  # Don't check self.running - use cancellation instead
                # Read from sync queue in non-blocking way
                try:
                    item = self.input_queue.get(timeout=ASYNC_QUEUE_TIMEOUT)
                except Empty:
                    await asyncio.sleep(ASYNC_SLEEP_DURATION)
                    continue

                if isinstance(item, _Sentinel):
                    # Output SENTINEL and exit
                    self.output_queue.put(SENTINEL)
                    break

                try:
                    # Process item asynchronously
                    result = await self._process_item_async(item, func_instance)

                    # Handle operations that produce multiple outputs
                    if result is not None:
                        if self.stage.operation in (Operation.FLAT_MAP, Operation.UNBATCH):
                            for r in result:
                                self.output_queue.put(r)
                        else:
                            self.output_queue.put(result)

                except Exception as e:
                    # Capture exception with traceback
                    exc_info = sys.exc_info()
                    tb_str = "".join(traceback.format_exception(*exc_info))
                    try:
                        self.exception_queue.put((e, tb_str), block=False)
                    except Full:
                        logger.debug("Exception queue full")
                    raise

        except asyncio.CancelledError:
            # Clean cancellation - output SENTINEL before exiting
            self.output_queue.put(SENTINEL)
            raise

    async def _process_item_async(self, item: Any, func: Callable) -> Any:
        """Process item using async function.

        Args:
            item: Item to process
            func: Initialized async function/class instance

        Returns:
            Processed result or None
        """
        operation = self.stage.operation

        if operation == Operation.MAP or operation == Operation.FLAT_MAP:
            return await func(item)
        elif operation == Operation.FILTER:
            if await func(item):
                return item
            return None
        elif operation == Operation.EACH:
            await func(item)
            return item
        elif operation == Operation.ADAPTER:
            return item
        else:
            # NOTE: BATCH/UNBATCH operations never reach AsyncExecutor because batch() and
            # unbatch() hardcode mode='thread' at __init__.py:189 and :209.
            # This else clause only handles unknown operations.
            return item

    def stop(self, check_exceptions=True):
        """Stop async executor and cleanup resources.

        Cancels all async tasks and waits for event loop to exit naturally.
        The loop stops automatically when _async_main() completes after task cancellation.

        Args:
            check_exceptions: If True, check and raise worker exceptions.
                            Set to False when exceptions already handled by caller.
        """
        # Idempotent: if already stopped, return immediately
        if self._stopped:
            return
        self._stopped = True

        self.running = False

        # Cancel all async tasks using call_soon_threadsafe (only if loop still running)
        if self.loop and not self.loop.is_closed() and self.worker_tasks:
            for task in self.worker_tasks:
                try:
                    self.loop.call_soon_threadsafe(task.cancel)
                except RuntimeError:
                    # Loop already closed, tasks already done
                    pass

        # Wait for loop thread to finish
        # NOTE: We don't call loop.stop() explicitly because:
        # 1. Tasks are cancelled above
        # 2. _async_main() handles CancelledError and completes gather()
        # 3. Loop exits naturally when _async_main() returns
        # Calling loop.stop() prematurely causes "Event loop stopped before Future completed"
        if self.loop_thread and self.loop_thread.is_alive():
            self.loop_thread.join(timeout=LOOP_SHUTDOWN_TIMEOUT)

        # Cleanup queues if multiprocessing type (type-safe)
        cleanup_error = None
        if self.queue_type == multiprocessing.Queue:
            try:
                self.input_queue.close()
                self.input_queue.join_thread()
                self.output_queue.close()
                self.output_queue.join_thread()
            except Exception as e:
                logger.error(f"Error closing multiprocessing queues: {e}")
                cleanup_error = e

        # Check for worker exceptions if requested (atomic get to avoid race)
        worker_exception = None
        if check_exceptions:
            try:
                exc, tb_str = self.exception_queue.get_nowait()
                logger.error(f"Worker exception:\n{tb_str}")
                worker_exception = exc
            except Empty:
                pass  # No exception

        # Raise exceptions with proper priority and chaining
        if worker_exception:
            if cleanup_error:
                raise worker_exception from cleanup_error
            raise worker_exception
        elif cleanup_error:
            raise RuntimeError("Queue cleanup failed") from cleanup_error
