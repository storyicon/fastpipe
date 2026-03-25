"""Test ExecutorFactory dependency injection."""

import queue


import fastpipe
from fastpipe._executors import ExecutorFactory
from fastpipe._types import Operation, Stage


class MockExecutor:
    """Mock executor for testing."""

    def __init__(self, stage, input_queue, output_queue, queue_type):
        self.stage = stage
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self, check_exceptions=True):
        self.stopped = True

    @property
    def exception_queue(self):
        """Return empty queue."""
        return queue.Queue()


class MockFactory:
    """Mock executor factory."""

    def __init__(self):
        self.executors_created = []

    def create(self, stage, input_queue, output_queue, queue_type):
        executor = MockExecutor(stage, input_queue, output_queue, queue_type)
        self.executors_created.append(executor)
        return executor


def test_pipeline_uses_custom_factory():
    """Test that Pipeline uses provided executor_factory."""
    factory = MockFactory()

    pipe = fastpipe.Pipeline(executor_factory=factory)
    pipe.map(lambda x: x * 2, workers=2)

    # Note: Can't easily test run() with mock executors as it needs real queue processing
    # This test verifies the factory is stored
    assert pipe._executor_factory is factory


def test_default_factory_creates_real_executors():
    """Test that default factory creates real executors."""
    factory = ExecutorFactory()

    stage = Stage(
        operation=Operation.MAP,
        func=lambda x: x * 2,
        workers=1,
        mode="thread",
        inbound_queue_size=None,
    )

    q1 = queue.Queue()
    q2 = queue.Queue()

    executor = factory.create(stage, q1, q2, queue.Queue)

    from fastpipe._executors import ThreadExecutor

    assert isinstance(executor, ThreadExecutor)
    assert executor.stage is stage


def test_factory_creates_correct_executor_types():
    """Test factory creates correct executor for each mode."""
    import multiprocessing

    from fastpipe._executors import AsyncExecutor, ProcessExecutor, ThreadExecutor

    factory = ExecutorFactory()

    # Thread executor
    stage_thread = Stage(
        operation=Operation.MAP, func=lambda x: x, workers=1, mode="thread", inbound_queue_size=None
    )
    exec_thread = factory.create(stage_thread, queue.Queue(), queue.Queue(), queue.Queue)
    assert isinstance(exec_thread, ThreadExecutor)

    # Process executor
    stage_process = Stage(
        operation=Operation.MAP,
        func=lambda x: x,
        workers=1,
        mode="process",
        inbound_queue_size=None,
    )
    exec_process = factory.create(
        stage_process, multiprocessing.Queue(), multiprocessing.Queue(), multiprocessing.Queue
    )
    assert isinstance(exec_process, ProcessExecutor)

    # Async executor
    async def async_func(x):
        return x

    stage_async = Stage(
        operation=Operation.MAP, func=async_func, workers=1, mode="async", inbound_queue_size=None
    )
    exec_async = factory.create(stage_async, queue.Queue(), queue.Queue(), queue.Queue)
    assert isinstance(exec_async, AsyncExecutor)

    print("✓ Factory creates correct executor types for each mode")


if __name__ == "__main__":
    test_pipeline_uses_custom_factory()
    test_default_factory_creates_real_executors()
    test_factory_creates_correct_executor_types()
    print("All executor factory tests passed!")
