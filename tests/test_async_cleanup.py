"""Test AsyncExecutor cleanup and lifecycle."""

import asyncio
import time


import fastpipe


async def async_process(x):
    """Async processing function."""
    await asyncio.sleep(0.01)
    return x * 2


def test_async_executor_loop_cleanup():
    """Test that AsyncExecutor properly closes event loop."""
    pipe = fastpipe.create().map(async_process, workers=3, mode="async")

    results = pipe.run([1, 2, 3, 4, 5])

    assert sorted(results) == [2, 4, 6, 8, 10]

    # Give time for cleanup
    time.sleep(0.5)

    # Check no extra event loops running
    # (hard to test directly, but at least verify it completes without hanging)
    print("✓ AsyncExecutor loop cleanup completed")


async def async_failing(x):
    """Async function that fails."""
    if x == 3:
        raise ValueError("Async fail at 3")
    return x * 2


def test_async_executor_exception_cleanup():
    """Test AsyncExecutor cleans up properly after exception."""
    pipe = fastpipe.create().map(async_failing, workers=2, mode="async")

    try:
        pipe.run(range(10))
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "Async fail at 3" in str(e)

    print("✓ AsyncExecutor exception cleanup completed")


def test_async_context_manager():
    """Test AsyncExecutor with context manager."""
    import queue

    from fastpipe._executors import AsyncExecutor
    from fastpipe._types import Operation, Stage

    async def async_double(x):
        return x * 2

    stage = Stage(
        operation=Operation.MAP, func=async_double, workers=2, mode="async", inbound_queue_size=None
    )

    input_q = queue.Queue()
    output_q = queue.Queue()

    # Use context manager
    with AsyncExecutor(stage, input_q, output_q, queue.Queue):
        # Put some data
        for i in range(3):
            input_q.put(i)

        # Signal end
        for _ in range(2):
            input_q.put(fastpipe._runtime.SENTINEL)

        # Wait a bit for async processing
        time.sleep(0.5)

    # Context manager should have cleaned up
    print("✓ AsyncExecutor context manager works")


if __name__ == "__main__":
    test_async_executor_loop_cleanup()
    test_async_executor_exception_cleanup()
    test_async_context_manager()
    print("All AsyncExecutor cleanup tests passed!")
