"""Tests for resource lifecycle management and cleanup."""

import threading
import time

import sys
from pathlib import Path

# Add parent directory to enable 'import fastpipe'
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastpipe


def test_feeder_thread_cleanup_normal():
    """Test that feeder thread terminates normally in iter()."""

    def process(x):
        return x * 2

    pipe = fastpipe.create().map(process, workers=2)

    # Consume iterator fully
    results = list(pipe.iter([1, 2, 3]))

    assert results == [2, 4, 6]

    # Give threads time to cleanup
    time.sleep(0.2)

    # Verify no threads leaked
    thread_names = [t.name for t in threading.enumerate()]
    assert not any("feeder" in name.lower() for name in thread_names), (
        f"Feeder thread still alive: {thread_names}"
    )


def test_feeder_thread_cleanup_early_termination():
    """Test feeder thread cleanup when breaking early from iter()."""

    def slow_process(x):
        time.sleep(0.1)
        return x

    pipe = fastpipe.create().map(slow_process, workers=1)

    # Break early from iteration
    count = 0
    for _result in pipe.iter(range(100)):
        count += 1
        if count >= 3:
            break  # Early termination

    assert count == 3

    # Give cleanup time
    time.sleep(1.0)

    # Verify cleanup completed
    thread_names = [t.name for t in threading.enumerate()]
    assert not any("feeder" in name.lower() for name in thread_names), (
        f"Feeder thread still alive after early break: {thread_names}"
    )


def test_context_manager_thread_executor():
    """Test ThreadExecutor context manager protocol."""
    import queue

    from fastpipe._executors import ThreadExecutor
    from fastpipe._types import Operation, Stage

    stage = Stage(
        operation=Operation.MAP,
        func=lambda x: x * 2,
        workers=2,
        mode="thread",
        inbound_queue_size=None,
    )

    input_q = queue.Queue()
    output_q = queue.Queue()

    # Use context manager
    with ThreadExecutor(stage, input_q, output_q, queue.Queue):
        # Put some data
        for i in range(5):
            input_q.put(i)

        # Signal end
        for _ in range(2):
            input_q.put(fastpipe._runtime.SENTINEL)

        # Collect results
        results = []
        sentinels = 0
        while sentinels < 2:
            item = output_q.get(timeout=1.0)
            if isinstance(item, fastpipe._runtime._Sentinel):
                sentinels += 1
            else:
                results.append(item)

    assert sorted(results) == [0, 2, 4, 6, 8]


def test_idempotent_shutdown():
    """Test that calling stop() multiple times is safe."""
    import queue

    from fastpipe._executors import ThreadExecutor
    from fastpipe._types import Operation, Stage

    stage = Stage(
        operation=Operation.MAP,
        func=lambda x: x * 2,
        workers=1,
        mode="thread",
        inbound_queue_size=None,
    )

    input_q = queue.Queue()
    output_q = queue.Queue()

    executor = ThreadExecutor(stage, input_q, output_q, queue.Queue)
    executor.start()

    # Stop multiple times - should be safe
    executor.stop()
    executor.stop()
    executor.stop()

    # Should not raise or hang


if __name__ == "__main__":
    test_feeder_thread_cleanup_normal()
    test_feeder_thread_cleanup_early_termination()
    test_context_manager_thread_executor()
    test_idempotent_shutdown()
    print("All resource cleanup tests passed!")
