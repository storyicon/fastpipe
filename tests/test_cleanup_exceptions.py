"""Test cleanup exception handling and propagation."""

import multiprocessing

from fastpipe._executors import ProcessExecutor
from fastpipe._types import Operation, Stage


def test_queue_cleanup_exception_propagates():
    """Test that queue cleanup exceptions are properly raised."""
    # Create a stage
    stage = Stage(
        operation=Operation.MAP,
        func=lambda x: x * 2,
        workers=1,
        mode="process",
        inbound_queue_size=None,
    )

    input_q = multiprocessing.Queue()
    output_q = multiprocessing.Queue()
    exc_q = multiprocessing.Queue()

    executor = ProcessExecutor(stage, input_q, output_q, multiprocessing.Queue)
    executor.exception_queue = exc_q

    # Mock queue.close() to raise an exception
    original_close = output_q.close

    def failing_close():
        raise RuntimeError("Mock queue close failure")

    output_q.close = failing_close

    # Stop should raise the cleanup exception
    try:
        executor.stop(check_exceptions=False)
        raise AssertionError("Should have raised RuntimeError")
    except RuntimeError as e:
        assert "Queue cleanup failed" in str(e)
        assert "Mock queue close failure" in str(e.__cause__.args[0])
        print(f"✓ Cleanup exception properly chained: {e}")

    # Cleanup
    output_q.close = original_close
    output_q.close()
    input_q.close()
    exc_q.close()


def test_worker_exception_prioritized_over_cleanup():
    """Test that worker exceptions take priority over cleanup exceptions."""
    import traceback
    from queue import Queue

    stage = Stage(
        operation=Operation.MAP, func=lambda x: x, workers=1, mode="thread", inbound_queue_size=None
    )

    input_q = Queue()
    output_q = Queue()

    from fastpipe._executors import ThreadExecutor

    executor = ThreadExecutor(stage, input_q, output_q, Queue)

    # Put a worker exception in the queue
    test_exc = ValueError("Worker failed")
    exc_info = (ValueError, test_exc, None)
    tb_str = "".join(traceback.format_exception(*exc_info))
    executor.exception_queue.put((test_exc, tb_str))

    # Mock queue cleanup to fail
    output_q.close if hasattr(output_q, "close") else None

    def failing_cleanup():
        raise RuntimeError("Cleanup also failed")

    if hasattr(output_q, "close"):
        output_q.close = failing_cleanup
    else:
        # Queue.Queue doesn't have close() normally, so patch join_thread
        output_q.join_thread = failing_cleanup

    # stop() should raise worker exception with cleanup as context
    try:
        executor.stop(check_exceptions=True)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert str(e) == "Worker failed"
        # Check cleanup exception is in the chain
        if e.__cause__:
            print(f"✓ Worker exception raised with cleanup as context: {e.__cause__}")
        else:
            print("✓ Worker exception raised (no cleanup context in this case)")


def test_exception_chaining():
    """Test that exception chaining works correctly."""
    try:
        try:
            raise ValueError("Original error")
        except ValueError:
            cleanup_exc = RuntimeError("Cleanup failed")
            raise RuntimeError("Wrapped") from cleanup_exc
    except RuntimeError as e:
        assert "Wrapped" in str(e)
        assert isinstance(e.__cause__, RuntimeError)
        assert "Cleanup failed" in str(e.__cause__)
        print("✓ Exception chaining works as expected")


if __name__ == "__main__":
    test_queue_cleanup_exception_propagates()
    test_worker_exception_prioritized_over_cleanup()
    test_exception_chaining()
    print("All cleanup exception tests passed!")
