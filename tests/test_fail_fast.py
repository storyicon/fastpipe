"""Tests for fail-fast exception handling."""

import threading
import time


import fastpipe


def test_exception_stops_pipeline_immediately():
    """Test that first exception stops entire pipeline (fail-fast)."""
    processed_count = threading.Lock()
    processed_items = []

    def failing_func(x):
        with processed_count:
            processed_items.append(x)

        if x == 5:
            raise ValueError("Intentional failure at 5")

        time.sleep(0.05)  # Slow processing
        return x * 2

    pipe = fastpipe.create().map(failing_func, workers=3)

    try:
        pipe.run(range(20))
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "Intentional failure at 5" in str(e)

    # Pipeline should stop quickly, not process all 20 items
    with processed_count:
        print(f"Processed {len(processed_items)} items before stopping")
        # Should be << 20 items due to fail-fast
        assert len(processed_items) < 15, (
            f"Pipeline didn't fail-fast, processed {len(processed_items)}/20 items"
        )


def test_exception_in_iter_propagates_immediately():
    """Test that exception in iter() propagates to consumer."""

    def failing_func(x):
        if x == 3:
            raise RuntimeError("Fail at 3")
        return x

    pipe = fastpipe.create().map(failing_func, workers=2)

    results = []
    try:
        for item in pipe.iter(range(10)):
            results.append(item)
    except RuntimeError as e:
        assert "Fail at 3" in str(e)

    # Should have collected some items before failure
    print(f"Collected {len(results)} items before exception")
    assert len(results) < 10, "Should not process all items after exception"


def test_stream_detects_worker_exception():
    """Test that PipelineStream.get() and put() detect worker exceptions."""

    def failing_func(x):
        if x == 2:
            raise ValueError("Fail at 2")
        time.sleep(0.01)
        return x * 2

    pipe = fastpipe.create().map(failing_func, workers=2)
    stream = pipe.stream()

    # Put some items
    stream.put(1)
    stream.put(2)  # This will cause failure
    stream.put(3)

    # Get first result (should work)
    result1 = stream.get(timeout=1.0)
    assert result1 == 2

    # Next get should detect worker exception
    try:
        for _ in range(5):
            stream.get(timeout=0.5)
    except (ValueError, RuntimeError) as e:
        # Should detect the worker exception
        print(f"Detected exception: {e}")
        assert "Fail at 2" in str(e) or "Pipeline has failed" in str(e)
    else:
        raise AssertionError("Should have raised exception")

    stream.close(drain=False)


def test_multiple_workers_first_exception_propagates():
    """Test with multiple workers, first exception propagates."""
    exceptions_raised = []

    def sometimes_fail(x):
        if x % 3 == 0 and x > 0:
            exc = ValueError(f"Fail at {x}")
            exceptions_raised.append(exc)
            raise exc
        return x

    pipe = fastpipe.create().map(sometimes_fail, workers=4)

    try:
        pipe.run(range(20))
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        print(f"Caught exception: {e}")
        # At least one exception should have been raised
        assert len(exceptions_raised) >= 1


if __name__ == "__main__":
    test_exception_stops_pipeline_immediately()
    test_exception_in_iter_propagates_immediately()
    test_stream_detects_worker_exception()
    test_multiple_workers_first_exception_propagates()
    print("All fail-fast tests passed!")
