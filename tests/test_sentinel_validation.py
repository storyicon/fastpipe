"""Test SENTINEL count validation and edge cases."""

import sys
from pathlib import Path

# Add parent directory to enable 'import fastpipe'
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastpipe


def test_adapter_sentinel_count():
    """Test that adapters correctly fan-in and fan-out SENTINELs."""

    def thread_func(x):
        return x * 2

    async def async_func(x):
        return x + 1

    # Mode transition: thread -> async -> thread
    # Should insert 2 adapters
    pipe = (
        fastpipe.create()
        .map(thread_func, workers=3, mode="thread")
        .map(async_func, workers=2, mode="async")
        .map(lambda x: x * 3, workers=4, mode="thread")
    )

    results = pipe.run([1, 2, 3])

    # Verify results are correct (indicates SENTINEL flow worked)
    expected = [(((x * 2) + 1) * 3) for x in [1, 2, 3]]
    assert sorted(results) == sorted(expected)

    print("✓ Adapter SENTINEL fan-in/fan-out works correctly")


def test_multiple_mode_transitions():
    """Test pipeline with multiple mode transitions."""

    def double(x):
        return x * 2

    pipe = (
        fastpipe.create()
        .map(double, workers=2, mode="thread")
        .map(double, workers=1, mode="process")
        .map(double, workers=3, mode="thread")
    )

    results = pipe.run([1, 2, 3])

    expected = [8, 16, 24]  # x * 2 * 2 * 2
    assert sorted(results) == expected

    print("✓ Multiple mode transitions work correctly")


def test_sentinel_with_batch_unbatch():
    """Test SENTINEL handling with BATCH and UNBATCH operations."""

    def process(x):
        return x * 2

    pipe = fastpipe.create().map(process, workers=2).batch(size=3).unbatch()

    results = pipe.run(range(10))

    expected = [x * 2 for x in range(10)]
    assert sorted(results) == sorted(expected)

    print("✓ SENTINEL handling with BATCH/UNBATCH works")


def test_partial_batch_sentinel_flush():
    """Test that partial batch is flushed when SENTINEL arrives."""

    def identity(x):
        return x

    pipe = fastpipe.create().map(identity, workers=1).batch(size=10)  # Batch size larger than input

    results = pipe.run([1, 2, 3, 4, 5])  # Only 5 items

    # Should get one batch with 5 items (partial batch flushed)
    assert len(results) == 1
    assert results[0] == [1, 2, 3, 4, 5]

    print("✓ Partial batch is flushed on SENTINEL")


if __name__ == "__main__":
    test_adapter_sentinel_count()
    test_multiple_mode_transitions()
    test_sentinel_with_batch_unbatch()
    test_partial_batch_sentinel_flush()
    print("All SENTINEL validation tests passed!")
