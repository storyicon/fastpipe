"""Test BATCH operation with ProcessExecutor.

Note: Multi-worker process mode does NOT guarantee order.
Tests check data completeness, not order.
"""

import sys
from pathlib import Path

# Add parent directory to enable 'import fastpipe'
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastpipe


def test_basic_batch_with_process_mode():
    """Test basic BATCH with process mode (order not guaranteed)."""
    data = list(range(1, 11))  # [1, 2, 3, ..., 10]
    pipe = fastpipe.create().map(lambda x: x, workers=2, mode="process").batch(size=3)

    results = pipe.run(data)

    # Check: correct number of batches
    assert len(results) == 4, f"Expected 4 batches, got {len(results)}"

    # Check: all data present (order doesn't matter with multiple workers)
    flattened = [item for batch in results for item in batch]
    assert sorted(flattened) == sorted(data), f"Data mismatch: {flattened} vs {data}"

    # Check: batch sizes correct (3, 3, 3, 1)
    batch_sizes = [len(batch) for batch in results]
    assert sorted(batch_sizes) == [1, 3, 3, 3], f"Batch sizes wrong: {batch_sizes}"


def test_process_batch_process_pipeline():
    """Test Process mode -> BATCH -> Process mode (with adapters)."""
    data = list(range(1, 9))  # [1, 2, 3, ..., 8]
    pipe = (
        fastpipe.create()
        .map(lambda x: x * 2, workers=2, mode="process")
        .batch(size=4)
        .map(lambda batch: sum(batch), workers=2, mode="process")
    )

    results = pipe.run(data)

    # Check: correct number of batches
    assert len(results) == 2, f"Expected 2 sums, got {len(results)}"

    # Check: total sum is correct (order doesn't matter)
    # [1,2,3,4,5,6,7,8] * 2 = [2,4,6,8,10,12,14,16], sum = 72
    total_sum = sum(results)
    expected_sum = sum(x * 2 for x in data)
    assert total_sum == expected_sum, f"Sum mismatch: {total_sum} vs {expected_sum}"


def test_partial_batch_handling():
    """Test partial batch handling at end."""
    data = list(range(1, 6))  # [1, 2, 3, 4, 5]
    pipe = fastpipe.create().map(lambda x: x, workers=2, mode="process").batch(size=3)

    results = pipe.run(data)

    # Check: correct number of batches
    assert len(results) == 2, f"Expected 2 batches, got {len(results)}"

    # Check: all data present
    flattened = [item for batch in results for item in batch]
    assert sorted(flattened) == sorted(data), f"Data mismatch: {flattened} vs {data}"

    # Check: batch sizes (3 and 2, but order may vary)
    batch_sizes = sorted([len(batch) for batch in results])
    assert batch_sizes == [2, 3], f"Batch sizes wrong: {batch_sizes}"
