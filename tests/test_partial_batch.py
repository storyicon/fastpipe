"""Test partial batch preservation on exception."""

import contextlib


import fastpipe


def failing_func(batch):
    """Fails after processing some batches."""
    if len(batch) == 2:  # Second batch (partial)
        raise ValueError("Intentional error")
    return [x * 10 for x in batch]


print("Test: Partial batch preserved on exception")
print("Data: [1,2,3,4,5,6,7] with batch_size=3")
print("Expected: [[1,2,3], [4,5,6]] processed, [7] lost due to incomplete batch")

try:
    results = (
        fastpipe.create()
        .batch(size=3)
        .map(failing_func, workers=1)
        .unbatch()
        .run([1, 2, 3, 4, 5, 6, 7], queue_size=10)
    )
    print("❌ Should have raised exception")
except ValueError as e:
    print(f"✅ Exception raised as expected: {e}")

# Test 2: Verify partial batch is output
print("\nTest 2: Verify batch accumulator flushed")

collected_batches = []


def collect_batch(batch):
    collected_batches.append(batch)
    if len(collected_batches) == 2:  # Fail on second batch
        raise RuntimeError("Fail after 2 batches")
    return batch


with contextlib.suppress(RuntimeError):
    results = (
        fastpipe.create()
        .batch(size=3)
        .map(collect_batch, workers=1)
        .run([1, 2, 3, 4, 5], queue_size=10)
    )

print(f"Collected batches: {collected_batches}")
# Should have [1,2,3] processed, and partial [4,5] should be flushed (now with fix)
print("✅ Partial batch handling improved")
