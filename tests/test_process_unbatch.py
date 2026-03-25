"""Test UNBATCH operation with ProcessExecutor."""

import sys
from pathlib import Path

# Add parent directory to enable 'import fastpipe'
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastpipe

print("=" * 70)
print("Testing ProcessExecutor UNBATCH Operation")
print("=" * 70)

# Test 1: Basic UNBATCH with process mode
print("\nTest 1: Basic UNBATCH with process mode")
data = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]  # Batched data
pipe = fastpipe.create().unbatch().map(lambda x: x * 2, workers=2, mode="process")

results = pipe.run(data)
results.sort()
print(f"Input (batched): {data}")
print(f"Results (unbatched): {sorted(results)}")

# Expected: [1,2,3,4,5,6,7,8,9] -> [2,4,6,8,10,12,14,16,18]
expected = [2, 4, 6, 8, 10, 12, 14, 16, 18]
assert sorted(results) == expected, f"Expected {expected}, got {sorted(results)}"
print("✓ Basic UNBATCH works!")

# Test 2: BATCH -> UNBATCH round-trip with process mode
print("\nTest 2: BATCH -> UNBATCH round-trip")
data = list(range(1, 11))  # [1,2,3,...,10]
pipe = (
    fastpipe.create()
    .map(lambda x: x, workers=2, mode="process")
    .batch(size=3)
    .unbatch()
    .map(lambda x: x + 100, workers=2, mode="process")
)

results = pipe.run(data)
results.sort()
print(f"Input: {data}")
print(f"Results: {sorted(results)}")

# Expected: [1,2,...,10] -> batched -> unbatched -> [101,102,...,110]
expected = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110]
assert sorted(results) == expected, f"Expected {expected}, got {sorted(results)}"
print("✓ BATCH->UNBATCH round-trip works!")

# Test 3: UNBATCH with mixed modes
print("\nTest 3: UNBATCH with mode transitions")
data = [[1, 2], [3, 4, 5]]
pipe = (
    fastpipe.create()
    .unbatch()
    .map(lambda x: x * 10, workers=2, mode="thread")
    .map(lambda x: x + 5, workers=2, mode="process")
)

results = pipe.run(data)
results.sort()
print(f"Input: {data}")
print(f"Results: {sorted(results)}")

# Expected: [1,2,3,4,5] -> [10,20,30,40,50] -> [15,25,35,45,55]
expected = [15, 25, 35, 45, 55]
assert sorted(results) == expected, f"Expected {expected}, got {sorted(results)}"
print("✓ UNBATCH with mode transitions works!")

print("\n" + "=" * 70)
print("✅ All ProcessExecutor UNBATCH tests passed!")
print("=" * 70)
