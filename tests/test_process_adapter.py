"""Test ADAPTER operation with ProcessExecutor (implicit via mode transitions)."""


import fastpipe

print("=" * 70)
print("Testing ProcessExecutor ADAPTER Operation (Mode Transitions)")
print("=" * 70)

# Test 1: Thread -> Process transition (ADAPTER inserted automatically)
print("\nTest 1: Thread -> Process transition")
data = list(range(1, 6))  # [1, 2, 3, 4, 5]
pipe = (
    fastpipe.create()
    .map(lambda x: x * 2, workers=3, mode="thread")
    .map(lambda x: x + 1, workers=2, mode="process")
)  # Mode change -> ADAPTER inserted

results = pipe.run(data)
results.sort()  # Results may be out of order due to parallelism
print(f"Input: {data}")
print(f"Results: {sorted(results)}")

# Expected: [1,2,3,4,5] -> [2,4,6,8,10] -> [3,5,7,9,11]
expected = [3, 5, 7, 9, 11]
assert sorted(results) == expected, f"Expected {expected}, got {sorted(results)}"
print("✓ Thread->Process transition works!")

# Test 2: Process -> Thread transition
print("\nTest 2: Process -> Thread transition")
data = list(range(1, 6))
pipe = (
    fastpipe.create()
    .map(lambda x: x * 3, workers=2, mode="process")
    .map(lambda x: x - 1, workers=3, mode="thread")
)  # Mode change -> ADAPTER inserted

results = pipe.run(data)
results.sort()
print(f"Input: {data}")
print(f"Results: {sorted(results)}")

# Expected: [1,2,3,4,5] -> [3,6,9,12,15] -> [2,5,8,11,14]
expected = [2, 5, 8, 11, 14]
assert sorted(results) == expected, f"Expected {expected}, got {sorted(results)}"
print("✓ Process->Thread transition works!")

# Test 3: Multiple mode transitions
print("\nTest 3: Multiple mode transitions")
data = list(range(1, 5))
pipe = (
    fastpipe.create()
    .map(lambda x: x, workers=2, mode="thread")
    .map(lambda x: x * 2, workers=2, mode="process")
    .map(lambda x: x + 10, workers=2, mode="thread")
)  # Two ADAPTERS inserted

results = pipe.run(data)
results.sort()
print(f"Input: {data}")
print(f"Results: {sorted(results)}")

# Expected: [1,2,3,4] -> [2,4,6,8] -> [12,14,16,18]
expected = [12, 14, 16, 18]
assert sorted(results) == expected, f"Expected {expected}, got {sorted(results)}"
print("✓ Multiple transitions work!")

print("\n" + "=" * 70)
print("✅ All ProcessExecutor ADAPTER tests passed!")
print("=" * 70)
print("\nNote: ADAPTER stages are automatically inserted at mode transitions.")
print("They handle SENTINEL fan-in/fan-out to match worker counts.")
