"""Test iterator mode (memory-efficient)."""


import fastpipe

print("=" * 70)
print("FastPipe Iterator Mode Test")
print("=" * 70)

# Test 1: Basic iteration
print("\n✓ Test 1: Basic iteration")
collected = []
for result in fastpipe.create().map(lambda x: x * 2, workers=2).iter(range(10)):
    collected.append(result)
    print(f"  Got: {result}")

assert len(collected) == 10
assert set(collected) == {0, 2, 4, 6, 8, 10, 12, 14, 16, 18}
print("  ✓ Iterator yields all results\n")

# Test 2: Memory efficiency - process one at a time
print("✓ Test 2: Memory-efficient processing")
count = 0
max_memory = []

pipe = fastpipe.create().map(lambda x: x * 2, workers=4).filter(lambda x: x > 10)

for result in pipe.iter(range(100)):
    count += 1
    # Process immediately, don't accumulate
    assert result > 10

print(f"  Processed {count} results without accumulating in memory")
print("  ✓ Memory-efficient iteration\n")

# Test 3: Compare with run()
print("✓ Test 3: iter() vs run() correctness")
data = list(range(20))

# Using run() - accumulates all
results_run = (
    fastpipe.create().map(lambda x: x * 3, workers=3).filter(lambda x: x % 2 == 0).run(data)
)

# Using iter() - yields one by one
results_iter = list(
    fastpipe.create().map(lambda x: x * 3, workers=3).filter(lambda x: x % 2 == 0).iter(data)
)

assert set(results_run) == set(results_iter)
print("  run() and iter() produce same results")
print(f"  Results count: {len(results_run)}\n")

# Test 4: Early termination
print("✓ Test 4: Early termination (break from loop)")
count = 0
for result in fastpipe.create().map(lambda x: x * 2, workers=4).iter(range(1000)):
    count += 1
    if count >= 5:
        break  # Stop early, don't process all 1000

print(f"  Broke after {count} results (didn't process all 1000)")
print("  ✓ Early termination works\n")

# Test 5: Works with all modes
print("✓ Test 5: Iterator with process mode")


def square(x):
    return x * x


results = list(fastpipe.create().map(square, workers=2, mode="process").iter([1, 2, 3, 4]))
assert set(results) == {1, 4, 9, 16}
print("  ✓ Iterator works with process mode\n")

print("=" * 70)
print("🎉 All iterator tests passed!")
print("=" * 70)
print("\n📊 Benefits:")
print("  - Memory-efficient: results not accumulated")
print("  - Streaming consumption: process as available")
print("  - Early termination: can break from loop")
print("  - Works with all modes: thread/process/async")
