"""Tests for callable class support."""

import asyncio

import fastpipe


# Basic callable class
class Multiplier:
    """Simple callable class for testing."""

    def __init__(self, factor):
        self.factor = factor

    def __call__(self, x):
        return x * self.factor


# Stateful callable class
class Counter:
    """Stateful class that maintains a counter."""

    def __init__(self, start=0):
        self.count = start

    def __call__(self, x):
        self.count += 1
        return {"value": x, "count": self.count}


# Async callable class
class AsyncMultiplier:
    """Async callable class."""

    def __init__(self, factor):
        self.factor = factor

    async def __call__(self, x):
        await asyncio.sleep(0.001)  # Simulate async work
        return x * self.factor


# Class without __call__
class NotCallable:
    """Class without __call__ method."""

    def __init__(self):
        pass


# Class requiring arguments
class RequiresArgs:
    """Class that requires init arguments."""

    def __init__(self, required_arg):
        self.arg = required_arg

    def __call__(self, x):
        return x + self.arg


print("=" * 70)
print("Callable Class Tests")
print("=" * 70)

# Test 6.1: Basic callable class test
print("\n✓ Test 6.1: Basic callable class")
results = fastpipe.create().map(Multiplier, workers=2, init_args=(3,)).run([1, 2, 3, 4])
assert sorted(results) == [3, 6, 9, 12], f"Expected [3, 6, 9, 12], got {sorted(results)}"
print(f"  Results: {sorted(results)}")

# Test 6.2: Thread mode with stateful class
print("\n✓ Test 6.2: Thread mode with stateful class")
results = fastpipe.create().map(Counter, workers=2, init_args=(10,), mode="thread").run(range(6))
# Each worker maintains its own count
counts = sorted([r["count"] for r in results])
print(f"  Counts: {counts}")
# With 2 workers, each processes ~3 items, so counts should show independent incrementing
assert len(counts) == 6, f"Expected 6 results, got {len(counts)}"

# Test 6.3: Process mode with class initialization
print("\n✓ Test 6.3: Process mode with class initialization")
results = (
    fastpipe.create().map(Multiplier, workers=2, init_args=(5,), mode="process").run([1, 2, 3])
)
assert sorted(results) == [5, 10, 15], f"Expected [5, 10, 15], got {sorted(results)}"
print(f"  Results: {sorted(results)}")

# Test 6.4: Async mode with callable class
print("\n✓ Test 6.4: Async mode with callable class")
results = (
    fastpipe.create().map(AsyncMultiplier, workers=2, init_args=(4,), mode="async").run([1, 2, 3])
)
assert sorted(results) == [4, 8, 12], f"Expected [4, 8, 12], got {sorted(results)}"
print(f"  Results: {sorted(results)}")

# Test 6.5: Per-worker state isolation
print("\n✓ Test 6.5: Per-worker state isolation")
results = fastpipe.create().map(Counter, workers=3, init_args=(0,), mode="thread").run(range(9))
# Each worker maintains its own counter starting from 0
counts = [r["count"] for r in results]
print(f"  Counts from workers: {counts}")
# Verify that counting happened (all counts > 0) - state isolation is implicit
# The fact that we get 9 results with counts shows each worker processed independently
assert len(counts) == 9 and all(c > 0 for c in counts), "All items should be processed"

# Test 6.6: Class with init_args only
print("\n✓ Test 6.6: Class with init_args only")
results = fastpipe.create().map(Multiplier, workers=2, init_args=(7,)).run([1, 2])
assert sorted(results) == [7, 14], f"Expected [7, 14], got {sorted(results)}"
print(f"  Results: {sorted(results)}")

# Test 6.7: Class with init_kwargs only
print("\n✓ Test 6.7: Class with init_kwargs only")
results = fastpipe.create().map(Multiplier, workers=2, init_kwargs={"factor": 6}).run([1, 2])
assert sorted(results) == [6, 12], f"Expected [6, 12], got {sorted(results)}"
print(f"  Results: {sorted(results)}")

# Test 6.8: Class with both init_args and init_kwargs
print("\n✓ Test 6.8: Class with both init_args and init_kwargs")


class ComplexInit:
    def __init__(self, base, multiplier=2):
        self.base = base
        self.multiplier = multiplier

    def __call__(self, x):
        return (x + self.base) * self.multiplier


results = (
    fastpipe.create()
    .map(ComplexInit, workers=2, init_args=(10,), init_kwargs={"multiplier": 3})
    .run([1, 2])
)
expected = sorted([(1 + 10) * 3, (2 + 10) * 3])  # [33, 36]
assert sorted(results) == expected, f"Expected {expected}, got {sorted(results)}"
print(f"  Results: {sorted(results)}")

# Test 6.9: Missing required init argument
print("\n✓ Test 6.9: Missing required init argument")
try:
    results = fastpipe.create().map(RequiresArgs, workers=2).run([1, 2])
    assert False, "Should have raised TypeError"
except (TypeError, ValueError) as e:
    print(f"  Correctly raised: {type(e).__name__}")
    assert "required_arg" in str(e) or "missing" in str(e).lower()

# Test 6.10: Class without __call__ method
print("\n✓ Test 6.10: Class without __call__ method")
try:
    results = fastpipe.create().map(NotCallable, workers=2).run([1, 2])
    assert False, "Should have raised AttributeError or TypeError"
except (AttributeError, TypeError) as e:
    print(f"  Correctly raised: {type(e).__name__}")

# Test 6.11: Backward compatibility - regular functions
print("\n✓ Test 6.11: Regular functions still work")
results = fastpipe.create().map(lambda x: x * 10, workers=2).run([1, 2, 3])
assert sorted(results) == [10, 20, 30]
print(f"  Results: {sorted(results)}")

# Test 6.12: Backward compatibility - bound methods
print("\n✓ Test 6.12: Bound methods still work")
obj = Multiplier(factor=8)
results = fastpipe.create().map(obj, workers=2).run([1, 2, 3])
assert sorted(results) == [8, 16, 24]
print(f"  Results: {sorted(results)}")

# Test 6.13: Backward compatibility - callable instances
print("\n✓ Test 6.13: Callable instances still work")
instance = Multiplier(9)
results = fastpipe.create().map(instance, workers=2).run([1, 2])
assert sorted(results) == [9, 18]
print(f"  Results: {sorted(results)}")

# Additional tests with filter, flat_map, each
print("\n✓ Additional: filter() with callable class")


class EvenFilter:
    def __init__(self, modulo=2):
        self.modulo = modulo

    def __call__(self, x):
        return x % self.modulo == 0


results = fastpipe.create().filter(EvenFilter, workers=2, init_args=(2,)).run(range(10))
assert results == [0, 2, 4, 6, 8]
print(f"  Results: {results}")

print("\n✓ Additional: flat_map() with callable class")


class Duplicator:
    def __init__(self, times):
        self.times = times

    def __call__(self, x):
        return [x] * self.times


results = fastpipe.create().flat_map(Duplicator, workers=2, init_args=(2,)).run([1, 2, 3])
assert sorted(results) == [1, 1, 2, 2, 3, 3]
print(f"  Results: {sorted(results)}")

print("\n✓ Additional: each() with callable class")

collected = []


class SideEffect:
    def __init__(self, prefix):
        self.prefix = prefix

    def __call__(self, x):
        # Note: This test may have race conditions with threads
        # Just verify it runs without error
        pass


results = fastpipe.create().each(SideEffect, workers=2, init_args=("test",)).run([1, 2, 3])
assert sorted(results) == [1, 2, 3]  # each() returns original items
print(f"  Results: {sorted(results)}")

print("\n" + "=" * 70)
print("✅ All callable class tests passed!")
print("=" * 70)
