"""Comprehensive test of all three execution modes."""

import asyncio


import fastpipe

print("=" * 70)
print("FastPipe - All Modes Test")
print("=" * 70)

# Test 1: Thread mode
print("\n✓ Test 1: Thread mode")
results = fastpipe.create().map(lambda x: x * 2, workers=3).run([1, 2, 3, 4, 5])
assert len(results) == 5
print(f"  Thread mode: {sorted(results)}")

# Test 2: Process mode
print("\n✓ Test 2: Process mode")


def square(x):
    return x * x


results = fastpipe.create().map(square, workers=3, mode="process").run([1, 2, 3, 4])
assert set(results) == {1, 4, 9, 16}
print(f"  Process mode: {sorted(results)}")

# Test 3: Async mode
print("\n✓ Test 3: Async mode")


async def async_add_ten(x):
    await asyncio.sleep(0.001)
    return x + 10


results = fastpipe.create().map(async_add_ten, workers=5, mode="async").run([1, 2, 3])
assert set(results) == {11, 12, 13}
print(f"  Async mode: {sorted(results)}")

# Test 4: Mixed thread→process
print("\n✓ Test 4: Mixed thread→process")


def add_five(x):
    return x + 5


results = (
    fastpipe.create()
    .map(lambda x: x * 2, workers=2, mode="thread")
    .map(add_five, workers=2, mode="process")
    .run([1, 2, 3])
)
assert set(results) == {7, 9, 11}
print(f"  Thread→Process: {sorted(results)}")

# Test 5: Mixed async→thread
print("\n✓ Test 5: Mixed async→thread")
results = (
    fastpipe.create()
    .map(async_add_ten, workers=3, mode="async")
    .map(lambda x: x * 2, workers=2, mode="thread")
    .run([1, 2, 3])
)
assert set(results) == {22, 24, 26}
print(f"  Async→Thread: {sorted(results)}")

# Test 6: Mixed thread→async
print("\n✓ Test 6: Mixed thread→async")
results = (
    fastpipe.create()
    .map(lambda x: x + 1, workers=2, mode="thread")
    .map(async_add_ten, workers=3, mode="async")
    .run([1, 2, 3])
)
assert set(results) == {12, 13, 14}
print(f"  Thread→Async: {sorted(results)}")

# Test 7: All three modes
print("\n✓ Test 7: All three modes (thread→process→async)")


async def async_double(x):
    return x * 2


results = (
    fastpipe.create()
    .map(lambda x: x + 1, workers=2, mode="thread")
    .map(square, workers=2, mode="process")
    .map(async_double, workers=3, mode="async")
    .run([1, 2, 3])
)
expected = {8, 18, 32}  # (1+1)²*2=8, (2+1)²*2=18, (3+1)²*2=32
assert set(results) == expected
print(f"  Thread→Process→Async: {sorted(results)}")

print("\n" + "=" * 70)
print("🎉 All modes and cross-mode combinations work perfectly!")
print("=" * 70)
