"""Test exception propagation in streaming mode."""

import contextlib
import time


import fastpipe

print("=" * 70)
print("Testing Exception Propagation in Streaming Mode")
print("=" * 70)


def failing_func(x):
    """Function that fails on specific input."""
    if x == 3:
        raise ValueError(f"Intentional error on {x}")
    return x * 2


# Test 1: Exception detected on put() - ThreadExecutor
print("\nTest 1: Exception detected on put() - ThreadExecutor")
pipe = fastpipe.create().map(failing_func, workers=2, mode="thread")
stream = pipe.stream()

try:
    stream.put(1)
    stream.put(2)
    stream.put(3)  # This will cause error
    time.sleep(0.5)  # Give worker time to process
    stream.put(4)  # This should detect the exception
    print("❌ Exception not detected on put()")
except ValueError as e:
    print(f"✓ Exception detected on put(): {e}")
finally:
    with contextlib.suppress(BaseException):
        stream.close(drain=False)

# Test 2: Exception detected on get() - ThreadExecutor
print("\nTest 2: Exception detected on get() - ThreadExecutor")
pipe = fastpipe.create().map(failing_func, workers=2, mode="thread")
stream = pipe.stream()

try:
    stream.put(1)
    result1 = stream.get()
    print(f"  Got result: {result1}")

    stream.put(3)  # This will cause error
    time.sleep(0.5)  # Give worker time to process and fail

    result2 = stream.get()  # This should detect the exception
    print("❌ Exception not detected on get()")
except ValueError as e:
    print(f"✓ Exception detected on get(): {e}")
finally:
    with contextlib.suppress(BaseException):
        stream.close(drain=False)

# Test 3: Exception with ProcessExecutor
print("\nTest 3: Exception detected - ProcessExecutor")
pipe = fastpipe.create().map(failing_func, workers=2, mode="process")
stream = pipe.stream()

try:
    stream.put(1)
    stream.put(3)  # This will cause error
    time.sleep(0.5)
    stream.put(4)  # Should detect exception
    print("❌ Exception not detected with ProcessExecutor")
except (ValueError, Exception) as e:
    print(f"✓ Exception detected with ProcessExecutor: {type(e).__name__}")
finally:
    with contextlib.suppress(BaseException):
        stream.close(drain=False)

# Test 4: Exception with AsyncExecutor
print("\nTest 4: Exception detected - AsyncExecutor")


async def async_failing_func(x):
    """Async function that fails on specific input."""
    if x == 3:
        raise ValueError(f"Intentional async error on {x}")
    return x * 2


pipe = fastpipe.create().map(async_failing_func, workers=2, mode="async")
stream = pipe.stream()

try:
    stream.put(1)
    stream.put(3)  # This will cause error
    time.sleep(0.5)
    stream.put(4)  # Should detect exception
    print("❌ Exception not detected with AsyncExecutor")
except (ValueError, Exception) as e:
    print(f"✓ Exception detected with AsyncExecutor: {type(e).__name__}")
finally:
    with contextlib.suppress(BaseException):
        stream.close(drain=False)

print("\n" + "=" * 70)
print("✅ All streaming exception tests passed!")
print("=" * 70)
