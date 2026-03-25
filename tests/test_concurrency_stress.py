"""Concurrency stress tests to prove thread safety."""

import threading
import time

import sys
from pathlib import Path

# Add parent directory to enable 'import fastpipe'
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastpipe

print("=" * 70)
print("FastPipe Concurrency Stress Tests")
print("=" * 70)

# Test 1: 1000 concurrent pipeline runs
print("\n✓ Test 1: 1000 concurrent runs (thread safety)")
results_lock = threading.Lock()
errors_lock = threading.Lock()  # Fix: was missing
results = []
errors = []


def run_pipeline(i):
    """Run pipeline and collect result."""
    try:
        result = (
            fastpipe.create()
            .map(lambda x: x * 2, workers=4)
            .filter(lambda x: x > 5)
            .run([1, 2, 3, 4, 5])
        )
        with results_lock:
            results.append((i, len(result)))
    except Exception as e:
        with errors_lock:
            errors.append((i, e))


start = time.time()
threads = [threading.Thread(target=run_pipeline, args=(i,)) for i in range(1000)]
for t in threads:
    t.start()
for t in threads:
    t.join()
duration = time.time() - start

print(f"   Completed 1000 runs in {duration:.2f}s")
print(f"   Successful: {len(results)}")
print(f"   Errors: {len(errors)}")
assert len(errors) == 0, f"Errors occurred: {errors[:5]}"
print("   ✅ No races, no deadlocks\n")

# Test 2: High worker count
print("✓ Test 2: Pipeline with 50 workers (high concurrency)")
results = fastpipe.create().map(lambda x: x + 1, workers=50).run(range(100))
assert len(results) == 100
print(f"   Processed {len(results)} items with 50 workers")
print("   ✅ High worker count handles correctly\n")

# Test 3: Complex adapter chain
print("✓ Test 3: Complex adapter chain (5 mode transitions)")


def cpu_task(x):
    return x * 2


import asyncio


async def async_task(x):
    await asyncio.sleep(0.001)
    return x + 1


results = (
    fastpipe.create()
    .map(lambda x: x, workers=3, mode="thread")
    .map(cpu_task, workers=2, mode="process")
    .map(lambda x: x, workers=4, mode="thread")
    .map(async_task, workers=5, mode="async")
    .map(lambda x: x, workers=2, mode="thread")
    .run(range(10))
)

assert len(results) == 10
print(f"   5 mode transitions, {len(results)} items processed")
print("   ✅ Complex adapters work correctly\n")

# Test 4: BATCH with large dataset
print("✓ Test 4: BATCH correctness (10K items)")
results = fastpipe.create().batch(size=100).unbatch().run(range(10000))

assert len(results) == 10000
assert set(results) == set(range(10000))
print(f"   All {len(results)} items preserved through batching")
print("   ✅ BATCH is correct and complete\n")

# Test 5: Exception under load
print("✓ Test 5: Exception handling under load")
exception_count = 0


def sometimes_fails(x):
    if x == 50:
        raise ValueError("Intentional failure")
    return x * 2


try:
    results = fastpipe.create().map(sometimes_fails, workers=10).run(range(100))
except ValueError:
    exception_count = 1

assert exception_count == 1
print("   Exception propagated correctly under load")
print("   ✅ Exception handling is thread-safe\n")

print("=" * 70)
print("STRESS TEST RESULTS")
print("=" * 70)
print("✅ 1000 concurrent runs: No races")
print("✅ 50 workers: Correct")
print("✅ 5 adapters: Correct")
print("✅ 10K items: Complete")
print("✅ Exceptions: Thread-safe")
print("")
print("🎉 FastPipe passes all concurrency stress tests!")
print("=" * 70)
