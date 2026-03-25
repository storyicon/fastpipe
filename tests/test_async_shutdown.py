"""Test AsyncExecutor shutdown fix."""

import asyncio
import time

import sys
from pathlib import Path

# Add parent directory to enable 'import fastpipe'
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastpipe


async def slow_async(x):
    """Slow async function."""
    await asyncio.sleep(0.5)
    return x * 2


print("Test: AsyncExecutor shutdown (should complete quickly)")
print("Creating pipeline with async mode...")

pipe = fastpipe.create().map(slow_async, workers=3, mode="async")

print("Starting streaming mode...")
stream = pipe.stream(queue_size=10)

print("Putting 2 items...")
stream.put(1)
stream.put(2)

print("Getting 1 result...")
result = stream.get(timeout=2)
print(f"Got: {result}")

print("Closing stream (should complete in <2s, not hang)...")
start = time.time()
stream.close(drain=False)
duration = time.time() - start

print(f"Close took: {duration:.2f}s")

if duration < 2:
    print("✅ AsyncExecutor stops cleanly (fixed!)")
else:
    print(f"❌ Still hanging (took {duration:.2f}s)")

assert duration < 2, f"Shutdown too slow: {duration:.2f}s"
