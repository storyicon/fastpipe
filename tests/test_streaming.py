"""Test streaming mode."""

import time

import sys
from pathlib import Path

# Add parent directory to enable 'import fastpipe'
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastpipe

print("Test 1: Basic streaming put/get")
pipe = fastpipe.create().map(lambda x: x * 2, workers=2)
stream = pipe.stream(queue_size=10)

# Put some items
for i in range(5):
    stream.put(i)
    print(f"  Put: {i}")

# Get results
results = []
for i in range(5):
    result = stream.get(timeout=2)
    print(f"  Got: {result}")
    results.append(result)

stream.close()
print(f"Results: {results}")
assert set(results) == {0, 2, 4, 6, 8}
print("✓ Basic streaming works!\n")

print("Test 2: Streaming with context manager")
pipe = fastpipe.create().map(lambda x: x + 10, workers=1)

with pipe.stream() as stream:
    stream.put(1)
    stream.put(2)
    r1 = stream.get()
    r2 = stream.get()
    print(f"  Results: {r1}, {r2}")
    assert {r1, r2} == {11, 12}

print("✓ Context manager works!\n")

print("Test 3: put_batch / get_batch")
pipe = fastpipe.create().filter(lambda x: x % 2 == 0, workers=1)
stream = pipe.stream()

stream.put_batch([1, 2, 3, 4, 5, 6])
time.sleep(0.2)  # Let it process

results = stream.get_batch(max_items=10, timeout=1)
stream.close()
print(f"  Results: {results}")
assert set(results) == {2, 4, 6}
print("✓ Batch operations work!\n")

print("✅ All streaming tests passed!")
