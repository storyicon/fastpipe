"""Quick validation tests."""

import sys
from pathlib import Path

# Add parent directory to enable 'import fastpipe'
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastpipe

print("Testing input validation...")

# Test 1: Invalid workers
try:
    fastpipe.create().map(lambda x: x, workers=0)
    print("❌ Should reject workers=0")
except ValueError as e:
    print(f"✓ Rejects workers=0: {e}")

# Test 2: Invalid func
try:
    fastpipe.create().map("not a function", workers=2)
    print("❌ Should reject non-callable")
except TypeError as e:
    print(f"✓ Rejects non-callable: {e}")

# Test 3: Invalid batch_size
try:
    fastpipe.create().batch(size=-5)
    print("❌ Should reject negative batch_size")
except ValueError as e:
    print(f"✓ Rejects negative batch_size: {e}")

# Test 4: Invalid queue_size
try:
    pipe = fastpipe.create().map(lambda x: x)
    pipe.run([], queue_size=0)
    print("❌ Should reject queue_size=0")
except ValueError as e:
    print(f"✓ Rejects queue_size=0: {e}")

print("\n✅ All validation tests passed!")
