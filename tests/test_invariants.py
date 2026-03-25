"""Property-based invariant tests to prevent systematic blind spots.

These tests verify that core properties hold across all configurations,
not just specific scenarios. This prevents bugs like the SENTINEL worker
mismatch from going undetected.
"""

import itertools

import sys
from pathlib import Path

# Add parent directory to enable 'import fastpipe'
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import fastpipe


def test_map_preserves_count_across_worker_variations():
    """Invariant: MAP is 1:1, so len(output) == len(input) for any worker config."""
    test_data = list(range(20))

    # Test all worker count combinations
    worker_configs = [1, 2, 5, 10]

    for w1, w2, w3 in itertools.product(worker_configs, repeat=3):
        pipe = (
            fastpipe.create()
            .map(lambda x: x + 1, workers=w1)
            .map(lambda x: x * 2, workers=w2)
            .map(lambda x: x - 1, workers=w3)
        )

        results = pipe.run(test_data)

        # Invariant: MAP preserves count
        assert len(results) == len(test_data), (
            f"MAP lost data with workers [{w1}, {w2}, {w3}]: {len(test_data)} → {len(results)}"
        )

        # Invariant: MAP produces correct values
        expected = [(x + 1) * 2 - 1 for x in test_data]
        assert sorted(results) == sorted(expected), (
            f"MAP produced wrong values with workers [{w1}, {w2}, {w3}]"
        )

    print(f"✓ MAP invariant holds for {len(worker_configs) ** 3} worker configurations")


def test_filter_reduces_count_correctly():
    """Invariant: FILTER may reduce count, but never increases it."""
    test_data = list(range(20))

    for w1, w2 in itertools.product([1, 2, 5], repeat=2):
        pipe = (
            fastpipe.create().map(lambda x: x, workers=w1).filter(lambda x: x % 2 == 0, workers=w2)
        )

        results = pipe.run(test_data)

        # Invariant: FILTER never increases count
        assert len(results) <= len(test_data), (
            f"FILTER increased count? {len(test_data)} → {len(results)}"
        )

        # Invariant: FILTER produces subset
        assert set(results).issubset({x for x in test_data if x % 2 == 0}), (
            "FILTER produced wrong items"
        )

    print(f"✓ FILTER invariant holds for {3**2} worker configurations")


def test_batch_preserves_total_items():
    """Invariant: BATCH regroups items, sum of batch sizes == input count."""
    test_data = list(range(15))

    batch_sizes = [2, 5, 10, 100]
    worker_configs = [1, 2, 5]

    for batch_size in batch_sizes:
        for w_before in worker_configs:
            pipe = fastpipe.create().map(lambda x: x, workers=w_before).batch(size=batch_size)

            results = pipe.run(test_data)

            # Invariant: Total items preserved
            total_items = sum(len(batch) for batch in results)
            assert total_items == len(test_data), (
                f"BATCH lost items: {len(test_data)} → {total_items} (workers={w_before}, batch={batch_size})"
            )

            # Invariant: All items present
            flattened = [item for batch in results for item in batch]
            assert sorted(flattened) == sorted(test_data), "BATCH changed or lost items"

    print(f"✓ BATCH invariant holds for {len(batch_sizes) * len(worker_configs)} configurations")


def test_flat_map_multiplies_correctly():
    """Invariant: FLAT_MAP with 1:N func produces N × input items."""
    test_data = [1, 2, 3, 4, 5]

    for w1, w2 in itertools.product([1, 2, 5], repeat=2):
        pipe = (
            fastpipe.create()
            .map(lambda x: x, workers=w1)
            .flat_map(lambda x: [x, x * 2, x * 3], workers=w2)
        )

        results = pipe.run(test_data)

        # Invariant: FLAT_MAP with 1:3 func produces 3x items
        assert len(results) == len(test_data) * 3, (
            f"FLAT_MAP lost items: {len(test_data) * 3} expected, {len(results)} got"
        )

    print(f"✓ FLAT_MAP invariant holds for {3**2} worker configurations")


def test_adapter_sentinel_count_invariant():
    """Invariant: Adapters correctly fan-in/fan-out SENTINELs."""
    test_data = list(range(10))

    # Test various worker count transitions
    transitions = [
        (1, 5),  # Fan-out
        (5, 1),  # Fan-in
        (2, 8),  # Fan-out
        (10, 2),  # Fan-in
        (3, 3),  # No change (should not insert adapter)
    ]

    for w1, w2 in transitions:
        pipe = (
            fastpipe.create()
            .map(lambda x: x, workers=w1, mode="thread")
            .map(lambda x: x * 2, workers=w2, mode="thread")
        )

        results = pipe.run(test_data)

        # Invariant: No data loss
        assert len(results) == len(test_data), (
            f"Adapter {w1}→{w2} lost data: {len(test_data)} → {len(results)}"
        )

        # Invariant: Correct values
        expected = [x * 2 for x in test_data]
        assert sorted(results) == sorted(expected), f"Adapter {w1}→{w2} produced wrong values"

    print(f"✓ ADAPTER invariant holds for {len(transitions)} worker transitions")


def test_mode_and_worker_change_together():
    """Invariant: Mode + worker changes both handled correctly."""
    test_data = list(range(10))

    async def async_func(x):
        return x * 2

    # Test all combinations of mode + worker changes
    configs = [
        ("thread", 2, "async", 5),
        ("async", 5, "thread", 1),
        ("thread", 1, "process", 3),
        ("process", 4, "thread", 2),
    ]

    for mode1, w1, mode2, w2 in configs:
        if mode1 == "async" or mode2 == "async":
            func1 = async_func if mode1 == "async" else lambda x: x
            func2 = async_func if mode2 == "async" else lambda x: x * 2
        else:

            def func1(x):
                return x

            def func2(x):
                return x * 2

        pipe = (
            fastpipe.create().map(func1, workers=w1, mode=mode1).map(func2, workers=w2, mode=mode2)
        )

        results = pipe.run(test_data)

        # Invariant: No data loss
        assert len(results) == len(test_data), (
            f"Lost data with {mode1}({w1})→{mode2}({w2}): {len(test_data)} → {len(results)}"
        )

    print(f"✓ Mode+Worker change invariant holds for {len(configs)} combinations")


if __name__ == "__main__":
    test_map_preserves_count_across_worker_variations()
    test_filter_reduces_count_correctly()
    test_batch_preserves_total_items()
    test_flat_map_multiplies_correctly()
    test_adapter_sentinel_count_invariant()
    test_mode_and_worker_change_together()
    print()
    print("🎉 All invariant tests passed!")
    print()
    print("These tests verify properties that MUST hold regardless of configuration.")
    print("If any fail, there's a fundamental bug in the pipeline architecture.")
