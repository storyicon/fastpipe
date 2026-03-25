"""Tests for SENTINEL worker count mismatch bug fix.

This tests the critical bug where adjacent stages with different worker counts
(but same mode) would lose data due to insufficient SENTINEL propagation.
"""


import fastpipe


def test_worker_count_increase():
    """Test: 1 worker -> 5 workers (same mode)."""

    def process(x):
        return x * 2

    pipe = (
        fastpipe.create()
        .map(lambda x: x, workers=1, mode="thread")
        .map(process, workers=5, mode="thread")
        .map(lambda x: x, workers=1, mode="thread")
    )

    results = pipe.run(list(range(20)))

    assert len(results) == 20, f"Lost data: expected 20, got {len(results)}"
    expected = [x * 2 for x in range(20)]
    assert sorted(results) == sorted(expected)
    print("✓ Test 1: 1→5→1 workers preserves all data")


def test_worker_count_decrease():
    """Test: 5 workers -> 1 worker (same mode)."""
    pipe = (
        fastpipe.create()
        .map(lambda x: x * 2, workers=5, mode="thread")
        .map(lambda x: x + 1, workers=1, mode="thread")
    )

    results = pipe.run(list(range(10)))

    assert len(results) == 10
    expected = [(x * 2) + 1 for x in range(10)]
    assert sorted(results) == sorted(expected)
    print("✓ Test 2: 5→1 workers preserves all data")


def test_multiple_worker_changes():
    """Test: Multiple worker count changes in pipeline."""
    pipe = (
        fastpipe.create()
        .map(lambda x: x, workers=1, mode="thread")
        .map(lambda x: x * 2, workers=3, mode="thread")
        .map(lambda x: x + 1, workers=5, mode="thread")
        .map(lambda x: x - 1, workers=2, mode="thread")
    )

    results = pipe.run(list(range(15)))

    assert len(results) == 15
    expected = [(x * 2) for x in range(15)]  # +1-1 cancels out
    assert sorted(results) == sorted(expected)
    print("✓ Test 3: Multiple worker changes (1→3→5→2) preserves all data")


def test_adapters_inserted_correctly():
    """Test: Verify adapters are inserted for worker count changes."""
    from fastpipe._runtime import insert_adapters

    pipe = (
        fastpipe.create()
        .map(lambda x: x, workers=2, mode="thread")
        .map(lambda x: x, workers=4, mode="thread")
        .map(lambda x: x, workers=2, mode="thread")
    )

    stages = insert_adapters(pipe._stages)

    # Should have: MAP(2) -> ADAPTER -> MAP(4) -> ADAPTER -> MAP(2)
    assert len(stages) == 5, f"Expected 5 stages, got {len(stages)}"

    assert stages[0].workers == 2
    assert stages[1].operation.name == "ADAPTER" and stages[1].workers == 1
    assert stages[2].workers == 4
    assert stages[3].operation.name == "ADAPTER" and stages[3].workers == 1
    assert stages[4].workers == 2

    print("✓ Test 4: Adapters correctly inserted for worker count changes")


def test_same_workers_no_adapter():
    """Test: No adapter inserted when worker counts are the same."""
    from fastpipe._runtime import insert_adapters

    pipe = (
        fastpipe.create()
        .map(lambda x: x, workers=3, mode="thread")
        .map(lambda x: x * 2, workers=3, mode="thread")
        .map(lambda x: x + 1, workers=3, mode="thread")
    )

    stages = insert_adapters(pipe._stages)

    # Should have no adapters (all same worker count and mode)
    assert len(stages) == 3, f"Should be 3 stages, got {len(stages)}"
    assert all(s.operation.name != "ADAPTER" for s in stages)

    print("✓ Test 5: No unnecessary adapters when workers are same")


def test_mode_and_worker_change():
    """Test: Both mode and worker count change."""

    async def async_func(x):
        return x * 2

    pipe = (
        fastpipe.create()
        .map(lambda x: x, workers=2, mode="thread")
        .map(async_func, workers=3, mode="async")
        .map(lambda x: x, workers=1, mode="thread")
    )

    results = pipe.run(list(range(10)))

    assert len(results) == 10
    expected = [x * 2 for x in range(10)]
    assert sorted(results) == sorted(expected)
    print("✓ Test 6: Mode + worker change both handled correctly")


if __name__ == "__main__":
    test_worker_count_increase()
    test_worker_count_decrease()
    test_multiple_worker_changes()
    test_adapters_inserted_correctly()
    test_same_workers_no_adapter()
    test_mode_and_worker_change()
    print()
    print("🎉 All worker count mismatch tests passed!")
