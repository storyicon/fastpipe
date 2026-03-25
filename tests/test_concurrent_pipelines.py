"""Test concurrent pipeline execution."""

import threading


import fastpipe


def test_concurrent_pipelines():
    """Test that multiple pipelines can run concurrently without issues."""

    def process(x):
        return x * 2

    def run_pipeline(pipeline_id, results_dict):
        """Run a pipeline and store results."""
        pipe = fastpipe.create().map(process, workers=2)
        results = pipe.run(range(10))
        results_dict[pipeline_id] = results

    # Run 10 pipelines concurrently
    threads = []
    results_dict = {}

    for i in range(10):
        t = threading.Thread(target=run_pipeline, args=(i, results_dict))
        t.start()
        threads.append(t)

    # Wait for all to complete
    for t in threads:
        t.join(timeout=5.0)

    # Verify all completed successfully
    assert len(results_dict) == 10, f"Only {len(results_dict)}/10 pipelines completed"

    expected = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
    for pipeline_id, results in results_dict.items():
        assert results == expected, f"Pipeline {pipeline_id} got wrong results: {results}"

    print("✓ 10 concurrent pipelines executed successfully")


def test_concurrent_pipelines_with_batch():
    """Test concurrent pipelines with BATCH operations (single-worker stateful)."""

    def process(x):
        return x * 2

    def run_pipeline(pipeline_id, results_dict):
        pipe = fastpipe.create().map(process, workers=2).batch(size=3).unbatch()
        results = pipe.run(range(10))
        results_dict[pipeline_id] = sorted(results)  # Sort for comparison

    threads = []
    results_dict = {}

    for i in range(5):
        t = threading.Thread(target=run_pipeline, args=(i, results_dict))
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=5.0)

    assert len(results_dict) == 5
    expected = sorted([0, 2, 4, 6, 8, 10, 12, 14, 16, 18])

    for pipeline_id, results in results_dict.items():
        assert results == expected, f"Pipeline {pipeline_id} got wrong results: {results}"

    print("✓ 5 concurrent pipelines with BATCH executed successfully")


if __name__ == "__main__":
    test_concurrent_pipelines()
    test_concurrent_pipelines_with_batch()
    print("All concurrent pipeline tests passed!")
