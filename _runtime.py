"""Runtime logic for pipeline building and execution.

This module handles pipeline construction: inserting cross-mode adapters,
selecting queue types, and creating the queue chain.

SENTINEL Propagation:
    SENTINEL is a special marker indicating end-of-stream. Its lifecycle:

    1. Input feeding:
       - main thread puts N SENTINELs (N = first stage workers)
       - Each worker in first stage gets one SENTINEL

    2. Normal stages:
       - Each worker receives 1 SENTINEL from upstream
       - Each worker outputs 1 SENTINEL to downstream
       - Result: N workers → N SENTINELs propagated

    3. Adapters (mode transitions):
       - Single adapter worker receives N SENTINELs from upstream
       - Adapter outputs M SENTINELs to downstream (M = downstream workers)
       - This converts SENTINEL count to match downstream

    4. Result collection:
       - main thread collects N SENTINELs (N = last stage workers)
       - When all N received, pipeline complete

Adapters (Mode or Worker Count Changes):
    When adjacent stages differ in execution mode OR worker count,
    an adapter stage is auto-inserted to bridge them. Adapter:
    - Always runs in thread mode (lightweight)
    - Forwards data unchanged
    - Handles SENTINEL fan-in/fan-out to match worker counts

    Examples requiring adapters:
    - Mode change: thread -> process (different modes)
    - Worker count: map(workers=2) -> map(workers=5) (same mode, different workers)
    - Both: thread(2 workers) -> async(3 workers)

Queue Type Selection:
    - If any stage uses process or async: multiprocessing.Queue (universal)
    - All thread stages: threading.Queue (lighter weight)
    - multiprocessing.Queue works for threads too (slight overhead)
"""

import multiprocessing
import queue
from dataclasses import dataclass

from ._types import AdapterInfo, Operation, Stage


class _Sentinel:
    """End-of-stream marker that works across processes.

    Using isinstance() instead of identity check allows it to work
    even after pickle/unpickle in multiprocessing.
    """

    pass


SENTINEL = _Sentinel()


def select_queue_type(stages: list[Stage]) -> type:
    """Select queue type based on stages.

    If any stage uses process mode, use multiprocessing.Queue (required for
    inter-process communication with serialization). Otherwise use queue.Queue
    (thread-safe, no serialization overhead).

    Note: async mode runs in threads, not processes, so it can use queue.Queue.
    """
    has_process = any(s.mode == "process" for s in stages)
    if has_process:
        return multiprocessing.Queue
    return queue.Queue


def detect_mode_transitions(stages: list[Stage]) -> list[int]:
    """Detect indices where adapters are needed (mode OR worker count changes).

    Adapters are required when:
    1. Mode changes (thread <-> process <-> async)
    2. Worker count changes (even within same mode)

    Returns list of indices i where adapter should be inserted after stages[i]
    """
    transitions = []
    for i in range(len(stages) - 1):
        # Need adapter if mode changes OR worker count changes
        if stages[i].mode != stages[i + 1].mode or stages[i].workers != stages[i + 1].workers:
            transitions.append(i)
    return transitions


def create_adapter_stage(
    from_mode: str, to_mode: str, upstream_workers: int, downstream_workers: int
) -> Stage:
    """Create adapter stage to bridge mode transitions.

    IMPORTANT: Adapter uses SINGLE worker because it must:
    1. Count all N incoming SENTINELs from upstream (stateful counter)
    2. Output M SENTINELs to downstream when count complete
    Multiple workers would cause race on the SENTINEL counter.

    Args:
        from_mode: Source mode
        to_mode: Destination mode
        upstream_workers: Number of workers in upstream stage (SENTINELs to receive)
        downstream_workers: Number of workers in downstream stage (SENTINELs to output)

    Returns:
        Adapter Stage with workers=1 (design invariant)
    """
    adapter_info = AdapterInfo(upstream_workers, downstream_workers)

    stage = Stage(
        operation=Operation.ADAPTER,
        func=lambda x: x,  # Identity - just forward data
        workers=1,  # CRITICAL: Must be 1 to avoid SENTINEL counting races
        mode="thread",  # Always thread for lightweight forwarding
        inbound_queue_size=None,
        batch_size=None,
        adapter_info=adapter_info,
    )

    # Design invariant validation
    assert stage.workers == 1, "ADAPTER stage must be single-worker (design invariant)"

    return stage


def insert_adapters(stages: list[Stage]) -> list[Stage]:
    """Insert adapter stages at mode transitions.

    Scans for mode changes and inserts adapter stages to bridge them.
    """
    if not stages:
        return stages

    transitions = detect_mode_transitions(stages)
    if not transitions:
        return stages  # No mode changes, no adapters needed

    # Insert adapters from end to start (to preserve indices)
    new_stages = stages.copy()
    for idx in reversed(transitions):
        from_mode = new_stages[idx].mode
        to_mode = new_stages[idx + 1].mode
        upstream_workers = new_stages[idx].workers
        downstream_workers = new_stages[idx + 1].workers
        adapter = create_adapter_stage(from_mode, to_mode, upstream_workers, downstream_workers)
        # Insert adapter between idx and idx+1
        new_stages.insert(idx + 1, adapter)

    return new_stages


def create_queue_chain(stages: list[Stage], queue_type: type, default_queue_size: int) -> list:
    """Create chain of queues connecting stages.

    Returns list of queues where queues[i] is the input queue for stages[i].
    The last queue (queues[len(stages)]) is the output queue.
    """
    queues = []

    # Create input queue for each stage
    for stage in stages:
        size = (
            stage.inbound_queue_size if stage.inbound_queue_size is not None else default_queue_size
        )

        if size <= 0:
            raise ValueError(f"queue_size must be positive integer, got {size}")

        # Create queue based on type
        if queue_type == queue.Queue:
            q = queue.Queue(maxsize=size)
        else:
            q = multiprocessing.Queue(maxsize=size)

        queues.append(q)

    # Add final output queue
    if queue_type == queue.Queue:
        queues.append(queue.Queue(maxsize=default_queue_size))
    else:
        queues.append(multiprocessing.Queue(maxsize=default_queue_size))

    return queues


@dataclass
class PipelineBuild:
    """Result of building a pipeline."""

    stages: list[Stage]  # Stages with adapters inserted
    queues: list  # Queue chain
    queue_type: type  # Queue class used
