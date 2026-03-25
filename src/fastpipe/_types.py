"""Core types for fastpipe.

This module contains fundamental types used across the codebase,
isolated to break circular dependencies.
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Optional


class Operation(Enum):
    """Stage operation types."""

    MAP = auto()
    FLAT_MAP = auto()
    FILTER = auto()
    EACH = auto()
    BATCH = auto()
    UNBATCH = auto()
    ADAPTER = auto()  # Special: forwards data, multiplies SENTINEL for downstream


class Mode(Enum):
    """Execution mode for pipeline stages."""

    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"

    def __str__(self):
        """Return string value for compatibility."""
        return self.value


@dataclass
class AdapterInfo:
    """Configuration for adapter stages (explicit type).

    Adapters bridge mode transitions by:
    1. Receiving upstream_workers SENTINELs from upstream
    2. Forwarding data unchanged
    3. Outputting downstream_workers SENTINELs to downstream
    """

    upstream_workers: int  # How many SENTINELs to receive
    downstream_workers: int  # How many SENTINELs to send


@dataclass
class Stage:
    """Internal stage representation."""

    operation: Operation
    func: Optional[Callable]
    workers: int
    mode: str  # 'thread', 'process', or 'async' (or Mode enum)
    inbound_queue_size: Optional[int]
    batch_size: Optional[int] = None  # For BATCH operation
    adapter_info: Optional[AdapterInfo] = None  # For ADAPTER operation (explicit field)
    init_args: tuple = ()  # For callable class initialization
    init_kwargs: Optional[dict] = None  # For callable class initialization

    def __post_init__(self):
        """Normalize mode to string for internal consistency."""
        if isinstance(self.mode, Mode):
            self.mode = self.mode.value  # Convert enum to string
        if self.init_kwargs is None:
            self.init_kwargs = {}
