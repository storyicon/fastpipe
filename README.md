# FastPipe

[![PyPI](https://img.shields.io/pypi/v/fastpipe.svg)](https://pypi.org/project/fastpipe/)
[![Python](https://img.shields.io/pypi/pyversions/fastpipe.svg)](https://pypi.org/project/fastpipe/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> Minimal parallel pipeline library for Python with thread/process/async execution, batch and streaming modes

Build parallel data processing pipelines with clean API. Zero dependencies, type-safe, perfect for ETL and single-machine parallel computing.

**Perfect for**: ETL pipelines • Data processing • API batch operations • Stream processing

## Quick Start

```python
import fastpipe

# Batch processing - collect all results
results = (fastpipe.create()
    .map(download, workers=4)
    .map(process, workers=4)
    .run(data))

# Iterator mode - memory-efficient for large datasets
for result in fastpipe.create().map(process, workers=4).iter(huge_data):
    save_to_db(result)

# Streaming mode - continuous put/get
stream = (fastpipe.create()
    .map(process, workers=4)
    .stream())

stream.put(item)
result = stream.get()
stream.close()
```

## Installation

```bash
pip install fastpipe
```

## Features

- **Three execution modes**: `thread`, `process`, `async` with automatic adapters
- **Three operation modes**: `run()` (batch), `iter()` (memory-efficient), `stream()` (continuous)
- **Built-in batching**: `batch()/unbatch()` for performance optimization
- **Backpressure control**: Bounded queues prevent OOM
- **Zero dependencies**: stdlib only
- **Thread-safe**: No locks needed

## API

### Operations

```python
pipe = fastpipe.create() \
    .map(func, workers=4, mode=fastpipe.Mode.THREAD) \
    .flat_map(func, workers=4) \
    .filter(predicate, workers=2) \
    .each(side_effect, workers=1) \
    .batch(size=10) \
    .unbatch()
```

**Available operations**:
- `map(func, workers=1)` - Transform each item (1:1)
- `flat_map(func, workers=1)` - Transform and flatten (1:N)
- `filter(predicate, workers=1)` - Keep matching items
- `each(func, workers=1)` - Side effects, return original
- `batch(size)` - Group into fixed-size batches
- `unbatch()` - Flatten batches back to items

### Execution Modes

```python
# Thread mode (I/O-bound)
.map(download, workers=10, mode=fastpipe.Mode.THREAD)

# Process mode (CPU-bound)
.map(compute, workers=4, mode=fastpipe.Mode.PROCESS)

# Async mode (high-concurrency I/O)
.map(async_fetch, workers=100, mode=fastpipe.Mode.ASYNC)

# Mix freely - automatic adapters inserted
```

**Mode selection**:
- `Mode.THREAD` - I/O-bound, 1-20 workers
- `Mode.PROCESS` - CPU-bound, 4-16 workers (bypasses GIL)
- `Mode.ASYNC` - I/O-bound, 50-1000+ workers (requires async functions)

### Running Pipelines

| Method | Use Case | Memory | Example |
|--------|----------|--------|---------|
| `run(data)` | Small datasets | O(n) | `pipe.run(data)` |
| `iter(data)` | Large datasets | O(1) | `for x in pipe.iter(data): save(x)` |
| `stream()` | Continuous | O(queue) | `stream.put(x); stream.get()` |

**Stream API**:
```python
stream = pipe.stream(queue_size=100)
stream.put(item)                  # Add item
result = stream.get()             # Get result
stream.close(drain=True)          # Shutdown
```

### Callable Classes

Pass class types with `init_args`/`init_kwargs` for efficient multiprocess execution:

```python
class ModelProcessor:
    def __init__(self, model_path, device='cpu'):
        # Each worker loads model independently
        self.model = load_model(model_path)
        self.device = device

    def __call__(self, data):
        return self.model.predict(data)

# ✅ Efficient: Only class definition is serialized
results = (fastpipe.create()
    .map(
        ModelProcessor,
        workers=4,
        mode='process',
        init_args=('model.pth',),
        init_kwargs={'device': 'cuda'}
    )
    .run(data))

# ❌ Inefficient: Entire model serialized to each worker
processor = ModelProcessor('model.pth')
results = pipe.map(processor, workers=4, mode='process').run(data)
```

**Benefits**:
- Avoids serializing large objects (models, databases)
- Per-worker resource management
- Independent worker state
- Works with non-picklable resources

**Supported on all operations**: `map()`, `filter()`, `flat_map()`, `each()`

## Examples

### Async High-Concurrency

```python
import fastpipe
import aiohttp

async def fetch(url):
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            return await r.text()

results = (fastpipe.create()
    .map(fetch, workers=100, mode=fastpipe.Mode.ASYNC)
    .map(parse, workers=4, mode=fastpipe.Mode.THREAD)
    .run(urls))
```

### Mixed CPU/IO Pipeline

```python
pipe = fastpipe.create() \
    .map(download, workers=10, mode=fastpipe.Mode.THREAD) \
    .batch(size=100) \
    .map(process_batch, workers=4, mode=fastpipe.Mode.PROCESS) \
    .unbatch() \
    .map(upload, workers=5, mode=fastpipe.Mode.THREAD)

results = pipe.run(urls)
```

### Kafka Stream Processing

```python
pipe = fastpipe.create() \
    .map(parse, workers=4) \
    .filter(validate, workers=2) \
    .map(transform, workers=4)

stream = pipe.stream(queue_size=500)

# Producer thread
for event in kafka_consumer:
    stream.put(event)

# Consumer thread
while running:
    result = stream.get()
    database.save(result)
```

## Error Handling

**Fail-fast**: First exception stops entire pipeline immediately.

```python
try:
    results = fastpipe.create().map(may_fail, workers=4).run(data)
except ValueError as e:
    print(f"Pipeline failed: {e}")
```

**Error tolerance**: Wrap your functions.

```python
def safe_process(x):
    try:
        return risky_operation(x)
    except Exception as e:
        logger.warning(f"Failed: {e}")
        return None

results = fastpipe.create() \
    .map(safe_process, workers=4) \
    .filter(lambda x: x is not None) \
    .run(data)
```

## vs pypeln

| Feature | pypeln | fastpipe |
|---------|--------|----------|
| Streaming mode | ❌ | ✅ |
| Async support | Separate module | Unified API |
| Mode switching | Change imports | Change parameter |
| Auto adapters | Manual | Automatic |
| Maintenance | Abandoned | Active |

## Architecture

**Thread-safety by design**:
- Stateful ops (BATCH, ADAPTER): Single worker (no races)
- Stateless ops (MAP, FILTER): Multiple workers (no shared state)
- Queues: Thread-safe (stdlib)

**Automatic adapters**: Inserted when mode OR worker count changes between stages.

**SENTINEL propagation**: End-of-stream markers coordinate worker shutdown across stages.

## Design Philosophy

**Provide primitives, not policies.**

FastPipe focuses on core parallel processing. You add retry (tenacity), metrics (prometheus), and timeouts as needed.

**Scope**:
- ✅ Single-machine parallel processing (10-1000 workers)
- ❌ Distributed computing → Use Ray, Dask, Spark
- ❌ Task queues → Use Celery, RQ

**Principles**:
1. **Simple over clever** - No magic, explicit API
2. **Reliable over feature-rich** - Thread-safe by design, fail-fast
3. **Composable over monolithic** - Small API, integrates with ecosystem
4. **No black magic** - No `PyThreadState_SetAsyncExc` hacks, no hidden state
5. **Performance without compromise** - Fast but correct

**Example**: No built-in retry. You choose your library:
```python
from tenacity import retry, stop_after_attempt

@retry(stop=stop_after_attempt(3))
def func(x):
    return risky_operation(x)

pipe.map(func, workers=4)  # Your policy, your choice
```

## FastPipe vs Ray

**Different scope, complementary tools**:

| | Ray | FastPipe |
|-|-----|----------|
| **Scope** | Multi-machine cluster | Single machine |
| **Execution** | Process-based tasks | Thread/process/async |
| **Data passing** | Always serialized (object store) | Zero-copy in thread/async |
| **Use case** | Distributed ML, TB-scale | ETL, data pipelines, GB-scale |

**Key difference: Serialization overhead**

Ray requires all data to pass through its object store (serialization/deserialization). FastPipe's thread/async modes avoid this entirely with zero-copy reference passing—crucial for large numpy arrays, models, or non-picklable objects.

**Complementary usage**:
```python
import ray, fastpipe

@ray.remote
def process_shard(shard):
    # Ray: distribute across cluster
    # FastPipe: optimize local processing
    return fastpipe.create().map(f, workers=10, mode='thread').run(shard)
```

**When to use**: Ray for multi-node coordination, FastPipe for single-machine efficiency.

## License

MIT
