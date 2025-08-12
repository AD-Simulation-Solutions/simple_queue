# simple_queue

A **simple** RabbitMQ streaming library that hides the complexity of exchanges and routing keys behind an easy-to-use queue-based API.

## Why simple_queue?

RabbitMQ is powerful but complex. Most applications just want to send messages to a queue and receive them - without dealing with exchanges, routing keys, bindings, and other AMQP concepts. 

**simple_queue** provides a clean, queue-focused API that handles all the RabbitMQ complexity under the hood.

## Key Features

- 🎯 **Queue-focused API**: Just specify a queue name - no exchanges or routing keys needed
- 🔄 **Automatic reconnection**: Handles connection drops seamlessly
- 🛡️ **Built-in error handling**: Retry logic for publishers, graceful error recovery for consumers
- 📦 **Smart message handling**: Automatic JSON serialization/deserialization with string fallback
- 🚫 **Overflow protection**: Configurable queue limits with reject-publish behavior
- 🔧 **Zero configuration**: Works out of the box with sensible defaults

## Installation

```bash
pip install simple_queue
```

Or install from source:

```bash
git clone <repository-url>
cd simple_queue
pip install .
```

## Quick Start

### Publishing Messages

```python
from simple_queue import queue_publisher

with queue_publisher(
    queue_name="task_queue",
    uri="amqp://guest:guest@localhost:5672/",
    queue_size=1000,
    retry_times=3,
    retry_delay=1.0
) as publisher:
    # Send any JSON-serializable data
    publisher.push({"task": "process_data", "id": 123})
    
    # Send simple strings
    publisher.push("Hello, World!")
    
    # Send lists
    publisher.push([1, 2, 3, "data"])
```

### Consuming Messages

```python
from simple_queue import queue_consumer

with queue_consumer(
    queue_name="task_queue", 
    uri="amqp://guest:guest@localhost:5672/",
    queue_size=1000
) as consumer:
    # Process messages continuously
    for message in consumer.read_continuous():
        print(f"Processing: {message}")
        # Your message processing logic here
```

## Public API

simple_queue exposes only **two functions** - keeping the API minimal and focused:

### `queue_publisher()`

Creates a message publisher for a specific queue.

```python
def queue_publisher(
    queue_name: str,      # Name of the queue to publish to
    uri: str,             # RabbitMQ connection URI
    queue_size: int,      # Maximum queue size (overflow protection)
    retry_times: int,     # Number of retry attempts on failure
    retry_delay: float    # Delay between retries in seconds
) -> QueuePublisher
```

**Returns**: A context manager that yields a publisher with a `push(message)` method.

### `queue_consumer()`

Creates a message consumer for a specific queue.

```python
def queue_consumer(
    queue_name: str,      # Name of the queue to consume from
    uri: str,             # RabbitMQ connection URI  
    queue_size: int       # Maximum queue size
) -> QueueConsumer
```

**Returns**: A context manager that yields a consumer with `read()` and `read_continuous()` methods.

## What Happens Under the Hood

simple_queue abstracts away RabbitMQ's exchange and routing key complexity by automatically creating and managing the necessary AMQP resources:

### For Each Queue

When you use `queue_publisher()` or `queue_consumer()` with a queue name like `"task_queue"`, simple_queue automatically:

1. **Creates a durable queue** named `task_queue` with:
   - `durable=True` (survives broker restarts)
   - `max_length=queue_size` (your specified limit)
   - `x-overflow=reject-publish` (rejects new messages when full)

2. **Creates a direct exchange** named `task_queue_exchange`

3. **Binds the queue to the exchange** using the queue name (`task_queue`) as the routing key

4. **Configures publisher confirms** for reliable message delivery

### Message Flow

```
Publisher → task_queue_exchange → (routing_key: task_queue) → task_queue → Consumer
```

This setup provides:
- **Reliable delivery**: Messages are persisted and survive broker restarts
- **Simple routing**: One queue = one exchange = one routing key
- **Overflow protection**: Queue rejects messages when full instead of dropping them
- **Direct delivery**: No complex routing logic - messages go straight to the intended queue

### Connection Management

- **Auto-reconnection**: Detects connection failures and reconnects automatically
- **Channel management**: Creates and manages RabbitMQ channels internally
- **Resource cleanup**: Properly closes connections and channels when context managers exit

## Message Handling

### Automatic Serialization

simple_queue automatically handles message serialization:

- **Dictionaries and lists**: Serialized to JSON
- **Strings**: Sent as-is  
- **Other types**: Converted to string representation

```python
# All of these work automatically
publisher.push({"user_id": 123, "action": "login"})  # JSON
publisher.push([1, 2, 3, 4])                         # JSON  
publisher.push("Simple string message")               # String
publisher.push(42)                                    # Converted to "42"
```

### Automatic Deserialization

When consuming, simple_queue tries to parse JSON first, falls back to strings:

```python
for message in consumer.read_continuous():
    # message is automatically:
    # - Parsed as dict/list if valid JSON
    # - Returned as string if not JSON
    print(type(message), message)
```

## Configuration

### Connection URI Format

```
amqp://username:password@hostname:port/virtual_host
```

**Examples:**
- Local development: `amqp://guest:guest@localhost:5672/`
- Production: `amqp://user:pass@rabbitmq.example.com:5672/prod`

### Parameters

| Parameter | Description | Publisher | Consumer |
|-----------|-------------|-----------|----------|
| `queue_name` | Name of the queue | ✓ | ✓ |
| `uri` | RabbitMQ connection string | ✓ | ✓ |
| `queue_size` | Maximum messages in queue | ✓ | ✓ |
| `retry_times` | Retry attempts on failure | ✓ | - |
| `retry_delay` | Seconds between retries | ✓ | - |

## Error Handling & Reliability

### Publisher Reliability
- **Retry logic**: Configurable retry attempts with delays
- **Publisher confirms**: Ensures messages are accepted by broker
- **Connection recovery**: Auto-reconnects on connection loss
- **Overflow handling**: Graceful handling when queue is full

### Consumer Reliability  
- **Auto-reconnection**: Seamlessly handles connection drops
- **Message acknowledgment**: Messages are ack'd only after successful processing
- **Error isolation**: Processing errors don't crash the consumer
- **Graceful shutdown**: Clean exit on KeyboardInterrupt

## Examples

The package includes working example scripts:

### Publisher Example
```bash
# Run the included publisher example
python examples/publisher_example.py
```

### Consumer Example  
```bash
# Run the included consumer example
python examples/consumer_example.py
```

### Example Scripts via Entry Points
After installation, you can also run:
```bash
simple-stream-publisher  # Starts the publisher example
simple-stream-consumer   # Starts the consumer example
```

## Requirements

- **Python**: 3.7+
- **RabbitMQ**: Any recent version
- **Dependencies**: 
  - `rabbitpy==2.0.1` (RabbitMQ client)
  - `pydantic==2.5.0` (Data validation)

## Development

### Setup
```bash
git clone <repository-url>
cd simple_queue
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -e ".[dev]"
```

### Testing
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=simple_queue

# Run specific test file
pytest tests/test_queue_publisher.py
```

### Code Quality
```bash
# Format code
black simple_queue/

# Type checking  
mypy simple_queue/

# Linting
flake8 simple_queue/
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
