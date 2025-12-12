# Producer and Consumer API

## Overview

The `worker-client` library now exposes two main classes for Redis-based messaging:

- **`Producer[T]`**: Sends typed messages to Redis streams
- **`Consumer[T]`**: Receives and processes typed messages from Redis streams

Both classes are fully generic and type-safe, using Protocol-based encoders and decoders.

## Architecture

```
Producer Flow:
  Typed Message (T) → Encoder → JSON String → Redis Stream

Consumer Flow:
  Redis Stream → JSON String → Decoder → Typed Message (T) → Handler
```

## Producer

### Creating a Producer

```python
from worker_client import Producer, Encoder

# Define your encoder
def my_encoder(message: MyType) -> str:
    return json.dumps({"field": message.field})

# Create producer
producer: Producer[MyType] = Producer(encoder=my_encoder)
```

### Sending Messages

```python
message = MyType(field="value")

message_id = producer.send(
    message=message,
    log_stream_key="worker:logs:12345",
    output_stream_key="worker:output:12345",
    remote_resource_id="resource_id",
)

producer.close()  # Clean up when done
```

### Producer Methods

- **`__init__(encoder, stream_key=UPSTREAM_KEY)`**: Initialize with encoder and optional stream key
- **`send(message, log_stream_key, output_stream_key, remote_resource_id)`**: Send a typed message, returns message ID
- **`close()`**: Close Redis connection

## Consumer

### Creating a Consumer

```python
from worker_client import Consumer, Decoder

# Define your decoder
def my_decoder(raw: bytes) -> MyType:
    data = json.loads(raw.decode('utf-8'))
    return MyType(field=data['field'])

# Define your handler
def my_handler(message: MyType) -> None:
    print(f"Processing: {message.field}")

# Create consumer
consumer: Consumer[MyType] = Consumer(
    decoder=my_decoder,
    handler=my_handler
)
```

### Running the Consumer

```python
consumer.run()  # Blocks and processes messages until interrupted
```

### Consumer Methods

- **`__init__(decoder, handler)`**: Initialize with decoder and handler
- **`run()`**: Start the main processing loop (blocking)
- **`log(message, level)`**: Send log message to log stream
- **`output(message)`**: Send output message to output stream
- **`acknowledge(message_id)`**: Acknowledge message processing
- **`health_check()`**: Check Redis connection health
- **`close()`**: Close Redis connection

## Type Safety

Both `Producer[T]` and `Consumer[T]` are generic:

- **Producer**: `encoder` must accept type `T` and return `str`
- **Consumer**: `decoder` must accept `bytes` and return type `T`, `handler` must accept type `T`

The type checker ensures:
- ✅ Encoder/decoder match the message type
- ✅ Handler receives correctly typed messages
- ✅ No runtime type errors
- ✅ Full IDE autocomplete

## Example

See `example_usage.py` for a complete working example with both Producer and Consumer.

## Protocols

### Encoder Protocol

```python
class Encoder(Protocol[T_contra]):
    def __call__(self, message: T_contra) -> str: ...
```

### Decoder Protocol

```python
class Decoder(Protocol[T_co]):
    def __call__(self, raw: bytes) -> T_co: ...
```

## Migration from Old Code

If you were using the consumer directly, the API remains the same:

```python
# Old way (still works)
consumer = Consumer(decoder=my_decoder, handler=my_handler)
consumer.run()

# New: You can also use Producer to send messages
producer = Producer(encoder=my_encoder)
producer.send(message, ...)
```
