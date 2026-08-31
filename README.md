**Helps planny_model workers to dialog with redis streams in order to:**

- get a job payload
- send logs
- send ongoing results
- send final result and acknowledge message

## 🎉 Full Type Hints Support

The library supports full generic types with `Consumer[T]`, allowing you to work with strongly-typed messages.

### Architecture

**JSON String Flow:**
1. **Redis** stores the payload as a JSON string
2. **Decoder** receives `bytes` (JSON string encoded as UTF-8) and returns typed object `T`
3. **Handler** receives the fully typed object `T`

```python
from worker_client import Consumer
from dataclasses import dataclass
import json

@dataclass
class MyMessage:
    task_id: str
    action: str

def my_decoder(raw: bytes) -> MyMessage:
    """Decoder converts JSON bytes → typed object"""
    data = json.loads(raw.decode('utf-8'))
    return MyMessage(**data)

def my_handler(message: MyMessage) -> None:
    """Handler receives typed object"""
    print(f"Task: {message.task_id}")  # ✅ Fully typed!

consumer: Consumer[MyMessage] = Consumer(
    decoder=my_decoder,
    handler=my_handler
)
consumer.run()  # Automatic processing with full type safety
```

### Key Concepts

- **`Consumer[T]`**: Generic consumer that processes messages of type `T`
- **`Decoder[T]`**: Protocol for functions that convert `bytes` → `T`
- **`ConsumerJob`**: Holds the raw JSON string payload from Redis
- **Type Safety**: The decoder ensures you get the exact type you expect

# Flow diagram

Generated with `sequence_diagram.md` typora file (see in this repo) 

Here in the example of Worker 1 execution after it received a message.

Workers are part of the same consumer group, this means each message will only be consumer by 1 worker.

![diagram.png](diagram.png)

# Flow description

Here is described the Worker 1 sequence

- Worker 1 is connecting to stream named after the worker version, pattern is "<major>_<minor>_<patch>" where values are fetched from worker environement (see below)
- Worker 1 wait for a message on that stream
- Server is sending a job payload on the stream, this payload includes 3 fields:
  - payload: the job payload (aka JobSerializerData)
  - log_stream_key: the name of the stream where worker logs message are expected to be sent
  - output_job_stream_key: the name of the stream where worker ongoing output payloads and final payload are expected to be sent
- Worker 1 start running and send logs and ongoing results to expected streams
- Worker 1 finish by sending final output payload and message acknowledgement
- Worker 1 go back to job stream and wait for a new message


# Job stream

This stream is created by worker if not exists.

Since different versions of workers may exist with different expected payloads. A convention is to:
- name the stream using convention "<major>_<minor>", all workers version that fit this are expected to be able to consume messages from this stream.
- consumer names use convention "<major>_<minor>_<patch>", so that we can identify which worker version has processed messages.


# Usage

## New Typed API (Recommended)

For full type safety, use the `Consumer[T]` generic approach:

```python
from worker_client import Consumer
from dataclasses import dataclass
import json

@dataclass
class JobPayload:
    task_id: str
    data: dict

def decoder(raw: bytes) -> JobPayload:
    return JobPayload(**json.loads(raw.decode('utf-8')))

def handler(message: JobPayload) -> None:
    print(f"Processing {message.task_id}")
    # Your logic here

consumer: Consumer[JobPayload] = Consumer(
    decoder=decoder,
    handler=handler
)
consumer.run()  # Handles everything automatically
```

## Legacy API (Still Supported)

The original API continues to work for backward compatibility:

Here is a job process example that instantiate a consumer, wait for a job process it and start again if needed

```
from worker_client import Consumer, LogMessage

consumer = Consumer()

while True:
    consumer.new_job()  # this will block until a job is received and update consumer.job instance 

    ##########################
    # process job & issue logs
    ##########################
    # access job payload
    payload = consumer.job.payload
    
    # do some treament then issue a log ...
    pre_processing_output = some_treatment(payload) 
    consumer.log(message="pre-processing done")  # issue a log with default level INFO

    # do some other treatment then issue a log ...
    some_more_treatment(pre_processing_output)
    consumer.log(message="resource ignored", level="WARNING")  # issue a warning
    
    # do some treatment then issue an output result ...
    output_value_int = last_treatment(pre_processing_output)
    consumer.output(message={"vars": {"person_one": output_value_int}})
  
    ########################################
    # Treatment finished, acknowledge message
    ########################################
    consumer.acknowledge()

    ####################################################################################
    # check if consumer has been asked to gracefully terminate before taking another job
    ####################################################################################
    if consumer.exit_loop:
        break  # This will make container to gracefully stop because main loop has been exited
```


# Log stream vs event stream

A job carries two reply streams for telemetry, because they have different readers:

| method | stream | read by | carries |
|---|---|---|---|
| `consumer.log(...)` | `log_stream_key` | people | human-readable lines |
| `consumer.event(...)` | `event_stream_key` | the backend | structured state: started, progress, failed, solver telemetry |

`event_stream_key` is **optional** in the upstream message. When it is absent,
`consumer.event()` falls back to the log stream, so a backend that does not send one
keeps working and no event is dropped. The fallback lives on `ConsumerJob.event_stream_key`,
so no call site has to check.

Both take `str` or a dict; a dict is JSON-encoded, since Redis stream fields are flat.
Serialised entries are distinguishable by their `type` field: `LOG`, `EVENT` or `OUTPUT`.

```python
consumer.log(message="pre-processing done")
consumer.event(message={"kind": "progress", "pct": 45})
consumer.event(message={"kind": "failed", "error": "infeasible"}, level="ERROR")
```

Unlike `output()`, `event()` never triggers the `remote_callback_url` POST -- it is
telemetry, not a result, so it is safe to call as often as needed.

# Env var setup

Here are default env vars setup. It can be defined in a `.ini` file that decouple can find and customized to your needs

```
# worker input stream key
STREAM_KEY_INPUT "worker_input_stream"

# consumers
CONSUMER_VERSION_MAJOR 0
CONSUMER_VERSION_MINOR 0
CONSUMER_NAME_PREFIX "worker_client_consumer"

# redis
REDIS_SCHEME "redis://"
REDIS_HOST "localhost"
REDIS_PORT 6379
REDIS_DB 0
REDIS_SSL_CERT_REQS "none"
REDIS_SSL_CERT_PATH None
REDIS_SSL_KEY_PATH None
```
