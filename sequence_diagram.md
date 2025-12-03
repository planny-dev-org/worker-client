

```mermaid
sequenceDiagram
    participant Redis output jobstreams
    participant Redis log streams
    participant Redis job stream


    Worker 1 (consumer group 1_0)->>Redis job stream: Wait message
    Worker 2 (consumer group 1_0)->>Redis job stream: Wait message
    Worker 3 (consumer group 1_0)->>Redis job stream: Wait message
    
    Redis job stream->>Worker 1 (consumer group 1_0): message(payload, log_stream_key, output_job_stream_key)
    Worker 1 (consumer group 1_0)->>Redis log streams: logs to stream identified by log_stream_key
    Worker 1 (consumer group 1_0)->>Redis output jobstreams: send ongoing result to stream identified by output_job_stream_key
    Worker 1 (consumer group 1_0)->>Redis log streams: logs to stream identified by log_stream_key
    Worker 1 (consumer group 1_0)->>Redis output jobstreams: send final result to stream identified by output_job_stream_key
    Worker 1 (consumer group 1_0)->>Redis job stream: acknowledge message
    Worker 1 (consumer group 1_0)->>Redis job stream: Wait message
    
    