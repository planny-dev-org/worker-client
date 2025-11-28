**Helps planny_model workers to dialog with redis streams in order to:**

- get a job payload
- send logs
- send ongoing results
- send final result and acknowledge message

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

Here is a job process example that instantiate a consumer, wait for a job process it and start again if needed

```
from worker_client import Consumer, LogMessage

consumer = Consumer()

while True:
    consumer.new_job()  # this will block until a job is received

    ##########################
    # process job & issue logs
    ##########################
    # some treament ...
    consumer.log(message="pre-processing done")  # issue a log with default level INFO

    # some treatment ...
    consumer.log(message="resource ignored", level="WARNING")  # issue a warning
    
    # some treatment then issue an output result ...
    consumer.output(message={"vars": {"person_one": 1}})
  
    #######################################
    # process finished, acknowledge message
    #######################################
    consumer.acknowledge()

    ####################################################################################
    # check if consumer has been asked to gracefully terminate before taking another job
    ####################################################################################
    if consumer.exit_loop:
        break  # This will make container to gracefully stop because main loop has been exited
```


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
