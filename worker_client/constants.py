OUTPUT_STREAM_FIELD_NAME: str = "output_stream_key"
LOG_STREAM_FIELD_NAME: str = "log_stream_key"
# Structured lifecycle and telemetry events, kept off the human-readable log
# stream. Optional in the upstream message: when it is absent, events fall back
# to the log stream, so a backend that does not send it keeps working unchanged.
EVENT_STREAM_FIELD_NAME: str = "event_stream_key"
REMOTE_RESOURCE_ID_FIELD_NAME: str = "remote_resource_id"
REMOTE_CALLBACK_URL_FIELD_NAME: str = "remote_callback_url"
PAYLOAD_FIELD_NAME: str = "payload"
