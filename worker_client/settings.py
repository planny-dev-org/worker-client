from decouple import config

# redis upstream key where job payloads are read from
UPSTREAM_KEY = config("STREAM_KEY_INPUT", "worker_input_stream")

# consumers
CONSUMER_VERSION_MAJOR = config("CONSUMER_VERSION_MAJOR", cast=int)
CONSUMER_VERSION_MINOR = config("CONSUMER_VERSION_MINOR", cast=int)
CONSUMER_NAME_PREFIX = config("CONSUMER_NAME_PREFIX")

# redis
REDIS_SCHEME = config("REDIS_SCHEME", "redis://")
REDIS_HOST = config("REDIS_HOST", "localhost")
REDIS_PORT = config("REDIS_PORT", 6379, cast=int)
REDIS_DB = config("REDIS_DB", 0, cast=int)
REDIS_SSL_CERT_REQS = config("REDIS_SSL_CERT_REQS", "none")
REDIS_SSL_CERT_PATH = config("REDIS_SSL_CERT_PATH", None)
REDIS_SSL_KEY_PATH = config("REDIS_SSL_KEY_PATH", None)
