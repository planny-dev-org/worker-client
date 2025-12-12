# Type Stubs

This directory contains type stubs (`.pyi` files) for third-party libraries that don't have built-in type annotations.

## Included Stubs

### `decouple.pyi`
Type stubs for `python-decouple` library's `config()` function with overloads to support:
- `config(option, default, cast)` → returns cast type
- `config(option, cast=...)` → returns cast type  
- `config(option, default)` → returns default type
- `config(option)` → returns str

### `decorator.pyi`
Type stubs for the `decorator` library's `decorator.decorator()` function that preserves function signatures.

### `redis/`
Type stubs for the `redis` library including:
- `Redis` class with methods: `from_url`, `ping`, `xgroup_create`, `xreadgroup`, `xread`, `xadd`, `xack`, `xpending`, `flushall`, `close`
- `exceptions` module with: `ResponseError`, `ConnectionError`, `RedisError`
- Proper return types for `decode_responses=True` mode (returns strings instead of bytes)

### `worker_client/`
Type stubs for the `worker_client` library itself (for external usage):
- `Consumer[T]` - Generic consumer class with decoder and handler
- `Producer[T]` - Generic producer class with encoder
- `Decoder[T]` and `Encoder[T]` - Protocol types for type-safe message transformation
- `ConsumerJob`, `Message` - Data classes
- `get_redis_client()` - Redis client factory
- `LEVEL_LITERAL`, `MSG_TYPE_LITERAL` - Type aliases

## Configuration

These stubs are configured in `pyproject.toml`:

```toml
[tool.pyright]
stubPath = "stubs"

[tool.mypy]
mypy_path = "stubs"
```

## Benefits

With these stubs in place:
- ✅ No `# type: ignore[import-untyped]` needed for decouple
- ✅ Fewer `# type: ignore[attr-defined]` for redis.exceptions
- ✅ Better autocomplete and type checking
- ✅ Type-safe configuration access
- ✅ Full generic type support for `Consumer[T]` and `Producer[T]`
- ✅ External packages can import worker_client with full type information

## Validation

Run `python test_stubs.py` to validate that all stubs are complete and correctly typed.
