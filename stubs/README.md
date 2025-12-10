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
