# Initial type-check failures

## Failed cases

The first strict mypy run reported two implementation typing errors:

- `subprocess.Popen[str].stdout` was narrowed to `IO[str]`, not the more specific `TextIO` alias.
- A collector list was inferred from its first concrete members and did not accept the optional
  Docker collector as the shared protocol type.
- The initial network fallback protocol declared named-tuple connection fields as writable, while
  psutil exposes them as read-only attributes.

## Why the initial method failed

Both annotations were narrower than the actual abstractions: subprocess exposes a generic text
stream, and the composed list intentionally contains different implementations of one protocol.

## Improved method

The redundant stdout annotation was removed so the subprocess type is preserved, and the source
list is explicitly annotated as `list[EventCollector]`. This keeps strict checking without casts or
`Any`. Network connection fields are expressed as read-only protocol properties so both psutil's
system-wide and per-process named tuples satisfy the contract.
