# Spark Python runtime compatibility failure

## Failure

The first real Docker Spark submission stopped before starting the stream with:

```text
ImportError: cannot import name 'UTC' from 'datetime'
```

The pinned Spark 3.5.8 Docker image runs Python 3.10, while `datetime.UTC` was added in Python
3.11. Local tests used the project's Python 3.12 virtual environment and therefore did not expose
the container runtime mismatch.

## Cause

The Spark job used a Python 3.11 convenience constant even though it executes inside the Spark
image rather than the native collector virtual environment.

## Improvement

The Spark job now uses `timezone.utc`, which provides the same timezone semantics on Python 3.10.
The collector can retain its documented Python 3.11+ requirement, while the independently deployed
Spark code remains compatible with the pinned container runtime. The Docker Spark submission is
rerun as part of the real end-to-end smoke test.
