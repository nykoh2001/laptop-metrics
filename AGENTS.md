## Project Overview

Build a local real-time system monitoring platform for macOS.

The system continuously collects host and process metrics, streams them through Kafka, standardizes the data using Spark Structured Streaming, performs streaming analytics, stores time-series metrics in ClickHouse, and visualizes results in Grafana.

The project is intended as a learning project for streaming data platforms and should resemble a simplified manufacturing data pipeline.

---

## Tech Stack

- Language: Python 3.11+
- Containerization: Docker Compose
- Streaming: Apache Kafka (KRaft mode)
- Stream Processing: Spark Structured Streaming
- Time-series Database: ClickHouse
- Visualization: Grafana

The metric collector runs directly on the macOS host.

---

## Development Principles

- Prefer a simple working implementation over a feature-complete implementation.
- Build the pipeline incrementally.
- Validate each stage before moving to the next.
- Keep each component loosely coupled.
- Avoid unnecessary abstractions until they become useful.
- Favor readability and maintainability over clever implementations.
- Do not make architectural changes or introduce new technologies unless explicitly requested by the user.

---

## Coding Guidelines

- Use Python type hints for all public APIs.
- Avoid using `Any` unless absolutely necessary.
- Use Google-style multi-line docstrings for every public function and class.
- Include `Args`, `Returns`, and `Raises` sections when applicable.
- Keep functions small and focused.
- Avoid global mutable state.
- Configuration should come from environment variables.
- Separate configuration, business logic, and infrastructure.
- Avoid duplicated code.
- Prefer composition over inheritance.

---

## Project Structure

The project structure may evolve, but should remain roughly organized as:

```text
collector/
spark/
clickhouse/
grafana/
docker/
configs/
scripts/
tests/
```

Avoid placing unrelated functionality in the same module.

---

## Metrics

Initially collect only basic metrics.

### Host Metrics

- CPU
- Memory
- Swap
- Disk usage
- Disk I/O
- Network I/O

### Process Metrics

- CPU usage
- Memory usage
- Disk I/O
- Process name
- PID

The schema will evolve over time.

---

## Testing

Every completed phase should be manually verified.

When adding automated tests:

- Prefer `pytest`.
- Test business logic separately from infrastructure.
- Mock external services whenever practical.
- Every new feature should include tests whenever practical.

---

## Logging

Use Python's `logging` module.

Avoid `print()` statements except for temporary debugging.

Log:

- Startup
- Shutdown
- Kafka connection
- Processing failures
- Unexpected exceptions

---

## Configuration

Configuration should be environment-variable based whenever possible.

Examples:

- Kafka broker
- Topic names
- Collection interval
- ClickHouse connection
- Log level

Provide a `.env.example`.

---

## Dependencies

- Do not introduce new production dependencies unless necessary.
- Prefer the Python standard library whenever practical.
- Explain why any new dependency is required before using it.

---

## Assumptions

- Do not assume missing requirements.
- If multiple reasonable implementations exist, choose the simplest one.
- If a design decision could significantly affect the architecture, explain the trade-offs before implementing it.

---

## Before Completing Any Task

Before considering a task complete:

- Ensure the project still runs.
- Run formatting, linting, type checking, and tests.
- Do not leave TODO placeholders for implemented features.
- Update documentation when behavior or architecture changes.
- Explain major design decisions when introducing new components.

---

## Future Enhancements

These features are intentionally out of scope for the first implementation but should be considered when designing the architecture.

- Canonical schema evolution
- Data validation
- Dead Letter Queue (DLQ)
- Schema versioning
- Process name normalization
- User-reported slowdown events
- Watermark handling
- Checkpoint recovery
- Multi-host support
- Linux and Windows collectors
- Root cause analysis
- ML-based anomaly detection

