# Docker-backed pipeline smoke test unavailable

## Failed case

`docker compose ps` and `docker info` could not connect to the local Docker daemon. Therefore the
real Kafka broker, Spark container, and ClickHouse server could not be started for an end-to-end
smoke test.

## Why the existing method failed

Docker Desktop or another compatible daemon was not running or accessible to the current user. This
is an expected optional-environment condition, not a reason to mount a Docker socket into the
collector, run with sudo, or grant container privileges.

## Improved verification method

Keep automated collector, serializer, Spark parsing, storage-guard, and HTTP sink tests independent
of Docker. When a user-accessible daemon is available, run the minimal commands in `README.md`, then
query `security_events` in ClickHouse. Do not alter host or container privileges solely to perform
the smoke test.
