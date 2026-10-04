# Security policy review findings

## Findings

The phase-1 review found four unsafe defaults in the initial implementation:

- development service ports used Docker's all-interface bind default;
- the host collector loaded unrelated downstream credentials from the shared `.env` file, and its
  optional Docker CLI child inherited the complete environment;
- the ClickHouse event table had no retention bound;
- Docker CLI output included the complete event object before attributes were discarded;
- Kafka-originated payload JSON reached the sink without a storage-boundary allowlist, type, size,
  or malformed-input check.

No captured telemetry, credentials, or local identifiers are included in this record.

## Why the previous approach was unsafe

All-interface port publishing can expose unauthenticated development services to adjacent networks.
Overbroad environment loading increases credential exposure if the collector or a child process is
compromised. Indefinite retention unnecessarily accumulates endpoint data. Trusting Kafka input lets
an unexpected producer bypass collector-side minimization and can fail or contaminate storage.
Reading complete Docker events can transiently expose labels or other actor attributes that are not
part of the telemetry allowlist.

## Safer implementation

- Compose-published ports bind explicitly to `127.0.0.1`.
- The native collector loads only its seven allowlisted settings, and the Docker CLI receives a
  minimal subprocess environment.
- Kafka and ClickHouse delete phase-1 events after seven days.
- Docker's output template emits only action, container ID, and source event time; input lines and
  the in-memory queue are bounded.
- The serializer and ClickHouse sink enforce event-type payload allowlists and size limits. The sink
  rejects malformed, oversized, incorrectly typed, directly identifying, or non-allowlisted rows
  without logging their contents.
- Regression tests inspect Compose and execution settings, retention DDL, environment isolation,
  rejected input, and sanitized logging.
