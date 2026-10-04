## Project Overview

Build a local real-time endpoint security telemetry pipeline for macOS. Local collectors observe
process state, network connections, listening ports, and optional Docker container lifecycle
events. Events use one versioned security envelope and flow through Kafka and Spark into
ClickHouse.

The project is implemented incrementally. Only the current roadmap phase should be built; later
phase capabilities must not be implemented early.

---

## Roadmap

1. Collect local process, network, and optional Docker telemetry in a common event schema.
2. Add Kafka raw, normalized, alert, and DLQ topics plus replay.
3. Add Spark validation, normalization, deduplication, watermarks, windows, and correlation.
4. Add rule-based threat detection, alerts, and visualization.
5. Add safe threat simulations, labels, detection metrics, latency, recovery, and reprocessing.
6. Add behavioral baselines and Isolation Forest anomaly detection.

The current implementation scope is phase 1.

---

## Tech Stack

- Language: Python 3.11+
- Host collection: psutil and an optional Docker CLI event stream
- Containerization: Docker Compose
- Streaming: Apache Kafka in KRaft mode
- Stream processing: Spark Structured Streaming
- Storage: ClickHouse

The collector runs directly on the macOS host. Kafka, Spark, and ClickHouse run in containers.

---

## Development Principles

- Prefer a simple working implementation over a feature-complete implementation.
- Build and validate one roadmap phase at a time.
- Keep collectors, event modeling, publishing, processing, and storage loosely coupled.
- Avoid unnecessary abstractions and new technologies.
- Favor readability and maintainability over clever implementations.
- Do not implement future roadmap features unless explicitly requested.

---

## Coding Guidelines

- Use Python type hints for all public APIs.
- Avoid `Any` unless absolutely necessary.
- Use Google-style multi-line docstrings for every public function and class.
- Include `Args`, `Returns`, and `Raises` sections when applicable.
- Keep functions small and focused.
- Avoid global mutable state.
- Read configuration from environment variables.
- Separate configuration, business logic, and infrastructure.
- Avoid duplicated code.
- Prefer composition over inheritance.

---

## Project Structure

Keep functionality roughly organized as:

```text
collector/    # host-side event model, collectors, and Kafka publisher
spark/        # minimal streaming jobs for the current phase
clickhouse/   # storage schema
schemas/      # common event contract and examples
configs/      # configuration files when needed
scripts/      # local operation helpers
tests/        # unit, integration, and failure documentation
docs/         # narrowly scoped project documentation and captured prompts
```

Avoid placing unrelated functionality in the same module.

---

## Security and Privacy

- Do not collect command-line arguments, environment variables, file contents, browser data, or
  other high-risk values by default.
- Emit a stable pseudonymous host ID, never a raw device name or machine identifier.
- Pseudonymize user identifiers and replace recognized user-home path prefixes with `$HOME`.
- Define collected fields with an allowlist; do not add opportunistically available fields.
- Treat Docker collection as optional; an absent CLI or unavailable daemon must not stop other
  collectors.
- Do not install osquery, request Endpoint Security permissions, invoke sudo, or change audit
  settings automatically.
- Do not add root execution, privileged containers, host PID or network namespaces,
  `CAP_SYS_PTRACE`, `CAP_SYS_ADMIN`, Full Disk Access, Endpoint Security entitlements, or a Docker
  socket mount to increase visibility.
- Allow null for operating-system fields that are unavailable because of permissions or races.
- Treat incomplete visibility as normal operation and report only non-sensitive aggregate counts.

---

## Testing

- Manually verify every completed phase when the environment permits it.
- Prefer pytest for automated tests.
- Test event modeling and collector logic separately from infrastructure.
- Mock external services whenever practical.
- Add tests for every new feature whenever practical.
- When an external service required by an opt-in integration test is available, set its
  `RUN_*_INTEGRATION_TESTS=1` flag and run the test; do not count a skipped integration test as
  verified.
- Record test-discovered failures and their corrections under the relevant `tests/<area>/docs/`
  directory.

---

## Logging

Use Python's `logging` module. Avoid `print()` except for temporary debugging. Log startup,
shutdown, Kafka connection, optional-source availability, processing failures, and unexpected
exceptions.

---

## Configuration

Configuration should be environment-variable based. Maintain `.env.example` for Kafka, collector,
Spark, and ClickHouse settings. Never commit local credentials or raw host identifiers.

---

## Dependencies

- Do not introduce new production dependencies unless necessary.
- Prefer the Python standard library whenever practical.
- Explain why a new dependency is required before adding it.

---

## Before Completing Any Task

- Ensure the project still runs.
- Run formatting, linting, type checking, and tests.
- Do not leave TODO placeholders for implemented features.
- Update documentation when behavior or architecture changes.
- Explain major design decisions when introducing new components.
- Clearly separate verified behavior from environment-dependent checks that could not be run.

## Roadmap Implementation Reports

- Create `docs/phase-N-implementation-report.md` when the originally specified implementation for
  roadmap phase N is completed, using `docs/phase-1-implementation-report.md` as the format
  reference.
- Record follow-up work that extends an already implemented phase in that phase's existing report.
- Do not create an implementation report for a roadmap phase that has not been implemented.
- Keep the report factual: describe the inspected baseline, changed files, event/data flow,
  automated and end-to-end verification, and remaining environment or security constraints.
- Separate verified results from checks that were not run. Do not turn capacity assumptions,
  planned behavior, or roadmap scope into measured results.
- Keep the report aligned with the current roadmap phase; do not use report writing as a reason to
  implement later-phase capabilities.

## Implementation Prompt Archive

- Add a prompt file under `docs/prompts/` only when the user explicitly asks to document a prompt.
- When asked to record prompts for a phase, record the initial prompt only. Do not add follow-up
  prompts unless the user explicitly asks to include them.
- Preserve the user's request and constraints without adding assistant plans or conclusions. Do not
  archive `.env` contents, credentials, raw host identifiers, or captured telemetry; redact such
  values only when needed and identify the redaction.
- Mention the added prompt record path in the implementation report or final response.

## Pull Request Descriptions

- Before creating or editing a pull request, read the applicable files under
  `.github/PULL_REQUEST_TEMPLATE/`. For a feature pull request, follow `feature.md` headings and
  instructions exactly.
- Include the template's `Architecture (Diagram)`, `Summary`, and `Verification` sections. Write the
  architecture as an English ASCII diagram based on the current source and Compose configuration;
  do not invent topics, partitions, consumer groups, tables, volumes, or services. Mark planned
  components explicitly.
- Write the summary and verification in Korean noun-phrase bullets, report only facts supported by
  the current diff or executed checks, and list no more than five verification items. Mark checks
  that were not run as `미실행 (사유: ...)`.
- Remove instructional HTML comments from the final pull request description.
- If a template refers to an example file that is absent from the repository, follow the available
  template itself and do not fabricate example content.


## Security Policy

The repository-wide security requirements are defined in `docs/security-policy.md`.

You MUST read the entire security policy before making changes involving any of the following:

- host or process telemetry collection;
- operating-system permissions or privileged APIs;
- Docker, containers, host mounts, namespaces, capabilities, or the Docker API;
- Kafka, Spark, ClickHouse, network listeners, or externally reachable services;
- credentials, secrets, user identifiers, file paths, logs, or captured telemetry;
- threat detection rules, anomaly detection, or security analytics;
- threat simulation, adversary emulation, or security testing;
- automated response actions;
- telemetry storage, retention, replay, export, or deletion;
- third-party security agents, system services, kernel features, or elevated dependencies.

The policy is mandatory, not optional reference material. Apply it during design, implementation, testing, documentation, and final review.

Before starting an applicable task:

1. Read `docs/security-policy.md` completely.
2. Identify which policy sections apply to the requested work.
3. Prefer partial functionality under least privilege over broader functionality requiring unsafe access.
4. Do not expand privileges, collection scope, network exposure, retention, or response capabilities beyond the current request.
5. If the requested implementation conflicts with the policy or requires a listed stop condition, do not work around it. Explain the requirement and risk, then request explicit user direction.

If `docs/security-policy.md` is missing, unreadable, or materially inconsistent with the current implementation, stop security-sensitive work and report the problem.

When completing an applicable task, include in the final report:

- the security policy sections applied;
- permissions granted and intentionally not granted;
- sensitive fields collected and intentionally excluded;
- service and network exposure;
- tests performed for security requirements;
- known visibility gaps and deferred risks.

Do not duplicate the full policy in this file. Keep detailed security requirements in `docs/security-policy.md`.
