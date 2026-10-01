## Security and Telemetry Safety

This policy contains mandatory repository-wide security requirements.
It applies to design, implementation, testing, documentation, and review.
Refer to `AGENTS.md` for the tasks that require this policy to be read.

Security telemetry may expose sensitive information about the host, users, processes, networks, containers, and development environment. Treat every change to telemetry collection, transport, storage, detection, simulation, and response as security-sensitive.

These requirements take precedence over implementation convenience.

### 1. Default Security Posture

- Apply least privilege, data minimization, secure defaults, and defense in depth.
- Prefer partial visibility with limited permissions over complete visibility with excessive permissions.
- Collect only the data required by the current explicitly requested stage.
- Do not add permissions, sensors, data fields, network exposure, or automated responses for hypothetical future features.
- When a requested feature requires elevated access, first identify:
  - the exact data or operation requiring it;
  - the minimum permission needed;
  - the security and privacy risks;
  - a lower-privilege alternative;
  - the behavior when permission is unavailable.
- Do not enable elevated access automatically. Report the requirement and request explicit user approval before proceeding.

### 2. Host Access and Privileges

- Run host collectors as an unprivileged native process whenever possible.
- On macOS, do not assume that a Docker container using host-related options can observe the macOS host. Docker Desktop containers run inside a Linux VM.
- Do not automatically use or add:
  - `sudo` or root execution;
  - privileged containers;
  - host PID, network, IPC, or user namespaces;
  - `CAP_SYS_PTRACE`, `CAP_SYS_ADMIN`, `CAP_DAC_READ_SEARCH`, or similarly broad capabilities;
  - Full Disk Access;
  - Endpoint Security entitlements;
  - unrestricted audit, eBPF, or kernel access;
  - host device mounts;
  - the Docker daemon socket.
- A read-only mount protects against writes but does not prevent disclosure of sensitive host information. Treat read-only host mounts as privileged access.
- If a process or field cannot be read, handle the failure gracefully. Use `null`, skip the event, or record a non-sensitive aggregate such as `access_denied_count`.
- Never weaken host security settings, disable platform protections, or modify audit policy solely to increase telemetry coverage without explicit approval.

### 3. Data Collection Allowlist

- Use an explicit field allowlist. Do not collect an object first and remove sensitive fields afterward.
- Unless explicitly approved, process telemetry may include only:
  - PID and PPID;
  - process name;
  - normalized executable path;
  - pseudonymous user identifier;
  - process start time;
  - explicitly approved process state fields.
- Unless explicitly approved, network telemetry may include only:
  - protocol;
  - connection state;
  - local and remote address classifications;
  - local and remote ports;
  - associated PID;
  - event time.
- Do not collect by default:
  - process command lines or arguments;
  - environment variables;
  - file contents;
  - clipboard contents;
  - browser history, cookies, sessions, or profiles;
  - shell history;
  - SSH keys or authentication material;
  - application credentials;
  - access tokens, API keys, passwords, or connection strings;
  - email, messages, personal documents, or unrelated user activity;
  - packet payloads;
  - memory dumps.
- Do not add opaque or unrestricted raw payload fields that bypass the schema allowlist.
- Apply input size limits to telemetry fields and reject or truncate unexpectedly large values safely.

### 4. Identity and Path Protection

- Do not transmit raw hostnames, account names, email addresses, device serial numbers, or other direct identifiers unless explicitly required.
- Generate stable pseudonymous host and user identifiers using a documented method.
- Keep salts and keys outside the repository. Do not hard-code them.
- Normalize user-specific paths. Replace home-directory prefixes with a neutral representation such as `$HOME`.
- Avoid storing full host bind-mount paths. Prefer classifications such as:
  - `project_directory`;
  - `temporary_directory`;
  - `user_home`;
  - `docker_socket`;
  - `host_root`;
  - `other`.
- Document whether an identifier is anonymous, pseudonymous, or directly identifying. Do not describe pseudonymous data as anonymous.

### 5. Secrets and Sensitive Configuration

- Never commit secrets, credentials, private keys, tokens, certificates, salts, real connection strings, or captured telemetry containing sensitive values.
- Do not place secrets in:
  - source code;
  - Dockerfiles or Compose files;
  - test fixtures;
  - README files;
  - `AGENTS.md`;
  - `docs/`;
  - `tests/docs/`;
  - `docs/prompts/`;
  - example configuration;
  - logs or exception messages.
- Sanitize prompts before saving them under `docs/prompts`. Replace any secret or identifying value with a clear placeholder.
- Use environment variables, local untracked configuration, or an appropriate secret-management mechanism.
- Provide `.example` configuration files containing placeholders only.
- Ensure sensitive local files, telemetry captures, database volumes, generated identifiers, logs, and credentials are excluded through `.gitignore` where appropriate.
- Error handling must not dump environment variables, complete configuration objects, raw telemetry payloads, or connection details.

### 6. Telemetry as Untrusted Input

- Treat every telemetry event as untrusted input, even when it originates from the local host.
- Validate schema version, field type, length, timestamp range, enum values, and required fields at trust boundaries.
- Do not construct shell commands from telemetry fields.
- Do not evaluate telemetry as code, templates, expressions, SQL fragments, or file paths.
- Use parameterized queries and safe serialization.
- Protect against malformed JSON, deeply nested payloads, oversized messages, invalid encodings, path traversal, log injection, and control characters.
- Preserve rejected-event metadata only when it is safe. Do not copy an unsafe raw payload into application logs.

### 7. Kafka, Spark, and ClickHouse Security

- Bind local development services to loopback or an isolated Docker network by default.
- Do not expose Kafka, Spark interfaces, ClickHouse, dashboards, or administrative ports on `0.0.0.0` unless explicitly requested and protected.
- Do not use unauthenticated remote listeners.
- If a service must be reachable outside the local machine, require explicit approval and configure appropriate authentication, authorization, and transport encryption.
- Use separate application and administrative credentials when credentials are required.
- Grant producers and consumers access only to the topics they need.
- Grant database users only the required tables and operations.
- Do not store credentials or secrets inside Kafka messages.
- Do not write unsafe raw events into Spark logs, checkpoints, dead-letter records, or ClickHouse error tables.
- Apply retention or TTL policies to raw telemetry. Do not retain sensitive telemetry indefinitely.
- Ensure replay cannot accidentally trigger automated remediation or destructive side effects.
- Make ingestion and replay idempotent where practical.

### 8. Docker and Container Security

- Run containers as non-root whenever possible.
- Drop all Linux capabilities by default and add only a narrowly justified capability.
- Enable `no-new-privileges` where supported.
- Prefer a read-only container root filesystem and writable temporary volumes with limited scope.
- Pin important images to an explicit version or digest. Avoid mutable tags for security-sensitive components.
- Do not mount the Docker socket directly into an application or telemetry collector container.
- Do not mount the host root, user home, credential directories, SSH directories, browser profiles, or unrelated project directories.
- Use read-only mounts for approved host paths unless write access is explicitly required.
- Treat access to the Docker API as equivalent to highly privileged control over the container environment.
- Docker telemetry collection must be optional. If it cannot be implemented safely, mark it unavailable and allow the remaining collectors to continue.
- Never weaken seccomp, AppArmor, SELinux, Docker Desktop isolation, or other runtime protections merely to simplify implementation.

### 9. Safe Threat Simulation

- Threat simulations must be harmless, bounded, reversible, and restricted to an explicitly created sandbox.
- Use synthetic files, synthetic identities, localhost endpoints, and isolated containers or virtual machines.
- Do not run real malware, ransomware, credential-stealing tools, reverse shells, destructive payloads, or unreviewed offensive scripts.
- Do not scan, probe, authenticate against, or send traffic to external systems that are not explicitly authorized.
- Do not use real credentials or intentionally trigger lockouts on real accounts.
- Do not modify real user documents, startup configuration, security settings, or production resources.
- Every simulation must define:
  - a unique simulation ID;
  - allowed resources and paths;
  - expected telemetry;
  - expected alerts;
  - a maximum runtime;
  - an automatic cleanup procedure;
  - a way to verify cleanup.
- Prefer a dry-run mode.
- A failed or interrupted simulation must still attempt cleanup.
- Higher-risk simulations must run in an isolated Linux VM or dedicated container environment, not directly on the macOS host.
- Review third-party emulation content before execution. Never execute downloaded scripts blindly.

### 10. Detection and Automated Response

- Default to alert-only behavior.
- Do not automatically kill processes, delete files, block network access, disable accounts, stop containers, or modify host configuration unless the user explicitly authorizes that response.
- Automated responses must be narrowly scoped, reversible where possible, time-bounded, and fully audited.
- Detection rules must state:
  - the data sources they require;
  - the behavior they detect;
  - known false-positive conditions;
  - their severity;
  - whether they are safe during replay.
- Do not claim that an anomaly is an attack without supporting evidence.
- Clearly distinguish:
  - observed events;
  - rule matches;
  - anomaly scores;
  - inferred threats;
  - confirmed incidents.
- Statistical and machine-learning results must not directly trigger destructive responses.

### 11. Dependencies and Supply Chain

- Minimize new dependencies.
- Use trusted upstream projects and official distribution channels.
- Pin dependency versions where practical.
- Do not use installation patterns such as downloading and directly piping remote content into a shell.
- Review scripts, container entrypoints, Compose files, and requested permissions before running third-party components.
- Do not automatically install host-level agents, kernel modules, audit configurations, or system services.
- Record newly introduced security-sensitive dependencies and explain why they are needed.

### 12. Testing Requirements

Security-sensitive changes must include appropriate tests.

At minimum, verify that:

- forbidden sensitive fields are absent from serialized events;
- command lines and environment variables are not collected;
- raw hostnames and user identifiers are not transmitted;
- user-specific paths are normalized;
- access-denied errors do not crash the collector;
- unavailable optional collectors degrade gracefully;
- malformed and oversized events are rejected safely;
- logs and exceptions do not contain raw telemetry or secrets;
- replay does not invoke automated response actions;
- Docker and Compose configurations do not introduce privileged mode, forbidden capabilities, host PID mode, unnecessary host mounts, or Docker socket mounts;
- service ports are not unintentionally exposed;
- telemetry retention and cleanup behavior work as documented.

When a test exposes a security failure:

- record the failure without copying sensitive data;
- explain the root cause;
- explain why the previous approach was unsafe;
- document the safer implementation in `tests/docs/`;
- add a regression test.

### 13. Documentation and Review

- Document the current visibility boundary: what is collected, what is intentionally excluded, and why.
- Document any permission that is requested or granted.
- Keep security documentation factual. Do not claim complete endpoint coverage or production-grade protection without evidence.
- Preserve unrelated user changes.
- Before completing a stage, review:
  - effective process and container privileges;
  - host mounts;
  - exposed ports;
  - collected fields;
  - stored identifiers;
  - retention behavior;
  - logs and test fixtures;
  - cleanup of temporary security data.
- In the final report, separate:
  - verified security properties;
  - known visibility gaps;
  - tests performed;
  - tests not performed;
  - permissions intentionally not granted;
  - risks deferred to a later stage.

### 14. Stop Conditions

Stop implementation and report the issue before proceeding if the work would require:

- disabling host security controls;
- granting privileged container access;
- granting process-memory inspection capabilities;
- mounting the Docker socket into an application container;
- collecting credentials, environment variables, private user content, or packet payloads;
- exposing an unauthenticated service outside the local machine;
- executing a destructive or externally directed threat simulation;
- adding an irreversible automated response;
- storing sensitive telemetry in the repository.

Do not work around these conditions silently.