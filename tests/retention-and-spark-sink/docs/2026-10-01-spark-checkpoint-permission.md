# Spark checkpoint permission failure

## Failure

The real Docker Spark submission failed while creating
`/opt/laptop-metrics/spark/checkpoints/security_events_clickhouse`. The code directory was mounted
read-only and the nested named volume root was owned by `root`, so the unprivileged `spark` user
could not create the checkpoint directory.

## Cause

The Compose configuration assumed that a named volume mounted below the read-only source mount
would be writable by the image's `spark` user. Docker created the volume root with ownership that
did not match UID 185 used by the Spark process.

## Improvement

The phase-1 job now stores its runtime checkpoint under `/tmp/laptop-metrics`, which is writable by
the existing unprivileged Spark user. The source-code mount remains read-only, and the service does
not gain root, privileged mode, additional capabilities, or a host mount.

Durable checkpoint recovery and replay guarantees remain outside phase 1. A future phase that
requires persistence should use a storage location provisioned with explicit non-root ownership
rather than changing the Spark process privileges.
