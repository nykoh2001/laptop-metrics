# Local Spark test failure in the restricted sandbox

## Failed case

Running `.venv/bin/python -m pytest --durations=10` started the local PySpark parsing test but
failed while its Java gateway tried to bind a loopback server socket:

```text
java.net.SocketException: Operation not permitted
pyspark.errors.exceptions.base.PySparkRuntimeError: [JAVA_GATEWAY_EXITED]
```

The run completed 15 tests successfully and skipped the opt-in Kafka integration test before the
Spark fixture failed during setup. No application assertion failed.

## Why the existing method failed

The local Spark test starts a JVM and Py4J gateway process. Py4J requires a local listening socket,
but the restricted command sandbox denied that socket bind. Configuring Spark's driver address as
`127.0.0.1` does not remove the gateway's listening-socket requirement.

## Improved verification method

Run the same pytest suite outside the restricted sandbox, where loopback socket binding is allowed.
The repository test remains an ordinary local Spark integration test so CI and developer machines
exercise the real Spark parser rather than replacing it with mocks. Kafka and full pipeline smoke
tests remain separately opt-in because they require Docker infrastructure.
