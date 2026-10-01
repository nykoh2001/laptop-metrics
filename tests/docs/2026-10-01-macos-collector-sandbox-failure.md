# macOS collector smoke-test failure in the restricted sandbox

## Failed case

A real one-cycle collector smoke test returned no events because both `psutil.process_iter()` and
`psutil.net_connections()` reached macOS `sysctl()` through psutil and received
`PermissionError: [Errno 1] Operation not permitted`.

## Why the existing method failed

The command sandbox blocks host process-table access. The aggregate collector behaved as designed:
it logged each source failure, continued to the next source, and exited without crashing. This does
not establish whether a normal host terminal can collect the events.

## Improved verification method

Do not broaden collector privileges to make the smoke test pass. Run the collector as a native
process from a normal user terminal when manually verifying the project. Keep per-source failure
isolation, null fields, and the readable-process network fallback because ordinary macOS privacy
settings can still make individual fields or sources unavailable. Docker availability is checked
separately; an unavailable daemon remains a normal optional-source state.
