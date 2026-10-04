# ClickHouse ISO timestamp input failure

## Failure

The first real Spark micro-batch reached ClickHouse but the HTTP insert returned status 400.
ClickHouse's query log reported that its default JSONEachRow parser could not parse an event time
formatted as an ISO-8601 UTC value ending in `Z`.

## Cause

Spark serializes timestamp columns as ISO-8601 strings. The ClickHouse HTTP insert did not specify
a datetime input mode, so the default parser expected a narrower DateTime64 representation.

## Improvement

The ClickHouse insert URL now sets `date_time_input_format=best_effort`. This accepts the validated
ISO-8601 UTC timestamps without rewriting their values in the Spark sink. The URL unit test asserts
that the setting is always present and that credentials remain outside the URL.

The diagnostic used the ClickHouse system query log. Event payloads are not copied into application
logs or this failure record.
