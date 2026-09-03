# ADR-006: Airflow schedules but does not transform

Status: Accepted

Airflow declares dependencies, retries, concurrency, and parameters. All transformation logic stays
in the packaged Spark application. This keeps logic testable and prevents scheduler metadata from
becoming an accidental data-processing runtime.

