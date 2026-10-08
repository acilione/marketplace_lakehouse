# Version and dependency policy

Verified on 1 September 2026. Floating image tags and unconstrained Python dependencies are
prohibited. The current compatibility set is:

| Component | Pin | Rationale |
|---|---:|---|
| Apache Spark / PySpark | 4.1.3 | Newest patch in Iceberg's maintained Spark 4.1 line |
| Scala binary | 2.13 | Required by Spark 4 |
| Java runtime | 21 image; 17+ supported locally | Matches official Spark images |
| Apache Iceberg | 1.11.0 | Newest release; publishes Spark 4.1 runtime |
| Apache Kafka | 4.2.0 | Current stable Kafka release |
| Airflow | 3.3.1 | Current stable with Python 3.10–3.14 support |
| Spark Airflow provider | 6.3.2 | Current provider release |
| PostgreSQL | 18.6 | Current supported security/bug-fix release |
| Apicurio Registry | 3.3.0 | Current stable registry release |
| Trino | 483 | Current stable release |
| Prometheus | 3.14.0 | Current stable release |
| Grafana | 13.2.0 | Current stable release |
| Terraform | 1.15.9 | Current stable CLI |
| Kubeflow Spark Operator chart | 2.5.2 | Current stable chart |
| React / React DOM | 19.2.8 | Current stable architecture UI runtime |
| Node.js types | 26.4.1 | Current declarations used by the control API |
| Confluent Kafka JavaScript client | 1.10.0 | Current supported client backed by librdkafka 2.15.0; read-only dashboard observer |
| Vite | 8.2.2 | Current stable frontend build tool |
| TypeScript | 6.0.3 | Newest stable release supported by typed ESLint 8.69 |
| Node.js build image | 24.20.0 | Current pinned LTS architecture site build runtime |
| Nginx runtime image | 1.31.4 | Current pinned architecture site runtime |
| Local MinIO server | RELEASE.2025-10-15T17-29-55Z | Official security release built from commit `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a` |
| Local MinIO client | RELEASE.2025-08-13T08-35-41Z | Existing client release built from commit `7394ce0dd2a80935aded936b09fa12cbb3cb8096` |

Independent Python versions are exact in `pyproject.toml`; architecture site dependencies are exact in
`architecture-site/package.json` and transitively locked in `architecture-site/package-lock.json`. Airflow is
installed using the Apache release constraint file; its provider is then pinned explicitly.

Primary release references: [Spark downloads](https://spark.apache.org/downloads.html),
[Iceberg engine compatibility](https://iceberg.apache.org/multi-engine-support/),
[Iceberg releases](https://iceberg.apache.org/releases/),
[Kafka downloads](https://kafka.apache.org/community/downloads/),
[Airflow installation](https://airflow.apache.org/docs/apache-airflow/stable/installation/),
[PostgreSQL releases](https://www.postgresql.org/docs/18/release.html), and the respective official
project release pages for the remaining components.

## Upgrade procedure

1. Create a dependency-only change; never combine a runtime upgrade with business logic.
2. Confirm the Spark/Iceberg/Scala matrix and Java/Python support.
3. Resolve and review transitive Maven and Python dependency changes; regenerate the lock evidence.
4. Run unit, contract, golden, integration, replay, idempotency, and checkpoint migration tests.
5. Build and scan the image, then test a candidate table branch against production-shaped data.
6. For streaming state changes, deploy a new query/checkpoint and execute controlled handover.
7. Record the tested image digest, rollback image, and table snapshot tag in the release evidence.

MinIO's public repository was archived in April 2026. Its community distribution is source-only;
the previously configured Docker Hub and Quay images are unavailable. The local Compose profile
therefore builds MinIO and `mc` from the official commit-pinned sources using
[`docker/minio/Dockerfile`](../docker/minio/Dockerfile), a digest-pinned Go toolchain and Alpine runtime,
and the upstream `go.sum` files. The server uses the
[final security release](https://github.com/minio/minio/releases/tag/RELEASE.2025-10-15T17-29-55Z)
instead of the older September binary. The images retain the upstream license and credits.
Production values must target a maintained cloud object store or supported S3-compatible distribution.
