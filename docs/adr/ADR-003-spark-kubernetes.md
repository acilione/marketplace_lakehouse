# ADR-003: Spark on Kubernetes

Status: Accepted

Production uses one Spark driver per application under the Spark Operator. Immutable digest-pinned
images, namespaces, quotas, workload identities, and pod security provide isolation and promotion.
This requires operator lifecycle management and monitoring; the laptop profile uses Spark standalone
only to preserve reproducibility.

