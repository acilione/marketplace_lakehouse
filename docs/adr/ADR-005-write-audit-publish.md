# ADR-005: Write-audit-publish

Status: Accepted

Broad or consumer-visible writes target an Iceberg candidate branch. Quality and reconciliation run
against that candidate before `main` is fast-forwarded. This protects consumers from incomplete or
invalid builds at the cost of branch cleanup, explicit approvals, and retained rollback references.

