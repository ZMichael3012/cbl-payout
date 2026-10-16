# E2E auxiliary files (cbl-payout)

Purpose: support runs per `test/e2e/E2E.md` and analysis of their artifacts.
There are no PowerShell run scripts here (convention `CBL-002` section 7).

| File/directory | Purpose |
| :-- | :-- |
| `summary.template.md` | The `summary.md` run report template |

The scenarios use only standard tools (docker compose, psql in the postgres container,
grpcurl in a container) and `test/e2e/saga_kafka_test.go` for the separate testcontainers
saga check. New auxiliary files are added here and recorded in this table.
