# summary.md run report template

Used for service local e2e runs (`test/e2e/E2E.md`). Copy into
`test/e2e/.runs/<UTC mark>/summary.md` at the start of the run and fill it in as the run
progresses (one summary per run; list all executed scenarios).

```markdown
# E2E run: cbl-payout, <UTC run start mark>

- Stand: `deploy/docker-compose.yml`, project `cbl-payout-e2e-<mark>`
- Code version: `<commit hash>`, branch `<branch>`
- Start: <UTC RFC 3339>, End: <UTC RFC 3339>
- Verdict: PASS | FAIL

## Steps

| # | Scenario | Step | Status | Artifact |
| :-- | :-- | :-- | :-- | :-- |
| 1 | both | down -v / up --build --wait | OK | compose-up.log, compose-ps.txt |
| 2 | schema | migrations + chains + barriers | OK | schema.log, schema-checks.txt |
| 3 | approval | gRPC votes + rate cases | OK | approval.log, approval-checks.txt |
| 4 | both | down -v | OK | compose-down.log |

## Key checks

| Scenario | Check | Expected | Actual | Status |
| :-- | :-- | :-- | :-- | :-- |

## Emergency dumps

<list on FAIL: compose ps, payout logs, failing SQL/grpcurl output>

## Notes

<deviations, retries, known flakes>
```
