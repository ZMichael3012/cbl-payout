# E2E cbl-payout: payout schema chains and two-operator approval

Run instruction for the local e2e scenarios of the service. A run is performed by an
agent platform-independently: docker, a command shell, psql (inside the stand) and
grpcurl (in a container). There are no PowerShell run scripts (customer decision of
16.10.2026, `CBL-002` section 7).

Repository: `cbl-payout`. Stand: `deploy/docker-compose.yml` (postgres, kafka,
schema-registry, payout in the `app` profile). The compose stand runs no saga: it has no
Ledger (KI-payout-10); the end-to-end saga is checked separately by `make test-e2e`
(Kafka+PostgreSQL on testcontainers, fake Ledger).

## 1. Scenarios

| Scenario | Checks | Contracts |
| :-- | :-- | :-- |
| `E2E_Payout_Schema` | goose migrations to version 5, the main chain requested -> reserved -> sent_to_gateway -> completed and the compensation chain requested -> approved -> reserved -> failed -> compensated with a per-step journal, the DB barriers (forbidden backward transition, append-only journal) | payout schema and state machine (CBL-10.1) |
| `E2E_Payout_Approval` | two-operator approval of a large payout (requested stays after one vote, approved after two), the operator veto gives rejected with failure_reason, a below-threshold payout replies FailedPrecondition `approval not required`, the vote is idempotent per operator, the approval journal is append-only (UPDATE and TRUNCATE rejected) | approvals (CBL-10.5), gRPC PayoutService ApprovePayout |

Each scenario starts from a clean stand (fresh `down -v`/`up`).

## 2. Isolation and parallelism

- Run mark: UTC lowercase `yyyymmddthhmmssz` (uppercase is rejected by compose project
  naming rules).
- Artifact directory: `test/e2e/.runs/<mark>/` (create before the start).
- Compose project: `cbl-payout-e2e-<mark>` (the compose file declares `name: cbl-payout`;
  the `-p` flag overrides it); the payout container is
  `cbl-payout-e2e-<mark>-payout-1`.
- Host ports 5432, 9092, 8081, 8080, 9080, 9280 are published; do not run two payout
  stands in parallel and stop other payout stands first.
- All commands run from the repository root.

## 3. Common run order

1. `<MARK>` = current UTC time in lowercase; create `test/e2e/.runs/<MARK>/`.
2. Stop the previous stand: `docker compose -p cbl-payout-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
   (output to `compose-down.log`; the first run has nothing to stop).
3. Start the stand: `docker compose -p cbl-payout-e2e-<MARK> -f deploy/docker-compose.yml --profile app up -d --build --wait`
   (`compose-up.log`); `docker compose ps` into `compose-ps.txt`.
4. Wait for migrations (retry up to 60 s):
   `docker compose -p ... exec -T postgres psql -U cbl -d payout -v ON_ERROR_STOP=1 -t -A -c "SELECT max(version_id) FROM goose_db_version"`
   must print a value >= 5.
5. Run the scenario (section 4 or 5), appending every SQL and grpcurl transcript to
   `<scenario>.log` and every OK/FAIL line to `<scenario>-checks.txt`.
6. Capture logs: `docker compose -p ... logs --no-color > compose-logs.txt`.
7. `summary.md` by the `.additionals/summary.template.md` template; stop the stand:
   `down -v` (append to `compose-down.log`).

Utility notes:

- psql helper: `docker compose -p ... exec -T postgres psql -U cbl -d payout -v ON_ERROR_STOP=1 -t -A -c "<SQL>"`;
  output is trimmed. For expected failures run without `-v ON_ERROR_STOP=1` and check
  the exit code and the error text.
- grpcurl helper (JSON via stdin, the stand network of the payout container):
  `echo <JSON> | docker run -i --rm --network container:cbl-payout-e2e-<MARK>-payout-1 fullstorydev/grpcurl:v1.9.1 -plaintext -max-time 10 -d @ localhost:9080 billing.payout.v1.PayoutService/<Method>`
  (a successful call exits 0 and prints the Payout JSON; errors print the status code to
  stderr).

## 4. E2E_Payout_Schema

1. Common run order, steps 1-4.
2. Main chain: seed a payout (`state` = `requested`, amount 1000 USDT/2, network `trc20`,
   destination `TJRabPrwbZy45sbavfcjinPJC18kjpRTv8`, merchant `m-e2e`, merchant_ref
   `ref-e2e`) plus the initial `payout_transitions` row (`'' -> requested`, version 1);
   then walk requesting -> reserved (v2) -> sent_to_gateway (v3, `gateway_payout_id` =
   `gw-e2e`, `provider` = `mock`) -> completed (v4, `txid` = `tx-e2e`, `completed_at`
   now()), one transition row per step. Expect `completed:4` and exactly 4 journal rows.
3. Compensation chain: a second payout walks requested -> approved (v2) -> reserved (v3)
   -> failed (v4, `failure_reason` = `gateway rejected`) -> compensated (v5). Expect
   `compensated:5`.
4. DB barrier checks (both must be REJECTED):
   - backward transition from the terminal state:
     `UPDATE payouts SET state = 'reserved', state_version = 5 WHERE id = '<main id>'`
     fails with `forbidden`;
   - journal rewrite: `UPDATE payout_transitions SET reason = 'rewritten' WHERE payout_id = '<main id>'`
     fails with `append-only`.
5. Checks go to `schema-checks.txt`.

## 5. E2E_Payout_Approval

1. Common run order, steps 1-4; after the migrations check sleep 3 s (migrations are
   applied before net.Listen; give the gRPC port time to come up). The stand policy
   (deploy compose `PAYOUT_APPROVAL_POLICY`) requires two operators above the large
   threshold.
2. Large payout (amount 5000000): seed `requested` with a fresh UUID.
   - first vote `{"payoutId":"<id>","operatorId":"op-1","approve":true,"reason":"checked"}`
     exits 0 and the response still carries `PAYOUT_STATE_REQUESTED`; DB state stays
     `requested:1`;
   - second vote `{"payoutId":"<id>","operatorId":"op-2","approve":true}` exits 0 and the
     response carries `PAYOUT_STATE_APPROVED`; DB state `approved:2`; exactly 2 rows in
     `payout_approvals` for the payout;
   - idempotent repeat of the op-1 vote exits 0 and does not change the state.
3. Veto: seed a payout (amount 7000000); `op-1` approves, then `op-2` votes
   `{"approve":false,"reason":"sanctions"}`; the response carries `PAYOUT_STATE_REJECTED`;
   DB `state || ':' || failure_reason` = `rejected:sanctions`.
4. Below threshold: seed a payout with amount 100; a vote exits non-zero with
   `approval not required` (FailedPrecondition).
5. Journal barrier: `UPDATE payout_approvals SET approve = false WHERE payout_id = '<id>'`
   is rejected with `append-only`; `TRUNCATE payout_approvals` is rejected (referenced in
   the error text).
6. Checks go to `approval-checks.txt`.

## 6. Mandatory run artifacts

- `summary.md` (template `.additionals/summary.template.md`; PASS/FAIL verdict, steps,
   key values, artifact list, time; one summary per scenario run or a combined one);
- `compose-ps.txt`, `compose-up.log`, `compose-down.log`, `compose-logs.txt`;
- `<scenario>.log` and `<scenario>-checks.txt` for each executed scenario;
- on failure: container states, payout container log fragments (saga, gRPC errors),
   the failing SQL and its raw output.

## 7. Auxiliary files (.additionals)

- `summary.template.md` - the `summary.md` template;
- `inventory.md` - the list of auxiliary files and their purpose.

## 8. Cleanup

`docker compose -p cbl-payout-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
at the end; `.runs/` is not committed; a repeated run must start from a clean stand.
