# E2E CBL-10.5: двухэтапное подтверждение выплат через gRPC на compose-стенде.
# Запуск из каталога сервиса: powershell -NoProfile -File test/e2e/approval.ps1
# Сценарий: compose up -> миграции (версия 5) -> grpcurl ApprovePayout:
# крупная выплата проходит два голоса разных операторов (requested -> approved),
# отклонение второго оператора дает rejected, малая выплата ниже порога
# отклоняется FailedPrecondition, журнал голосов append-only.
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ComposeFile = Join-Path $RepoRoot "deploy\docker-compose.yml"
$PayoutContainer = "cbl-payout-payout-1"

function Invoke-Psql([string]$Sql) {
    $output = docker compose -f $ComposeFile exec -T postgres psql -U cbl -d payout -v ON_ERROR_STOP=1 -t -A -c $Sql
    if ($LASTEXITCODE -ne 0) { throw "psql failed: $($output -join "`n")" }
    return ($output -join "`n").Trim()
}

function Assert-PsqlFails([string]$Sql, [string]$What, [string]$ExpectedText) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = docker compose -f $ComposeFile exec -T postgres psql -U cbl -d payout -v ON_ERROR_STOP=1 -t -A -c $Sql 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($code -eq 0) { throw "$What was not rejected: $($output -join "`n")" }
    if (($output -join "`n") -notmatch $ExpectedText) { throw "$What rejected with unexpected error: $($output -join "`n")" }
}

function Invoke-Grpcurl([string]$Json) {
    # JSON передается через stdin (-d @): Windows argv искажает кавычки.
    # stderr (ожидаемые ошибки gRPC) не должен превращаться в terminating error.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = $Json | docker run -i --rm --network "container:$PayoutContainer" fullstorydev/grpcurl:v1.9.1 -plaintext -max-time 10 -d '@' localhost:9080 billing.payout.v1.PayoutService/ApprovePayout 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    return @{ Output = ($output -join "`n"); Code = $code }
}

function New-PayoutRow([string]$Id, [string]$Key, [int]$AmountMinor) {
    Invoke-Psql "INSERT INTO payouts (id, merchant_id, idempotency_key, request_hash, state, amount_minor, amount_asset, amount_decimals, network, destination_address, merchant_ref) VALUES ('$Id', 'm-approval', '$Key', 'hash', 'requested', $AmountMinor, 'USDT', 2, 'trc20', 'TJRabPrwbZy45sbavfcjinPJC18kjpRTv8', 'ref')" | Out-Null
    Invoke-Psql "INSERT INTO payout_transitions (payout_id, from_state, to_state, state_version, reason) VALUES ('$Id', '', 'requested', 1, 'created')" | Out-Null
}

# Invoke-Compose выполняет docker compose, не превращая его stderr (прогресс,
# остановка контейнеров) в terminating error при EAP=Stop.
function Invoke-Compose([string[]]$ComposeArgs) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        docker compose -f $ComposeFile @ComposeArgs | Out-Null
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    return $code
}

Push-Location $RepoRoot
try {
    Invoke-Compose @("--profile", "app", "down", "-v") | Out-Null
    $upCode = Invoke-Compose @("--profile", "app", "up", "-d", "--build")
    if ($upCode -ne 0) { throw "docker compose up failed" }

    $migrated = $false
    $version = "unknown"
    foreach ($attempt in 1..30) {
        try {
            $version = Invoke-Psql "SELECT max(version_id) FROM goose_db_version"
            if ([int]$version -ge 5) { $migrated = $true; break }
        } catch {
            # сервер еще поднимается
        }
        Start-Sleep -Seconds 2
    }
    if (-not $migrated) { throw "payout migrations are not applied (version $version)" }
    # Миграции применяются до net.Listen: даем gRPC-порту подняться.
    Start-Sleep -Seconds 3

    # 1. Крупная выплата: два голоса разных операторов доводят до approved.
    $largeId = [guid]::NewGuid().ToString()
    New-PayoutRow $largeId "key-large" 5000000

    $first = Invoke-Grpcurl "{`"payoutId`":`"$largeId`",`"operatorId`":`"op-1`",`"approve`":true,`"reason`":`"checked`"}"
    if ($first.Code -ne 0) { throw "first approval failed: $($first.Output)" }
    if ($first.Output -notmatch "PAYOUT_STATE_REQUESTED") { throw "first vote must keep requested: $($first.Output)" }

    $state = Invoke-Psql "SELECT state || ':' || state_version FROM payouts WHERE id = '$largeId'"
    if ($state -ne "requested:1") { throw "unexpected state after first vote: $state" }

    $second = Invoke-Grpcurl "{`"payoutId`":`"$largeId`",`"operatorId`":`"op-2`",`"approve`":true}"
    if ($second.Code -ne 0) { throw "second approval failed: $($second.Output)" }
    if ($second.Output -notmatch "PAYOUT_STATE_APPROVED") { throw "second vote must approve: $($second.Output)" }

    $state = Invoke-Psql "SELECT state || ':' || state_version FROM payouts WHERE id = '$largeId'"
    if ($state -ne "approved:2") { throw "unexpected state after second vote: $state" }
    $votes = Invoke-Psql "SELECT count(*) FROM payout_approvals WHERE payout_id = '$largeId'"
    if ($votes -ne "2") { throw "unexpected votes count: $votes" }

    # Идемпотентный повтор голоса: состояние и число голосов не меняются.
    $repeat = Invoke-Grpcurl "{`"payoutId`":`"$largeId`",`"operatorId`":`"op-1`",`"approve`":true}"
    if ($repeat.Code -ne 0) { throw "idempotent repeat failed: $($repeat.Output)" }
    $state = Invoke-Psql "SELECT state || ':' || state_version FROM payouts WHERE id = '$largeId'"
    if ($state -ne "approved:2") { throw "repeat must not change state: $state" }

    # 2. Отклонение: один оператор апрувит, второй отклоняет - rejected.
    $rejectId = [guid]::NewGuid().ToString()
    New-PayoutRow $rejectId "key-reject" 7000000

    $approve = Invoke-Grpcurl "{`"payoutId`":`"$rejectId`",`"operatorId`":`"op-1`",`"approve`":true}"
    if ($approve.Code -ne 0) { throw "approve before reject failed: $($approve.Output)" }
    $reject = Invoke-Grpcurl "{`"payoutId`":`"$rejectId`",`"operatorId`":`"op-2`",`"approve`":false,`"reason`":`"sanctions`"}"
    if ($reject.Code -ne 0) { throw "reject failed: $($reject.Output)" }
    if ($reject.Output -notmatch "PAYOUT_STATE_REJECTED") { throw "reject must finish rejected: $($reject.Output)" }

    $state = Invoke-Psql "SELECT state || ':' || failure_reason FROM payouts WHERE id = '$rejectId'"
    if ($state -ne "rejected:sanctions") { throw "unexpected rejected state: $state" }

    # 3. Малая выплата ниже порога: апрув не требуется (FailedPrecondition).
    $smallId = [guid]::NewGuid().ToString()
    New-PayoutRow $smallId "key-small" 100
    $small = Invoke-Grpcurl "{`"payoutId`":`"$smallId`",`"operatorId`":`"op-1`",`"approve`":true}"
    if ($small.Code -eq 0) { throw "small payout must reject approval: $($small.Output)" }
    if ($small.Output -notmatch "approval not required") { throw "unexpected small payout error: $($small.Output)" }

    # 4. Журнал голосов append-only.
    Assert-PsqlFails "UPDATE payout_approvals SET approve = false WHERE payout_id = '$largeId'" "approval rewrite" "append-only"
    Assert-PsqlFails "TRUNCATE payout_approvals" "approval truncate" "TRUNCATE"

    Write-Host "OK: migrations=$version, two-operator approval=approved:2, reject=rejected, below-threshold rejected, journal append-only"
} finally {
    Invoke-Compose @("--profile", "app", "down", "-v") | Out-Null
    Pop-Location
}
