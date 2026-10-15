# E2E CBL-10.1: схема выплаты и автомат состояний на compose-стенде payout.
# Запуск из каталога сервиса: powershell -NoProfile -File test/e2e/schema.ps1
# Сценарий: compose up -> миграции (версия 3) -> основная цепочка
# requested -> reserved -> sent_to_gateway -> completed и компенсационная
# requested -> approved -> reserved -> failed -> compensated,
# запрещенные переходы и append-only журнал отклоняются вторым барьером БД.
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ComposeFile = Join-Path $RepoRoot "deploy\docker-compose.yml"

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

Push-Location $RepoRoot
try {
    # Чистый стенд: остатки прошлого запуска приводят к конфликту имен.
    docker compose -f $ComposeFile --profile app down -v 2>$null | Out-Null
    docker compose -f $ComposeFile --profile app up -d --build
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

    $migrated = $false
    $version = "unknown"
    foreach ($attempt in 1..30) {
        try {
            $version = Invoke-Psql "SELECT max(version_id) FROM goose_db_version"
            if ($version -eq "3") { $migrated = $true; break }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $migrated) { throw "payout migrations are not applied (version $version)" }

    # Основная цепочка выплаты: каждая версия получает строку журнала.
    $payoutId = [guid]::NewGuid().ToString()
    Invoke-Psql "INSERT INTO payouts (id, merchant_id, idempotency_key, request_hash, state, amount_minor, amount_asset, amount_decimals, network, destination_address, merchant_ref) VALUES ('$payoutId', 'm-e2e', 'key-e2e', 'hash-e2e', 'requested', 1000, 'USDT', 2, 'trc20', 'TJRabPrwbZy45sbavfcjinPJC18kjpRTv8', 'ref-e2e')" | Out-Null
    Invoke-Psql "INSERT INTO payout_transitions (payout_id, from_state, to_state, state_version, reason) VALUES ('$payoutId', '', 'requested', 1, 'created')" | Out-Null

    $chain = @(
        @{ From = "requested"; To = "reserved"; Version = 2; Extra = "" },
        @{ From = "reserved"; To = "sent_to_gateway"; Version = 3; Extra = ", gateway_payout_id = 'gw-e2e', provider = 'mock'" },
        @{ From = "sent_to_gateway"; To = "completed"; Version = 4; Extra = ", txid = 'tx-e2e', completed_at = now()" }
    )
    foreach ($step in $chain) {
        Invoke-Psql "UPDATE payouts SET state = '$($step.To)', state_version = $($step.Version)$($step.Extra) WHERE id = '$payoutId'" | Out-Null
        Invoke-Psql "INSERT INTO payout_transitions (payout_id, from_state, to_state, state_version, reason) VALUES ('$payoutId', '$($step.From)', '$($step.To)', $($step.Version), 'e2e')" | Out-Null
    }

    $state = Invoke-Psql "SELECT state || ':' || state_version FROM payouts WHERE id = '$payoutId'"
    if ($state -ne "completed:4") { throw "unexpected state after chain: $state" }
    $history = Invoke-Psql "SELECT count(*) FROM payout_transitions WHERE payout_id = '$payoutId'"
    if ($history -ne "4") { throw "unexpected history length: $history" }

    # Компенсационная цепочка: апрув, резерв, неудача шлюза, компенсация (release).
    $compensatedId = [guid]::NewGuid().ToString()
    Invoke-Psql "INSERT INTO payouts (id, merchant_id, idempotency_key, request_hash, state, amount_minor, amount_asset, amount_decimals, network, destination_address, merchant_ref) VALUES ('$compensatedId', 'm-e2e', 'key-comp', 'hash-comp', 'requested', 2000, 'USDT', 2, 'trc20', 'TJRabPrwbZy45sbavfcjinPJC18kjpRTv8', 'ref-comp')" | Out-Null
    Invoke-Psql "INSERT INTO payout_transitions (payout_id, from_state, to_state, state_version, reason) VALUES ('$compensatedId', '', 'requested', 1, 'created')" | Out-Null

    $compChain = @(
        @{ From = "requested"; To = "approved"; Version = 2; Extra = "" },
        @{ From = "approved"; To = "reserved"; Version = 3; Extra = "" },
        @{ From = "reserved"; To = "failed"; Version = 4; Extra = ", failure_reason = 'gateway rejected'" },
        @{ From = "failed"; To = "compensated"; Version = 5; Extra = "" }
    )
    foreach ($step in $compChain) {
        Invoke-Psql "UPDATE payouts SET state = '$($step.To)', state_version = $($step.Version)$($step.Extra) WHERE id = '$compensatedId'" | Out-Null
        Invoke-Psql "INSERT INTO payout_transitions (payout_id, from_state, to_state, state_version, reason) VALUES ('$compensatedId', '$($step.From)', '$($step.To)', $($step.Version), 'e2e')" | Out-Null
    }

    $compState = Invoke-Psql "SELECT state || ':' || state_version FROM payouts WHERE id = '$compensatedId'"
    if ($compState -ne "compensated:5") { throw "unexpected compensation state: $compState" }

    # Переход назад из терминального состояния отклоняет триггер (второй барьер).
    Assert-PsqlFails "UPDATE payouts SET state = 'reserved', state_version = 5 WHERE id = '$payoutId'" "backward transition" "forbidden"

    # Журнал переходов append-only.
    Assert-PsqlFails "UPDATE payout_transitions SET reason = 'rewritten' WHERE payout_id = '$payoutId'" "transition rewrite" "append-only"

    Write-Host "OK: migrations=$version, chain=completed:4, compensation=compensated:5, forbidden transition and rewrite rejected"
} finally {
    docker compose -f $ComposeFile --profile app down -v | Out-Null
    Pop-Location
}
