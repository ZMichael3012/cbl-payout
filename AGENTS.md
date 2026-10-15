# AGENTS.md - cbl-payout

Saga выплат и возвратов с компенсациями и двухэтапным подтверждением.

Обязательные глобальные правила: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, контракты `../../.specs/CBL-003-contracts.md`.
Длинное тире (U+2014) запрещено, используется только дефис.

## 1. Границы

In scope: автомат выплаты, резервы, payout.commands, компенсации, возвраты.

Out of scope: другие сервисы платформы; взаимодействие только через контракты `cbl-protos`
(gRPC), Kafka и REST периметра (см. `CBL-000`, раздел 2).

## 2. Контракты

- REST: административный /healthz, /readyz
- gRPC: сервер PayoutService (CreatePayout/GetPayout - 10.2, ApprovePayout - 10.5;
  CreatePayout принимает refund_invoice_id - возврат, 10.6)
- Kafka продюсирует: payout.commands, payout.events (через outbox, 10.2;
  события возвратов несут refund_invoice_id, 10.6)
- Kafka потребляет: gateway.payout.events (группа `cbl-payout-gateway`; ledger.events - 10.4/10.7)
- Хранилище: PostgreSQL payout (payouts, payout_transitions, payout_approvals, outbox, inbox)

Контракты заморожены в `cbl-protos`: CF-1 gRPC, CF-2 события + аддитивные поля
возвратов `cbl-protos@cf-2.1` (CBL-10.6); см. `CBL-004`.

## 3. Архитектура

Слои: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Библиотеки: стандартный набор `CBL-002` (Gin, GORM + pgx-драйвер, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` запрещен, миграции только goose, только вперед.
Схема (10.1): `payouts` (состояние саги, `state_version`, суммы, реквизиты получателя,
`reserve_id` удержания), `payout_transitions` (append-only журнал), `outbox`, `inbox`.
Апрувы (10.5): `payout_approvals` (append-only голоса операторов, unique
`(payout_id, operator_id)`), политика порогов - `internal/domain/approval.go`,
сервис - `internal/usecase/approval.go` (locking read + CAS + откат конфликтной
попытки), транспорт - `internal/transport/payout_grpc.go`.
Сага (10.2): `internal/usecase/payout.go` (Create/Get/Advance: Reserve Ledger ->
reserved -> outbox payout.commands -> sent_to_gateway; компенсация failed ->
compensated), `internal/usecase/payout_events.go` (конверты payout.events и
payout.commands), `internal/usecase/gateway_payout_consumer.go` (submitted/
confirmed/failed, row-лок и привязка вердикта к gateway_payout_id),
`internal/ledgerclient/ledger.go` (gRPC Ledger, дедлайн 1 с), аккаунт удержания -
`internal/domain/account_plan.go` (UUIDv5 плана счетов, golden-векторы).
Возвраты (10.6): `payouts.refund_invoice_id` (миграция 00005: CHECK канонического
UUID, частичный индекс, неизменяемость вторым барьером), канонизация и включение
в `request_hash` только при непустом значении, маркер в каждом `payout.events`.
Матрица переходов - `internal/domain/state.go`, дублируется триггером БД
(`payouts_transition_guard`); переходы только через `storage.Repository.Transition`/
`TransitionTx` с CAS по `state_version`. `Create` принудительно ставит `requested`/1 и
зачищает данные саги (второй барьер - `payouts_insert_guard`).
main применяет миграции (`MIGRATIONS_DIR`) и настраивает пул БД (по умолчанию 20,
потолок 100, lifetime 30 минут).

## 4. Команды

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers: PostgreSQL)
make e2e          # compose-стенд: schema.ps1 (схема и цепочки) + approval.ps1 (grpcurl)
make test-e2e     # сквозная сага на testcontainers: Kafka+PostgreSQL, фейковый Ledger
make vet          # go vet ./...
make lint         # golangci-lint в Docker (контекст services/)
make up           # docker compose -f deploy/docker-compose.yml --profile app up -d --build
make down         # docker compose -f deploy/docker-compose.yml down -v
```

`make test-e2e` - сквозная сага (Kafka+PostgreSQL на testcontainers, фейковый
Ledger); compose-стенды сагу не гоняют: в них нет Ledger (KI-payout-10).

## 5. Порты и окружение

HTTP `8080`, gRPC `9080`, метрики `9280`.
Переменные (применимые к сервису): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`
(обязателен), `MIGRATIONS_DIR` (по умолчанию `./migrations`), `DB_MAX_OPEN_CONNS`
(по умолчанию 20, потолок 100), `DB_MAX_IDLE_CONNS` (по умолчанию 10),
`PAYOUT_APPROVAL_POLICY` (JSON ярусов апрува, см. 10.5: fail-safe default 2
оператора, невалидное значение - отказ старта; меняется только рестартом),
`LEDGER_ADDR` (gRPC Ledger для саги; пусто - шаг саги недоступен, WARN),
`KAFKA_BROKERS` (relay outbox и consumer gateway.payout.events; пусто - WARN);
`SCHEMA_REGISTRY_URL`, `REDIS_ADDR` - 10.4/0.5.

## 6. Правила разработки

- Конвейер: SDD-спека в `.specs/` -> архитектура -> контракты -> тесты вперед -> код ->
  ревью 4 агентами (без контекста, архитектура, контекст, безопасность - блокирующий) ->
  AGENTS.md -> финальные прогоны -> линтеры -> коммит.
- Покрытие unit+integration не ниже 80%; денежные сценарии только на реальных PostgreSQL и
  Kafka (testcontainers); локальный e2e в docker-compose обязателен.
- Деньги только int64 в минимальных единицах плюс decimals; float запрещен.
- Docstring на русском в стиле godoc (первая строка с имени сущности, точка в конце);
  комментарии объясняют почему; ошибки через `%w`; панки в бизнес-коде запрещены.
- Коммит: `CBL-<эпик>.<номер>: <краткое сообщение>`, спека с тем же префиксом в том же коммите.

## 7. SLO и метрики

SLO уточняется в задаче 14.4. Базовые метрики: latency p95/p99, доля ошибок, outbox lag,
consumer lag; дашборды после задачи 0.7.

## 8. Trade-offs

| ID | Решение | Плюсы | Минусы и компенсация |
| :-- | :-- | :-- | :-- |
| T-PAYOUT-01 | Матрица переходов в домене плюс триггеры БД (второй барьер): BEFORE INSERT фиксирует `requested`/версию 1 и пустые данные саги, BEFORE UPDATE проверяет пары, инкремент `state_version`, неизменяемые поля (суммы, реквизиты получателя, `reserve_id` после фиксации); журнал append-only | обход автомата, подмена адреса/суммы и двойная компенсация невозможны; аудит полный | дублирование матрицы в Go и SQL; прямой DML-роль может дописать журнал в обход приложения (модификация и удаление запрещены) | матрица зеркалируется полным перебором пар в integration-тесте; прямой DML вне модели угроз dev-стенда (роли prod - KI-payout-03) |
| T-PAYOUT-02 | Оптимистичные переходы: чтение под row-локом и CAS по `state_version`; tx-вариант `TransitionTx` для записи перехода и outbox-строки одной транзакцией | корректная история, нет глобальных локов; сага 10.2 и consumer 10.4 атомарны с событиями | при конкуренции один из переходов получает `ErrStateConflict` и повторяется | повторы в саге/consumer-е; метрика после 0.7 |
| T-PAYOUT-03 | `failed` не терминален: единственный исходящий переход `failed -> compensated`; компенсация - release удержания в Ledger (без проводок, T-LED-06) или закрытие саги без release, если резерва не было | у выплаты один финальный статус компенсации, удержание не зависает; освобождение не создает фиктивного движения | сага обязана довести компенсацию после рестарта; очередь компенсаций - частичный индекс саги (включает `failed`) | фоновое восстановление саги - 10.4/10.7; ретеншен/KI |
| T-PAYOUT-04 | Состояния внутренних шагов саги (`reserved`, `sent_to_gateway`, `rejected`) не публикуются в `payout.events` (CF-2): события только `requested/approved/completed/failed/compensated` | контракт стабилен, потребители (Notification 8.x, ClickHouse 11.x) видят бизнес-вехи, а не внутренние шаги | мерчант не получает вебхук об отказе оператора (`rejected`) до решения 10.5/7.4 | эскалация типа события - владелец контракта (CF-2/CF-3) при требовании Notification |
| T-PAYOUT-05 | Данные саги заполняются только переходами (`reserve_id`, `gateway_payout_id`, `provider`, `txid`, `failure_reason`, `completed_at`, `fee_*`); `Create` зачищает переданные значения, INSERT guard их запрещает | тихая подмена контекста саги до апрува невозможна | guardian-логика дублируется в репозитории и триггере | тесты: INSERT с ready-полями отклоняется; `Create` с предзаполненными полями их зачищает |
| T-PAYOUT-06 | Двухэтапность апрува = два разных оператора (уникальность `(payout_id, operator_id)`), решение оператора неизменяемо (append-only), вето одного достаточно; `required` в голосе - снимок политики, решение - текущей политикой | правило двух человек структурно (повтор идемпотентен, обход через смену решения невозможен); аудит голосов полный | оператор не может передумать после голоса (append-only); прогресс апрува не виден в CF-1 `Payout` (12.2); политика вниз между голосами завершает выплату при повторе | реестр операторов и RBAC - 12.1/12.2; read-контур голосов - 12.2; дрейф вниз обязан учитывать гейт 10.2 (re-gate `requested`) |
| T-PAYOUT-07 | Политика порогов апрува (10.5) с fail-safe: без настройки - два оператора, нулевое значение структуры не «выключает» апрув; строгий разбор JSON (обязательные поля, запрет `null`/неизвестных полей) и отказ старта при опечатке | опечатка в конфигурации не отключает ручное подтверждение выплат; конфигурируемость без релиза | жёсткость: любое изменение формата - через рестарт и лог `Describe`; `default:0` (легальный) требует осознанного решения оператора | лог старта `payout approval policy configured`; алерт на `default=0` при дежурстве (runbook); пороги по USD-эквиваленту вручную, как у 4.5 |
| T-PAYOUT-08 | Сага (10.2): порядок «внешняя идемпотентная операция Ledger -> CAS-переход»; в consumer-е внешний вызов выполняется внутри транзакции Guard, выплата читается под row-локом; вердикт шлюза обязан нести `gateway_payout_id`, совпадающий с зафиксированным `submitted` | терминализация не опережает внешний шаг; конкурентные confirmed/failed сериализуются (commit и release не перемешиваются); повторы безопасны (ключи reserve/commit/release) | транзакция Guard удерживается до 1 с на вызов Ledger (до ~3 с с повторами), throughput consumer-а ограничен Ledger; вердикт до `submitted` уходит в DLQ (потерянный submitted - ручной разбор, KI-payout-10) | эмиссия команды и переход в `sent_to_gateway` одной транзакцией; повторы плана до 3; DLQ-алерт в runbook; при росте нагрузки - двухфазная схема как в kit T-KK5 |
| T-PAYOUT-09 | Возврат (10.6) - поле `refund_invoice_id`, а не отдельный тип: автомат и сага общие (CBL-000: «частный случай выплаты»); Notification различает `refund.*` по маркеру | нет новых состояний/ветвлений, общие гарантии идемпотентности и компенсации; часть контракта (аддитивные поля, `cf-2.1`) | Payout не проверяет существование/владельца счета и остаток (нет данных - database-per-service): агрегация остатка и ownership обязательны в Billing API 7.x; произвольный маркер при неаутентифицированном CreatePayout - KI-payout-02 | реестр/проверка остатка - 7.x (спека 7.4); типы `refund.*` - 8.x; маркер неизменяем вторым барьером; канонизация UUID и связь с request_hash |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-payout-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-payout-02 | Нет mTLS и authN на gRPC PayoutService (9080) и топиках: с 10.2 `CreatePayout` любого `merchant_id` создает выплату и резервирует средства чужого мерчанта (Reserve в Ledger), `GetPayout` отдает полный Payout; с 10.6 `CreatePayout` с произвольным `refund_invoice_id` инициирует `refund.*` уведомления мерчанту со чужим/выдуманным счетом, а `GetPayout` раскрывает чужой маркер (Payout не проверяет существование и владельца счета - обязанность 7.x); `ApprovePayout` (10.5) с произвольными `operator_id` набирает кворум или необратимо отклоняет; produce в `gateway.payout.events` позволяет подтвердить/компенсировать чужую `sent_to_gateway`-выплату без реальной отправки (привязка к `gateway_payout_id` - только второй барьер; подделанный `submitted` его перекрывает); `payout.commands` позволяет подделывать команды шлюзу (кросс-ссылки KI-ledger-02, KI-payment-04) | prod-выкладка запрещена; gRPC и топики не публиковать за доверенную сеть | mTLS/авторизация 0.5/0.6; ACL: produce `gateway.payout.events` только Connector, `payout.commands` только cbl-payout; RBAC операторов - 12.1/12.2; ownership `refund_invoice_id` и остаток счета - 7.x; разбор инцидентов - runbook | open |
| KI-payout-09 | В CF-1 `ApprovePayoutRequest` нет `idempotency_key`: идемпотентность опирается на строку голоса `(payout_id, operator_id)`; при исчерпании CAS-повторов клиент (12.2) обязан повторить вызов, иначе голос тихо теряется | повторная доставка безопасна для того же оператора; сетевые сбои требуют retry-политики клиента | расширение контракта ключом - вопрос владельца CF при 12.2; в runbook: `Aborted` = повторить вызов | open |
| KI-payout-10 | Нет фонового восстановления зависших саг: `requested`/`approved` без движения (сбой Reserve), `failed` без компенсации, «orphan» active-резерв после сбоя между Reserve и транзакцией записи, вердикт до `submitted` (DLQ) | выплата или удержание зависает до ручного вмешательства; в compose-стендах Ledger нет - сага недоступна | 10.4 (перепроверка статуса) и 10.7 (хаос-тесты/восстановление); до этого - возобновление повтором Create/Approve и runbook | open |
| KI-payout-11 | `provider` выплаты не заполняется: CF-2 `GatewayPayoutEvent` не несет провайдера, `submitted` фиксирует только `gateway_payout_id`/`txid` | `payouts.provider` и поле provider в `payout.events` пусты (влияет на Reconciler 9.x/отчетность) | заполнение в 10.4 (перепроверка статуса/маршрут) или расширение CF-2 владельцем контракта | open |
| KI-payout-03 | Рантайм и миграции под одним DSN: владелец схемы может снять триггеры второго барьера | в dev-контуре барьер снимается владельцем БД (как KI-payment-03) | разделение ролей migrator/runtime при выходе в prod (0.4/0.6) | open |
| KI-payout-04 | Нет ретеншена и партиционирования `payouts`, `payout_transitions`, `outbox` | рост БД и бэкапов | задача 14.2 (как KI-ledger-09, KI-payment-02) | open |
| KI-payout-05 | `account_id` счета выплат (UUIDv5 плана счетов) в 10.2 воспроизводится по golden-векторам (KI-ledger-08) | риск дрейфа с леджером | перенос вывода в cbl-kit до 10.2/7.4; векторы обязательны в тестах | open |
| KI-payout-06 | Провайдер шлюза не выбран (KI-01 системный): `CreatePayout` в Connector (10.3) не реализован | блокирует сквозной поток 10.2/10.4/10.7 с реальным шлюзом | mock-gateway/заглушки до выбора провайдера | open |
| KI-payout-07 | Race detector недоступен локально (нет CGO на Windows) | конкурентные тесты без `-race` | CI (0.2) | open |
| KI-payout-08 | gRPC reflection включен безусловно | избыточное раскрытие API в prod | отключить в prod-профиле при наблюдаемости 0.7 | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-10.1 | 2026-10-15 | done, ревью закрыто (схема payouts/payout_transitions, автомат с двойным барьером и CAS, `reserve_id`, e2e обе цепочки, покрытие 91.6%) | `.specs/CBL-10.1-payout-schema.md` | fcd3b8a |
| CBL-10.5 | 2026-10-15 | done, ревью закрыто (политика порогов апрува с fail-safe, журнал голосов append-only, два разных оператора и вето, gRPC ApprovePayout, e2e grpcurl, покрытие 90.5%) | `.specs/CBL-10.5-approvals.md` | 8a6e49a |
| CBL-10.2 | 2026-10-15 | done, ревью закрыто (сага: резерв Ledger из golden-векторов, payout.commands через outbox, consumer gateway.payout.events с привязкой вердикта, commit/release, компенсация, continuation апрува, e2e Kafka) | `.specs/CBL-10.2-saga.md` | d5788fc |
| CBL-10.6 | 2026-10-15 | done, ревью закрыто (возвраты: refund_invoice_id в контрактах cf-2.1 и payout.events, CHECK/индекс/неизменяемость, канонизация и хэш, e2e возврата) | `.specs/CBL-10.6-refunds.md` | этот коммит |

## 11. Runbook

Автомат выплаты (10.1):

1. Состояние и история: `SELECT state, state_version, reserve_id, gateway_payout_id
   FROM payouts WHERE id = '...'`; путь переходов - `SELECT from_state, to_state,
   state_version, reason, occurred_at FROM payout_transitions WHERE payout_id = '...'
   ORDER BY state_version`.
2. `ErrStateConflict` в логах: конкурентный переход (сага 10.2 против consumer-а
   10.4); вызывающий повторяет цикл с чтением свежей версии (`Get`/`PayoutTx`).
3. Правка состояния напрямую в БД невозможна (триггер): исправление - только
   допустимый переход через приложение; `payout_transitions` append-only.
4. Зависшие выплаты (нет движения): `SELECT id, state, updated_at FROM payouts
   WHERE state IN ('requested','approved','reserved','sent_to_gateway','failed')
   AND updated_at <= now() - interval '15 minutes' ORDER BY updated_at` - разбор
   саги в 10.4/10.7; незакомпенсированное `failed` - приоритет (удержание).
5. `reserve_id` зафиксирован при `reserved` и неизменяем; компенсация
   (`failed -> compensated`) обязана использовать именно его (release).

Апрувы (10.5):

1. Политика печатается при старте (`payout approval policy configured`);
   менять - только рестартом. `default=0` (апрув не требуется) требует
   осознанного решения: сверить с логом и алертить при дежурстве.
2. Голоса оператора: `SELECT operator_id, approve, required, reason, created_at
   FROM payout_approvals WHERE payout_id = '...' ORDER BY id` - аудит кворума;
   `required` - требования политики на момент голоса. Прямой UPDATE/DELETE
   невозможен (append-only); решение оператора неизменяемо.
3. Оператор получает `FailedPrecondition approval not required` - сумма ниже
   порога; `FailedPrecondition operator vote is immutable` - попытка сменить
   решение; `Aborted` - конфликт состояния, повторить вызов (KI-payout-09).
4. `requested`-выплата без голосов дольше SLA - проверить политику в логе
   старта и число голосов; добить голосом через `ApprovePayout` (Back-office
   12.2); после mTLS (0.5/0.6) сверять операторов с реестром (KI-payout-02).

Сага (10.2):

1. Состояние выплаты: `SELECT state, state_version, reserve_id, gateway_payout_id,
   provider, failure_reason FROM payouts WHERE id = '...'`; журнал -
   `payout_transitions ORDER BY state_version`. Outbox: неопубликованные строки
   `SELECT id, topic, headers->>'event_type' FROM outbox WHERE published_at IS
   NULL ORDER BY id` (без Kafka копятся, KI-payout-04/02).
2. Вердикт ушел в DLQ `gateway.payout.events.dlq`: причина в `x-error`; типовые -
   вердикт до `submitted` или чужой `gateway_payout_id` (привязка), невалидная
   fee/строки. Разбор: сверить `gateway_payout_id`/`txid` с Connector; повтор
   возможен только после устранения причины (ручной replay, 10.4).
3. Зависшая `requested`/`approved` (нет движения): проверить `LEDGER_ADDR`,
   лог `payout saga continuation failed`; повтор `CreatePayout`/`ApprovePayout`
   с тем же ключом возобновляет сагу; фоновое восстановление - KI-payout-10
   (10.4/10.7). `ErrLedgerUnavailable` - Ledger недоступен, повторить.
4. `failed` без `compensated` (удержание не освобождено): приоритетный разбор;
   release идемпотентен (`payout:{id}:release`), доводится повтором события или
   операторской процедурой 10.7; «orphan» active-резерв в Ledger по `ref_id`
   выплаты - KI-payout-10.
5. Подозрительное завершение/компенсация (KI-payout-02): сверить
   `gateway_payout_id`/`txid` с Connector; при подтвержденной подделке - разбор
   инцидента (доступ к топику/gRPC), денежные исправления - только сторно
   (10.x); prod запрещен до ACL/mTLS (0.5/0.6).

Возвраты (10.6):

1. Поиск возвратов: `SELECT id, state, refund_invoice_id, merchant_ref FROM
   payouts WHERE refund_invoice_id = '<invoice_id>' ORDER BY created_at`
   (частичный индекс `payouts_refund_idx`); агрегация частичных возвратов и
   остаток счета - Billing API 7.x.
2. Возврат на неизвестный/чужой счет (KI-payout-02): сверить
   `refund_invoice_id` с Billing API и записями платежей; события `refund.*`
   мерчанту могли быть отправлены - уведомить 8.x/7.x; денежные исправления -
   только сторно после разбора.
3. Маркер возврата неизменяем; ошибка «immutable fields cannot change» при
   правке - ожидаемая (прямой SQL запрещен).
