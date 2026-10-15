# CBL-10.1. Автомат выплаты и схема БД

| Поле | Значение |
| :-- | :-- |
| Эпик | E10, Payout/Refund Service (Saga) |
| Репозиторий | `cbl-payout` |
| Размер | M |
| Волна и параллельная группа | W2-PAYOUT |
| Зависит от | CF-1 (`billing/payout/v1`), CF-2 (`billing/events/v1`), 1.2 (cbl-kit/outbox), 1.3 (cbl-kit/inbox) |
| Версия контрактов | `cbl-protos@cf-2` |
| Статус | done, ревью закрыто |

## 1. Контекст и цель

Payout владеет конечным автоматом выплаты (`CBL-000`, раздел 2) и ведет сагу
`requested -> approved -> reserved -> sent_to_gateway -> completed/failed` с
компенсацией удержания (`compensated`) и отклонением оператором (`rejected`).
Задача создает фундамент эпика: схему БД (`payouts`, `payout_transitions`,
outbox/inbox), доменную матрицу переходов и `state_version` с оптимистической
блокировкой - для 10.2 (сага), 10.4 (неизвестный статус), 10.5 (апрувы),
10.6 (возвраты), 10.7 (хаос-тесты).

Состояния и потоки зафиксированы `CBL-000` (раздел 5, выплата), `CBL-003`
(раздел 6: `payout.events` - `requested/approved/completed/failed/compensated`) и
CF-1 (`billing/payout/v1/PayoutState`).

## 2. Границы

In scope: миграции 00001 - 00003, домен конечного автомата, storage-слой
переходов с CAS, обвязка main (миграции, пул БД), выравнивание каркаса
репозитория под стандартный набор (go.mod, линтер v2, Makefile, Dockerfile,
compose), тесты unit/integration, e2e проверки схемы на compose.

Out of scope: gRPC-обработчики PayoutService (10.2, 10.5), сага резерв-команда-commit
(10.2), consumer-ы `ledger.events`/`gateway.payout.events` (10.2, 10.4), лимиты и
апрувы (10.5), возвраты (10.6), публикация `payout.commands`/`payout.events` (10.2),
перепроверка статуса (10.4).

## 3. Функциональные требования

FR-1. Схема `payouts` (миграция 00001): `id` uuid PK (UUIDv7), `merchant_id`
(1 - 128), `idempotency_key` (1 - 255) + `request_hash` (1 - 128) с
уникальностью `(merchant_id, idempotency_key)`, `state` (CHECK по каноническим
состояниям), `state_version bigint >= 1`, `amount_minor > 0`, `amount_asset`
(`^[A-Z0-9]{2,16}$`), `amount_decimals` (0 - 18), тройка `fee_minor`/`fee_asset`/
`fee_decimals` (nullable, all-or-nothing, `>= 0` при задании), `network`
(1 - 64), `destination_address` (1 - 256), `merchant_ref` (not null, до 255),
`reserve_id` (идентификатор удержания Ledger, nullable до шага `reserved`),
`gateway_payout_id`, `provider`, `txid`, `failure_reason` (nullable),
`created_at`, `updated_at`, `completed_at` (CHECK: только для `completed` и
обязателен для него). Индексы: `(merchant_id, created_at DESC, id DESC)` для
истории; частичный `(updated_at) WHERE state IN ('requested','approved',
'reserved','sent_to_gateway','failed')` для восстановления саги и фоновых задач
(10.4, 10.7): `failed` включен, чтобы незакомпенсированное удержание не зависло.

FR-2. История переходов `payout_transitions` (append-only, триггер UPDATE/DELETE/
TRUNCATE): `payout_id` (FK), `from_state` (пусто для создания; CHECK по словарю),
`to_state` (CHECK по словарю), `state_version`, `reason`, `occurred_at`;
уникальность `(payout_id, state_version)`. Журнал - аудит состояний и источник
timeline для 10.4/10.6.

FR-3. Матрица переходов (домен, единственный источник правил):

```
requested       -> approved | reserved | rejected | failed
approved        -> reserved | rejected | failed
reserved        -> sent_to_gateway | failed
sent_to_gateway -> completed | failed
failed          -> compensated
терминальные: completed, compensated, rejected
```

Семантика: `requested` - заявка создана (10.2: мелкая проходит лимиты и идет в
резерв, крупная ждет апрува 10.5); `approved` - апрув оператора пройден
(10.5); `reserved` - удержание в Ledger создано (2.4); `sent_to_gateway` -
команда отправлена в Connector (`payout.commands`); `completed`/`failed` -
результат внешнего шага; `failed -> compensated` - компенсация: release
удержания (или закрытие саги без release, если резерва не было) - выполняет
10.2; `rejected` - отказ оператора до резерва (терминальный). Переходы назад и
из терминальных состояний запрещены; подписи `allowed(from, to)` и
`terminal(state)`. Самопереход (обновление данных без смены состояния:
`gateway_payout_id`, `fee`, повтор подтверждения) допустим только в
нетерминальных состояниях, увеличивает `state_version` на 1 и пишется в журнал.

FR-4. `state_version`: при создании 1, каждый принятый переход (включая
самопереход) увеличивает ровно на 1. Переход выполняется оптимистично: чтение
под row-локом и `UPDATE ... WHERE id = ? AND state_version = ?`; конфликт версии -
`domain.ErrStateConflict` для повторов вызывающего (сага 10.2, consumer 10.4);
в той же транзакции пишется строка `payout_transitions`.

FR-5. Второй барьер БД: BEFORE INSERT триггер `payouts` требует
`state='requested' AND state_version=1` и пустые данные саги (`reserve_id`,
`gateway_payout_id`, `provider`, `txid`, `failure_reason`, `completed_at`,
`fee_*`): подмена контекста до переходов невозможна; BEFORE UPDATE триггер
проверяет пары `(OLD.state, NEW.state)` по матрице,
`NEW.state_version = OLD.state_version + 1`, неизменяемость полей (`id`,
`merchant_id`, `idempotency_key`, `request_hash`, `created_at`, `amount_*`,
`network`, `destination_address`, `merchant_ref` - реквизиты получателя нельзя
подменить после апрува/отправки) и `reserve_id` после его фиксации;
самопереходы в терминальных состояниях запрещены. Матрица Go и SQL проверяются
полным перебором всех пар состояний в integration-тесте.

FR-6. Миграции 00002 (outbox) и 00003 (inbox) - reference-DDL из `cbl-kit`
(`CBL-003`, раздел 6), как в леджере (2.1) и payment (4.1).

FR-7. Storage: `Create` (вставка `requested` версии 1 - состояние, версия и
данные саги принудительно приводятся к начальным, строка истории версии 1 в
одной транзакции; занятый ключ идемпотентности - `ErrDuplicateKey`),
`Transition`/`TransitionTx` (CAS по `state_version` + строка журнала;
tx-вариант - для записи перехода и outbox-строки в одной транзакции саги 10.2
и consumer-а 10.4), `PayoutTx` (чтение выплаты в переданной транзакции Guard),
`Get` (выплата и история в одной read-only транзакции repeatable read).

FR-8. Обвязка: main применяет миграции (`MIGRATIONS_DIR`, по умолчанию
`./migrations`) до старта серверов, настраивает пул БД (`DB_MAX_OPEN_CONNS`
по умолчанию 20, потолок 100; `DB_MAX_IDLE_CONNS` по умолчанию 10; lifetime
30 минут), требует `PG_DSN`. Каркас приводится к стандартному набору: go.mod
(GORM + pgx-драйвер, UUID, testify, testcontainers, cbl-kit/cbl-protos с
replace), `.golangci.yml` v2 (gosec), Makefile (test-integration, e2e, test-e2e,
lint с контекстом `services/`), Dockerfile (общий контекст `services/`),
локальный compose (postgres `payout`, профиль `app`).

## 4. Контракты

Внешние контракты не меняются (CF-1/CF-2 заморожены): домен `State` соответствует
`billing.payout.v1.PayoutState` (маппинг 1:1 по именам, проверяется тестом).
Соответствие состояний событиям `payout.events` (10.2): `requested`, `approved`,
`completed`, `failed`, `compensated`; состояния `reserved`, `sent_to_gateway`,
`rejected` события в CF-2 не имеют (внутренние шаги саги).

## 5. Модель данных и миграции

```sql
payouts(id, merchant_id, idempotency_key, request_hash, state, state_version,
        amount_minor, amount_asset, amount_decimals,
        fee_minor, fee_asset, fee_decimals,
        network, destination_address, merchant_ref, reserve_id,
        gateway_payout_id, provider, txid, failure_reason,
        created_at, updated_at, completed_at)
payout_transitions(id bigserial, payout_id, from_state, to_state, state_version,
                   reason, occurred_at, UNIQUE (payout_id, state_version))
outbox, inbox - как в CBL-003 (раздел 6)
```

Миграции только вперед; 00001 создает таблицы выплаты и триггеры, 00002/00003 -
инфраструктурные таблицы событий.

## 6. Конечные автоматы и правила

Матрица FR-3; состояния и переходы дублируются триггером БД (FR-5) и
проверяются полным перебором пар в integration-тесте. Разница с автоматом
платежа (4.1): `failed` не терминален - из него возможна только компенсация
(10.2), а состояния `reserved`/`sent_to_gateway` отражают шаги саги и не
публикуются в `payout.events`.

## 7. Edge cases

Создание с версией 1 (переданные состояние, версия и данные саги игнорируются;
прямой INSERT с другим состоянием или готовым `gateway_payout_id` отклоняет
BEFORE INSERT триггер); конкурентные переходы из одной версии (ровно один
выигрывает); устаревший переход после более нового (отказ, версия и журнал не
растут); переход назад (`sent_to_gateway -> reserved`), переход из терминального
(`completed -> failed`), неизвестная пара (домен и триггер); самопереход в
нетерминальном состоянии (разрешен, пишет строку журнала) и в терминальном
(запрещен); прямой UPDATE в обход CAS; UPDATE неизменяемых полей (включая
`amount_*`, `network`, `destination_address`, `merchant_ref`); смена
`reserve_id` после фиксации; UPDATE/DELETE/TRUNCATE журнала; запись журнала с
неизвестным `from_state`/`to_state`; дубликат `(merchant_id, idempotency_key)`;
нечисловые границы: нулевая/отрицательная сумма, пустой актив, decimals вне
диапазона, пустые `network`/`destination_address`, строки сверх лимитов длины;
частичная тройка `fee`; `completed` без `completed_at` и `completed_at` вне
`completed`; компенсация без failed (10.2 вызывает строго `failed ->
compensated`); повтор `failed -> compensated` (терминальный отказ либо конфликт
версии); откат внешней транзакции с `TransitionTx` (переход и журнал не
фиксируются).

## 8. Нефункциональные требования

Транзакционность: переход и его история - одна транзакция; конкурентность -
оптимистичные повторы (T-PAY-02-подобно, как в леджере и payment); журнал
append-only - аудит; дедлайн CreatePayout/GetPayout 2 с (CF-1) укладывается в
чтение/запись по PK; частичный индекс саги ограничивает выборки фоновых задач
(10.4) нетерминальными состояниями.

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Unit (домен) | вся матрица (каждая разрешенная пара), запрещенные пары (назад, из терминального, неизвестные состояния), `terminal()`, маппинг `PayoutState` 1:1, инкремент версии, самопереходы |
| Integration (storage) | Create с историей v1 и принудительным `requested` (данные саги зачищаются); CAS-переход и инкремент; конкурентные переходы (ровно один); устаревшая версия (версия и журнал не растут); полный перебор всех пар против триггера (зеркало матрицы); прямой INSERT/UPDATE в обход автомата; неизменяемые поля (включая реквизиты получателя) и `amount_*`; `reserve_id` фиксируется при резерве и неизменяем после; самопереход пишет строку журнала; `completed_at` обязателен для `completed` и запрещен вне него; повтор компенсации - терминальный отказ; `TransitionTx` откатывается вместе с внешней транзакцией; словарь `from_state`/`to_state` журнала; append-only; CHECK-и сумм/активов/шкал/адреса/длин/частичнойfee; уникальность ключа идемпотентности |
| E2E | compose: миграции применены (версия 3), цепочка `requested -> reserved -> sent_to_gateway -> completed` и компенсационная `requested -> approved -> reserved -> failed -> compensated` с записью каждого перехода, запрещенный переход и перезапись журнала отклоняются вторым барьером |
| Load | 14.1: payout отдельным профилем не нагружается |

Целевое покрытие unit+integration: не ниже 80%.

## 10. План работ и коммит

Тесты вперед (домен, storage integration), затем миграции, domain/state.go,
storage/repository.go, обвязка main и каркаса, e2e-скрипт стенда. Коммит
`CBL-10.1: автомат выплаты и схема БД`; спека в том же коммите.

## 11. Открытые вопросы и решения

- `reserve_id` (идентификатор удержания Ledger) хранится в `payouts` с 10.1:
  `CommitReserve`/`ReleaseReserve` принимают только `reserve_id` (`ledger.proto`),
  поиска резерва по `ref_id` нет; без сохраненного идентификатора компенсация
  после рестарта саги (10.2/10.7) невозможна. Поле фиксируется при переходе в
  `reserved` и неизменяемо вторым барьером.
- `failed` не терминален: сага обязана закрыть удержание (`failed ->
  compensated`); для отказов до резерва 10.2 переводит `failed -> compensated`
  без вызова Ledger (release не требуется), чтобы у выплаты был один
  финальный статус компенсации. Отклонение оператора до резерва - отдельное
  терминальное `rejected`.
- Связь возврата с исходной выплатой/счетом (10.6): в CF-1 `CreatePayoutRequest`
  нет `invoice_id`; поле `merchant_ref` хранится для корреляции (Billing API
  7.4). Если 10.6 потребует явной ссылки, контракт расширяется владельцем
  (`billing/payout/v1` - CF-1, `billing/events/v1` - CF-2) отдельной задачей.
- Апрувы (10.5) не добавляют состояний: `requested -> approved` покрывает
  двухэтапность; колонки оператора/времени апрува и идемпотентность повторного
  `ApprovePayout` (в CF-1 нет `idempotency_key`) - 10.5, добавляются миграцией
  при необходимости.
- Политика too-early для `gateway.payout.events` (результат может прийти раньше
  `sent_to_gateway` из-за гонки саги и consumer-а): ранний `submitted/confirmed`
  обязан парковаться/повторяться, а не уходить в DLQ; формулируется в 10.2/10.4
  (образец - KI-payment-05).
- `reserved -> failed` (команда не ушла) и `reserved -> sent_to_gateway`:
  выбор ветки - 10.2; матрица допускает обе.
- Релапсы Unknown-статуса (10.4): `sent_to_gateway` остается нетерминальным,
  перепроверка статуса не меняет состояние до вердикта; пары матрицы уже
  покрывают оба исхода.
- Неизменяемость `destination_address`/`network`/`merchant_ref`: легитимных
  сценариев изменения нет; смена адреса - новая выплата (проверка в 10.2).

## 12. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (15.10.2026). BLOCKER безопасности (подмена
`destination_address`/`network`/`merchant_ref` самопереходом в обход второго
барьера) закрыт расширением immutable-набора триггера и тестами по каждому
полю; BLOCKER архитектуры (схема не хранит `reserve_id` - компенсация 10.2
после рестарта невозможна) закрыт колонкой `reserve_id` с фиксацией при
резерве и неизменяемостью. MAJOR закрыты: `failed` включен в частичный индекс
саги (удержание не зависнет без фонового обзора); добавлены tx-варианты
`TransitionTx`/`PayoutTx` под транзакцию Guard 10.2/10.4; INSERT guard требует
пустые данные саги, `Create` их зачищает; добавлены CHECK-и длин строк и
all-or-nothing `fee`; `completed` требует `completed_at`; тесты расширены
(все неизменяемые поля, словарь `from_state`, границы, повтор компенсации,
откат внешней транзакции, журнал самоперехода). AGENTS.md обновлен в этом
коммите (T-PAYOUT-01..05, KI, runbook, журнал). Принято без изменений:
журнал пишется приложением (дизайн 4.1, прямой DML вне модели угроз - KI о
ролях), сообщение `ErrDuplicateKey` с идентификаторами (образец payment,
валидация на входе 10.2), `reflection.Register` и DSN runtime/migrations -
существующие KI платформы.

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | MAJOR: `make test-e2e` - пустышка (нет Go-package); MINOR: не покрыты повтор компенсации, журнал самоперехода, неизменяемые поля кроме amount_minor, границы; NIT: формулировка FR-5, опечатка, комментарий, мертвая ветка, exit-код main, `$version`, кэш Dockerfile, UUIDv7 | цель убрана до 10.2; тесты добавлены (повтор, журнал, поля, границы); FR-5 переформулирован; опечатки исправлены; ветка оставлена с комментарием; exit-код как в payment (вне 10.1); `$version` инициализирован; COPY по образцу payment; UUIDv7 документирован |
| Архитектуры | BLOCKER: нет `reserve_id`; MAJOR: `failed` вне индекса саги, нет tx-хука под outbox, реквизиты не immutable; MINOR: дрейф версий go.mod, `test-e2e`, `completed_at` nullable, partial fee, поля 10.5, нормализация событий-состояний, too-early 10.2/10.4, тесты, AGENTS; NIT: опечатки, network-формат, `readyz`, e2e SQL | `reserve_id` добавлен; индекс расширен; `TransitionTx`/`PayoutTx`; immutable реквизиты; go.mod выровнен по единому набору; цель убрана; `completed_at` обязателен; fee all-or-nothing; 10.5/10.6 и too-early зафиксированы в §11; тесты расширены; AGENTS обновлен; опечатки исправлены; network CHECK непустоты; `readyz` без БД до 0.7; e2e SQL осознан |
| С контекстом | MAJOR: AGENTS не обновлен, `reserve_id`, `test-e2e`, спека не закрыта; MINOR: дрейф версий, immutable реквизиты, тесты неизменяемых полей, миграции 10.5/10.6 в §11, CF-2 vs CF-3, KI-01/KI-ledger-08; NIT: тавтологичный assert, комментарий main, `$version`, словарь from_state, smoke-строка, README, лишняя kafka в compose, fee all-or-nothing | AGENTS (T-PAYOUT-*, KI, runbook, журнал); `reserve_id`; цель убрана; спека закрыта; go.mod; immutable; тесты; §11 дополнен (10.5, 10.6, CF-2); KI; assert убран; комментарий; `$version`; словарь; smoke отражен; README - NIT вне 10.1; kafka оставлена (10.2 сразу использует); CHECK добавлен |
| Безопасности | BLOCKER: подмена `destination_address`/`network`/`merchant_ref`; MAJOR: аудит не обязателен на уровне БД, `failed` вне индекса, нет CHECK длин/форматов, saga-поля на INSERT; MINOR: текст ошибки, `completed_at`, UUIDv7, интерполяция e2e, `test-e2e`, пароли compose, reflection; NIT: digest, .dockerignore | immutable реквизиты + тесты; принято (дизайн 4.1, вне модели угроз - T-PAYOUT-01); индекс; CHECK длин и проверки в тестах; INSERT guard + `Create` зачистка; сообщение как в payment; `completed_at` обязателен; задокументирован; e2e безопасен (GUID); цель убрана; пароли как в payment (dev); reflection - KI (0.7); digest/.dockerignore - 0.2 |
