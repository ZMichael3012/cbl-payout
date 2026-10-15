# CBL-10.2. Сага выплаты: резерв, команда, результат, компенсация

| Поле | Значение |
| :-- | :-- |
| Эпик | E10, Payout/Refund Service (Saga) |
| Репозиторий | `cbl-payout` |
| Размер | L |
| Волна и параллельная группа | W2-PAYOUT |
| Зависит от | 10.1, 10.5 (политика апрува), 2.4 (резервы Ledger), CF-2 |
| Версия контрактов | `cbl-protos@cf-2` (`billing/payout/v1`, `billing/events/v1`) |
| Статус | done, ревью закрыто |

## 1. Контекст и цель

Сага выплаты (`CBL-000` раздел 5): `requested (Ledger Reserve) -> reserved ->
sent_to_gateway (payout.commands -> Connector) -> completed | failed`, при
`failed` - компенсация: release удержания (`compensated`); уже проведенные
операции исправляются сторно, резерв - не проводка (T-LED-06). Внешний шаг
(шлюз) отделен от внутреннего: команда уходит через `payout.commands` (outbox),
результат приходит в `gateway.payout.events` (inbox). Неизвестный статус после
таймаута - задача 10.4; здесь - счастливый путь и компенсация.

Задача реализует оркестратор саги поверх контрактов 10.1/10.5:
`CreatePayout`/`GetPayout` (CF-1), шаг резерва (gRPC Ledger 2.4),
эмиссию `payout.commands`/`payout.events` через outbox (10.1),
consumer `gateway.payout.events` и компенсацию.

## 2. Границы

In scope: `CreatePayout`/`GetPayout` (usecase + transport), `Advance` (шаг
резерва и команды), клиент Ledger (Reserve/Commit/Release), outbox-эмиссия
событий выплаты (requested/approved/completed/failed/compensated) и команды,
consumer `gateway.payout.events` (submitted/confirmed/failed), компенсация,
непрерывность после апрува (10.5), обвязка main (relay, consumer, LEDGER_ADDR),
тесты unit/integration/e2e (Kafka).

Out of scope: перепроверка статуса после таймаута (10.4 - запрос статуса через
Connector), восстановление зависших саг фоном (10.4/10.7), реальный провайдер в
Connector (10.3, KI-01), возвраты (10.6), RBAC операторов (12.x), mTLS (0.5/0.6).

## 3. Функциональные требования

FR-1. `CreatePayout` (CF-1): вход `idempotency_key`, `merchant_id`, `amount`
(Money), `network`, `destination_address`, `merchant_ref`. Валидация: лимиты
строк (merchant_id `[A-Za-z0-9_-]{1,128}` - форма плана счетов Ledger 2.6,
ключ 1 - 255, network 1 - 64, адрес 1 - 256, merchant_ref до 255 без
управляющих символов), amount `> 0`, актив в верхнем регистре 2 - 16 символов,
decimals 0 - 18; `request_hash` - sha256 канонического представления тела с
длинами полей (length-prefix: склейка через разделитель допускала бы коллизии
разных тел). Создание - 10.1 `Create` (`requested`, версия 1): занятый
`(merchant_id, idempotency_key)` с тем же hash - идемпотентный возврат
существующей выплаты (повтор возобновляет сагу: если апрув не требуется, а
выплата в `requested|approved`, выполняется `Advance` - транспортный сбой
Reserve не оставляет заявку навсегда); с другим hash - `ErrIdempotencyConflict`
(в транспорт - `AlreadyExists`). После создания: если политика 10.5 требует
апрув - выплата ждет голосов (гейт), иначе немедленно `Advance`.

FR-2. `Advance` (шаг «резерв + команда») для `requested` (апрув не требуется)
и `approved`: счет удержания - детерминированный UUIDv5 плана счетов Ledger
(`merchant/<asset>/<decimals>/<merchant_id>`, алгоритм и golden-векторы
`CBL-003` §9, KI-ledger-08); `Reserve` (gRPC, дедлайн 1 с, ключ
`payout:{id}:reserve`, `ref_id` = id выплаты). Успех: одна транзакция БД -
переход в `reserved` (патч `reserve_id`), переход в `sent_to_gateway`,
outbox-запись `payout.commands` (PayoutCommand: payout_id, amount, network,
address, idempotency_key `payout:{id}:command`); события `payout.events` для
`reserved`/`sent_to_gateway` не публикуются (CF-2 их не имеет). Бизнес-отказ
Ledger (нет счета, недостаточно средств - `InvalidArgument`/`FailedPrecondition`/
`UnknownAccount`): переход `requested|approved -> failed` с причиной и сразу
компенсация без release (резерва нет) - `failed -> compensated`. Транспортная
ошибка (timeout/unavailable): ошибка наверх, сага не двигается - повторный
`Advance` (клиент 7.4 или 10.4) безопасен: Reserve идемпотентен по ключу.
Повторный `Advance` при состоянии вне `requested|approved` - идемпотентный
возврат текущей выплаты.

FR-3. Эмиссия `payout.events` (outbox, по образцу payment 4.7): sink на
`Repository.TransitionTx` и `Create` (событие `payout.requested`); типы
`requested`, `approved`, `completed`, `failed`, `compensated`; `reserved`,
`sent_to_gateway`, `rejected` типа не имеют и пропускаются. Конверт: topic
`payout.events`, key `payout_id`, заголовки `event_type` `payout.<state>`,
`producer` `cbl-payout`, `schema_version` `v1`, `occurred_at` (зеркалит тело),
`traceparent` из контекста. Тело `PayoutEvent`: payout_id, merchant_id, type,
amount (Money), fee (при полной тройке), txid, provider, failure_reason,
state_version, occurred_at.

FR-4. Consumer `gateway.payout.events` (группа `cbl-payout-gateway`, топик +
retry 5s/1m/10m + DLQ механикой kit; обработка в транзакции Guard, выплата
читается под row-локом - вердикты одной выплаты сериализуются и не могут
перемешать commit/release):
- `submitted`: из `sent_to_gateway` - self-переход с `gateway_payout_id` и
  `txid`; из `requested`/`approved`/`reserved` - too early (повтор через
  retry); из терминальных - stale (WARN, ack);
- `confirmed`: требует непустой `gateway_payout_id`, совпадающий с
  зафиксированным `submitted` (привязка вердикта к реальной отправке; событие
  до `submitted` или с чужим идентификатором - DLQ), подтверждение без
  `reserve_id` - DLQ без обращения к Ledger; далее `CommitReserve` (ключ
  `payout:{id}:commit`), затем CAS-переход `sent_to_gateway -> completed`
  (патч txid, fee, completed_at); ошибка Ledger/конфликт - повтор события
  (commit идемпотентен);
- `failed`: та же привязка к `gateway_payout_id`; `sent_to_gateway -> failed`
  (reason) и компенсация: release (при заданном `reserve_id`) перед переходом
  `failed -> compensated`; для состояния `failed` (повтор события после
  частичного успеха) - только release + `compensated`; из терминальных - stale.
Невалидное событие (невалидный payout_id, отрицательная/несовпадающая fee,
decimals вне 0 - 18, управляющие символы, сверхдлинные строки) - DLQ.
Неизвестный непустой тип enum подтверждается с WARN (BACKWARD_TRANSITIVE
может добавить типы, как в 4.3); неизвестный платеж - повтор.

FR-5. Компенсация: release вызывается только при `reserve_id != NULL`;
`failed -> compensated` без резерва (отказы до резерва) не обращается к
Ledger. Порядок «внешняя идемпотентная операция, затем CAS-переход»: сбой
между шагами безопасен (повтор события/саги доводит), переходы не откатывают
уже выполненные операции Ledger. Конфликт версии в consumer - повтор плана
(до 3, как 4.3).

FR-6. Транспорт: `CreatePayout` (ответ payout_id + state), `GetPayout` (полный
`Payout`); маппинг ошибок: `storage.ErrNotFound` -> `NotFound`,
валидация/`ErrIdempotencyConflict` -> `InvalidArgument`/`AlreadyExists`,
`ErrSagaUnavailable`/`ErrLedgerUnavailable` -> `FailedPrecondition`
(клиент повторяет вызов, операции идемпотентны), конфликты -> `Aborted`;
`ApprovePayout` - из 10.5.

FR-7. Обвязка: `LEDGER_ADDR` - gRPC-клиент Ledger (deadline 1 с на вызов);
при `KAFKA_BROKERS` - publisher, relay outbox (команды и события) и consumer
`gateway.payout.events`; без Ledger/Kafka сервис стартует с WARN, шаг саги
отвечает `ErrSagaUnavailable`, вердикты шлюза повторяются до настройки Ledger,
approve-контур 10.5 работает. После успешного апрува (и при идемпотентном
повторе по одобренной выплате) `ApprovalService` вызывает `Advance`
best-effort с собственным таймаутом (отмена клиента не убивает шаг): выплата
продолжает сагу без ручного вмешательства; ошибки логируются, не возвращаются
клиенту; зависшие `approved` (сбой Advance) - восстановление 10.4/10.7.

## 4. Контракты

- gRPC: `billing.payout.v1.PayoutService` CreatePayout/GetPayout (CF-1);
  клиент `billing.ledger.v1.LedgerService` Reserve/CommitReserve/ReleaseReserve
  (дедлайн 1 с; idempotency_key обязателен, `account_id` - UUIDv5 плана).
- Kafka: продюсер `payout.commands` (ключ payout_id; PayoutCommand) и
  `payout.events` (PayoutEvent); консьюмер `gateway.payout.events`
  (GatewayPayoutEvent; группа `cbl-payout-gateway`), retry/DLQ kit.
- Хранилище: `payouts.reserve_id`, `gateway_payout_id`, `provider`, `txid`,
  `fee_*` (10.1); outbox (10.1); новых таблиц нет.

## 5. Модель данных и миграции

Миграций нет: схема 10.1 содержит все поля саги. `request_hash` создания
заполняется в FR-1 и используется для идемпотентной сверки повторов.

## 6. Конечные автоматы и правила

Переходы саги по матрице 10.1: `requested -> reserved | failed`,
`approved -> reserved | failed`, `reserved -> sent_to_gateway | failed`,
`sent_to_gateway -> completed | failed`, `failed -> compensated`. Все переходы -
`TransitionTx` (CAS + журнал), события `payout.events` - из FR-3. Порядок
локов: `payouts` -> `outbox` (порядок 10.5 сохраняется).

## 7. Edge cases

Повторный `CreatePayout` с тем же ключом и телом (идемпотентный возврат с
возобновлением саги), с другим телом (`AlreadyExists`; хэш разделяет границы
полей даже при разделителе внутри значений); повторный `Advance`
(идемпотентен); Reserve успел, но транзакция записи упала (повтор Reserve по
ключу вернет тот же reserve_id, дубль удержания невозможен); business-отказ
Ledger (failed + compensated без release, каноническая причина без внутренних
деталей); Ledger недоступен (ошибка наверх, состояние не меняется); команда
опубликована, ответ потерян (10.4); `submitted` после `confirmed` (stale);
вердикт до `submitted` или с чужим `gateway_payout_id` (DLQ, Ledger не
вызывается); `confirmed` без резерва (DLQ); `failed` после `completed` (stale,
WARN); повтор `failed` после `compensated` (release по ключу - идемпотентный
no-op, переход запрещен - stale); событие для неизвестной выплаты (повтор,
затем DLQ); невалидное событие, включая управляющие символы и многобайтовые
строки сверх лимита (DLQ; усечение причины - по границе руны, без invalid
UTF-8); неизвестный непустой тип enum (WARN, ack); апрув не требуется (малая
выплата сразу в сагу); апрув требуется (ждет голосов, после апрува -
continuation); сага без Kafka (команда в outbox, публикация позже).

## 8. Нефункциональные требования

Дедлайн CreatePayout 2 с (CF-1): резерв - 1 с, далее транзакция БД;
идемпотентность всех внешних шагов (ключи Ledger, outbox, inbox); at-least-once
доставка; consumer - в транзакции Guard (атомарность inbox и переходов);
логи без сумм и адресов (payout_id, state, версия, ключ операции); traceparent
из каналов/контекста.

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Unit (домен) | UUIDv5 счета удержания по golden-векторам (`merchant/USDT/2/m-1` -> `58686062-...`), невалидные ключи |
| Unit (usecase) | CreatePayout: валидация, идемпотентный повтор, конфликт тела, малая -> Advance, крупная -> гейт; Advance: резерв + две транзакции + команда, business-отказ -> failed+compensated, транспортная ошибка; consumer: план событий по состояниям (submitted/confirmed/failed), stale/too-early, commit/release перед переходами, отсутствие резерва |
| Integration | реальный PostgreSQL + фейковый Ledger: полный счастливый путь (requested -> reserved -> sent_to_gateway -> completed) с команда-строкой outbox и событиями; компенсация (failed -> release -> compensated); бизнес-отказ резерва; идемпотентные повторы; события `payout.events` из переходов (requested/approved/completed/failed/compensated); журнал и версии |
| E2E | Kafka + PostgreSQL (testcontainers): `CreatePayout` (usecase) -> outbox -> relay -> `payout.commands`; внешний «коннектор» читает команду и публикует `gateway.payout.events submitted/confirmed` -> payout завершает сагу (commit + completed), `payout.events` содержит requested/completed; ветка failed -> compensated |
| Load | 14.1 |

Целевое покрытие unit+integration: не ниже 80%.

## 10. План работ и коммит

Тесты вперед (домен, usecase, integration, e2e Kafka), затем domain-вывод счета,
storage sink событий, usecase/PayoutService + consumer + клиент Ledger,
transport CreatePayout/GetPayout, обвязка main (relay, consumer, LEDGER_ADDR,
continuation апрува), AGENTS.md. Коммит
`CBL-10.2: сага выплаты: резерв, команда, результат, компенсация`; спека в том
же коммите.

## 11. Открытые вопросы и решения

- Порядок «внешняя идемпотентная операция -> CAS-переход» (не наоборот):
  terminal-состояния не фиксируются раньше внешнего шага, иначе компенсация
  или завершение невозможны; повтор события доводит сагу.
- Вызовы Ledger в consumer-е выполняются внутри транзакции Guard (T-PAYOUT-08):
  row-лок выплаты сериализует вердикты (конкурентные confirmed/failed не могут
  перемешать commit и release), внешняя операция идемпотентна, повтор события
  безопасен; цена - транзакция удерживается до 1 с на вызов, throughput
  consumer-а ограничен Ledger.
- `payout.commands` публикуется in-tx вместе с переходом в `sent_to_gateway`
  (outbox): состояние отражает «команда принята к отправке», реальная отправка -
  relay/Connector; отличие от «ответа шлюза» документируется в 10.4.
- Привязка вердиктов: `confirmed`/`failed` обязаны нести `gateway_payout_id`,
  совпадающий с зафиксированным `submitted`; событие до `submitted` или с чужим
  идентификатором уходит в DLQ. Это защита от преждевременного commit/release
  по подделанному событию (KI-payout-02); при пропаже `submitted` (потерянный
  вебхук) восстановление - 10.4/9.x.
- `provider` в CF-2 `GatewayPayoutEvent` отсутствует: `submitted` фиксирует
  `gateway_payout_id`/`txid`, `provider` остается NULL до 10.4 (перепроверка
  статуса/маршрут) или расширения CF-2 (KI-payout-11).
- Счёт удержания - merchant-счёт мерчанта по `merchant_id` (как кредитует
  ledger 2.7, `MerchantAccountKey(merchant_id, asset, decimals)`); `merchant_ref`
  выплаты - внешняя ссылка мерчанта и в план счетов не входит; форматы
  `merchant_id` в CreatePayout и плане счетов совпадают.
- Без Ledger (compose-стенды 10.1/10.5) шаг саги недоступен: `ErrSagaUnavailable`
  (FailedPrecondition); e2e саги - на testcontainers с фейковым Ledger.
- Восстановление зависших саг (requested/approved без резерва, failed без
  компенсации, «orphan» active-резерв после сбоя между Reserve и транзакцией,
  потерянный `submitted`) фоном - 10.4/10.7 (KI-payout-10); здесь - возобновление
  при повторе Create/Approve и best-effort continuation.
- fee и txid приходят из `gateway.payout.events` (CF-2); частичная тройка fee
  отклоняется (DLQ).

## 12. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (15.10.2026). BLOCKER закрыты: привязка вердиктов -
`confirmed`/`failed` обязаны нести `gateway_payout_id`, совпадающий с
зафиксированным `submitted`, вердикт до `submitted` - DLQ без вызова Ledger
(подделанное событие не завершает и не компенсирует выплату); row-лок выплаты
в consumer-е сериализует вердикты (commit и release не перемешиваются).
MAJOR закрыты: `request_hash` переведен на length-prefix (коллизии на границе
полей невозможны); формат `merchant_id` выровнен с планом счетов Ledger
(выплата не зависает на выводе счета); повтор `CreatePayout` возобновляет сагу
(транспортный сбой Reserve не оставляет заявку); continuation апрува вызывается
и на идемпотентном повторе, с собственным таймаутом (отмена клиента не убивает
шаг); `ErrLedgerUnavailable` -> FailedPrecondition; из логов убраны суммы,
`failure_reason` для отказов Ledger канонизирован; усечение причины по границе
руны; fee decimals и управляющие символы валидируются; `confirmed` без резерва -
DLQ без вызова Ledger; неизвестный непустой enum - WARN/ack (совместимость
BACKWARD_TRANSITIVE); AGENTS.md обновлен (T-PAYOUT-08, KI-payout-10/11, runbook).
MINOR/NIT закрыты: тест classify кодов Ledger, тесты continuation и коллизий
хэша, ключи идемпотентности проверяются в фейках, `event_type` e2e из типа
события, ConsumerOption вместо фиктивного сервиса, лог перехода только по факту.
Принято без изменений: дублирование мелких helper-ов (паттерн платформы),
`provider` до 10.4 (KI-payout-11), конфигурация без env-override группы
consumer-а (0.7), testcontainers 0.34.0 (единый набор, govulncheck - 0.2).

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | MAJOR: provider не заполняется (CF-2 без поля), сумма в логах, Internal для ErrLedgerUnavailable, merchant_id несовместим с планом; MINOR: continuation/approved-события/Reserve-после-краша без тестов, confirmed без резерва, fee decimals, байтовое усечение, тавтологичный ассерт, e2e event_type, AGENTS, traceparent; NIT: фиктивный сервис для опций, clock команды, лог перехода, дубли helper-ов | KI-payout-11 + спека; лог очищен; FailedPrecondition; формат выровнен; тесты добавлены; guard добавлен; валидация; усечение по рунам; исправлено; event_type исправлен; AGENTS; зафиксировано; ConsumerOption; принято; исправлено; принято |
| Архитектуры | BLOCKER: коллизии request_hash; MAJOR: зависание requested/approved (continuation/replay), Ledger в Guard-транзакции без фиксации, merchant_id vs план, provider, AGENTS/KI-02; MINOR: continuation в request-path, байтовое усечение, сумма в логе, unknown enum -> DLQ, Redis-эшелон, testcontainers-понижение; NIT: ConsumerOption, readyz, NotFound-маскировка, порядок outbox | length-prefix + тест; возобновление replay + continuation на повторе + WithoutCancel/таймаут; T-PAYOUT-08; формат выровнен; KI-payout-11; AGENTS + KI-02; собственный таймаут; по рунам; лог очищен; WARN/ack как 4.3; зафиксировано (быстрый путь Redis - 7.4); принято; ConsumerOption; KI до 0.7; как 4.3; T-PAYOUT-08 |
| С контекстом | BLOCKER: AGENTS.md/спека не закрыты; MAJOR: merchant_id, request_hash, сумма в логе, replay без Advance, provider; MINOR: classify 0%, continuation без теста, конкурентные ветки без тестов, e2e submitted, коды gRPC, consumer без Ledger, compose без LEDGER_ADDR, дубли, ассерты, мёртвый код, ссылка KI, лог, timeout; NIT: ConsumerOption, санитайз reason, go.mod | AGENTS/спека закрыты; исправлено; тесты; continuation-тесты; fake conflicts; e2e submitted; Unavailable; WARN; зафиксировано; принято; исправлено; `_ =` убран; ссылка; лог; 12m; ConsumerOption; канонизация; выровнен по набору |
| Безопасности | BLOCKER: подделка gateway.payout.events двигает деньги без привязки к команде; MAJOR: суммы/тексты Ledger в логах и событиях, control-символы/UTF-8, гонка commit/release без лока; MINOR: traceparent в логе, коллизии хэша, merchant_id, confirmed без резерва, бизнес-отказ как retry, continuation-ctx, provider, версии, коды; NIT: outbox payload (KI-04), UNSPECIFIED, fee без верхней границы, reflection, фейки | привязка gateway_payout_id + вердикт-до-submitted DLQ + row-лок; канонические причины, суммы убраны; валидация control/UTF-8, усечение по рунам; locking read; зафиксировано; length-prefix; выровнено; guard; зафиксировано в runbook; WithoutCancel/таймаут; KI-payout-11; единый набор, govulncheck в 0.2; FailedPrecondition; KI-04; принято (4.3); принято; KI-08; тесты добавлены |
