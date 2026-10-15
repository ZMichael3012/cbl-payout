-- +goose Up
-- Возврат - частный случай выплаты (CBL-000, раздел 5): исходный счет возврата
-- хранится для корреляции и маркировки событий refund.* (CBL-10.6).
ALTER TABLE payouts
    ADD COLUMN refund_invoice_id text
    CHECK (refund_invoice_id IS NULL OR refund_invoice_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

CREATE INDEX payouts_refund_idx
    ON payouts (refund_invoice_id) WHERE refund_invoice_id IS NOT NULL;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION payouts_transition_guard() RETURNS trigger AS $$
DECLARE
    allowed boolean;
BEGIN
    -- Реквизиты получателя, суммы и маркер возврата неизменяемы: подмена
    -- адреса/сети/ссылки на счет после создания увела бы деньги.
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.merchant_id IS DISTINCT FROM OLD.merchant_id
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
        OR NEW.amount_minor IS DISTINCT FROM OLD.amount_minor
        OR NEW.amount_asset IS DISTINCT FROM OLD.amount_asset
        OR NEW.amount_decimals IS DISTINCT FROM OLD.amount_decimals
        OR NEW.network IS DISTINCT FROM OLD.network
        OR NEW.destination_address IS DISTINCT FROM OLD.destination_address
        OR NEW.merchant_ref IS DISTINCT FROM OLD.merchant_ref
        OR NEW.refund_invoice_id IS DISTINCT FROM OLD.refund_invoice_id THEN
        RAISE EXCEPTION 'payouts: immutable fields cannot change';
    END IF;
    -- Идентификатор удержания Ledger фиксируется при резерве и не меняется.
    IF OLD.reserve_id IS NOT NULL AND NEW.reserve_id IS DISTINCT FROM OLD.reserve_id THEN
        RAISE EXCEPTION 'payouts: reserve_id is immutable once set';
    END IF;
    IF NEW.state_version <> OLD.state_version + 1 THEN
        RAISE EXCEPTION 'payouts: state_version must increment by 1 (got % -> %)',
            OLD.state_version, NEW.state_version;
    END IF;
    -- Самопереход (обновление данных саги) допустим только в нетерминальных
    -- состояниях; терминальные неизменяемы.
    IF NEW.state = OLD.state THEN
        IF OLD.state IN ('completed', 'compensated', 'rejected') THEN
            RAISE EXCEPTION 'payouts: terminal state % is immutable', OLD.state;
        END IF;
    ELSE
        allowed := (OLD.state, NEW.state) IN (
            ('requested', 'approved'), ('requested', 'reserved'), ('requested', 'rejected'), ('requested', 'failed'),
            ('approved', 'reserved'), ('approved', 'rejected'), ('approved', 'failed'),
            ('reserved', 'sent_to_gateway'), ('reserved', 'failed'),
            ('sent_to_gateway', 'completed'), ('sent_to_gateway', 'failed'),
            ('failed', 'compensated')
        );
        IF NOT allowed THEN
            RAISE EXCEPTION 'payouts: transition % -> % is forbidden', OLD.state, NEW.state;
        END IF;
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

-- +goose Down
DROP INDEX IF EXISTS payouts_refund_idx;
ALTER TABLE payouts DROP COLUMN IF EXISTS refund_invoice_id;

-- +goose StatementBegin
-- Восстановление прежнего тела триггера (00001): после отката колонки
-- ссылка на NEW.refund_invoice_id сломала бы любые UPDATE.
CREATE OR REPLACE FUNCTION payouts_transition_guard() RETURNS trigger AS $$
DECLARE
    allowed boolean;
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.merchant_id IS DISTINCT FROM OLD.merchant_id
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
        OR NEW.amount_minor IS DISTINCT FROM OLD.amount_minor
        OR NEW.amount_asset IS DISTINCT FROM OLD.amount_asset
        OR NEW.amount_decimals IS DISTINCT FROM OLD.amount_decimals
        OR NEW.network IS DISTINCT FROM OLD.network
        OR NEW.destination_address IS DISTINCT FROM OLD.destination_address
        OR NEW.merchant_ref IS DISTINCT FROM OLD.merchant_ref THEN
        RAISE EXCEPTION 'payouts: immutable fields cannot change';
    END IF;
    IF OLD.reserve_id IS NOT NULL AND NEW.reserve_id IS DISTINCT FROM OLD.reserve_id THEN
        RAISE EXCEPTION 'payouts: reserve_id is immutable once set';
    END IF;
    IF NEW.state_version <> OLD.state_version + 1 THEN
        RAISE EXCEPTION 'payouts: state_version must increment by 1 (got % -> %)',
            OLD.state_version, NEW.state_version;
    END IF;
    IF NEW.state = OLD.state THEN
        IF OLD.state IN ('completed', 'compensated', 'rejected') THEN
            RAISE EXCEPTION 'payouts: terminal state % is immutable', OLD.state;
        END IF;
    ELSE
        allowed := (OLD.state, NEW.state) IN (
            ('requested', 'approved'), ('requested', 'reserved'), ('requested', 'rejected'), ('requested', 'failed'),
            ('approved', 'reserved'), ('approved', 'rejected'), ('approved', 'failed'),
            ('reserved', 'sent_to_gateway'), ('reserved', 'failed'),
            ('sent_to_gateway', 'completed'), ('sent_to_gateway', 'failed'),
            ('failed', 'compensated')
        );
        IF NOT allowed THEN
            RAISE EXCEPTION 'payouts: transition % -> % is forbidden', OLD.state, NEW.state;
        END IF;
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd
