-- +goose Up
CREATE TABLE payouts (
    id                  uuid PRIMARY KEY,
    merchant_id         text        NOT NULL CHECK (merchant_id <> '' AND length(merchant_id) <= 128),
    idempotency_key     text        NOT NULL CHECK (idempotency_key <> '' AND length(idempotency_key) <= 255),
    request_hash        text        NOT NULL CHECK (request_hash <> '' AND length(request_hash) <= 128),
    state               text        NOT NULL CHECK (state IN (
        'requested', 'approved', 'reserved', 'sent_to_gateway',
        'completed', 'failed', 'compensated', 'rejected'
    )),
    state_version       bigint      NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    amount_minor        bigint      NOT NULL CHECK (amount_minor > 0),
    amount_asset        text        NOT NULL CHECK (amount_asset ~ '^[A-Z0-9]{2,16}$'),
    amount_decimals     int         NOT NULL CHECK (amount_decimals BETWEEN 0 AND 18),
    fee_minor           bigint      CHECK (fee_minor IS NULL OR fee_minor >= 0),
    fee_asset           text        CHECK (fee_asset IS NULL OR fee_asset ~ '^[A-Z0-9]{2,16}$'),
    fee_decimals        int         CHECK (fee_decimals IS NULL OR fee_decimals BETWEEN 0 AND 18),
    CHECK ((fee_minor IS NULL) = (fee_asset IS NULL)
        AND (fee_minor IS NULL) = (fee_decimals IS NULL)),
    network             text        NOT NULL CHECK (network <> '' AND length(network) <= 64),
    destination_address text        NOT NULL CHECK (destination_address <> '' AND length(destination_address) <= 256),
    merchant_ref        text        NOT NULL DEFAULT '' CHECK (length(merchant_ref) <= 255),
    reserve_id          text,
    gateway_payout_id   text,
    provider            text,
    txid                text,
    failure_reason      text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    completed_at        timestamptz,
    CHECK (completed_at IS NULL OR state = 'completed'),
    CHECK (state <> 'completed' OR completed_at IS NOT NULL)
);

CREATE UNIQUE INDEX payouts_merchant_idempotency_idx
    ON payouts (merchant_id, idempotency_key);
CREATE INDEX payouts_merchant_created_idx
    ON payouts (merchant_id, created_at DESC, id DESC);
CREATE INDEX payouts_saga_idx
    ON payouts (updated_at)
    WHERE state IN ('requested', 'approved', 'reserved', 'sent_to_gateway', 'failed');

CREATE TABLE payout_transitions (
    id            bigserial PRIMARY KEY,
    payout_id     uuid        NOT NULL REFERENCES payouts (id),
    from_state    text        NOT NULL DEFAULT '' CHECK (from_state IN (
        '', 'requested', 'approved', 'reserved', 'sent_to_gateway',
        'completed', 'failed', 'compensated', 'rejected'
    )),
    to_state      text        NOT NULL CHECK (to_state IN (
        'requested', 'approved', 'reserved', 'sent_to_gateway',
        'completed', 'failed', 'compensated', 'rejected'
    )),
    state_version bigint      NOT NULL CHECK (state_version >= 1),
    reason        text        NOT NULL DEFAULT '',
    occurred_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (payout_id, state_version)
);

-- +goose StatementBegin
CREATE FUNCTION payouts_transition_guard() RETURNS trigger AS $$
DECLARE
    allowed boolean;
BEGIN
    -- Recipient details and amounts are immutable: substituting the address or
    -- network after creation would divert the money, so the second barrier is closed before approval and sending.
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
    -- The Ledger hold identifier is fixed at reserve time and does not change.
    IF OLD.reserve_id IS NOT NULL AND NEW.reserve_id IS DISTINCT FROM OLD.reserve_id THEN
        RAISE EXCEPTION 'payouts: reserve_id is immutable once set';
    END IF;
    IF NEW.state_version <> OLD.state_version + 1 THEN
        RAISE EXCEPTION 'payouts: state_version must increment by 1 (got % -> %)',
            OLD.state_version, NEW.state_version;
    END IF;
    -- A self-transition (saga data update) is allowed only in non-terminal
    -- states; terminal states are immutable.
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

CREATE TRIGGER payouts_transition_guard_trigger
    BEFORE UPDATE ON payouts
    FOR EACH ROW EXECUTE FUNCTION payouts_transition_guard();

-- +goose StatementBegin
CREATE FUNCTION payouts_insert_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.state <> 'requested' OR NEW.state_version <> 1 THEN
        RAISE EXCEPTION 'payouts: new payout must start in requested with state_version 1';
    END IF;
    -- Saga data appears only through transitions: silent substitution of the
    -- context (a ready gateway_payout_id or reserve_id on creation) is forbidden.
    IF NEW.reserve_id IS NOT NULL OR NEW.gateway_payout_id IS NOT NULL
        OR NEW.provider IS NOT NULL OR NEW.txid IS NOT NULL
        OR NEW.failure_reason IS NOT NULL OR NEW.completed_at IS NOT NULL
        OR NEW.fee_minor IS NOT NULL OR NEW.fee_asset IS NOT NULL OR NEW.fee_decimals IS NOT NULL THEN
        RAISE EXCEPTION 'payouts: saga fields must be empty on insert';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

CREATE TRIGGER payouts_insert_guard_trigger
    BEFORE INSERT ON payouts
    FOR EACH ROW EXECUTE FUNCTION payouts_insert_guard();

-- +goose StatementBegin
CREATE FUNCTION payout_transitions_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'payout_transitions is append-only: % is forbidden', TG_OP;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

CREATE TRIGGER payout_transitions_append_only_trigger
    BEFORE UPDATE OR DELETE ON payout_transitions
    FOR EACH ROW EXECUTE FUNCTION payout_transitions_append_only();

-- +goose StatementBegin
CREATE FUNCTION payout_transitions_no_truncate() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'payout_transitions is append-only: TRUNCATE is forbidden';
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

CREATE TRIGGER payout_transitions_no_truncate_trigger
    BEFORE TRUNCATE ON payout_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION payout_transitions_no_truncate();

-- +goose Down
DROP TRIGGER IF EXISTS payout_transitions_no_truncate_trigger ON payout_transitions;
DROP FUNCTION IF EXISTS payout_transitions_no_truncate();
DROP TRIGGER IF EXISTS payout_transitions_append_only_trigger ON payout_transitions;
DROP FUNCTION IF EXISTS payout_transitions_append_only();
DROP TRIGGER IF EXISTS payouts_insert_guard_trigger ON payouts;
DROP FUNCTION IF EXISTS payouts_insert_guard();
DROP TRIGGER IF EXISTS payouts_transition_guard_trigger ON payouts;
DROP FUNCTION IF EXISTS payouts_transition_guard();
DROP TABLE IF EXISTS payout_transitions;
DROP TABLE IF EXISTS payouts;
