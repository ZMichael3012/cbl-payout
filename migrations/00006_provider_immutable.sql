-- +goose Up
-- The provider is fixed once recorded (17.11, KI-payout-11): a substitution
-- would misattribute the payout in reporting and reconciliation. The guard is
-- recreated with the additional rule; the rest of the 10.1 body is unchanged.
-- +goose StatementBegin
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
    IF OLD.provider IS NOT NULL AND NEW.provider IS DISTINCT FROM OLD.provider THEN
        RAISE EXCEPTION 'payouts: provider is immutable once set';
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

-- +goose Down
-- +goose StatementBegin
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
