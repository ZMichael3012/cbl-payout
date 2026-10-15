-- +goose Up
CREATE TABLE payout_approvals (
    id          bigserial PRIMARY KEY,
    payout_id   uuid        NOT NULL REFERENCES payouts (id),
    operator_id text        NOT NULL CHECK (operator_id ~ '^[A-Za-z0-9._:@-]{1,128}$'),
    approve     boolean     NOT NULL,
    reason      text        NOT NULL DEFAULT '' CHECK (length(reason) <= 255 AND reason !~ '[[:cntrl:]]'),
    required    int         NOT NULL CHECK (required BETWEEN 1 AND 10),
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (payout_id, operator_id)
);

-- +goose StatementBegin
CREATE FUNCTION payout_approvals_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'payout_approvals is append-only: % is forbidden', TG_OP;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

CREATE TRIGGER payout_approvals_append_only_trigger
    BEFORE UPDATE OR DELETE ON payout_approvals
    FOR EACH ROW EXECUTE FUNCTION payout_approvals_append_only();

-- +goose StatementBegin
CREATE FUNCTION payout_approvals_no_truncate() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'payout_approvals is append-only: TRUNCATE is forbidden';
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
-- +goose StatementEnd

CREATE TRIGGER payout_approvals_no_truncate_trigger
    BEFORE TRUNCATE ON payout_approvals
    FOR EACH STATEMENT EXECUTE FUNCTION payout_approvals_no_truncate();

-- +goose Down
DROP TRIGGER IF EXISTS payout_approvals_no_truncate_trigger ON payout_approvals;
DROP FUNCTION IF EXISTS payout_approvals_no_truncate();
DROP TRIGGER IF EXISTS payout_approvals_append_only_trigger ON payout_approvals;
DROP FUNCTION IF EXISTS payout_approvals_append_only();
DROP TABLE IF EXISTS payout_approvals;
