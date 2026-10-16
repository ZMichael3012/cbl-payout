-- +goose Up
-- Reference DDL from cbl-kit/migrations/outbox.sql (.specs/CBL-003-contracts.md).
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    event_id     uuid        NOT NULL UNIQUE,
    topic        text        NOT NULL,
    key          text        NOT NULL,
    headers      jsonb       NOT NULL,
    payload      bytea       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS outbox;
