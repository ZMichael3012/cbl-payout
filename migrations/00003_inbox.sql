-- +goose Up
-- Reference DDL from cbl-kit/migrations/inbox.sql (.specs/CBL-003-contracts.md).
CREATE TABLE inbox (
    consumer     text        NOT NULL,
    event_id     uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- +goose Down
DROP TABLE IF EXISTS inbox;
