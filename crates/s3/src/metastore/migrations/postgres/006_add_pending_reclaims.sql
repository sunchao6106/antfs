CREATE TABLE IF NOT EXISTS pending_reclaims (
    blob_id      BYTEA         NOT NULL,
    queued_at    BIGINT        NOT NULL,
    PRIMARY KEY (blob_id)
);
