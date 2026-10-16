CREATE TABLE IF NOT EXISTS pending_reclaims (
    blob_id      VARBINARY(8)  NOT NULL,
    queued_at    BIGINT        NOT NULL,
    PRIMARY KEY (blob_id)
);
