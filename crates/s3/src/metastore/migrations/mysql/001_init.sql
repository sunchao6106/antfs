CREATE TABLE IF NOT EXISTS buckets (
    name        VARCHAR(63)   NOT NULL PRIMARY KEY,
    owner       VARCHAR(128)  NOT NULL,
    created_at  BIGINT        NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS named_objects (
    bucket       VARCHAR(63)   NOT NULL,
    `key`        VARCHAR(1024) NOT NULL,
    version      BIGINT        NOT NULL,
    blob_id      VARBINARY(8)  NOT NULL,
    state        SMALLINT      NOT NULL DEFAULT 1,
    size         BIGINT        NOT NULL,
    etag         VARCHAR(128),
    content_type VARCHAR(256),
    deleted_at   BIGINT,
    created_at   BIGINT        NOT NULL,
    user_meta    TEXT,
    parts_manifest TEXT,
    PRIMARY KEY (bucket, `key`(255), version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE INDEX idx_no_list ON named_objects (bucket, `key`(255), state, deleted_at);

CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id    VARCHAR(64)   NOT NULL PRIMARY KEY,
    bucket       VARCHAR(63)   NOT NULL,
    `key`        VARCHAR(1024) NOT NULL,
    created_at   BIGINT        NOT NULL,
    content_type VARCHAR(256),
    user_meta    TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE INDEX idx_mu_bucket ON multipart_uploads (bucket, `key`(255));

CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id    VARCHAR(64)   NOT NULL,
    part_number  INT           NOT NULL,
    blob_id      VARBINARY(8)  NOT NULL,
    size         BIGINT        NOT NULL,
    etag         VARCHAR(128)  NOT NULL,
    PRIMARY KEY (upload_id, part_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS s3_credentials (
    access_key   VARCHAR(128)  NOT NULL PRIMARY KEY,
    secret_key   VARCHAR(256)  NOT NULL,
    owner        VARCHAR(128)  NOT NULL,
    created_at   BIGINT        NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
