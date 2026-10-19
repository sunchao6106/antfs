-- s3-bucket-policy:桶策略 JSON 文档列(数据面跨租户授权;put 时已全验,存坏读侧 fail-closed)。
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS policy TEXT NULL;
