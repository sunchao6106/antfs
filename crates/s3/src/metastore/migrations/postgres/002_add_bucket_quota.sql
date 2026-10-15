-- 桶级存储配额(s3-bucket-quota):quota 0 = 不限;used_bytes = 该桶 state=1 行 size 之和,
-- 由 put/delete/complete 的记账事务维护(见 metastore/pg.rs),GC purge 不触碰。
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS quota_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS used_bytes BIGINT NOT NULL DEFAULT 0;

-- 存量数据回填(已建库升级路径;新库为空操作)。
UPDATE buckets SET used_bytes = COALESCE((
    SELECT SUM(size) FROM named_objects
    WHERE named_objects.bucket = buckets.name AND state = 1 AND deleted_at IS NULL
), 0);
