-- 桶级存储配额(s3-bucket-quota,与 postgres/002 同构):quota 0 = 不限;
-- used_bytes = 该桶 state=1 行 size 之和,由 put/delete/complete 的记账事务维护,GC purge 不触碰。
ALTER TABLE buckets ADD COLUMN quota_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE buckets ADD COLUMN used_bytes BIGINT NOT NULL DEFAULT 0;

-- 存量数据回填(已建库升级路径;新库为空操作)。
UPDATE buckets b SET b.used_bytes = COALESCE((
    SELECT SUM(o.size) FROM named_objects o
    WHERE o.bucket = b.name AND o.state = 1 AND o.deleted_at IS NULL
), 0);
