-- s3-multipart-complete-checksum:per-part 校验持久化 + 对象 checksum kind。
-- part 行值 = UploadPart 已验证的最强声明(单片完整值,四族最大 sha256 b64 = 44 chars);
-- named_objects.checksum_b64 扩 TEXT(composite 上界 10000 片 × 4B ≈ 53KB);
-- checksum_type NULL = FULL_OBJECT(历史行/单写行),COMPOSITE = complete 级装配值。
ALTER TABLE multipart_parts ADD COLUMN IF NOT EXISTS part_checksum_algo VARCHAR(8) NULL;
ALTER TABLE multipart_parts ADD COLUMN IF NOT EXISTS part_checksum_b64 VARCHAR(64) NULL;
ALTER TABLE named_objects ADD COLUMN IF NOT EXISTS checksum_type VARCHAR(12) NULL;
ALTER TABLE named_objects ALTER COLUMN checksum_b64 TYPE TEXT;
