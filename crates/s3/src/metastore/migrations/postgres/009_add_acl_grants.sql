-- s3-acl-xml:ACL XML 授予面(vanilla READ grants;AllUsers 走既有 canned 列)。
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS acl_grants TEXT NULL;
ALTER TABLE named_objects ADD COLUMN IF NOT EXISTS acl_grants TEXT NULL;
