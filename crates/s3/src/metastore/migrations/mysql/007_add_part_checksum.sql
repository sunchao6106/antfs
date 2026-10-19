ALTER TABLE multipart_parts ADD COLUMN part_checksum_algo VARCHAR(8) NULL;
ALTER TABLE multipart_parts ADD COLUMN part_checksum_b64 VARCHAR(64) NULL;
ALTER TABLE named_objects ADD COLUMN checksum_type VARCHAR(12) NULL;
ALTER TABLE named_objects MODIFY COLUMN checksum_b64 TEXT NULL;
