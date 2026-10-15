# s3-uploadpart-reclaim-retraction — 设计说明(纯对账,零代码)

## 为什么没有代码改动

实读两方言 `put_part` 与 handler(pg.rs 304-337、mysql.rs 337-363、handlers/multipart.rs `upload_part`):

```text
SELECT blob_id FROM multipart_parts WHERE upload_id=$1 AND part_number=$2   -- 先取被替换行(pg.rs:314)
INSERT ... ON CONFLICT (upload_id, part_number) DO UPDATE ...               -- 再 UPSERT(pg.rs:322)
Ok(old.map(...))                                                            -- 返回旧 blob(pg.rs:336)
```

handler:`if let Some(old) = old_blob { st.router.delete(&old).await }` —— 同请求墓碑,幂等。MySQL 侧同构(前置 SELECT + `ON DUPLICATE KEY UPDATE`,返回同)。

考古:`git show de5f123:crates/s3/src/handlers/multipart.rs` 确认 de5f123(S3 网关初版)已含「返回旧 blob + 即刻删除」模式;`8924dc7`(s3-gc)未触及该路径(git show 复核,其增量属 complete 遮蔽删除接线)。

死点模型一致性:重传路径与 put_object/complete 遮蔽路径同属「blob 死点 = 语义动作时刻」(见 `2026-10-15-s3-gc` 的 MODIFIED 回写)。原文「becomes reclaimable」是死点模型确立前的过渡措辞,与实现不符,MODIFIED 回写。

## 并发窗口(真留观,非本 change 范围)

两个并发 PUT 同 partNumber:交错 SELECT(同读旧值)→ 先后 UPSERT(行终指后者新 blob)→ 两 handler 各删同一旧 blob(幂等,无害);**先提交者的新 blob 失去引用 → 孤儿**。后果是空间泄漏,不影响读语义(读走行,内容为后提交者)。触发门槛:同 uploadId 同 partNumber 并发重传(客户端行为罕见)。修法方向(PG,单语句原子化):

```sql
WITH old AS (
    SELECT blob_id FROM multipart_parts WHERE upload_id = $1 AND part_number = $2 FOR UPDATE
), up AS (
    INSERT INTO multipart_parts (upload_id, part_number, blob_id, size, etag)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (upload_id, part_number)
    DO UPDATE SET blob_id = EXCLUDED.blob_id, size = EXCLUDED.size, etag = EXCLUDED.etag
)
SELECT (SELECT blob_id FROM old) AS old_blob_id;
```

MySQL 以事务内 `SELECT ... FOR UPDATE` 两步等价原子化。留独立 change。
