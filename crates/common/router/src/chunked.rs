//! router 层恒切下沉(`upload-zero-spool` design.md §12,2026-10-21 落地;
//! 读侧裁决⑨/⑩:拼装感知屏蔽在本层,上层只见**逻辑对象**):
//!
//! - **写**:`put` / `put_auto` 族收到**超过片宽的裸 Bytes** 时内部切片 —— 片 = 普通多数派
//!   blob(发号轮转散向不同可写分区,热点分摊),尾笔写**拼装描述符**(组合位落库)。
//!   对齐 Ambry `NonBlockingRouter`:恒切、片宽可 0 = 关。
//! - **读**:拼装对象自动拼回**真数据**([`Router::get`] 整读 / [`Router::get_object_stream`]
//!   流式 / [`Router::stat_object`] 元数据);普通对象原字节。
//!   [`Router::get_blob`] 是唯一的裸字节口(供 S3 等"已知普通 blob"的单请求热路径)。
//!
//! 持久性口径(对齐网关段 stream_upload,CLAUDE.md §3.3):
//! - 片与描述符各是一次**多数派写**(507 换分区重试由内环自带);
//! - 任一片或描述符失败 → 已入库片**尽力墓碑**(删失败记日志与指标,不影响原错误回报;
//!   墓碑少数派落位时由拉复制"墓碑恒胜"补齐)后透传原错误;
//! - 崩溃窗孤儿片与本一致残余(无人认领;启用 frontend 收账簿的仓由清扫任务兜底,
//!   SDK 直连路径无对账面,与 Ambry 同位置同残余);
//! - 重试安全:调用方重试拿到全新 blobId,互不相扰。

use std::io;

use antfs_blob::StoreKey;
use antfs_descriptor::ChunkRef;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};

use crate::{Error, Router};

/// 恒切判定(纯函数便于边界单测):`Ok(Some(片数))` = 走恒切管线;`Ok(None)` = 单笔
/// 直写(片宽 0 关 / body 未过片宽);`Err(TooLarge)` = 片数超描述符容量的前置拒绝
/// (一字节不写,无需回收)。
pub(crate) fn chunk_plan(width: u64, len: usize) -> Result<Option<usize>, Error> {
    if width == 0 || len as u64 <= width {
        return Ok(None);
    }
    let chunks = (len as u64).div_ceil(width);
    let capacity = antfs_descriptor::MAX_CHUNKS as u64 * width;
    if len as u64 > capacity {
        return Err(Error::TooLarge {
            len: len as u64,
            max: capacity,
        });
    }
    Ok(Some(chunks as usize))
}

/// 逐片写入的结果:成功 = 全部片;失败 = 已入库存的部分片 + 原错误(供调用方墓碑回收)。
enum Chunks {
    Ok(Vec<ChunkRef>),
    Failed(Vec<ChunkRef>, Error),
}

/// 逻辑对象元数据([`Router::stat_object`]).
///
/// 拼装对象:`size` = **各片之和**(逻辑大小)、`chunks` = 片数;已删/已过期/描述符不可读
/// 时退回描述符自身 stat(尽力语义,与旧网关 HEAD 口径一致)。普通对象:`chunks = None`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStat {
    pub size: u64,
    pub expiration_ms: i64,
    pub deleted: bool,
    pub expired: bool,
    /// 拼装对象的片数;`None` = 普通对象。
    pub chunks: Option<u32>,
}

impl ObjectStat {
    fn plain(meta: crate::BlobStat) -> Self {
        ObjectStat {
            size: meta.size,
            expiration_ms: meta.expiration_ms,
            deleted: meta.deleted,
            expired: meta.expired,
            chunks: None,
        }
    }
}

impl Router {
    // ── 写:恒切管线 ─────────────────────────────────────────────────────────────

    /// 入口闸:带 `reject` 计数的恒切判定。`Ok(true)` = 走恒切管线。
    pub(crate) fn chunked_gate(&self, len: usize) -> Result<bool, Error> {
        match chunk_plan(self.cfg.put_bytes_chunk_bytes, len) {
            Ok(plan) => Ok(plan.is_some()),
            Err(e) => {
                self.metrics
                    .put_chunked_total
                    .with_label_values(&["reject"])
                    .inc();
                Err(e)
            }
        }
    }

    /// 逐片普通多数派写。每片 ≤ 片宽 → 调单笔直写内环 `put_auto_keyed`
    /// (发号 + 多数派 + 507 换分区重试):既免二次过闸,也打断 async 递归。
    async fn write_chunk_refs(&self, data: &Bytes, expiration_ms: i64) -> Chunks {
        // Validate 已保证片宽 ≤ 1 GiB;64 位 usize 直转无损,32 位平台上 1 GiB 亦在其内。
        let width = usize::try_from(self.cfg.put_bytes_chunk_bytes)
            .expect("put_bytes_chunk_bytes validated to <= 1 GiB");
        let chunks = match chunk_plan(width as u64, data.len()) {
            Ok(Some(n)) => n,
            // 入口闸已过滤:此处只剩确证不变量违背。
            _ => unreachable!("chunked pipeline entered without a plan"),
        };
        let mut refs: Vec<ChunkRef> = Vec::with_capacity(chunks);
        for idx in 0..chunks {
            let start = idx * width;
            let end = if idx + 1 == chunks {
                data.len()
            } else {
                start + width
            };
            match self
                .put_auto_keyed(data.slice(start..end), expiration_ms, false)
                .await
            {
                Ok((key, _report)) => {
                    self.metrics.put_chunked_chunks_total.inc();
                    refs.push(ChunkRef {
                        id: *key.as_bytes(),
                        size: (end - start) as u64,
                        expiration_ms,
                    });
                }
                Err(e) => return Chunks::Failed(refs, e),
            }
        }
        Chunks::Ok(refs)
    }

    /// 共用失败出口:已入库片尽力墓碑 + `fail` 计数,原错误透传。
    async fn chunked_failed(&self, acked: &[ChunkRef], e: Error) -> Error {
        self.tombstone_chunk_refs(acked).await;
        self.metrics
            .put_chunked_total
            .with_label_values(&["fail"])
            .inc();
        e
    }

    /// 孤儿片回收:逐片写墓碑,尽力而为 —— 删失败只记指标与日志,不影响向调用方回报的
    /// 原错误(孤儿是垃圾、不是数据损坏)。
    async fn tombstone_chunk_refs(&self, refs: &[ChunkRef]) {
        for r in refs {
            self.metrics.put_chunked_orphan_deletes_total.inc();
            if let Some(key) = StoreKey::from_slice(&r.id) {
                if let Err(e) = self.delete(&key).await {
                    tracing::warn!(
                        blob_id = %hex::encode(r.id),
                        error = %e,
                        "chunked-put orphan cleanup failed"
                    );
                }
            }
        }
    }

    /// 恒切管线(既定 key 形):`put_with_ttl` 的片宽越线分支。描述符在**调用方 key**上
    /// 以组合位落库,同片 TTL;调满多数派才返回。
    pub(crate) async fn put_with_ttl_chunked(
        &self,
        key: StoreKey,
        data: Bytes,
        expiration_ms: i64,
    ) -> crate::Result<crate::PutReport> {
        let refs = match self.write_chunk_refs(&data, expiration_ms).await {
            Chunks::Failed(acked, e) => {
                return Err(self.chunked_failed(&acked, e).await);
            }
            Chunks::Ok(refs) => refs,
        };
        // 片数已由入口闸 ≤ MAX_CHUNKS、session 为空 ⇒ encode 的结构性错误不可达。
        let descriptor = antfs_descriptor::encode(&[], &refs)
            .expect("chunks <= MAX_CHUNKS and empty session make encode infallible");
        match self
            .put_composed_with_ttl(key, Bytes::from(descriptor), expiration_ms)
            .await
        {
            Ok(report) => {
                self.metrics
                    .put_chunked_total
                    .with_label_values(&["ok"])
                    .inc();
                Ok(report)
            }
            Err(e) => Err(self.chunked_failed(&refs, e).await),
        }
    }

    /// 恒切管线(自动发号形):`put_auto_with_ttl` 的片宽越线分支;返回描述符 blobId。
    pub(crate) async fn put_auto_with_ttl_chunked(
        &self,
        data: Bytes,
        expiration_ms: i64,
    ) -> crate::Result<(StoreKey, crate::PutReport)> {
        let refs = match self.write_chunk_refs(&data, expiration_ms).await {
            Chunks::Failed(acked, e) => {
                return Err(self.chunked_failed(&acked, e).await);
            }
            Chunks::Ok(refs) => refs,
        };
        let descriptor = antfs_descriptor::encode(&[], &refs)
            .expect("chunks <= MAX_CHUNKS and empty session make encode infallible");
        match self
            .put_auto_composed_with_ttl(Bytes::from(descriptor), expiration_ms)
            .await
        {
            Ok((key, report)) => {
                self.metrics
                    .put_chunked_total
                    .with_label_values(&["ok"])
                    .inc();
                Ok((key, report))
            }
            Err(e) => Err(self.chunked_failed(&refs, e).await),
        }
    }

    // ── 读:拼装感知(裁决⑨/⑩,上层无感)────────────────────────────────────

    /// 逻辑对象元数据(HEAD 口径);语义见 [`ObjectStat`]。
    pub async fn stat_object(&self, key: &StoreKey) -> crate::Result<ObjectStat> {
        let meta = self.stat(key).await?;
        // 已删/已过期或"描述符"超格式上限 → 逻辑大小不可知,退回裸 stat 口径。
        if !meta.composed || meta.deleted || meta.expired || meta.size > antfs_descriptor::MAX_BYTES
        {
            return Ok(ObjectStat::plain(meta));
        }
        // 描述符 ≤ 1 MiB 有界读,读出即知各片尺寸;读/解析失败退回裸口径(尽力,与旧
        // 网关 HEAD 的吞错语义一致 —— 真损坏由 GET 读路径响亮报)。
        if let Ok(body) = self.get_blob(key).await {
            if let Ok(refs) = antfs_descriptor::parse(&body) {
                let chunks =
                    u32::try_from(refs.len()).expect("descriptor parse enforces len <= MAX_CHUNKS");
                return Ok(ObjectStat {
                    size: refs.iter().map(|r| r.size).sum(),
                    expiration_ms: meta.expiration_ms,
                    deleted: meta.deleted,
                    expired: meta.expired,
                    chunks: Some(chunks),
                });
            }
        }
        Ok(ObjectStat::plain(meta))
    }

    /// 拼装描述符解析(`composed` 位已由调用方判定,此处只管结构)。
    fn parse_descriptor(&self, body: &[u8]) -> crate::Result<Vec<ChunkRef>> {
        if body.len() as u64 > antfs_descriptor::MAX_BYTES {
            return Err(Error::BadComposed("descriptor exceeds the size bound"));
        }
        antfs_descriptor::parse(body).map_err(|_| Error::BadComposed("descriptor parse failed"))
    }

    /// 逻辑对象整读([`Router::get`] 的实现体):`stat ‖ 裸读` 并发(额外一个 stat 请求,
    /// 延迟被并发藏住);`composed` → 到手的裸读重用为描述符,逐片取回校验拼装。
    pub(crate) async fn get_object(&self, key: &StoreKey) -> crate::Result<Bytes> {
        let (meta, body) = tokio::join!(self.stat(key), self.get_blob(key));
        let meta = meta?;
        if meta.deleted || meta.expired {
            // 与数据面 GET 的拒绝语义对齐:已删/已过期的对象不再是"取内容"的语义。
            return Err(Error::NotFound {
                pid: key.partition_id(),
            });
        }
        let body = body?;
        if !meta.composed {
            return Ok(body);
        }
        let refs = self.parse_descriptor(&body)?;
        self.metrics.get_chunked_total.inc();
        self.assemble_refs(&refs).await
    }

    /// 逐片取回并核对描述符记录的尺寸(不一致 = 上游损坏:响亮报,不静默忽略)。
    async fn assemble_refs(&self, refs: &[ChunkRef]) -> crate::Result<Bytes> {
        let mut out: Vec<u8> = Vec::new();
        for r in refs {
            let chunk_key = StoreKey::from_slice(&r.id)
                .ok_or(Error::BadComposed("chunk id is not a valid blob id"))?;
            let chunk = self.get_blob(&chunk_key).await?;
            if chunk.len() as u64 != r.size {
                return Err(Error::BadComposed("chunk size mismatch"));
            }
            out.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(out))
    }

    /// **逻辑流式读**(网关下载/大对象的标准口;内存 = 单片,不落盘不整读):
    /// - 普通对象:R=1 开流直通;压缩帧走流式解码管道(有界通道 + 解码线程);
    /// - 拼装对象:描述符解析 + 首片预热(整对象不可读 → 明确失败,不发到一半才断,
    ///   对齐旧网关预检语义),逐片流式吐出,片尺寸核对入流内;
    ///
    /// 返回 `(逻辑长度, 流)` —— 逻辑长度即 Content-Length(调用方据此装配响应头)。
    pub async fn get_object_stream(
        &self,
        key: &StoreKey,
    ) -> crate::Result<(u64, BoxStream<'static, io::Result<Bytes>>)> {
        let meta = self.stat(key).await?;
        if meta.deleted || meta.expired {
            return Err(Error::NotFound {
                pid: key.partition_id(),
            });
        }
        if !meta.composed {
            return self.get_object_stream_plain(key, meta.size).await;
        }
        let body = self.get_blob(key).await?;
        let refs = self.parse_descriptor(&body)?;
        let first = refs
            .first()
            .ok_or(Error::BadComposed("descriptor has no chunk"))?;
        let first_key = StoreKey::from_slice(&first.id)
            .ok_or(Error::BadComposed("chunk id is not a valid blob id"))?;
        self.stat(&first_key).await?;
        let len: u64 = refs.iter().map(|r| r.size).sum();
        self.metrics.get_chunked_total.inc();
        let router = self.clone();
        let stream = futures::stream::iter(refs).then(move |r| {
            let router = router.clone();
            async move {
                let id = hex::encode(r.id);
                let Some(chunk_key) = StoreKey::from_slice(&r.id) else {
                    return Err(io::Error::other("bad chunk id in descriptor"));
                };
                match router.get_blob(&chunk_key).await {
                    Ok(b) if b.len() as u64 == r.size => Ok(b),
                    Ok(b) => {
                        tracing::warn!(
                            chunk_id = %id,
                            expected = r.size,
                            actual = b.len(),
                            "stitched chunk size mismatch"
                        );
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("chunk {id} size mismatch"),
                        ))
                    }
                    Err(e) => Err(io::Error::other(format!("chunk {id} unavailable: {e}"))),
                }
            }
        });
        Ok((len, stream.boxed()))
    }

    /// 普通对象的逻辑流式:R=1 开流直通(网络字节原样进管道;内存 = 传输块级)。
    async fn get_object_stream_plain(
        &self,
        key: &StoreKey,
        stored_len: u64,
    ) -> crate::Result<(u64, BoxStream<'static, io::Result<Bytes>>)> {
        let (resp, addr) = self.open_replica_stream(key, None).await?;
        let addr = std::sync::Arc::new(addr);
        let out = resp
            .bytes_stream()
            .map(move |r| r.map_err(|e| io::Error::other(format!("http to replica {addr}: {e}"))));
        Ok((stored_len, out.boxed()))
    }

    // ── 读:部分读(blob-range-read,窗口读;裁决⑭:恒 stream 式)──────────────

    /// **逻辑对象窗口流式读**(`Range` 语义的 router 承载):
    /// - 普通对象:数据面 `?offset=&length=` 窗口流(R=1;内存 = 传输块级);
    /// - 拼装对象:窗口映射到**覆盖片**—— 只取覆盖窗口的片、边界片裁边
    ///   (Ambry `getStoreKeysInByteRange` 同型;10⁴ 倍浪费的消解点);
    ///   片完整性核对入流内(与整读同口径),尺寸不符 = 响亮错不静默。
    ///
    /// 契约:`start + len ≤ 逻辑总长` 由调用方保证(越界 416 是网关层语义,
    /// 前置 `stat_object` 即知逻辑总长);已删/已过期 → `NotFound`;片数指标同整读。
    pub async fn get_object_range_stream(
        &self,
        key: &StoreKey,
        start: u64,
        len: u64,
    ) -> crate::Result<BoxStream<'static, io::Result<Bytes>>> {
        let meta = self.stat(key).await?;
        if meta.deleted || meta.expired {
            return Err(Error::NotFound {
                pid: key.partition_id(),
            });
        }
        if !meta.composed {
            let (resp, addr) = self.open_replica_stream(key, Some((start, len))).await?;
            let addr = std::sync::Arc::new(addr);
            let out = resp.bytes_stream().map(move |r| {
                r.map_err(|e| io::Error::other(format!("http to replica {addr}: {e}")))
            });
            return Ok(out.boxed());
        }
        let body = self.get_blob(key).await?;
        let refs = self.parse_descriptor(&body)?;
        // 窗口 → 覆盖片(片字段:skip = 片内跳过,take = 采纳字节)。
        let mut covers: Vec<(ChunkRef, u64, u64)> = Vec::new();
        let mut cursor = 0u64;
        let want_end = start + len;
        for r in &refs {
            let chunk_start = cursor;
            let chunk_end = cursor + r.size;
            cursor = chunk_end;
            if chunk_end <= start {
                continue;
            }
            if chunk_start >= want_end {
                break;
            }
            let skip = start.max(chunk_start) - chunk_start;
            let take = want_end.min(chunk_end) - (chunk_start + skip);
            covers.push((*r, skip, take));
        }
        // 尾片断流防御:描述符总和 < 窗口右端 = 上游损坏(响亮,不静默截尾)。
        let covered: u64 = covers.iter().map(|(_, _, t)| *t).sum();
        if covered != len {
            return Err(Error::BadComposed(
                "descriptor total is smaller than the window",
            ));
        }
        if let Some((r, _, _)) = covers.first() {
            if let Some(first_key) = StoreKey::from_slice(&r.id) {
                self.stat(&first_key).await?;
            }
        }
        self.metrics.get_chunked_total.inc();
        let router = self.clone();
        let stream = futures::stream::iter(covers).then(move |(r, skip, take)| {
            let router = router.clone();
            async move {
                let id = hex::encode(r.id);
                let Some(chunk_key) = StoreKey::from_slice(&r.id) else {
                    return Err(io::Error::other("bad chunk id in descriptor"));
                };
                match router.get_blob(&chunk_key).await {
                    Ok(b) if b.len() as u64 == r.size => {
                        Ok(b.slice(skip as usize..(skip + take) as usize))
                    }
                    Ok(b) => {
                        tracing::warn!(
                            chunk_id = %id,
                            expected = r.size,
                            actual = b.len(),
                            "stitched chunk size mismatch (range)"
                        );
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("chunk {id} size mismatch"),
                        ))
                    }
                    Err(e) => Err(io::Error::other(format!("chunk {id} unavailable: {e}"))),
                }
            }
        });
        Ok(stream.boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_plan_boundaries() {
        const W: u64 = 4096;
        // 关(0)= 永不切;未过片宽 = 单笔;恰等片宽不切(> 才切)。
        assert!(matches!(chunk_plan(0, usize::MAX), Ok(None)));
        assert!(matches!(chunk_plan(W, 0), Ok(None)));
        assert!(matches!(chunk_plan(W, W as usize), Ok(None)));
        // 切片数 = ceil;恰过界 1 字节 → 2 片。
        assert!(matches!(chunk_plan(W, W as usize + 1), Ok(Some(2))));
        assert!(matches!(chunk_plan(W, 2 * W as usize), Ok(Some(2))));
        assert!(matches!(chunk_plan(W, 2 * W as usize - 1), Ok(Some(2))));
        // 容量边界 = MAX_CHUNKS × 宽:恰在容量内切满片数;越一字节 → TooLarge 前置拒绝。
        let cap = antfs_descriptor::MAX_CHUNKS as u64 * W;
        assert!(matches!(
            chunk_plan(W, cap as usize),
            Ok(Some(antfs_descriptor::MAX_CHUNKS))
        ));
        match chunk_plan(W, cap as usize + 1) {
            Err(Error::TooLarge { len, max }) => {
                assert_eq!(len, cap + 1);
                assert_eq!(max, cap);
            }
            other => panic!("want TooLarge, got {other:?}"),
        }
    }
}
