# Design — store-composed-backfill

## 现状事实链(全部源码核实,复述自 blob.rs/index/server.rs 实读)

| 机制 | 事实 | 对回填的含义 |
|---|---|---|
| id 占用 | `put_ttl_inner`:`index.find(key).is_some() → AlreadyExists`(墓碑后同) | 不能经公开写口重写同 id |
| 恢复重建 | `rebuild_index_tolerant` 顺序扫 log,逐条 `index.add`(upsert) | 同 key 后写者胜 appends 的既有语义 |
| 索引语义 | active 跳表 `insert` + `find` = active → sealed 新到旧 | newest-wins,置换发布天然成立 |
| 压实选择 | `for_each_record` + `find(key).offset == off` 才搬运 | 置换后旧记录自动判死、新记录带位搬运 |
| 置换纪律范本 | `apply_replicated_inner`(append → index.add → 旧段 live→dead → valid_stats 换值) | `mark_composed` 逐字对齐该纪律 |
| 并发写/删/压实 | 均持 `write_lock` | check-then-publish 原子,无复活窗口 |
| 复制 apply | 本地 PUT + 对端非墓碑 → `Skipped` | 各副本各自回填;拉到别家的带位记录不引发状态回退 |
| 甄别 | frontend 嗅探 = 前 4 字节 MAGIC + `decode_descriptor` 严格 parse | 回填复用**同一 parse**(共享 crate)即口径一致 |

## Decisions

### D1:回填 = 引擎内置置换写(`BlobStore::mark_composed`),不是重写/新写

追加同 key 新记录(flags |= `FLAG_COMPOSED`、字节与 expiration 原样),索引 newest-wins
替换。**不引入新不变量**:三态一致性(直读 stat / 重开索引自盘 / 重建扫 log)沿用既有
语义;崩溃任何一格:最坏 = 位没落成 → 幂等重跑再迁,无中间损坏态(append 未发布 = 老条
目仍可用;已发布 = 新条目完整)。墓碑对置换后的新记录照常生效(delete 就在同锁后面排
队)。outcome 枚举:`Migrated / Already / NotFound / Deleted`(调用方如实计数)。

### D2:赢家迭代 = 索引层 `for_each` + `find` 比对(不物化全量键集)

`BlobStore::for_each_entry_winner(f: FnMut(StoreKey, IndexValue))`:遍历 active +
各 sealed 段条目,仅对 "`find(key).offset == entry.offset`"(当前赢家,与压实同一判据)
回调。内存 O(1),单键 O(log n);admin 扫描的一次性近似快照(与压实并发时,锁定写锁的
压实与扫描串行化,不产生撕裂)。

### D3:甄别口径 = "parse 严格通过"(行为奇偶校验),不查 chunk 引用

- 真回填面:存量真描述符 → 落位,行为不变(嗅探拼装 → 置信拼装);
- 存量伪件(parse 合法):**不查 chunk 存在性**。理由:chunk 可在任意分区,存在性校验需
  集群读,把一次性回填变成分布式扫描;且回填它**不改变现状行为**(今日嗅探同样拼装)。
  残余风险记档:翻转后此类对象继续拼装(同今),② 的存量面不因此=batch 修复,但**新写
  无②**(新描述符必带位,普通上传永不起位);
- ASTC 前缀非 descriptor(parse 失败):**不落位** → 翻转后 GET 返回原始字节
  = 失真①修复(502 → 200 原字节,行为变化即修 bug)。
- 过期候选跳过(留给压实回收):行为同今(嗅探对已过期对象本来也读不回)。

### D4:驱动形态 = store admin 同步端点(一次一报)

`POST /admin/backfill-composed`:单飞闸(并发 → 409 busy);`spawn_blocking` 下全副本
串行扫 + 置换;响应即报告:`{replicas, scanned, candidates, migrated, already,
deleted, invalid, expired, failed[]}`(failed 截前 16 条带 pid/key/error)。
不做异步任务面/进度状态:幂等重跑即续(对齐 sse-rotation "可续跑 = 构造所得" 哲学)。
**无需停写**:check-then-publish 原子性由 write_lock 保证(与运维手册建议错峰并行不悖)。
零新配置、零新监控(§7 免表义务);tracing 记 per-replica 汇总与 error 明细。

### D5:格式单点 = `antfs-descriptor`(common 家族新 crate,零依赖)

MAGIC/VERSION/MAX_CHUNKS/DESCRIPTOR_MAX_BYTES/header-entry 尺寸 + `encode/parse`
(纯字节,id 载体 `[u8; 8]`,不依赖 StoreKey/引擎——frontend 边界转 StoreKey 一次)。
frontend 编解码改薄壳(错误映射保留,IoC 无涉);stitch/auto-chunk 既有用例零回归 =
**字节级等价的守望**。Store 变 `{antfs-blob, antfs-descriptor}` 依赖即可甄别。

### D6:frontend `composed_flag_only`(默认 false)——誓约开关

- 置位行为:无位行**不嗅探**——GET 走原有体型路径但跳过 MAGIC 判定,返回原始字节;
  HEAD 无位行免读数据体(逻辑大小 = 原始 size);有位行置信路径逐字不动(超限 502 口径不变)。
- **誓约**(文档大字、config 注释复述):翻开前必须对**每个 store** 跑过回填并核过
  报告(migrated+already 与预期相符、failed 清零、invalid 全部可解释),否则存量真描述符
  会以"普通对象"身份返回**描述符本体字节**(静默错内容)。默认关 ⇒ 现网逐字节等价。
- 升级顺序:① 全集群升到本版(读路径兼容无位硬要求不变)→ ② 错峰逐 store 回填
  → ③ 核各报告 → ④ 开 `composed_flag_only` → ⑤ (可选观察期后)嗅探路径可删的
  决策另立。

### D7(追加,2026-10-19 问询裁决):MAGIC 前缀**不随位去**(为什么 ASTC 头不能删)

"置信翻转后路由靠位,MAGIC 是不是可以干掉了?"——**不能**,四条硬理由:

1. **持久格式契约(§3.4)**:`ASTC` 前缀自有史以来随**每一条**描述符落盘;任何旧版本二进制的
   `parse` 第一件事就是比 magic(先于 version 字节)——新写描述符若不带 magic,旧读者一律
   `BadMagic` 502 ⇒ 前后兼容破裂。append-only 世界里"删字段"没有旧读者容忍路径
   (不像"加字段走默认"那样可滑)。
2. **位与字节互证(纵深防御)**:置位行进入拼装路径后仍过同一 parse——位说"是描述符"、
   字节必须自证"格式合法"。若某天位被误置在普通对象上(管理事故/未来 bug),这半道闸门
   把它变成响亮 502 而非静默组装垃圾。拆掉 magic = 拆穿甲。
3. **回填甄别依赖**:存量扫的漏斗第一格就是"MAGIC 形似"——它把普通小对象挡在甄别面外
   (invalid 计数才保持"撞车专列"的语义)。
4. **收益不对称**:省的是每条描述符 4 字节(描述符量级几百字节,≤4%);代价是破坏
   自旋兼容 + 拆防御 + 丢甄别语义。**位让 magic 的"路由"职责退役(翻转后),但它的
   "格式自述"职责永久保留** —— 这正是 D5"机制先行、翻转"设计的另一半。

### D8:devcluster 真面弧

裸描述符字节经 frontend 普通 PUT 落库(= 忠实模拟存量无位行)→ GET 靠嗅探拼装(今行为)
→ `POST /admin/backfill-composed` → 报告 migrated=1 → GET 仍拼装(置信路径),
`metadata` 端点见 `composed:true`;ASTC 前缀普通 blob 同拍不被落位(对勘 ① 的修复前提)。

## 测试策略

- **descriptor crate**:roundtrip/坏 magic/坏版本/计数越界/截断/尾随字节(格式契约钉死)。
- **引擎**:mark_composed 五态(Migrated/Already/NotFound/Deleted + 幂等二跑);
  stat 置位;**重开索引仍置位**;**open-rebuild-index 走 log 仍置位**(崩溃弧);
  置换后 delete 照常(无复活);压实搬新弃旧(dead 字节对账);seg_bytes/valid_stats 对账;
  expiration 原样;put 同 key 仍 AlreadyExists(既有不变量未破)。
- **store 端点**:混装三素材(真描述符/ASTC 前缀普通/已删描述符)→ 报告计数逐项断言
  + 引擎态逐项断言;**回填前已删的键绝不被复活**(Deleted 计数 + stat 仍墓碑);
  二跑幂等(migrated 0/already n);并发第二发 409。
- **frontend**:旋钮关:ASTC 前缀 502 钉住(既有负例不动=存量行为守望)。旋钮开:
  ① ASTC 前缀普通 blob → **200 原字节**(修复实证);② 无位合法描述符 → 200 原始
  描述符字节(翻转语义实录);③ 有位 stitch 拼装照常(置信路径不受旋钮影响);
  ④ HEAD 无位行不读数据体(逻辑 size = 原始 size,stitched 头缺席)。
- devcluster:D7 弧(curl 全链)。

## Risks / Trade-offs

- **翻转误用**(未回填先开旋钮)= 静默错内容:以"誓约文档 + 默认关 + 报告验收清单"
  三重对冲;不设自动检测(回填完成度无中心真值,强行造 = 引入中心元数据,违 D3 架构)。
- **置换冗余字节**:回填瞬时把每条候选的记录字节翻一倍(压实自然回收旧记录)。一次性、
  上限 = 候选字节总量(真描述符本就 ≤1 MiB/条);错峰运维即可。
- **存量伪描述符永久带位**(D3 口径):行为同今不劣化,残余记档;_engine 无补救杠杆的
  决策与 blob-composed-flag 一致("可见性 ≠ 操作权"同族,不引入中心元数据面)。
- **大 store 同步端点时长**:百万键全桶扫描分钟级;curl/proxy 超时属运维调参面
  (–-max-time);不引入异步任务面(简单优先,YAGNI;真需要时另立项)。
