# Tasks — store-composed-backfill

## A. 格式单点

- [x] `crates/common/descriptor`(`antfs-descriptor`,零依赖):MAGIC/VERSION/MAX_CHUNKS/
      MAX_BYTES/HEADER/ENTRY 常量 + `ChunkRef{id,size,expiration_ms}`(id=[u8;8])+
      `encode/parse`(“尾随字节”严格)+ 自有错误枚举 + 单测
      (roundtrip/坏 magic/坏版本/计数上下界/截断/尾随)
- [x] workspace members 增行;frontend 依赖换用(server.rs 编解码改薄壳,
      DESCRIPTOR_* 常量指到 crate;stitch 既有用例零回归 = 字节等价守望)

## B. 引擎(blob)

- [x] `PersistentIndex::for_each_entry`(active + sealed 原样条目遍历)
- [x] `BlobStore::for_each_entry_winner`(find 比对当前赢家才回调 —— 与压实同判据)
- [x] `BlobStore::mark_composed(key) -> Result<MarkComposedOutcome>`
      (write_lock + check_key + 五态;置换纪律逐字对齐 apply_replicated:
      append → index.add → 旧段 live→dead → valid_stats 换值;字节/expiration 原样)
- [x] 单测族:五态 + 幂等二跑;stat 置位;**重开索引**仍置位;**rebuild 走 log**仍置位;
      置换后 delete 无复活;压实搬新弃旧 + dead 字节对账;seg_bytes/valid_stats 对账;
      expiration 原样;put 同 key 仍 AlreadyExists

## C. store 端点

- [x] `POST /admin/backfill-composed`:单飞闸(并发 409)→ spawn_blocking 全副本串行:
      winner 迭代 → 过滤(≤MAX_BYTES、非删、非过期、无位)→ get 字节 → 严格 parse →
      mark_composed;报告 JSON(replicas/scanned/candidates/migrated/already/deleted/
      invalid/expired/failed[≤16]);tracing per-replica 汇总
- [x] 测试:三素材混装(真描述符/ASTC 前缀普通/已删描述符)→ 报告 + 引擎态双断言;
      **回填前已删绝无复活**;二跑幂等;并发 409;admin 路由豁免(D2 口径)不动真

## D. frontend 旋钮

- [x] `FrontendConfig::composed_flag_only`(默认 false,注释含誓约)→ AppState 透传
- [x] download:无位 + 旋钮开 → 不做 MAGIC 判定,返回原始字节(路径与内存语义不变);
      有位置信路径逐字不动;HEAD:旋钮开时无位行免读数据体(逻辑 size=原始 size)
- [x] rest.rs:旋钮关(既有 ASTC 前缀 502 负例**不动** = 存量行为守望) +
      旋钮开新用例:①ASTC 前缀 → 200 原字节(修复实证)/ ②无位合法描述符 → 200 原始
      描述符字节 / ③有位 stitch 拼装照常 / ④HEAD 无位行不读数据体

## E. 文档与门禁

- [x] frontend.md:§8 配置行(默认/誓约/升级顺序)+ §4 读序段补翻转语义
- [x] store.md:admin 端点段(语义/报告/单飞/「无需停写,check-then-publish 原子」/
      错峰建议/移交清单)+ DEVLOG 步 76
- [x] 门禁:workspace 全绿 + clippy 0 + fmt 净 + `openspec validate` 过 + devcluster
      真面(D7 弧:裸描述符普通 PUT → GET 嗅探拼装 → 回填 → 报告 → GET 置信拼装 +
      metadata composed:true + ASTC 普通件不落位)

## 落地记录(已回填)

- descriptor 5/5;blob **103/103**(+4:五态/重开重建/压实/赢家迭代/幂等);store
  **149/149**(+ `tests/backfill_composed.rs` 2/2);frontend lib 11 + rest **19/19**(+2);
  workspace **78 套 771/0**;clippy 0;fmt 净;validate 过。
- devcluster 真面:裸描述符普通 PUT → 嗅探拼装 → 三 store 回填(migrated=3/invalid=3)
  → 置信拼装 + metadata composed=true → ASTC 普通件 502(旋钮未开)→ 二跑 migrated=0/
  already=3(幂等)。
- 追加裁决(用户问询触发,design D7;同日复议修正):ASTC 前缀**不随位去** ——原稿
  "旧版本二进制"前提被驳回(仓库无部署态旧二进制,如实收回);真实理由 = 旧**数据**后向
  parse 认 v1 + 双 parse/迁移成本 vs 4 B 收益(简单优先,不做);位已独占路由职
  (`composed_flag_only`),magic 只留格式校验职。格式 v2(纯位判)判别式可行而 收益不在,
  通道在 version。
- 文档-实现漂移顺修(问询扫描所得):store.md §12.13 残留①(admin 巡检周期化已由
  admin-scrub-schedule 落地)、frontend.md §11 待办遗留尾巴与被否决段"编解码已下沉
  antfs-descriptor"、design README 路线档 3 行"另立项"完结更正。
