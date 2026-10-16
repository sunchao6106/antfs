# blob-composed-flag 任务

## 1. 引擎层(`antfs-blob`)

- [x] 1.1 /**value.rs FLAG_COMPOSED+is_composed+模块头 flags 行;composed_flag_roundtrip 测绿** `index/value.rs`:`FLAG_COMPOSED = 1<<4` + `is_composed()`;模块头注释 flags
  语义行更新;单测(置位 roundtrip/位 4 保留/旧值 flags=0 → not composed)
- [x] 1.2 /**put_ttl_inner/put_from_reader_inner/publish_put flags 参数化;put_composed_ttl/put_composed_from_reader 落位;旧签名零破坏** `blob.rs` 写口 flags 化:`put_ttl_inner` / `put_from_reader_inner` /
  `publish_put` 增 `flags: u8` 私参;新公开 `put_composed_ttl` /
  `put_composed_from_reader`(文档注释对齐既有款);旧签名与 flags=0 行为逐字不动
- [x] 1.3 /**BlobMeta.composed+stat 填充;composed_flag_survives_reopen_and_rebuild 三态保位测绿(直读/重开/重建)** `BlobMeta.composed` + `stat()` 填充;**传递核对**(零代码即继承,取用例
  锁定):恢复重建(`open_rebuild_index`)后 stat 仍 composed;`v.flags ==
  decoded.flags` 漂移核对在新位上成立
- [x] 1.4 /**引擎 95+ 绿;墓碑位与 composed 位互不干扰(构造即无;roundtrip 用例盖)** 引擎单测:组合写→stat/get 回环;重建索引保位;墓碑不置位(构造即无)
  ;删除线上的组合位一生(可选快速核对)

## 2. store 数据面

- [x] 2.1 /**PutParams serde default composed;inline/流式/无 CL 三路条件分派;store 139+ 绿** `PutParams` 增 `#[serde(default)] composed: Option<bool>`;put_blob 三写
  路径按位分派(inline/流式/无 CL)→ 引擎组合口/既有口
- [x] 2.2 /**MetadataDto.composed+get_blob_metadata 填充** `MetadataDto` 增 `composed: bool`(带文档注释);`get_blob_metadata` 填充

## 3. router SDK

- [x] 3.1 /**BlobStat #[serde(default)] composed;blob_stat_tolerates_missing_compoased_field 测绿(双 json)** `BlobStat` 增 `#[serde(default)] composed: bool`(旧 store JSON 容缺);
  serde 兼容单测(无字段 → false 不报错)
- [x] 3.2 /**put_url 携位(query 合法拼接);put_file_to 显式 false;put_keyed/put_auto_keyed 提取(环体逐字;grep 两处 put_body 调用均携 composed)** `put_url` 增 composed 位(query 合法拼接);`put_body` 增参;仲裁环参数化
  提取(`put_keyed` / `put_auto_keyed` 各含 composed 私参;**环体代码逐字搬运**,
  语义零变化)
- [x] 3.3 /**put_composed/put_composed_with_ttl/put_auto_composed_with_ttl 三件套;旧名=包装,零调用面破坏** 新公开组合三件套:`put_composed` / `put_composed_with_ttl` /
  `put_auto_composed_with_ttl`;旧公开名成为 composed=false 包装(签名与语义零破坏)

## 4. frontend 闭环

- [x] 4.1 /**stitch→put_composed;auto-chunk→put_auto_composed_films_with_ttl;注释同步** 两描述符写点切换:`POST /stitch`(→ `put_composed`)与自动分片描述符
  (→ `put_auto_composed_with_ttl`);注释同步(「描述符是个普通 blob」句更新)
- [x] 4.2 /**download flag 优先:置位→拼装路径(超限 502 不降级);无位→嗅探逐字保留;⚠注释改指 D5 冻结** `download` flag 优先分派:composed → 描述符路径(超 DESCRIPTOR_MAX 即
  502,不静默降级);无位 → 既有嗅探兜底逐字保留;⚠ 失真注释更新(指向 D5 记档)
- [x] 4.3 /**head 注释清理(乱码修;两形态同流说明)** `head_blob` 注释更新(弧结构不动:小对象读描述符对两形态同流)

## 5. 测试

- [x] 5.1 /**cargo test -p antfs-blob 95+ 零回归** 引擎:1.1/1.4 单测全绿;workspace `cargo test` 既有 e2e 零回归
- [x] 5.2 /**兼容双问:前向 roundtrip 保位+旧读不炸;后向 flags=0→not composed;router 旧 JSON 容缺** §3.4 兼容双问兑现(前端:置位值旧读路径 get/漂移核对;后端:flags=0
  旧值 → not composed 走嗅探)+ router serde 兼容单测
- [x] 5.3 /**frontend e2e 17 绿:stitch 走 flag 路径断言不变;plain_blob_starting_with_magic(502)仍绿=存量面守望** frontend e2e:既有 stitch/自动分片用例全部通过(新描述符走 flag 路径——
  断言不变即证语义等价);辅查既有普通小对象/ASTC 前缀负例(如 in-mail)仍绿
- [x] 5.4 /**PG 87/87 + MySQL 87/87(引擎层改在 s3 下全量守望);clippy 0;fmt 干净** 双言 s3 e2e 全量复跑(S3 层零改动,守望回归)+ clippy 0 + fmt 干净

## 6. 文档与收尾

- [x] 6.1 /**frontend.md §4/§11、cluster.md 数据面三处、store.md 记录+索引 flags 行、design README 路线✅、DEVLOG 67** `frontend.md`:§4(拼接:写置位/读 flag 优先+兜底)、§11(待办首条划线,
  翻转条件记档)、数据面表行注;`cluster.md` 数据面 PUT/metadata 行;`store.md`
  数据面端点注(如有);DEVLOG 步 67;design README 路线档 3 条目 2 ✅
- [x] 6.2 /**workspace 72 套件绿;tasks 勾满;归档执行** workspace test 绿;clippy/fmt 干净;tasks 勾满(证据);归档
  (openspec archive blob-composed-flag --yes)待用户验收后执行
