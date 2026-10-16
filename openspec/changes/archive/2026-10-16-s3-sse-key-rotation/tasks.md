# Tasks

## 1. metastore 扫描 API(双方言)

- [x] 1.1 `MetaStore` trait 加 `scan_sse_objects(bucket, after, limit)`;复用两方言既有 `*ObjectRow` 行映射(mod.rs trait 补条)
- [x] 1.2 pg.rs 实现:`($1 IS NULL OR (bucket, key) > ($1,$2)) AND ($4 IS NULL OR bucket=$4) ORDER BY bucket,key LIMIT $3`(整 `ObjectMeta` 吐回)
- [x] 1.3 mysql.rs 实现:同构(行构造比较 + `key` 反引号 + LIMIT ?)
- [x] 1.4 既有套件全绿:双言 133/133 各言(含 lib 42 忽略项),零回归

## 2. 轮换核心 + CLI

- [x] 2.1 `sse_rotation.rs`:`RotationReport`/`RotationFailure`/`rotate(st, old, new, batch, bucket)` 分页循环 + 失败中止带进度;`TOOL_SCAN_BATCH=1000`
- [x] 2.2 逐行守卫:alg≠AES256 中止;key_id≠旧钥跳过(行不动);`sse+parts_manifest` 乱序态中止;行写失败同请求墓碑**新** blob(旧行引用无损)
- [x] 2.3 `main.rs` `--rotate-sse-key <file>`(前置校验旧钥已配/新钥可载/新≠旧;停机警告;报告 + 退出码 0/1/2;不监听不 GC)
- [x] 2.4 lib 单测:无独立单元面(核心为 IO 编排弧,e2e 直调 `rotate()` 覆盖;守卫逻辑经 3.3/3.4 断言 error 文案与进度)

## 3. e2e(双言,#[ignore],单线程门禁)

- [x] 3.1 换钥读回:3 SSE(含 >64KiB 多分块,batch=2 强制分页)+ 明文对照;首跑 3/3 → 重跑 0/3(幂等);`restart_with_sse_master`(新端口第二网关实例 = 换钥重启)后 GET/HEAD 字节/etag/size/SSE 头/checksum(mode 回显)/x-amz-meta 全等;旧 blob `poll_tombstoned` + WAL 无残行;新主钥下新 PUT 正常
- [x] 3.2 他代 key 行(直插 `key_id 0123…`):报告 1 skip;行 version/blob_id/描述符字面不变且 blob 未被墓碑
- [x] 3.3 坏 alg(`{"alg":"DES"…}`)中止:Failure 定位 `b-bad.bin` + error 含 "unknown sse algorithm" + report 进度 scanned=2/rotated=1
- [x] 3.4 manifest+SSE 乱序态中止:同 3.3 形态(error 含 "parts manifest")
- [x] 3.5 双言全量:PG 10/10、MySQL 10/10(sse.rs);全量 e2e 133/133/言零回归
- 实证一课:首版无桶域 → 共享测试库沉淀 71 行打穿精确计数 → 扫描加桶域(断言域隔离,GC 同款纪律),设计文档 D 节同步

## 4. 门禁与文档

- [x] 4.1 workspace 72 套件 719 绿;clippy --workspace --all-targets 0 告警;fmt --check 干净
- [x] 4.2 s3.md:config 表 `sse_master_key_file` 行轮换指针;「局限」行收口;新「主钥轮换」小节(工序五步/语义/可续跑/响亮中止/残留件/停机硬前提)
- [x] 4.3 README 路线档 5 条目 1 ✅;DEVLOG 步骤 68(动机/落地/验证/发运)
- [x] 4.4 openspec validate;归档;单 commit 推送
