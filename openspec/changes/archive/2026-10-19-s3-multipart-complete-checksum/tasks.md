# Tasks

## 1. migration + 类型形态

- [x] 1.1 007 双方言 SQL:`multipart_parts` +`part_checksum_algo/…_b64`;`named_objects` +`checksum_type`、`checksum_b64`→TEXT
- [x] 1.2 `StoredChecksum` +`kind`(`ChecksumKind` FULL_OBJECT/COMPOSITE);`from_row` 三列后向兼容(NULL → FULL_OBJECT;半写/野值宽容告警)+ lib 单测锁
- [x] 1.3 `PartMeta` +`checksum`(part 行写/读同型)

## 2. metastore 两方言

- [x] 2.1 put_part INSERT/UPSERT 带两列;重传换行幂等不变
- [x] 2.2 complete_multipart 锁内终判:composite 推导 + SDK 供值比对 + 部件族性/同族裁决;FULL_OBJECT 已验值落行;`CompleteChecksum` 枚传入、result 回吐终值
- [x] 2.3 读路径三列化(get_object/scan SELECTs、`*ObjectRow` +`checksum_type` → `from_row`);put_object INSERT 带 kind
- [x] 2.4 Fake 签名同步(complete_multipart + session_parts);trait +`session_parts`

## 3. handler 弧

- [x] 3.1 put_part 最强已验值随行落库(应答回显面不变,注释同步翻转)
- [x] 3.2 complete 请求头裁决:type 缺省 FULL_OBJECT/野值 InvalidArgument/声明无值 InvalidRequest/SHA×COMPOSITE 与双 CRC family InvalidRequest/CRC64NVME 501(收集器复用 + `ensure_full_object_type` 抽门)
- [x] 3.3 FULL_OBJECT 预检:`session_parts` 映射 XML manifest → `PartStream` 流式哈希逐声明比对(至多一片内存;最强落参)
- [x] 3.4 complete 应答回显(algo + type 按行 kind)
- [x] 3.5 GET/HEAD 两 builder 现场与 attributes XML 三元组 kind 化;PUT/Part 应答恒 FULL_OBJECT

## 4. e2e 双言 + 门禁

- [x] 4.1 composite 路径:三片 crc32 自动推导回显 + mode-GET 同值 COMPOSITE;裸片对照会话应答/行零变化;e2e 双言 PG 18/18 + MySQL 18/18(multipart.rs 含新六件)
- [x] 4.2 SDK 供值恶→善重试闭环;FULL_OBJECT 双片 sha256/sha1(多声明存最强,GET 断言 sha256 恰在、sha1 不回)+ 坏值零副作用
- [x] 4.3 拒面:混族/半值/全裸(name-carrying 文案断言)、SHA×COMPOSITE、野 type、无值声明、CRC64NVME 501
- [x] 4.4 后向:`from_row` 单测(legacy→FULL_OBJECT、COMPOSITE、野值、半写四态);migration 007 在既有共享库在线升级全量跑通为证
- [x] 4.5 双言全量 138/138 零回归 + workspace 72 套 720 绿 + clippy 0 + fmt 净;文档四落(s3.md 矩阵两行/回显段/「明示不做」翻转/新小节;README 档 2 尾注;DEVLOG 步 69);归档 + 单 commit push
