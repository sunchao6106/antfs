# s3-checksums 任务

## 1. 纯函数层(无 DB,单测即全语义)

- [x] 1.1 /**checksum.rs 落地;crc32fast/crc32c/sha1/base64 依赖引入** 依赖 + `checksum.rs`:crc32fast / crc32c / sha1 / base64;`ChecksumAlgo`
  registry(header/?algo/digest_len/precedence);`compute(algo, data) -> base64`;
  `severity sortBy`;`parse value`(严格 base64 + 长度校验 → InvalidArgument)
- [x] 1.2 /**collect/merge_trailer_claims 落地;单测五件全绿(向量/优先级/矩阵/拒绝/mode)** 收集器:`collect_checksum_headers(headers) -> Vec<ChecksumClaim>`(已知族;
  未知同族头 → NotImplemented;type 头只认 FULL_OBJECT;sdk-algorithm 忽略)+
  `merge_trailer(claims, sink, declared_trailer) → claims'`(声明未到 400;同算法
  双载体 400)
- [x] 1.3 /**known_digest_vectors 公开向量(y/Q5Jg==、4waSgw== 等)+ 优先级 + 收集矩阵 + 拒绝矩阵 + mode 门 五件绿** 单测:四算法已知向量("123456789" CRC32 `CBD...`→`y9VD4A==` / CRC32C
  `5wYSgw==`、"abc" SHA1/SHA256)、坏 base64、长度不符、优先级序、收集器矩阵
  (未知 501/双载体 400/声明缺失)

## 2. 错误面 + trailer 管

- [x] 2.1 /**BadDigest{checksum_form} 变体(code/400/message 三处接齐)** `error.rs`:`BadDigest { checksum_form: String }`(code=`BadDigest` 400,
  message 点名载体与期望/实得形态)
- [x] 2.2 /**TrailerSink + ChunkedTrailers extensions;decodes_unsigned_trailer_body 补 sink 断言绿** `chunked.rs`:`TrailerSink = Arc<Mutex<HashMap<String,String>>>`;
  decoder trailer 行解析处收进 sink;`wrap_request` 注 `req.extensions()`;既有
  「ignored」删换为收集;分帧三态测试补 trailer sink 断言

## 3. MetaStore 持久化

- [x] 3.1 /**migrations/{postgres,mysql}/005_add_object_checksum.sql 两可空列;双言 e2e 真跑(迁移自动应用)绿** `migrations/{postgres,mysql}/005_add_object_checksum.sql`(PG `ADD COLUMN
  IF NOT EXISTS`,MySQL 直 ADD;两可空列)
- [x] 3.2 /**PutObjectMeta/ObjectMeta + StoredChecksum::from_pair;pg/mysql INSERT(13/14 列)+SELECT+Row 映射;测试构造同步** `types.rs`:`ObjectMeta.checksum: Option<StoredChecksum>` +
  `PutObjectMeta.checksum`;pg/mysql INSERT/SELECT/Row 映射三处(pg put_object、
  get_object SELECT 列、mysql 同步);既有用例构造处补 `checksum: None`

## 4. 写/读路径

- [x] 4.1 /**put_object:验证先于 blob 写(零孤儿);存储最强一份;响应回显双头;e2e checksum_roundtrip_all_algos/rejection/precedence 绿** `put_object`:read 前取 sink → body 后 `collect`+`verify`(先于 router.put)→
  存选 → `PutObjectMeta.checksum` → 响应回显双头;SSE 明文口径(即顺序天然)
- [x] 4.2 /**upload_part 同管(不持久化);响应回显;e2e part_checksum_verify 绿** `upload_part`:同验证管(不持久化),响应回显算出值
- [x] 4.3 /**GET/HEAD checksum-mode 回显(200/206;304/412 不回);mode 非法 400;四族回环断言绿** GET/HEAD `object_read_builder`/`head_object`:`x-amz-checksum-mode: ENABLED`
  (非法值 400,读门前置在 get/head 入口)回显;object_read_builder 注入点(200/206);
  304 响应不加
- [x] 4.4 /**attributes Checksum 接受+双元素/无值省略;objectparts 501 维持;e2e checksum_attributes_face + copy boundaries 更新绿** `get_object_attributes`:`checksum` 合法请求集;有值回 XML 双元素
  (`xml.rs` ObjectAttributesOut + 序列化),无值省略;ObjectParts 501 维持

## 5. 测试(双言实跑)

- [x] 5.1 /**object e2e +5(四族/拒绝/优先级+覆盖/attributes/SSE 明文)双言绿** e2e `tests/object.rs`:四族 roundtrip(算值置头,断言 200+回显+GET/HEAD
  mode 回显+attributes 面)/失配 400 BadDigest 且 GET 404/坏 base64 400/长度不符
  /多算法优先级(SHA256 存,GET 回 sha256 头)/unknown 501/双载体 400/mode 非法值
  400/SSE 明文 roundtrip/无校验 PUT→GET mode 无头(历史缺省自然)/overwrite 无
  校验头→回显消失
- [x] 5.2 /**chunked e2e +3(trailer 真值回环+回显/错值 400 零写/声明未到 400/双裁体 400)双言绿** e2e trailer 三态(chunked 面,复用既有分帧用例构造器):UNSIGNED-TRAILER
  带正确 trailer 值 200+持久(SIGNED 包链不破)/值错 400 零写/声明未到 400
- [x] 5.3 /**multipart e2e +1(part 校验回环+装配行缺省)双言绿** e2e `tests/multipart.rs`:part 校验 roundtrip(回显)/失配 400 无行
  /complete 后装配行 GET mode 无校验头(缺省面)
- [x] 5.4 /**PG 85/85 + MySQL 85/85(--ignored --test-threads=1);76 存量零回归** 双言全量实跑(PG + MySQL,`--ignored --test-threads=1`)零回归

## 6. 文档与收尾

- [x] 6.1 /**s3.md:操作映射 2 行/矩阵 3 行/新节「灵活校验」/流式分帧+属性节语义更新/后续方向;design README 路线档 2 ✅;DEVLOG 步 65** `docs/design/s3.md`:矩阵 PUT 行升级 + 新「灵活校验」节(算法表/载体/
  错误面/优先级/明文口径/501 家族/已知不做);操作映射 PUT/GET/HEAD 注记;
  「流式分帧」节 trailer 语义更新(值验证)、「拷贝与属性」501 面更新
- [x] 6.2 /**workspace 72 套件绿;clippy --workspace --all-targets 0;fmt 干净;lib 95 绿** workspace test / clippy 0 / fmt 干净;DEVLOG 步 65;tasks 勾满;
  归档(用户验收后)
