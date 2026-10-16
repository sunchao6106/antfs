# s3-read-compat 任务

## 1. 纯函数层(无 DB,单测即全语义)

- [x] 1.1 /**evaluate_read_conditions + ReadOutcome 三态(object.rs);条件矩阵/短路序/坏日期三测绿** `object.rs`:`evaluate_read_conditions(meta, headers) -> ReadOutcome{Proceed/NotModified/PreconditionFailed}`(RFC 7232 §6 求值序:If-Match 412 短路 > If-Unmodified-Since 412 > If-None-Match 304 短路 > If-Modified-Since 304;`*` 通配;坏日期 400)——复用 `strip_etag_quotes`/`parse_http_date_ms`/`http_date_from_ms`
- [x] 1.2 /**if_range_passes 八例测绿(强 ETag/`W/` 失配/日期两向/解析失败)** `object.rs`:`if_range_passes(meta, val) -> bool`(ETag 形=剥引号强相等,`W/` 视失配;日期形 ≥ created_at;解析失败=false)
- [x] 1.3 /**manifest_part_window + intersect_windows(range.rs);部分越界/零尺寸片/空交边界测绿** `range.rs`:`manifest_part_window(parts, n) -> Option<PartWindow{start,len,total}>` + `intersect(a, b) -> Option<ByteWindow>`
- [x] 1.4 /**serialize_list_bucket_result_v1 + serialize_location_constraint(xml.rs);V1 五形 + 两 region 测绿** `xml.rs`:`serialize_list_bucket_result_v1`(恒 Name/Prefix/Marker/MaxKeys/IsTruncated;Delimiter 给定时;NextMarker 仅 delimiter+trunc;Contents 含 StorageClass;无 KeyCount/NextContinuationToken)+ `serialize_location_constraint(region)`(us-east-1 → 空元素)
- [x] 1.5 /**s3 lib 90 绿(新增 9 单测);clippy 0/fmt 干净** 单测:条件矩阵(四头各 最小集 + 短路两例 + 坏日期)、if_range 五例、part_window/intersect 边界(越界/单 part/空交)、V1 序列化形态断言、Location 两 region

## 2. GET/HEAD 读路径(handler)

- [x] 2.1 /**server.rs:S3QueryParams 增 marker/location;GET 下探 part_number+头;HEAD 接 Request;分派序 acl→uploads→location→list(V2/V1)** `server.rs`:`S3QueryParams` 增 `marker`/`location`;`object_get_handler` 把 `part_number` + 条件/If-Range 头下探给 `get_object`;`object_head_handler` 接 Request,条件头下探给 `head_object`
- [x] 2.2 /**get_object 重构:条件→manifest 前移(Σ 对账)→If-Range→resolve_read_window;304 空件;Last-Modeed=created_at 全路径;e2e 76/76 含 conditional/if_range/part_number 三测** `object.rs get_object`:元数据后先 `evaluate_read_conditions`(304=空体+ETag+Last-Modified,412=PreconditionFailed;零 blob 读)→ `if_range_passes` 失败清除 Range/partNumber 窗口(→200 全对象)→ partNumber 解析(multipart:manifest_part_window 片窗,越界 416;单片对象:part1 全幅 206,其余 416)→ 与 Range 求交(空交 416)→ 既有三分支(plain 切片/SSE 解密切片/multipart PartStream)按窗口发 206 + Content-Range 全对象坐标;全路径补 `Last-Modified`
- [x] 2.3 /**head_object 条件 304/412 空体 + Last-Modified;HEAD 忽略 Range/partNumber e2e 断言** `object.rs head_object`:接条件头(304/412 空体)+ `Last-Modified`;Range/partNumber 忽略(既有)

## 3. LIST V1 + Location

- [x] 3.1 /**list_objects_v1(marker→start_after;NextMarker=delimiter+truc 回 metastore 排他 token)+ get_bucket_location;e2e v1 两测绿** `bucket.rs`:`list_objects_v1`(marker→start_after,共用 `list_objects` 核心;NextMarker=delimiter+trunc 时回 metastore 排他 token)与 `get_bucket_location`;`server.rs bucket_get_handler` 分派序 acl→uploads→location→list(list-type=2 → V2,否则 V1)
- [x] 3.2 /**既有 6 list 用例全带 list-type=2,PG 76/68 存量零回归实跑证实** e2e 佐证默认翻 V1 零回归:PG 全量跑既有 6 list 用例(V2 显式带 list-type=2)

## 4. 测试(双言实跑)

- [x] 4.1 /**conditional_get_head_matrix:四头矩阵+短路+`*`+400+HEAD 三态 绿** e2e `tests/object.rs`:条件矩阵(IF-GET: If-Match 过/失配、If-None-Match 304+ETag+Last-Modified、`*`、If-Modified-Since 304、If-Unmodified-Since 412、坏日期 400、HEAD 304/412)、If-Range(过=206/etag 失配=200 全/date 过/解析失败=200)、Last-Modified 于 200/304
- [x] 4.2 /**part_number_multipart/part_number_single/part_number_sse 三测绿(片对拍/0 越界 416/集合/If-Range 双向/空对象)** e2e `tests/multipart.rs` 或 `object.rs`:partNumber 矩阵(multipart 片字节对拍+Content-Range 全对象坐标、0/越界 416、单片对象 part1=全幅 206/part2=416、SSE 单片 part1、partNumber+Range 交集、If-Range 失败+partNumber=200 整对象)
- [x] 4.3 /**v1_shape_and_pagination(union 无缺漏)+ v1_delimiter_next_marker(排他 token 断言)+ location_us_east_1_empty 绿** e2e `tests/list.rs`:V1(裸 GET 形态断言:Marker 空/无 KeyCount/StorageClass;max-keys 分页 union 无缺漏;delimiter+NextMarker 走页;无 delimiter 截断不回 NextMarker)+ location(默认 region 空元素断言)
- [x] 4.4 /**PG 76/76 + MySQL 76/76(--ignored --test-threads=1);零回归** 双言实跑(本机真 PG + MySQL 8.4 容器,`--ignored --test-threads=1`):两方言全量 e2e 通过,零回归

## 5. 文档与收尾

- [x] 5.1 /**s3.md:操作映射 +3 行(ListObjectsV1/GetBucketLocation)、矩阵 2 行升级、新节「条件读与读兼容面」、后续方向补 ✅ 行** `docs/design/s3.md`:功能矩阵 GET/HEAD 行、「Range 读与流式装配」节后新增「条件读与读兼容面」(四头求值序、304 形态、If-Range 闸、partNumber 窗口、V1 形态与 NextMarker 规则、location);操作映射 GET/HEAD/列表行注 query。
- [x] 5.2 /**workspace 72 套件 exit 0;clippy --workspace --all-targets 0 告警;fmt --check 干净** `cargo test --workspace` 退出码 0;`cargo clippy --workspace --all-targets` 0 告警;`cargo fmt -p antfs-s3` 干净。
- [x] 5.3 /**delta 场景(get/head 条件 12、If-Range×2、partNumber×6、V1×4、location×3)↔ e2e 1:1;openSpec archive 由用户验收后发起(本文件即勾画证据)** 三件套自检:delta 场景(get/head 条件、If-Range×2、partNumber×5、V1×4、location×3)↔ e2e 用例 1:1 对上;归档(archive + DEVLOG 步)待用户验收后发起。
