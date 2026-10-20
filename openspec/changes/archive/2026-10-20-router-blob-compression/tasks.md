# Tasks: router-blob-compression

## 1. Codec & Config

- [x] 1.1 `compression.rs`:帧常量/编解码/闸门纯函数 + 单测矩阵(kept/low_ratio/too_small/disabled/零长/roundtrip/非帧透传)
- [x] 1.2 `RouterConfig` 4 新项 + Validate(1.05..=100? ratio 范围;level 1..=19)

## 2. Put / Get 接线

- [x] 2.1 put 路径(`put_keyed`/`put_auto_keyed` Bytes 形态,含 composed)过闸;prom 打点(attempts/saved)
- [x] 2.2 `get_from` bulk 探测解压 + e2e:开/关两态 roundtrip + 逐字节等价
- [x] 2.3 `drain_to_file` 流式:窥 17B → 帧管道(channel + spawn_blocking 解码 + orig_len 校验);e2e:大件流式 roundtrip + auto 分片路径

## 3. Spec & Docs & Closure

- [x] 3.1 spec delta:ADDED requirements(帧格式与三闸/读侧透明/复制不透明正交/监控契约)
- [x] 3.2 `router.md` 压缩节 + 配置项清单 + 监控项清单双表;`common.md` 引用行;DEVLOG 步 83
- [x] 3.3 workspace 全绿 + devcluster 两态真面闭环 + validate/archive + 单 commit push + CI 盯绿

## 落地记录(已回填)

- codec 单测(+probe)7 个(4 算法 × kept/roundtrip/too_small/low_ratio/disabled/零长
  + 非帧透传 + orig_len 谎报响错 + 未知算法号响错 + 4 算法混存齐读);e2e 新 2:
  `compression_roundtrip_disabled_default_and_enabled`(默认关逐字节零变化 →
  开态 gzip kept/节省过半/bulk + get_file 流式产出=orig_len 恒等/too_small 原样)、
  `compression_survives_replication_frame_passthrough`(RF=2 帧透传读回一致);
  **workspace 77 套 775/0;clippy 0;fmt 净**。
- devcluster 真面(默认 gzip):开态 inline 512 KiB roundtrip 恒等 + tiny 原样;
  `router_compression_attempts_total{kept}=1`、`{too_small}=1`、
  `router_compression_saved_bytes_total=520838`(512 KiB 重复文本);5 MiB 高熵
  (比率闸回落)与 5 MiB 自动分片 roundtrip 恒等;关态 5 MiB roundtrip 恒等。
- 实现坑记档:①put_keyed 与 put_auto_keyed 两写口须对称接线(漏接 auto 口则全部
  网关写无压缩,真面 saved_bytes=0 抓出);②get_file 的 peek 溢收块必须判帧后再
  分派(判帧前引去解码管道会把非帧对象喂解码器 —— e2e put_get_roundtrip 抓出);
  ③lz4 FrameEncoder 须显式 finish()(Drop 不补帧尾)。
