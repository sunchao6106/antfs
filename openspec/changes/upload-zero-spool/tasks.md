# Tasks: upload-zero-spool

> ①④⑤⑥⑦ 已落地;⑧(router 下沉)与 ⑬(压缩整体移除)已落地(2026-10-21);
> ②A 道型(A 段)已冻结(2026-10-22,裁决 #16);B 段(router 压缩两遍)随 ⑬ 作废,任务条目已删。

- [x] 0. 裁决①:大文件不压缩被否决;采纳 ≤上界内存帧 + >上界两遍流式(已于修订档落定,
      状态见 proposal「裁决记录」)
- [x] 0b. 裁决④:网关墙③采边收边分片(design.md §9 方案已落;B1 下载面留待另裁)
- [x] 3b. 网关(④):`POST /blobs` 分档重写 —— 小/中档 `to_bytes` 有界(中档核验后才写);
      大档与无 CL 流式分片(攒满即入库 + EOF 对账 + 逐片墓碑清理,`stream_upload` 替代
      `auto_chunk_store` + `stage_body` 撤)+ `frontend_upload_chunk_cleanups_total` +
      测试面(design §9.5:零 TMPDIR/断流清理/窗内单 blob/中档/超长 session)+ frontend.md
      §3/§5/§9 双表(2026-10-21 落地:frontend 11+23 测试,workspace 77 套 811/0)
- [x] 3c. 恒切 + 公共上限 + 收账 + S3 双闸(2026-10-21 实施):
      auto_chunk_enabled 废(chk 恒切);[upload] max_object_bytes 公共表(common.toml 链,
      REST/S3 同值);upload_session_wal_dir+清扫(frontend);S3 put_object 闸门 +
      complete 合计闸;metrics:清扫×2;测试:lh WAL 24 整、frontend 16+24/0(+1 WAL e2e)、
      s3 106/0(+limits.rs 双闸 2 用例)、openspec 24 维度;文档:前端/通用/S3 三 md 双表续。
- [x] 3d. router 下沉(⑧)已实施(2026-10-21):恒切管线 + 逻辑读口(get /
      get_object_stream / get_object_range_stream / stat_object)+ 裸口 get_blob +
      失败墓碑 + 交叉校验(frontend/s3 `validate_all` 防二次切)+ compression 整体移除(⑬,
      重开=TODO)+ 读侧零盘(⑫)+ 读口纯 stream(⑭);e2e:恒切往返/散布/窗口/409 回收/关 0;
      REST Range 206/416。见 design §12 落地记录与 proposal 裁决 8-11。
- [x] 4-7 合并冻结记档(2026-10-22,裁决 #16):A 道型(lane 预留 / 三段化 / 旋钮×2+监控×4 /
      测试面 I1-I6)不做。理由:恒切(⑧)后 store 单笔=片宽级,"长传输占写锁"的原始痛点不复
      存在;网络-写锁串扰已由"先暂存、再进引擎写锁"隔断;调研(Ambry/SeaweedFS/CubeFS/
      FastDFS)无一在 store 层做磁盘暂存区。重启条件(任一命中再评估):①恢复"单笔大直传
      store";②写锁等待队列观测到队头阻塞证据;③出现内存契约扛不住的直连形态。
- [x] 8. 收官改签:本变更以"A 冻结 + B 作废"封账;替选小补丁已按所有者极简形态落地(裁决 #18:PUT 内存整收,staged/staging_dir 整体退役,无新旋钮);archive 时机待所有者确认。

## 落地记录

- 2026-10-22 裁决 #16:A 段冻结、B 段三件套随 ⑬ 作废删除。
- 2026-10-22 裁决 #17:store 裸默认自洽(max_body 2→5 MiB 盖过 router 默认片宽)。
- 2026-10-22 裁决 #18:暂存中转整体退役 —— 所有者采极简形态(带 CL ≤ 上限一律内存整收,
  无新旋钮、无并发闸),staged 路径 / `staging_dir` / `INLINE_BODY_MAX` 一并移除;原"内联
  门槛小补丁"不再需要。
