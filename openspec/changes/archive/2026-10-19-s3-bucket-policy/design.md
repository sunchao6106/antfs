# s3-bucket-policy — Design

## Context

- 授权现状全集中在 server.rs 两道门:`require_bucket_owner`(写/管理)与
  `require_public_read`(读族:GET/HEAD object、ListObjectsV1/V2——canned
  public-read 放匿名)。策略只须收编这两个判定点,call-site 清单如下(实现时逐位
  换为 `authorize`):object_put(PutObject)/object_get+attributes(GetObject)/
  object_head(GetObject)/object_delete(Abort 或 DeleteObject)/object_post(
  create-multipart / complete = PutObject)/GET ?uploadId(ListMultipartUploadParts)/
  GET bucket ?uploads(ListBucketMultipartUploads)/GET bucket 无参(ListBucket)/
  HEAD bucket(ListBucket)/copy_object 内部源读门与终写门。

## D1:评估序 = explicit Deny > policy Allow > 既有兜底

一条 `Deny` 匹配(勿论 Allow 是否同在)→ 403;否则任一 `Allow` 匹配 → 放行;
否则回现行语义(owner 蕴含 / 读族 public-read / 其余 owner-only 403)。owner 蕴含
允许可以被显式 Deny 压住(owner 是 root 的 S3 邻语义;root 免疫仅限策略管理面,
见 D3)。每动作自带兜底基线(read 族 public 兜底 / write 族 owner 兜底)编码在
`PolicyAction::fallback_type`。

## D2:语言子集 = put 时全验,存坏 = fail-closed

解析/校验只在 put 策略时做一次(版本/Effect/Principal/Action/Resource 形态;
不支持的字段(Condition/Not* 家族/arn: principals)→ 400 指名)。存入库的即合法
形态;**读侧解析失败**(理论不可达=人工改库)→ 500 fail-closed:鉴权面宁可拒绝
服务,不可静默放行。资源合法性含「属本桶」校验(put handler 传入 bucket);
尾缀 `*` 之外不实现 glob。

## D3:策略永不评估策略管理面

put/get/delete `?policy` 恒走 owner 门(不查策略)——防「Deny 自己的
DeleteBucketPolicy」把桶锁死。同一裁定延伸:?acl / ?location / quota / batch-delete
本轮不评估(管理面出界记档;batch 逐键评估 + object ACL XML = 后续项)。

## D4:Principal = owner 标识字符串

无 IAM/账号 ARN 体系;canonical id 即凭证 `owner`(创建凭证时的属主字符串)。
`"*"` = 任何签名者或匿名。`{"AWS": "arn:..."}` → 400(不支持即拒)。
评估时:匿名请求(ctx = None)只与 `*` 或语句列表中非 owner 串位无关地**只能**
被 `*` 匹配。

## D5:动作-资源几何

对象级动作(GetObject/PutObject/DeleteObject/Abort/…)与桶级动作(ListBucket/
ListBucketMultipartUploads)分别匹配 `arn:…:::{bucket}/{key}` 形与桶裸 ARN 形
(AWS 同几何:桶 ARN 不授权对象动作)。对象资源支持 `{bucket}`、`{bucket}/key`
(精确)与 `{bucket}/prefix*`(前缀)。`*` 动作 = 全动作(两几何都试)。

## 交付边界

- 不做:Condition 运算符、IAM ARN principal、非尾缀 glob、bypass 点
  (admin/operator 面不经过数据面门)、object ACL、策略版本历史(单文档覆写)。
- 策略 JSON 大小上界 20 KiB(`read_bounded` 复用,越限 400 EntityTooLarge 家族)。
