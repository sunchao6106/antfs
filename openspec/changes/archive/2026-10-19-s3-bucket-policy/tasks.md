# Tasks

## 1. 持久化

- [x] 1.1 migration 008 双方言(buckets +policy TEXT NULL)
- [x] 1.2 BucketMeta +policy(head_bucket 载入;list_buckets 不带,column 注记)
- [x] 1.3 trait +set_bucket_policy(两方言 UPDATE 行语义;fake 同步;xml.rs 测试体字面量补 None)

## 2. policy.rs

- [x] 2.1 PolicyAction 七动作 + `*` + s3 名/对象级-桶级两几何(兜底基线外移为 GateFallback 参数)
- [x] 2.2 parse 全验:Version/Effect/Principal/AWS-key 形/Action 名单/资源属本桶/尾缀 `*` 独 glob/Sid ASCII/Condition&Not* 家族响亮 400(三 lib 单测:拒族/评估序/几何)
- [x] 2.3 evaluate(owner Option / anonymous 仅 `*`)→ Deny|Allow|NotApplicable,Deny 优先短路
- [x] 2.4 fail-closed 入口 parse_trusted(存坏 → 上层 500)

## 3. 门与 API

- [x] 3.1 authorize + GateFallback{PublicRead,OwnerOnly} + fallback_gate 兜底逐位保形(require_public_read 移除并入;require_bucket_owner 留管理面)
- [x] 3.2 11 处数据面门点换门:object get(+?attributes)/head/delete(abort|delete)/post(create-multipart|complete)/PUT 面(put/copy/part)、GET bucket(uploads|列举)、HEAD bucket、GET ?uploadId(ListParts)、copy 源门(GetObject,带 key)
- [x] 3.3 S3QueryParams +policy;三 handler(put:read_bounded + parse 全验 + upsert;get:404 NoSuchBucketPolicy 语义[error.rs 新变体];delete:幂等 204)+ GET/PUT/DELETE 三路由(恒 owner)
- [x] 3.4 管理面编码注释(?policy/?acl/?location/batch/delete bucket 均 require_bucket_owner,D3)

## 4. e2e + 门禁 + 收尾

- [x] 4.1 跨租户前缀读六断言(界外/列举/匿名三 403 + 原文回吐 + 删后回私有);跨租户 scoped 写(资源域 403/动作单向 403)
- [x] 4.2 Deny 压 owner 蕴含 + 管理面永不锁死(换策略/删策略 200/204) + allow-* 解封
- [x] 4.3 坏文档七形态 400(非 JSON/错版本/野动作/IAM-ARN/他桶资源/Condition/20KiB 超限)+ 零存库 + 删除幂等
- [x] 4.4 fail-closed(直改库注坏 → 500;复原后 200 + 桶复原无污染)
- [x] 4.5 双言全量 143/143+143/143 零回归 + policy 双言 6+6;workspace 73 套 722 绿;clippy 0;fmt 净;文档四落(s3.md 矩阵/桶策略节/尚未支持行、README、DEVLOG 70);归档;单 commit push
