# s3-acl-xml —— ACL XML 授予面(Put/Get ACL body 形态 + 桶/对象两域)

## Why

矩阵 116 行留的唯一余项(「object 级 ACL / XML body 形式 ACL 面」;`s3-canned-acl`
当时显式 501 出界)。用户 2026-10-19 指令完结批——C1 校验、C2 策略已推,本件收官。

## What Changes

- **桶 ACL XML(PUT/GET ?acl)**:无 `x-amz-acl` 头时接受 AccessControlPolicy body——
  `FULL_CONTROL`(owner)/`READ`(owner / 他 owner ID / AllUsers URI)。AllUsers READ →
  直接映射 canned public-read 列;他 owner READ → `buckets.acl_grants` TEXT 列(migration
  009,JSON grant 集),评估位于策略 NotApplicable 的兜底 deny 末腿(s3:ListBucket 对应
  桶域;纯内联,零额外查询)。`WRITE` 家族不受支持 → 400 响亮(不做即拒)。Owner ID ≠
  桶 owner → 403。GET 回吐 XML(grants 有则 grants 形态,无则 canned 形态,原样兼容)。
- **对象 ACL XML(PUT/GET /{key}?acl)**:同语法(grants 只允许 READ 家族);存
  `named_objects.acl_grants`;**评估 leg** = authorize NotApplicable 兜底将拒时,GetObject
  动作补一记 get_object 查对象 grants(deny-路径独享,常态零开销;policy Allow/Deny
  与 owner 蕴含全部先于它——授予永不能压显式 Deny/策略,只能抬兜底)。
- 评估序终形:policy Deny > policy Allow > ACL grants > owner 蕴含/canned 兜底。
- 出界记档:object canned ACL(`x-amz-acl` on PutObject)*维持不支持*;READ_ACP/WRITE_ACP
  权限字 400;ACL 版本无(Policy 管理面同门,owner 恒 setter)。

## Impact

- e2e 双言(桶 XML grant 跨租户 List 但不 Get;对象 grants 补 Get;AllUsers→public;
  坏权限/坏 owner 拒;GET 回吐;删除对象 grants 随行而去)。
- migration 009 双幂等;既有面零破坏(canned 头路径逐字不动)。
