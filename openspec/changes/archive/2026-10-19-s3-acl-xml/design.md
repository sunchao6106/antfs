# s3-acl-xml — Design

## D1:ACL grants = 兜底层的补充腿,不是平行授权源

评估序:`policy Deny > policy Allow > ACL grants > 兜底`(owner/canned)。grants 只在
策略 NotApplicable 且兜底将 403 时才查询——授权语义上 ACL 永远**抬不赢**显式策略
Deny,结构上门只多一个 else 腿。AWS 真序(policy 与 ACL 的显式 deny 并列优先)在本
网关收敛为:Deny 恒胜,ACL 仅为策略缺位时的细粒度通道(differ 编排记档)。

## D2:桶 grants 内联、对象 grants deny-路径懒查

`BucketMeta.acl_grants` 随 head_bucket 已载(filepath 列,零额外查询), authorize
就地解析。对象的 grants 需要对象行——只在兜底将拒时补一发 get_object(deny 路径
独享,owner/allow 常态零成本)。拒绝路径多一查询是可接受代价(拒绝非热路径)。

## D3:AllUsers → canned 列直映射;不做 ACL 平行存储

AllUsers READ 与既有 `public-read` 语义完全同构(S3 本就是同一件事的两种皮),不做
第二份 AllUsers state՗。他 owner READ 才入 grants 列。GET ?acl 回吐序列化按"有 grants
出 grants 形态,否则 canned 形态"。

## D4:管理面恒 owner(策略/ACL 同门)

PUT ?acl 恒 owner 门(策略永不评估策略面同工——ACL 改坏不会低价锁死,因 owner 永
可改回)。对象 PUT ?acl 即行内 UPDATE 对 state=1 当前行,不 bump 版本(ACL 是元数据
面,非数据写;S3 非版本桶同语义)。
