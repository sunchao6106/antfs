# Tasks

- [x] 1. migration 009 双方言(buckets/named_objects +acl_grants TEXT)+ BucketMeta/ObjectMeta 扩列(head/get_object SELECT)+ trait `set_bucket_acl_grants`/`set_object_acl_grants`(两方言 UPDATE 行语义;fake 同步;解析树字面量补字段)
- [x] 2. xml.rs:parse_access_control_policy(Grantee ID/URI 互斥、AllUsers URI 白名单、MalformedXML)+ serialize_access_control_policy_for_grants(含 <ID>/<URI> 元素与 DisplayName 转义)
- [x] 3. authorize 两腿:桶 grants(NotApplicable 拒腿,内联)+ 对象 grants(GetObject,deny-路径懒查行;匿名经 AllUsers 认领——首版缺陷,测试逼出);`acl_grants_allow` 存坏 fail-close
- [x] 4. PUT/GET ?acl:桶 XML/canned 双路径(换置语义同清 grants)+ 对象两 handler + server.rs 两路由分支(管理面恒 owner);权限白名单校验(FULL_CONTROL 仅自授 / READ;W 族与 AC 族 400;Owner 校验;对象不存在 404)
- [x] 5. e2e 双言 aclxml.rs 6+6(几何/次序/匿读/映射/五拒/换置);object.rs 旧 501 边界断言按新契约翻转;全量双言 150+150;workspace 74 套 722;clippy 0;fmt 净;文档(s3.md 矩阵收官/API 行/新节、DEVLOG 71);归档 2026-10-19-s3-acl-xml;单 commit push
