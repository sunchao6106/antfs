## 1. frontend healthCheck 路由

- [ ] 1.1 `crates/frontend/src/server.rs`:新增 `GET /healthCheck` 路由与 handler——恒 `200` + `content-type: text/plain` + 正文 `GOOD`;零鉴权、零状态依赖(不触碰 `AppState` 的 router/master)、零指标写入
- [ ] 1.2 `crates/frontend/tests/rest.rs`:新增集成用例——匿名 `GET /healthCheck` 得 200/`GOOD`/text-plain;与对象读写并存互不干扰(spec 两个场景至少各一条断言;「指标不增」按场景 3 断言 in-flight/请求计数不变)

## 2. 文档同步(CLAUDE.md §6/§7)

- [ ] 2.1 `docs/design/frontend.md`:契约小节补 `GET /healthCheck` 行(标注「进程存活,非集群健康;免鉴权」防 readiness 误用)
- [ ] 2.2 `docs/DEVLOG.md`:补步骤 42(healthCheck 落地 + A1 裁决乙路线归档指向 `s3-object-metadata` change)

## 3. 收口验证

- [ ] 3.1 `cargo fmt --all --check` + `cargo clippy -p antfs-frontend --all-targets -- -D warnings` + `cargo test -p antfs-frontend` 全绿;推送后 CI 全绿
