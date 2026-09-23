# syntax=docker/dockerfile:1

# ---- 构建阶段:编译工作区里的服务端二进制 ----
FROM rust:1-bookworm AS builder

# tonic-prost-build(crates/master/build.rs)需要 protoc。
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
# 先只拷清单与源码:改文档/CI 不会打掉依赖编译的缓存层。
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY benchmark ./benchmark

RUN cargo build --release --locked \
      -p antfs-master \
      -p antfs-store \
      -p antfs-frontend \
      -p antfs-admin \
      -p antfs-devcluster

# ---- 运行时阶段:只带二进制 ----
FROM debian:bookworm-slim

LABEL org.opencontainers.image.source="https://github.com/sunchao6106/antfs" \
      org.opencontainers.image.title="antfs" \
      org.opencontainers.image.description="antfs — a distributed blob storage system in Rust, purpose-built for massive small files" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --create-home --uid 10001 antfs \
 && mkdir -p /data && chown antfs:antfs /data

COPY --from=builder \
     /src/target/release/antfs-master \
     /src/target/release/antfs-store \
     /src/target/release/antfs-frontend \
     /src/target/release/antfs-admin \
     /src/target/release/antfs-devcluster \
     /usr/local/bin/

USER antfs
WORKDIR /data
VOLUME ["/data"]

# master raft / master gRPC / store HTTP ×3 / admin web / frontend REST
EXPOSE 7100 7101 7200 7201 7202 7300 7400

# 默认起一键本地集群(master + 3×store + admin + frontend);
# `--host 0.0.0.0` 是容器里能被 `-p` 发布端口访问的前提。
# 也可以改用单个组件,例如:
#   docker run --entrypoint antfs-master ...  (antfs-store / antfs-frontend / antfs-admin 同理)
#
# 本镜像从源码编译(仓库尚无预编译二进制产物)。等 Release 里有了跨平台二进制后,可改成
# 下载二进制 + alpine 的构建方式:镜像更小、构建秒级、无需 protoc 与编译器。
ENTRYPOINT ["antfs-devcluster"]
CMD ["--dir", "/data", "--host", "0.0.0.0"]

# 无 curl 依赖的存活探测:能连上 admin web 端口即视为就绪。
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s \
  CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/7300' || exit 1
