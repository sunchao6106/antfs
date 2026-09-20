//! 冒烟测试:验证 monoio + io_uring 在当前 Linux 环境可用。
//! 写一个文件再读回,全程走 io_uring completion。

use monoio::fs::File;

#[monoio::main]
async fn main() {
    let path =
        std::env::args().nth(1).unwrap_or_else(|| "/var/tmp/monoio_smoke.bin".to_string());

    // 写
    let file = File::create(&path).await.expect("create");
    let payload = b"io_uring hello from monoio\n".to_vec();
    let want = payload.len();
    let (res, _buf) = file.write_at(payload, 0).await;
    let n = res.expect("write_at");
    file.sync_all().await.expect("sync_all");
    file.close().await.expect("close");
    assert_eq!(n, want, "short write");
    println!("[write] {n} bytes via io_uring -> {path}");

    // 读回
    let file = File::open(&path).await.expect("open");
    let buf = vec![0u8; want];
    let (res, buf) = file.read_at(buf, 0).await;
    let m = res.expect("read_at");
    file.close().await.expect("close");
    println!("[read ] {m} bytes: {}", String::from_utf8_lossy(&buf[..m]).trim_end());

    assert_eq!(&buf[..m], b"io_uring hello from monoio\n");
    println!("[ok] monoio + io_uring 读写闭环验证通过");
}
