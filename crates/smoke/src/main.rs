//! 冒烟测试:验证同步阻塞(buffered)IO 在当前环境可用。
//! 写一个文件再读回,走 std::fs 同步 IO + sync_all。

use std::os::unix::fs::FileExt;

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/var/tmp/antfs_smoke.bin".to_string());

    // 写
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .expect("create");
    let payload = b"sync buffered hello from antfs\n";
    file.write_all_at(payload, 0).expect("write_at");
    file.sync_all().expect("sync_all");
    drop(file);
    println!("[write] {} bytes via std::fs -> {path}", payload.len());

    // 读回
    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("open");
    let mut buf = vec![0u8; payload.len()];
    file.read_exact_at(&mut buf, 0).expect("read_at");
    drop(file);

    let m = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    println!(
        "[read ] {m} bytes: {}",
        String::from_utf8_lossy(&buf[..m]).trim_end()
    );
    assert_eq!(&buf[..m], &payload[..m]);
    println!("[ok] 同步 buffered IO 读写闭环验证通过");

    let _ = std::fs::remove_file(&path);
}
