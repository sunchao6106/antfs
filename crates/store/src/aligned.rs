//! 4096 字节对齐缓冲,满足 O_DIRECT 的内存/长度对齐要求,并实现 monoio 的
//! `IoBuf`/`IoBufMut`(owned-buffer 完成式 IO)。

use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};

use monoio::buf::{IoBuf, IoBufMut};

/// O_DIRECT 对齐粒度。4096 覆盖 512/4096 扇区盘(是二者的安全超集)。
pub const ALIGN: usize = 4096;

#[inline]
pub const fn align_up(v: u64, a: u64) -> u64 {
    (v + a - 1) & !(a - 1)
}

#[inline]
pub const fn align_down(v: u64, a: u64) -> u64 {
    v & !(a - 1)
}

/// 拥有一块 `ALIGN` 对齐的堆内存。容量恒为 `ALIGN` 的整数倍。
///
/// 内存在分配时清零,因此按 `capacity()` 读取始终是已初始化的(无 UB)。
/// 结构体移动时底层堆分配地址不变 —— 满足 monoio 对缓冲地址稳定的要求。
pub struct AlignedBuf {
    ptr: *mut u8,
    cap: usize,
    len: usize,
    layout: Layout,
}

impl AlignedBuf {
    /// 分配至少 `cap` 字节、向上取整到 `ALIGN` 的对齐缓冲。
    pub fn with_capacity(cap: usize) -> Self {
        let cap = std::cmp::max(align_up(cap as u64, ALIGN as u64) as usize, ALIGN);
        let layout = Layout::from_size_align(cap, ALIGN).expect("valid aligned layout");
        // SAFETY: layout size > 0。
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Self {
            ptr,
            cap,
            len: 0,
            layout,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 设置"已初始化"长度(供写盘时决定写多少字节)。
    #[inline]
    pub fn set_len(&mut self, n: usize) {
        assert!(n <= self.cap, "set_len {n} > cap {}", self.cap);
        self.len = n;
    }

    /// 整块容量的只读视图(始终已初始化,见类型文档)。
    #[inline]
    pub fn as_cap_slice(&self) -> &[u8] {
        // SAFETY: ptr 指向 cap 字节、已在分配时清零。
        unsafe { std::slice::from_raw_parts(self.ptr, self.cap) }
    }

    /// 整块容量的可写视图。
    #[inline]
    pub fn as_cap_mut(&mut self) -> &mut [u8] {
        // SAFETY: 独占 &mut,ptr 指向 cap 字节。
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.cap) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr/layout 来自本类型的 alloc_zeroed。
        unsafe { dealloc(self.ptr, self.layout) }
    }
}

// SAFETY: AlignedBuf 唯一拥有其堆分配;移动结构体不移动底层内存。
unsafe impl IoBuf for AlignedBuf {
    fn read_ptr(&self) -> *const u8 {
        self.ptr
    }
    fn bytes_init(&self) -> usize {
        self.len
    }
}

// SAFETY: 同上;write_ptr 返回稳定的堆地址。
unsafe impl IoBufMut for AlignedBuf {
    fn write_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
    fn bytes_total(&mut self) -> usize {
        self.cap
    }
    unsafe fn set_init(&mut self, pos: usize) {
        self.len = pos;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_rounds_up_to_align() {
        assert_eq!(AlignedBuf::with_capacity(1).capacity(), ALIGN);
        assert_eq!(AlignedBuf::with_capacity(ALIGN).capacity(), ALIGN);
        assert_eq!(AlignedBuf::with_capacity(ALIGN + 1).capacity(), 2 * ALIGN);
    }

    #[test]
    fn ptr_is_aligned_and_zeroed() {
        let b = AlignedBuf::with_capacity(2 * ALIGN);
        assert_eq!(b.read_ptr() as usize % ALIGN, 0);
        assert!(b.as_cap_slice().iter().all(|&x| x == 0));
    }

    #[test]
    fn align_helpers() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
        assert_eq!(align_down(4097, 4096), 4096);
        assert_eq!(align_down(4096, 4096), 4096);
    }
}
