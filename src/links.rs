//! Parallel union-find over linked user IDs.
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

pub struct LinkTable {
    pub slots: &'static [[AtomicU64; 2]], // key, !name (0 until named)
    pub parent: &'static [AtomicU32],
    pub mask: usize,
}

/// Zeroed memory backed by huge pages where possible; never freed (the process ends with _exit).
fn huge_zeroed(bytes: usize) -> *mut u8 {
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "link table mmap failed");
        libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
        p as *mut u8
    }
}

#[inline(always)]
pub fn mix(x: u64) -> u64 {
    let z = (x ^ (x >> 31)).wrapping_mul(0x7FB5_D329_728E_A185);
    let z = (z ^ (z >> 27)).wrapping_mul(0x81DA_DEF4_BC2D_D44D);
    z ^ (z >> 33)
}

impl LinkTable {
    pub fn new(cap_hint: usize) -> Self {
        let cap = cap_hint.next_power_of_two().max(1024);
        let slots: &'static [[AtomicU64; 2]] =
            unsafe { std::slice::from_raw_parts(huge_zeroed(cap * 16) as *const [AtomicU64; 2], cap) };
        let parent: &'static [AtomicU32] =
            unsafe { std::slice::from_raw_parts(huge_zeroed(cap * 4) as *const AtomicU32, cap) };
        LinkTable { slots, parent, mask: cap - 1 }
    }

    /// Slot of key, inserting it. Thread-safe.
    #[inline(always)]
    pub fn insert(&self, k: u64) -> u32 {
        let mut i = (mix(k) as usize) & self.mask;
        let mut probes = 0;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            let cur = s[0].load(Relaxed);
            if cur == k {
                return i as u32;
            }
            if cur == 0 {
                match s[0].compare_exchange(0, k, Relaxed, Relaxed) {
                    Ok(_) => {
                        return i as u32;
                    }
                    Err(now) if now == k => return i as u32,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
            probes += 1;
            assert!(probes <= self.mask, "link table full");
        }
    }

    /// parent[x] == 0 means x is a root; otherwise parent[x] - 1 is its parent (always a smaller slot).
    #[inline(always)]
    pub fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Relaxed);
            if p == 0 {
                return x;
            }
            let pp = p - 1;
            let gp = self.parent[pp as usize].load(Relaxed);
            if gp != 0 {
                let _ = self.parent[x as usize].compare_exchange_weak(p, gp, Relaxed, Relaxed);
                x = gp - 1;
            } else {
                return pp;
            }
        }
    }

    /// Lock-free union: the larger root is linked under the smaller one. Safe to run
    /// concurrently with inserts and other unions.
    #[inline(always)]
    pub fn union(&self, a: u32, b: u32) {
        let (mut a, mut b) = (a, b);
        loop {
            a = self.find(a);
            b = self.find(b);
            if a == b {
                return;
            }
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            if self.parent[hi as usize].compare_exchange(0, lo + 1, Relaxed, Relaxed).is_ok() {
                return;
            }
        }
    }

    /// key -> person name, for linked keys.
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<u64> {
        let mut i = (mix(k) as usize) & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            let cur = s[0].load(Relaxed);
            if cur == k {
                return Some(!s[1].load(Relaxed));
            }
            if cur == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        let i = (mix(k) as usize) & self.mask;
        unsafe {
            std::arch::x86_64::_mm_prefetch(self.slots.as_ptr().add(i) as *const i8, std::arch::x86_64::_MM_HINT_T0)
        };
    }
}
