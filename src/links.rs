//! Parallel union-find over linked user IDs.
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

pub struct LinkTable {
    pub slots: Vec<[AtomicU64; 2]>, // key, !name (0 until named)
    pub parent: Vec<AtomicU32>,
    pub mask: usize,
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
        // zeroed allocations: pages are only touched when used
        let slots: Vec<[AtomicU64; 2]> = unsafe {
            let mut v = std::mem::ManuallyDrop::new(vec![[0u64; 2]; cap]);
            Vec::from_raw_parts(v.as_mut_ptr() as *mut [AtomicU64; 2], cap, cap)
        };
        let parent: Vec<AtomicU32> = unsafe {
            let mut v = std::mem::ManuallyDrop::new(vec![0u32; cap]);
            Vec::from_raw_parts(v.as_mut_ptr() as *mut AtomicU32, cap, cap)
        };
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
                        self.parent[i].store(i as u32, Relaxed);
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

    #[inline(always)]
    pub fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Relaxed);
            if p == x {
                return x;
            }
            let gp = self.parent[p as usize].load(Relaxed);
            if gp != p {
                let _ = self.parent[x as usize].compare_exchange_weak(p, gp, Relaxed, Relaxed);
            }
            x = gp;
        }
    }

    /// Lock-free union: the larger root is linked under the smaller one.
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
            if self.parent[hi as usize].compare_exchange(hi, lo, Relaxed, Relaxed).is_ok() {
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
