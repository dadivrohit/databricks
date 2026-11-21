//! Shopping Sessions: fast pipeline. See rules.md.
mod fast;
mod parquet;

use fast::*;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const EXOTIC: u64 = 1 << 63;

// ---------------------------------------------------------------- utilities

#[inline(always)]
fn mix(x: u64) -> u64 {
    let z = (x ^ (x >> 31)).wrapping_mul(0x7FB5_D329_728E_A185);
    let z = (z ^ (z >> 27)).wrapping_mul(0x81DA_DEF4_BC2D_D44D);
    z ^ (z >> 33)
}

/// Open-addressing u64 -> u64 map, interleaved slots; key 0 = empty.
struct U64Map {
    slots: Vec<[u64; 2]>,
    mask: usize,
    len: usize,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { slots: vec![[0, 0]; cap], mask: cap - 1, len: 0 }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u64> {
        let mut i = (mix(k) as usize) & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s[0] == k {
                return Some(s[1]);
            }
            if s[0] == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        let i = (mix(k) as usize) & self.mask;
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                self.slots.as_ptr().add(i) as *const i8,
                std::arch::x86_64::_MM_HINT_T0,
            )
        };
    }
    fn get_or_insert(&mut self, k: u64, v: u64) -> u64 {
        if (self.len + 1) * 10 > self.slots.len() * 7 {
            self.grow();
        }
        let mut i = (mix(k) as usize) & self.mask;
        loop {
            let s = &mut self.slots[i];
            if s[0] == k {
                return s[1];
            }
            if s[0] == 0 {
                *s = [k, v];
                self.len += 1;
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old = std::mem::take(&mut self.slots);
        let cap = old.len() * 2;
        self.slots = vec![[0, 0]; cap];
        self.mask = cap - 1;
        for s in old {
            if s[0] != 0 {
                let mut i = (mix(s[0]) as usize) & self.mask;
                while self.slots[i][0] != 0 {
                    i = (i + 1) & self.mask;
                }
                self.slots[i] = s;
            }
        }
    }
}

/// Strings that do not fit the compact key form get a global index.
struct Interner {
    inner: Mutex<(HashMap<Vec<u8>, u32>, Vec<Vec<u8>>)>,
}

impl Interner {
    fn new() -> Self {
        Interner { inner: Mutex::new((HashMap::new(), Vec::new())) }
    }
    #[cold]
    fn intern(&self, s: &[u8]) -> u64 {
        let mut g = self.inner.lock().unwrap();
        if let Some(&i) = g.0.get(s) {
            return EXOTIC | i as u64;
        }
        let i = g.1.len() as u32;
        g.1.push(s.to_vec());
        g.0.insert(s.to_vec(), i);
        EXOTIC | i as u64
    }
    fn snapshot(&self) -> Vec<Vec<u8>> {
        self.inner.lock().unwrap().1.clone()
    }
}

#[inline(always)]
unsafe fn key_of(p: *const u8, n: usize, prefix: u8, interner: &Interner) -> u64 {
    let k = unsafe { short_key(p, n, prefix) };
    if k != 0 { k } else { interner.intern(unsafe { std::slice::from_raw_parts(p, n) }) }
}

struct Keys {
    prefix: u8,
    exotic: Vec<Vec<u8>>,
}

impl Keys {
    fn bytes(&self, k: u64, out: &mut Vec<u8>) {
        if k & EXOTIC != 0 {
            out.extend_from_slice(&self.exotic[(k & !EXOTIC) as usize]);
        } else {
            unpack_short(k, self.prefix, out);
        }
    }
    #[inline(always)]
    fn cmp(&self, a: u64, b: u64) -> std::cmp::Ordering {
        if (a | b) & EXOTIC == 0 || a == b {
            return a.cmp(&b);
        }
        self.cmp_slow(a, b)
    }
    #[cold]
    fn cmp_slow(&self, a: u64, b: u64) -> std::cmp::Ordering {
        let mut x = Vec::new();
        let mut y = Vec::new();
        self.bytes(a, &mut x);
        self.bytes(b, &mut y);
        x.cmp(&y)
    }
}

// ---------------------------------------------------------------- CSV (scalar, for small files and slow lines)

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// "<prefix><digits without leading zero>" -> number, if canonical and small.
#[inline(always)]
fn canon_num(b: &[u8], prefix: &[u8]) -> Option<usize> {
    if b.len() <= prefix.len() || b.len() > prefix.len() + 7 || &b[..prefix.len()] != prefix {
        return None;
    }
    let d = &b[prefix.len()..];
    if d[0] == b'0' {
        return None;
    }
    let mut v = 0usize;
    for &c in d {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return None;
        }
        v = v * 10 + x as usize;
    }
    Some(v)
}

/// Exact string -> id lookup with a fast array for canonical "<prefix>N".
struct Lookup {
    prefix: &'static [u8],
    arr: Vec<u32>, // id+1, 0 = absent
    map: HashMap<Vec<u8>, u32>,
}

impl Lookup {
    fn new(prefix: &'static [u8]) -> Self {
        Lookup { prefix, arr: Vec::new(), map: HashMap::new() }
    }
    fn insert(&mut self, s: &[u8], id: u32) {
        match canon_num(s, self.prefix) {
            Some(n) if n < 4_000_000 => {
                if self.arr.len() <= n {
                    self.arr.resize(n + 1, 0);
                }
                self.arr[n] = id + 1;
            }
            _ => {
                self.map.insert(s.to_vec(), id);
            }
        }
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> Option<u32> {
        if let Some(n) = canon_num(s, self.prefix) {
            if n < 4_000_000 {
                return match self.arr.get(n) {
                    Some(&v) if v != 0 => Some(v - 1),
                    _ => None,
                };
            }
        }
        if self.map.is_empty() {
            return None;
        }
        self.map.get(s).copied()
    }
}

/// Rows of a small 2-column CSV (header skipped, malformed dropped).
fn small_csv2(path: &Path, mut f: impl FnMut(&[u8], &[u8])) {
    let data = fs::read(path).unwrap_or_default();
    let mut lines = data.split(|&c| c == b'\n');
    lines.next();
    let mut fl = [(0usize, 0usize); 2];
    let n_lines = data.iter().filter(|&&c| c == b'\n').count();
    for (i, line) in lines.enumerate() {
        if i + 1 >= n_lines + 1 && line.is_empty() {
            break; // trailing newline
        }
        if split::<2>(line, &mut fl) {
            f(&line[fl[0].0..fl[0].1], &line[fl[1].0..fl[1].1]);
        }
    }
}

// ---------------------------------------------------------------- files & chunks

struct Mapped {
    ptr: *const u8,
    len: usize,
}
unsafe impl Send for Mapped {}
unsafe impl Sync for Mapped {}

impl Mapped {
    fn open(path: &Path) -> Mapped {
        use std::os::unix::io::AsRawFd;
        let f = fs::File::open(path).expect("open");
        let len = f.metadata().unwrap().len() as usize;
        if len == 0 {
            return Mapped { ptr: std::ptr::NonNull::dangling().as_ptr(), len: 0 };
        }
        let p = unsafe {
            libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE | if std::env::var("POP").is_ok() { libc::MAP_POPULATE } else { 0 }, f.as_raw_fd(), 0)
        };
        assert!(p != libc::MAP_FAILED, "mmap failed");
        Mapped { ptr: p as *const u8, len }
    }
    fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

#[derive(Clone, Copy)]
struct Chunk {
    file: u32,
    kind: u8, // 0 events, 1 clicks, 2 links
    start: usize,
    end: usize,
}

const CHUNK: usize = 2 << 20;

fn make_chunks(files: &[(Mapped, u8)], kind: u8, v: &mut Vec<Chunk>) {
    for (i, (m, k)) in files.iter().enumerate() {
        if *k != kind {
            continue;
        }
        let mut s = 0;
        while s < m.len {
            let e = (s + CHUNK).min(m.len);
            v.push(Chunk { file: i as u32, kind, start: s, end: e });
            s = e;
        }
    }
}

const SLACK: usize = 256;

/// For every line starting in [start, end) (header skipped): f(ptr, len, fields).
/// ptr..ptr+len+64 is always readable.
#[inline(always)]
fn for_lines<const N: usize>(
    data: &[u8],
    start: usize,
    end: usize,
    pad: &mut Vec<u8>,
    mut f: impl FnMut(*const u8, &[(usize, usize); N]),
) {
    let len = data.len();
    let mut pos = if start == 0 {
        match memchr::memchr(b'\n', data) {
            Some(i) => i + 1,
            None => return,
        }
    } else if data[start - 1] == b'\n' {
        start
    } else {
        match memchr::memchr(b'\n', &data[start..]) {
            Some(i) => start + i + 1,
            None => return,
        }
    };
    let base = data.as_ptr();
    let mut fl = [(0usize, 0usize); N];
    while pos < end {
        if pos + SLACK <= len {
            let p = unsafe { base.add(pos) };
            let (nl, cm, qm) = unsafe { masks128(p) };
            if nl != 0 {
                let l = nl.trailing_zeros() as usize;
                let lm = (1u128 << l) - 1;
                if unsafe { fields::<N>(p, l, cm & lm, qm & lm, &mut fl) } {
                    f(p, &fl);
                }
                pos += l + 1;
                continue;
            }
        }
        // slow path: long line or near the end of the file
        let e = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => len,
        };
        let line = &data[pos..e];
        pad.clear();
        pad.extend_from_slice(line);
        pad.resize(line.len() + SLACK, 0);
        if split::<N>(&pad[..line.len()], &mut fl) {
            f(pad.as_ptr(), &fl);
        }
        pos = e + 1;
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
#[repr(C)]
struct EventRec {
    ekey: u64,
    ukey: u64,
    ingest: i32, // seconds relative to window start (saturating)
    ts: u32,     // ms relative to window start (valid only)
    info: u32,   // valid:1 <<31 | purchase:1 <<30 | product-or-cat
    price: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct ClickRaw {
    ukey: u64,
    ckey: u64,
    ts: i32,
    camp: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PEv {
    person: u64,
    ts: u32,
    pc: u32, // price (0 = not a purchase)
    cat: u32,
    _pad: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct ClickRec {
    person: u64,
    ckey: u64,
    ts: i32,
    camp: u32,
}

const P_BITS: u32 = 10;
const Q_BITS: u32 = 10;
const NP: usize = 1 << P_BITS;
const NQ: usize = 1 << Q_BITS;

#[inline(always)]
fn epart(k: u64) -> usize {
    (mix(k) >> (64 - P_BITS)) as usize
}
#[inline(always)]
fn pbucket(k: u64) -> usize {
    (mix(k.wrapping_add(0x1234_5678_9ABC_DEF1)) >> (64 - Q_BITS)) as usize
}

struct UserRow {
    person: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: i32, // -1 = null
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

struct Links {
    map: U64Map, // linked ID key -> person name key
}

fn union_find(pairs: Vec<Vec<(u64, u64)>>, users: &Interner) -> Links {
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    let mut idx = U64Map::with_capacity(total + 16);
    let mut keys: Vec<u64> = Vec::with_capacity(total);
    let mut parent: Vec<u32> = Vec::with_capacity(total);
    #[inline(always)]
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        let mut r = x;
        while parent[r as usize] != r {
            r = parent[r as usize];
        }
        while parent[x as usize] != r {
            let n = parent[x as usize];
            parent[x as usize] = r;
            x = n;
        }
        r
    }
    for v in &pairs {
        for &(a, b) in v {
            let ia = idx.get_or_insert(a, keys.len() as u64) as u32;
            if ia as usize == keys.len() {
                keys.push(a);
                parent.push(ia);
            }
            let ib = idx.get_or_insert(b, keys.len() as u64) as u32;
            if ib as usize == keys.len() {
                keys.push(b);
                parent.push(ib);
            }
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    drop(pairs);
    let uk = Keys { prefix: b'u', exotic: users.snapshot() };
    let n = keys.len();
    let mut name: Vec<u64> = vec![0; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let k = keys[i];
        if name[r] == 0 || uk.cmp(k, name[r]) == std::cmp::Ordering::Less {
            name[r] = k;
        }
    }
    // reuse the index table: key -> name
    for s in idx.slots.iter_mut() {
        if s[0] != 0 {
            s[1] = name[parent[s[1] as usize] as usize];
        }
    }
    Links { map: idx }
}

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data_dir = PathBuf::from(if args.len() >= 3 { args[1].clone() } else { env("DATA_DIR", "/data") });
    let out_dir = PathBuf::from(if args.len() >= 3 { args[2].clone() } else { env("OUTPUT_DIR", "/output") });
    let nthreads: usize = env("NCPU", "0")
        .parse()
        .ok()
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4));
    let timing = std::env::var("TIMING").is_ok();
    let lap = |what: &str| {
        if timing {
            eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1e3, what);
        }
    };
    let cal = Calendar::new();

    // manifest
    let manifest = fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let grab = |key: &str| -> i64 {
        let i = manifest.find(key).expect("key");
        let rest = &manifest[i + key.len()..];
        let q1 = rest.find('"').unwrap();
        let rest = &rest[q1 + 1..];
        let q2 = rest.find('"').unwrap();
        let mut b = rest[..q2].as_bytes().to_vec();
        let n = b.len();
        b.resize(n + 64, 0);
        unsafe { cal.ts_ms(b.as_ptr(), n) }.expect("window ts")
    };
    let ws = grab("\"window_start\"");
    let we = grab("\"window_end\"");
    let first_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - first_day + 1).max(1) as usize;
    let ws_s = ws.div_euclid(1000);

    // products & categories
    let mut prod_rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    small_csv2(&data_dir.join("products.csv"), |a, b| prod_rows.push((a.to_vec(), b.to_vec())));
    let mut cats: Vec<Vec<u8>> = Vec::new();
    {
        let mut seen: HashMap<&[u8], ()> = HashMap::new();
        for (_, c) in &prod_rows {
            if seen.insert(c.as_slice(), ()).is_none() {
                cats.push(c.clone());
            }
        }
    }
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let cat_id = |s: &[u8]| cats.binary_search_by(|c| c.as_slice().cmp(s)).unwrap() as u32;
    let unknown_cat = cat_id(b"unknown");
    let mut products = Lookup::new(b"p");
    {
        let mut cache: HashMap<Vec<u8>, u32> = HashMap::new();
        for (p, c) in &prod_rows {
            let id = match cache.get(c) {
                Some(&i) => i,
                None => {
                    let i = cat_id(c);
                    cache.insert(c.clone(), i);
                    i
                }
            };
            products.insert(p, id);
        }
    }
    // campaigns
    let mut crows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    small_csv2(&data_dir.join("campaigns.csv"), |a, b| crows.push((a.to_vec(), b.to_vec())));
    let mut campaigns = Lookup::new(b"c_");
    for (i, (c, _)) in crows.iter().enumerate() {
        campaigns.insert(c, i as u32);
    }
    let ncamp = crows.len();
    lap("lookups");

    // map files
    let mut files: Vec<(Mapped, u8)> = Vec::new();
    for (sub, kind) in [("events", 0u8), ("clicks", 1u8), ("links", 2u8)] {
        for p in csv_files(&data_dir.join(sub)) {
            files.push((Mapped::open(&p), kind));
        }
    }
    let mut chunks = Vec::new();
    make_chunks(&files, 2, &mut chunks);
    let nlink_chunks = chunks.len();
    make_chunks(&files, 1, &mut chunks);
    make_chunks(&files, 0, &mut chunks);
    lap("mmap");

    let users = Interner::new();
    let ekeys = Interner::new();
    let ckeys = Interner::new();

    // ---- parse everything; the last link chunk's thread runs union-find meanwhile
    let total_event_bytes: usize = files.iter().filter(|f| f.1 == 0).map(|f| f.0.len).sum();
    let next = AtomicUsize::new(0);
    let links_done = AtomicUsize::new(0);
    let link_pairs: Mutex<Vec<Vec<(u64, u64)>>> = Mutex::new(Vec::new());
    let links_out: Mutex<Option<Links>> = Mutex::new(None);
    struct Parsed {
        _arena: Arena,
        ev: Parts<EventRec>,
        cl: Vec<ClickRaw>,
    }
    let arena_cap = (total_event_bytes / nthreads) * 2 + (512 << 20);
    let parsed: Vec<Parsed> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut arena = Arena::new(arena_cap);
                    let mut ev: Parts<EventRec> = Parts::new(NP, 256);
                    let mut cl: Vec<ClickRaw> = Vec::new();
                    let mut pairs: Vec<(u64, u64)> = Vec::new();
                    let mut sc = Scanner::new();
                    let mut ts = Ts::new(&cal);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(c) = chunks.get(i) else { break };
                        let data = files[c.file as usize].0.bytes();
                        match c.kind {
                            2 => {
                                sc.scan::<2>(data, c.start, c.end, |p, f| unsafe {
                                    let (a, b) = (f[0], f[1]);
                                    if a.0 == a.1 || b.0 == b.1 {
                                        return;
                                    }
                                    let ka = key_of(p.add(a.0), a.1 - a.0, b'u', &users);
                                    let kb = key_of(p.add(b.0), b.1 - b.0, b'u', &users);
                                    pairs.push((ka, kb));
                                });
                                link_pairs.lock().unwrap().push(std::mem::take(&mut pairs));
                                if links_done.fetch_add(1, Ordering::AcqRel) + 1 == nlink_chunks {
                                    let all = std::mem::take(&mut *link_pairs.lock().unwrap());
                                    let tu = Instant::now();
                                    *links_out.lock().unwrap() = Some(union_find(all, &users));
                                    if timing { eprintln!("  union-find {:.1} ms", tu.elapsed().as_secs_f64() * 1e3); }
                                }
                            }
                            1 => {
                                sc.scan::<4>(data, c.start, c.end, |p, f| unsafe {
                                    let (fc, fu, fa, ft) = (f[0], f[1], f[2], f[3]);
                                    if fc.0 == fc.1 || fu.0 == fu.1 {
                                        return;
                                    }
                                    let t = match ts.ms(p.add(ft.0), ft.1 - ft.0) {
                                        Some(t) if t >= ws - WEEK && t < we => t,
                                        _ => return,
                                    };
                                    let Some(camp) = campaigns.get(std::slice::from_raw_parts(p.add(fa.0), fa.1 - fa.0))
                                    else {
                                        return;
                                    };
                                    let ckey = key_of(p.add(fc.0), fc.1 - fc.0, b'k', &ckeys);
                                    let ukey = key_of(p.add(fu.0), fu.1 - fu.0, b'u', &users);
                                    cl.push(ClickRaw { ukey, ckey, ts: (t - ws) as i32, camp });
                                });
                            }
                            _ => {
                                sc.scan::<7>(data, c.start, c.end, |p, f| unsafe {
                                    let f0 = f[0];
                                    if f0.0 == f0.1 {
                                        return;
                                    }
                                    let ekey = key_of(p.add(f0.0), f0.1 - f0.0, b'e', &ekeys);
                                    let f3 = f[3];
                                    let ingest_s = ts.ingest_secs(p.add(f3.0), f3.1 - f3.0).unwrap_or(i64::MAX / 4000);
                                    let ingest = (ingest_s - ws_s).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                                    let f1 = f[1];
                                    let mut valid = f1.0 != f1.1;
                                    let ukey = if valid { key_of(p.add(f1.0), f1.1 - f1.0, b'u', &users) } else { 0 };
                                    let f2 = f[2];
                                    let mut tsr = 0u32;
                                    match ts.ms(p.add(f2.0), f2.1 - f2.0) {
                                        Some(t) if t >= ws && t < we => {
                                            tsr = (t - ws) as u32;
                                            valid &= ingest_s * 1000 - t <= DAY;
                                        }
                                        _ => valid = false,
                                    }
                                    let f4 = f[4];
                                    let n4 = f4.1 - f4.0;
                                    let tp = p.add(f4.0);
                                    let mut purchase = false;
                                    if n4 == 4 {
                                        let w = ld32(tp);
                                        valid &= w == u32::from_le_bytes(*b"view") || w == u32::from_le_bytes(*b"cart");
                                    } else if n4 == 8 && ld64(tp) == u64::from_le_bytes(*b"purchase") {
                                        purchase = true;
                                    } else {
                                        valid = false;
                                    }
                                    let f5 = f[5];
                                    valid &= f5.0 != f5.1;
                                    let mut price = 0u32;
                                    let mut cat = 0u32;
                                    if purchase && valid {
                                        let f6 = f[6];
                                        match fast::price(p.add(f6.0), f6.1 - f6.0) {
                                            Some(v) => {
                                                price = v;
                                                cat = products
                                                    .get(std::slice::from_raw_parts(p.add(f5.0), f5.1 - f5.0))
                                                    .unwrap_or(unknown_cat);
                                            }
                                            None => valid = false,
                                        }
                                    }
                                    let info = ((valid as u32) << 31) | cat;
                                    ev.push(&mut arena, epart(ekey), EventRec { ekey, ukey, ingest, ts: tsr, info, price });
                                });
                            }
                        }
                    }
                    Parsed { _arena: arena, ev, cl }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let links = match links_out.into_inner().unwrap() {
        Some(l) => l,
        None => union_find(std::mem::take(&mut *link_pairs.lock().unwrap()), &users),
    };
    let lmap = &links.map;
    lap("parsed (+union-find)");

    // ---- dedup events per partition; route survivors and clicks to person buckets
    let click_pieces: Vec<(usize, usize, usize)> = {
        let mut v = Vec::new();
        for (t, p) in parsed.iter().enumerate() {
            let n = p.cl.len();
            let mut s = 0;
            while s < n {
                let e = (s + 65536).min(n);
                v.push((t, s, e));
                s = e;
            }
        }
        v
    };
    let next = AtomicUsize::new(0);
    struct Routed {
        _arena: Arena,
        ev: Parts<PEv>,
        cl: Parts<ClickRec>,
    }
    let route_cap = (total_event_bytes / nthreads) + (512 << 20);
    let routed: Vec<Routed> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut arena = Arena::new(route_cap);
                    let mut out: Parts<PEv> = Parts::new(NQ, 256);
                    let mut outc: Parts<ClickRec> = Parts::new(NQ, 128);
                    let mut buf: Vec<EventRec> = Vec::new();
                    let mut table: Vec<(u64, u32)> = Vec::new();
                    let mut win: Vec<bool> = Vec::new();
                    let mut surv: Vec<u32> = Vec::new();
                    loop {
                        let w = next.fetch_add(1, Ordering::Relaxed);
                        if w < click_pieces.len() {
                            let (t, a, b) = click_pieces[w];
                            let src = &parsed[t].cl[a..b];
                            for (i, c) in src.iter().enumerate() {
                                if i + 8 < src.len() {
                                    lmap.prefetch(src[i + 8].ukey);
                                }
                                let person = lmap.get(c.ukey).unwrap_or(c.ukey);
                                outc.push(&mut arena, pbucket(person), ClickRec { person, ckey: c.ckey, ts: c.ts, camp: c.camp });
                            }
                            continue;
                        }
                        let p = w - click_pieces.len();
                        if p >= NP {
                            break;
                        }
                        buf.clear();
                        for t in &parsed {
                            for sl in t.ev.slices(p) {
                                buf.extend_from_slice(sl);
                            }
                        }
                        let n = buf.len();
                        if n == 0 {
                            continue;
                        }
                        let cap = (n * 2).next_power_of_two();
                        let mask = cap - 1;
                        table.clear();
                        table.resize(cap, (0, 0));
                        win.clear();
                        win.resize(n, false);
                        for i in 0..n {
                            let r = &buf[i];
                            let mut h = (mix(r.ekey) as usize) & mask;
                            loop {
                                let slot = &mut table[h];
                                if slot.0 == 0 {
                                    *slot = (r.ekey, i as u32);
                                    win[i] = true;
                                    break;
                                }
                                if slot.0 == r.ekey {
                                    let j = slot.1 as usize;
                                    if r.ingest < buf[j].ingest {
                                        win[j] = false;
                                        win[i] = true;
                                        slot.1 = i as u32;
                                    }
                                    break;
                                }
                                h = (h + 1) & mask;
                            }
                        }
                        surv.clear();
                        for i in 0..n {
                            if win[i] && buf[i].info >> 31 != 0 {
                                surv.push(i as u32);
                            }
                        }
                        for (x, &i) in surv.iter().enumerate() {
                            if x + 8 < surv.len() {
                                lmap.prefetch(buf[surv[x + 8] as usize].ukey);
                            }
                            let r = &buf[i as usize];
                            let person = lmap.get(r.ukey).unwrap_or(r.ukey);
                            out.push(
                                &mut arena,
                                pbucket(person),
                                PEv { person, ts: r.ts, pc: r.price, cat: r.info & 0x7FFF_FFFF, _pad: 0 },
                            );
                        }
                    }
                    Routed { _arena: arena, ev: out, cl: outc }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("dedup+route");

    // ---- sessions & attribution per person bucket
    let ckeys_k = Keys { prefix: b'k', exotic: ckeys.snapshot() };
    let next = AtomicUsize::new(0);
    struct Out {
        rows: Vec<UserRow>,
        camp: Vec<[i64; 4]>,
    }
    let ncols = ncamp + 1; // last = organic
    let results: Vec<Out> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut o = Out { rows: Vec::new(), camp: vec![[0; 4]; ncols * ndays] };
                    let mut evs: Vec<(u64, u32, u32)> = Vec::new(); // (idx<<32|ts, price, cat)
                    let mut cls: Vec<(u64, u64, u32)> = Vec::new(); // (idx<<32|ts', ckey, camp)
                    let mut pkeys: Vec<u64> = Vec::new();
                    let mut ltab: Vec<(u64, u32)> = Vec::new();
                    let mut count: Vec<u32> = vec![0; ncamp];
                    let mut active: Vec<u32> = Vec::new();
                    let mut pos: Vec<u32> = vec![0; ncamp];
                    let mut catsum: Vec<(u32, i64)> = Vec::new();
                    loop {
                        let q = next.fetch_add(1, Ordering::Relaxed);
                        if q >= NQ {
                            break;
                        }
                        let n: usize = routed.iter().map(|t| t.ev.len(q)).sum();
                        if n == 0 {
                            continue;
                        }
                        let cap = (n * 2).next_power_of_two();
                        let mask = cap - 1;
                        ltab.clear();
                        ltab.resize(cap, (0, 0));
                        pkeys.clear();
                        evs.clear();
                        for t in &routed {
                            for e in t.ev.slices(q).flatten() {
                                let mut h = (mix(e.person) as usize) & mask;
                                let idx = loop {
                                    let slot = &mut ltab[h];
                                    if slot.0 == e.person {
                                        break slot.1;
                                    }
                                    if slot.0 == 0 {
                                        *slot = (e.person, pkeys.len() as u32);
                                        pkeys.push(e.person);
                                        break slot.1;
                                    }
                                    h = (h + 1) & mask;
                                };
                                evs.push(((idx as u64) << 32 | e.ts as u64, e.pc, e.cat));
                            }
                        }
                        evs.sort_unstable_by_key(|e| e.0);
                        cls.clear();
                        for t in &routed {
                            for c in t.cl.slices(q).flatten() {
                                let mut h = (mix(c.person) as usize) & mask;
                                let idx = loop {
                                    let slot = ltab[h];
                                    if slot.0 == c.person {
                                        break slot.1 as i64;
                                    }
                                    if slot.0 == 0 {
                                        break -1;
                                    }
                                    h = (h + 1) & mask;
                                };
                                if idx >= 0 {
                                    let tsu = (c.ts as u32) ^ 0x8000_0000;
                                    cls.push(((idx as u64) << 32 | tsu as u64, c.ckey, c.camp));
                                }
                            }
                        }
                        cls.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| ckeys_k.cmp(a.1, b.1)));
                        cls.dedup_by(|b, a| a.0 == b.0 && a.1 == b.1);
                        let mut ci = 0usize;
                        let mut i = 0usize;
                        while i < evs.len() {
                            let idx = evs[i].0 >> 32;
                            let person = pkeys[idx as usize];
                            let mut j = i;
                            while j < evs.len() && evs[j].0 >> 32 == idx {
                                j += 1;
                            }
                            while ci < cls.len() && cls[ci].0 >> 32 < idx {
                                ci += 1;
                            }
                            let c0 = ci;
                            while ci < cls.len() && cls[ci].0 >> 32 == idx {
                                ci += 1;
                            }
                            let pc = &cls[c0..ci];
                            let cts = |x: &(u64, u64, u32)| ((x.0 as u32) ^ 0x8000_0000) as i32 as i64;
                            let (mut lo, mut hi) = (0usize, 0usize);
                            let mut k = i;
                            let mut cur_day: i32 = i32::MIN;
                            let mut row = UserRow { person, day: 0, sessions: 0, events: 0, revenue: 0, top: -1 };
                            catsum.clear();
                            while k < j {
                                let start = (evs[k].0 as u32) as i64;
                                let day = ((ws + start).div_euclid(DAY) - first_day) as i32;
                                if day != cur_day {
                                    if cur_day != i32::MIN {
                                        o.rows.push(finish_row(&row, &mut catsum, cur_day, first_day));
                                        row = UserRow { person, day: 0, sessions: 0, events: 0, revenue: 0, top: -1 };
                                    }
                                    cur_day = day;
                                }
                                let mut prev = start;
                                let mut m = k;
                                let mut rev = 0i64;
                                let mut purchases = false;
                                while m < j {
                                    let e = evs[m];
                                    let t = (e.0 as u32) as i64;
                                    if t - prev > GAP {
                                        break;
                                    }
                                    prev = t;
                                    if e.1 > 0 {
                                        purchases = true;
                                        rev += e.1 as i64;
                                        match catsum.iter_mut().find(|x| x.0 == e.2) {
                                            Some(x) => x.1 += e.1 as i64,
                                            None => catsum.push((e.2, e.1 as i64)),
                                        }
                                    }
                                    m += 1;
                                }
                                row.sessions += 1;
                                row.events += (m - k) as u32;
                                row.revenue += rev;
                                while hi < pc.len() && cts(&pc[hi]) <= start {
                                    let c = pc[hi].2 as usize;
                                    if count[c] == 0 {
                                        pos[c] = active.len() as u32;
                                        active.push(c as u32);
                                    }
                                    count[c] += 1;
                                    hi += 1;
                                }
                                while lo < hi && cts(&pc[lo]) < start - WEEK {
                                    let c = pc[lo].2 as usize;
                                    count[c] -= 1;
                                    if count[c] == 0 {
                                        let p = pos[c] as usize;
                                        let last = *active.last().unwrap();
                                        active[p] = last;
                                        pos[last as usize] = p as u32;
                                        active.pop();
                                    }
                                    lo += 1;
                                }
                                let dayu = day as usize;
                                let credited = if hi > lo {
                                    let t = pc[hi - 1].0;
                                    let mut x = hi - 1;
                                    while x > lo && pc[x - 1].0 == t {
                                        x -= 1;
                                    }
                                    pc[x].2 as usize
                                } else {
                                    ncamp
                                };
                                let cell = &mut o.camp[credited * ndays + dayu];
                                cell[0] += 1;
                                cell[1] += purchases as i64;
                                cell[2] += rev;
                                for &a in &active {
                                    o.camp[a as usize * ndays + dayu][3] += 1;
                                }
                                k = m;
                            }
                            if cur_day != i32::MIN {
                                o.rows.push(finish_row(&row, &mut catsum, cur_day, first_day));
                            }
                            for &a in &active {
                                count[a as usize] = 0;
                            }
                            active.clear();
                            i = j;
                        }
                    }
                    o
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("sessions");

    // ---- write outputs
    let ukeys = Keys { prefix: b'u', exotic: users.snapshot() };
    let ud_dir = out_dir.join("user_daily");
    let cd_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    fs::create_dir_all(&cd_dir).unwrap();
    std::thread::scope(|s| {
        for (ti, o) in results.iter().enumerate() {
            let ukeys = &ukeys;
            let cats = &cats;
            let ud_dir = &ud_dir;
            s.spawn(move || {
                let rows = &o.rows;
                let n = rows.len();
                let mut uid = Vec::with_capacity(n * 14);
                let mut tmp = Vec::new();
                let mut top = Vec::new();
                let mut defined = vec![0u8; n.div_ceil(8) + 1];
                for (r_i, r) in rows.iter().enumerate() {
                    tmp.clear();
                    ukeys.bytes(r.person, &mut tmp);
                    uid.extend_from_slice(&(tmp.len() as u32).to_le_bytes());
                    uid.extend_from_slice(&tmp);
                    if r.top >= 0 {
                        defined[r_i / 8] |= 1 << (r_i % 8);
                        let c = &cats[r.top as usize];
                        top.extend_from_slice(&(c.len() as u32).to_le_bytes());
                        top.extend_from_slice(c);
                    }
                }
                use parquet::{Column, Logical, Values};
                let cols = vec![
                    Column { name: "user_id", values: Values::Bytes(uid), defined: None, logical: Logical::String },
                    Column {
                        name: "day",
                        values: Values::I32(rows.iter().map(|r| r.day).collect()),
                        defined: None,
                        logical: Logical::Date,
                    },
                    Column {
                        name: "sessions",
                        values: Values::I32(rows.iter().map(|r| r.sessions as i32).collect()),
                        defined: None,
                        logical: Logical::None,
                    },
                    Column {
                        name: "events",
                        values: Values::I32(rows.iter().map(|r| r.events as i32).collect()),
                        defined: None,
                        logical: Logical::None,
                    },
                    Column {
                        name: "revenue_cents",
                        values: Values::I64(rows.iter().map(|r| r.revenue).collect()),
                        defined: None,
                        logical: Logical::None,
                    },
                    Column { name: "top_category", values: Values::Bytes(top), defined: Some(defined), logical: Logical::String },
                ];
                let bytes = parquet::encode(&cols, n);
                fs::write(ud_dir.join(format!("part-{ti}.parquet")), bytes).unwrap();
            });
        }
        let mut agg = vec![[0i64; 4]; ncols * ndays];
        for o in &results {
            for (a, b) in agg.iter_mut().zip(&o.camp) {
                for x in 0..4 {
                    a[x] += b[x];
                }
            }
        }
        let mut cid = Vec::new();
        let mut ch = Vec::new();
        let mut day = Vec::new();
        let mut v: [Vec<i64>; 4] = Default::default();
        let mut n = 0;
        for c in 0..ncols {
            for d in 0..ndays {
                let a = agg[c * ndays + d];
                if a[0] == 0 && a[3] == 0 {
                    continue;
                }
                let (id, chan): (&[u8], &[u8]) =
                    if c == ncamp { (b"organic", b"organic") } else { (&crows[c].0, &crows[c].1) };
                cid.extend_from_slice(&(id.len() as u32).to_le_bytes());
                cid.extend_from_slice(id);
                ch.extend_from_slice(&(chan.len() as u32).to_le_bytes());
                ch.extend_from_slice(chan);
                day.push((first_day + d as i64) as i32);
                for x in 0..4 {
                    v[x].push(a[x]);
                }
                n += 1;
            }
        }
        use parquet::{Column, Logical, Values};
        let [s0, s1, s2, s3] = v;
        let cols = vec![
            Column { name: "campaign_id", values: Values::Bytes(cid), defined: None, logical: Logical::String },
            Column { name: "channel", values: Values::Bytes(ch), defined: None, logical: Logical::String },
            Column { name: "day", values: Values::I32(day), defined: None, logical: Logical::Date },
            Column { name: "sessions", values: Values::I64(s0), defined: None, logical: Logical::None },
            Column { name: "purchasing_sessions", values: Values::I64(s1), defined: None, logical: Logical::None },
            Column { name: "revenue_cents", values: Values::I64(s2), defined: None, logical: Logical::None },
            Column { name: "assisted_sessions", values: Values::I64(s3), defined: None, logical: Logical::None },
        ];
        fs::write(cd_dir.join("part-0.parquet"), parquet::encode(&cols, n)).unwrap();
    });
    lap("written");
    unsafe { libc::_exit(0) };
}

fn finish_row(row: &UserRow, catsum: &mut Vec<(u32, i64)>, day: i32, first_day: i64) -> UserRow {
    let mut top = -1i32;
    let mut best = -1i64;
    for &(c, v) in catsum.iter() {
        if v > best || (v == best && (c as i32) < top) {
            best = v;
            top = c as i32;
        }
    }
    catsum.clear();
    UserRow {
        person: row.person,
        day: (first_day + day as i64) as i32,
        sessions: row.sessions,
        events: row.events,
        revenue: row.revenue,
        top,
    }
}
