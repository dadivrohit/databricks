//! Shopping Sessions: fast pipeline. See rules.md.
mod parquet;

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
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Open-addressing u64 -> u64 map; key 0 is reserved as empty.
struct U64Map {
    keys: Vec<u64>,
    vals: Vec<u64>,
    mask: usize,
    len: usize,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1, len: 0 }
    }
    #[inline]
    fn get(&self, k: u64) -> Option<u64> {
        let mut i = (mix(k) as usize) & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if kk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Returns the value for k, inserting `v` if absent.
    fn get_or_insert(&mut self, k: u64, v: u64) -> u64 {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = (mix(k) as usize) & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                return self.vals[i];
            }
            if kk == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                self.len += 1;
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn set(&mut self, k: u64, v: u64) {
        let mut i = (mix(k) as usize) & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k || kk == 0 {
                if kk == 0 {
                    self.len += 1;
                }
                self.keys[i] = k;
                self.vals[i] = v;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old_k = std::mem::take(&mut self.keys);
        let old_v = std::mem::take(&mut self.vals);
        let cap = old_k.len() * 2;
        self.keys = vec![0; cap];
        self.vals = vec![0; cap];
        self.mask = cap - 1;
        self.len = 0;
        for (k, v) in old_k.into_iter().zip(old_v) {
            if k != 0 {
                self.set(k, v);
            }
        }
    }
}

/// Strings that do not fit the compact ID form get a global index.
struct Interner {
    inner: Mutex<(HashMap<Vec<u8>, u32>, Vec<Vec<u8>>)>,
}

impl Interner {
    fn new() -> Self {
        Interner { inner: Mutex::new((HashMap::new(), Vec::new())) }
    }
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
fn sym(c: u8) -> u64 {
    // '0'..'9' -> 1..10, 'a'..'z' -> 11..36, else 0 (invalid)
    let d = c.wrapping_sub(b'0');
    if d < 10 {
        return d as u64 + 1;
    }
    let l = c.wrapping_sub(b'a');
    if l < 26 {
        return l as u64 + 11;
    }
    0
}

/// prefix + 1..8 chars of [0-9a-z] -> order-preserving 48-bit key (nonzero).
#[inline(always)]
fn pack(b: &[u8], prefix: u8) -> Option<u64> {
    let n = b.len();
    if !(2..=9).contains(&n) || b[0] != prefix {
        return None;
    }
    let mut k = 0u64;
    for i in 1..9 {
        let s = if i < n {
            let s = sym(b[i]);
            if s == 0 {
                return None;
            }
            s
        } else {
            0
        };
        k = (k << 6) | s;
    }
    Some(k)
}

fn unpack(k: u64, prefix: u8, out: &mut Vec<u8>) {
    out.push(prefix);
    for i in 0..8 {
        let s = (k >> (6 * (7 - i))) & 63;
        if s == 0 {
            break;
        }
        out.push(if s <= 10 { b'0' + (s - 1) as u8 } else { b'a' + (s - 11) as u8 });
    }
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
            unpack(k, self.prefix, out);
        }
    }
    fn cmp(&self, a: u64, b: u64) -> std::cmp::Ordering {
        if (a | b) & EXOTIC == 0 {
            return a.cmp(&b);
        }
        if a == b {
            return std::cmp::Ordering::Equal;
        }
        let mut x = Vec::new();
        let mut y = Vec::new();
        self.bytes(a, &mut x);
        self.bytes(b, &mut y);
        x.cmp(&y)
    }
}

// ---------------------------------------------------------------- CSV

/// Splits a line into exactly N fields, stripping whole-field quotes.
/// Returns false if malformed.
#[inline(always)]
fn split<const N: usize>(line: &[u8], f: &mut [(u32, u32); N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut field = 0usize;
    let mut start = 0usize;
    let n = line.len();
    let mut i = 0;
    loop {
        // find end of field
        let mut j = i;
        let mut quotes = 0u32;
        while j < n {
            let c = line[j];
            if c == b',' {
                break;
            }
            quotes += (c == b'"') as u32;
            j += 1;
        }
        // field = line[start..j]
        let (mut s, mut e) = (start, j);
        if quotes != 0 {
            if quotes != 2 || e - s < 2 || line[s] != b'"' || line[e - 1] != b'"' {
                return false;
            }
            s += 1;
            e -= 1;
        }
        if field >= N {
            return false;
        }
        f[field] = (s as u32, e as u32);
        field += 1;
        if j >= n {
            break;
        }
        i = j + 1;
        start = i;
    }
    field == N
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 { Some(a as i64 * 10 + c as i64) } else { None }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn civil(b: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    const MD: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let md = if mo == 2 && leap { 29 } else { MD[(mo - 1) as usize] };
    if d > md {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000)
}

/// Epoch ms, or None if bad. Values beyond i64 are None (outside any window).
#[inline(always)]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 && b[19] == b'Z' {
        return civil(b);
    }
    if n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base = civil(b)?;
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if b[19] == b'+' { base - off } else { base + off });
    }
    if n == 0 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

/// digits only, value 1..=10_000_000
#[inline(always)]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // keep scanning only to reject non-digits; value already too big
            for &c2 in b {
                if c2.wrapping_sub(b'0') >= 10 {
                    return None;
                }
            }
            return None;
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

/// "<prefix><digits without leading zero>" -> number, if canonical and small.
#[inline(always)]
fn canon_num(b: &[u8], prefix: &[u8]) -> Option<usize> {
    if b.len() <= prefix.len() || &b[..prefix.len()] != prefix || b.len() > prefix.len() + 7 {
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
    fn new(prefix: &'static [u8], entries: &[(Vec<u8>, u32)]) -> Self {
        let mut arr = Vec::new();
        let mut map = HashMap::new();
        for (s, id) in entries {
            match canon_num(s, prefix) {
                Some(n) if n < 4_000_000 => {
                    if arr.len() <= n {
                        arr.resize(n + 1, 0);
                    }
                    arr[n] = id + 1;
                }
                _ => {
                    map.insert(s.clone(), *id);
                }
            }
        }
        Lookup { prefix, arr, map }
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> Option<u32> {
        if let Some(n) = canon_num(s, self.prefix) {
            if n < 4_000_000 {
                // canonical strings never live in the map
                return match self.arr.get(n) {
                    Some(&v) if v != 0 => Some(v - 1),
                    _ => None,
                };
            }
        }
        self.map.get(s).copied()
    }
}

fn read_small_csv(path: &Path, n: usize) -> Vec<Vec<Vec<u8>>> {
    let data = fs::read(path).unwrap_or_default();
    let mut rows = Vec::new();
    let mut lines = data.split(|&c| c == b'\n');
    lines.next();
    let mut f = vec![(0u32, 0u32); n];
    for line in lines {
        let ok = match n {
            2 => {
                let mut a = [(0, 0); 2];
                let r = split::<2>(line, &mut a);
                f.copy_from_slice(&a);
                r
            }
            _ => unreachable!(),
        };
        if ok {
            rows.push(f.iter().map(|&(s, e)| line[s as usize..e as usize].to_vec()).collect());
        }
    }
    rows
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
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE | libc::MAP_POPULATE,
                f.as_raw_fd(),
                0,
            )
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

const CHUNK: usize = 4 << 20;

fn make_chunks(files: &[(Mapped, u8)], kinds: &[u8]) -> Vec<Chunk> {
    let mut v = Vec::new();
    for (i, (m, k)) in files.iter().enumerate() {
        if !kinds.contains(k) {
            continue;
        }
        let mut s = 0;
        while s < m.len {
            let e = (s + CHUNK).min(m.len);
            v.push(Chunk { file: i as u32, kind: *k, start: s, end: e });
            s = e;
        }
    }
    // big chunks first is irrelevant (all same size); keep order
    v
}

/// Calls f(line) for every line starting in [start, end) (header skipped).
#[inline(always)]
fn for_lines(data: &[u8], start: usize, end: usize, mut f: impl FnMut(&[u8])) {
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
    while pos < end {
        let e = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => data.len(),
        };
        f(&data[pos..e]);
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
    info: u64,   // price:24 | cat:16 <<24 | type:2 <<40 | valid <<42
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PEv {
    person: u64,
    ts: u32,
    price: u32, // 0 unless purchase
    cat: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct ClickRec {
    person: u64,
    ckey: u64,
    ts: i32, // ms relative to window start
    camp: u32,
}

const P_BITS: u32 = 9; // event partitions
const Q_BITS: u32 = 9; // person buckets
const NP: usize = 1 << P_BITS;
const NQ: usize = 1 << Q_BITS;

#[inline(always)]
fn epart(k: u64) -> usize {
    (mix(k) >> (64 - P_BITS)) as usize
}
#[inline(always)]
fn pbucket(k: u64) -> usize {
    (mix(k ^ 0x5555) >> (64 - Q_BITS)) as usize
}

struct UserRow {
    person: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: i32, // -1 = null
}

// ---------------------------------------------------------------- main

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data_dir = PathBuf::from(if args.len() >= 3 { args[1].clone() } else { env("DATA_DIR", "/data") });
    let out_dir = PathBuf::from(if args.len() >= 3 { args[2].clone() } else { env("OUTPUT_DIR", "/output") });
    let nthreads: usize = env("NCPU", "0").parse().ok().filter(|&n| n > 0).unwrap_or_else(|| {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
    });
    let timing = std::env::var("TIMING").is_ok();
    let lap = |what: &str| {
        if timing {
            eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1e3, what);
        }
    };

    // manifest
    let manifest = fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let grab = |key: &str| -> i64 {
        let i = manifest.find(key).expect("key");
        let rest = &manifest[i + key.len()..];
        let q1 = rest.find('"').unwrap();
        let rest = &rest[q1 + 1..];
        let q2 = rest.find('"').unwrap();
        parse_ts(rest[..q2].as_bytes()).expect("window ts")
    };
    let ws = grab("\"window_start\"");
    let we = grab("\"window_end\"");
    let first_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - first_day + 1).max(1) as usize;

    // products & categories
    let prows = read_small_csv(&data_dir.join("products.csv"), 2);
    let mut cats: Vec<Vec<u8>> = prows.iter().map(|r| r[1].clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let cat_id = |s: &[u8]| cats.binary_search_by(|c| c.as_slice().cmp(s)).unwrap() as u32;
    let unknown_cat = cat_id(b"unknown");
    let prod_entries: Vec<(Vec<u8>, u32)> = prows.iter().map(|r| (r[0].clone(), cat_id(&r[1]))).collect();
    let products = Lookup::new(b"p", &prod_entries);
    // campaigns
    let crows = read_small_csv(&data_dir.join("campaigns.csv"), 2);
    let camp_entries: Vec<(Vec<u8>, u32)> =
        crows.iter().enumerate().map(|(i, r)| (r[0].clone(), i as u32)).collect();
    let campaigns = Lookup::new(b"c_", &camp_entries);
    let ncamp = crows.len();
    lap("lookups");

    // map files
    let mut files: Vec<(Mapped, u8)> = Vec::new();
    for (sub, kind) in [("events", 0u8), ("clicks", 1u8), ("links", 2u8)] {
        for p in csv_files(&data_dir.join(sub)) {
            files.push((Mapped::open(&p), kind));
        }
    }
    lap("mmap");

    let users = Interner::new();
    let ekeys = Interner::new();
    let ckeys = Interner::new();
    let ukey_of = |b: &[u8]| -> u64 { pack(b, b'u').unwrap_or_else(|| users.intern(b)) };

    // ---- links
    let link_chunks = make_chunks(&files, &[2]);
    let next = AtomicUsize::new(0);
    let link_pairs: Vec<Vec<(u64, u64)>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut out = Vec::new();
                    let mut f = [(0u32, 0u32); 2];
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(c) = link_chunks.get(i) else { break };
                        let data = files[c.file as usize].0.bytes();
                        for_lines(data, c.start, c.end, |line| {
                            if !split::<2>(line, &mut f) {
                                return;
                            }
                            let a = &line[f[0].0 as usize..f[0].1 as usize];
                            let b = &line[f[1].0 as usize..f[1].1 as usize];
                            if a.is_empty() || b.is_empty() {
                                return;
                            }
                            out.push((ukey_of(a), ukey_of(b)));
                        });
                    }
                    out
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("links parsed");

    // union-find over linked keys
    let total_pairs: usize = link_pairs.iter().map(|v| v.len()).sum();
    let mut idx = U64Map::with_capacity(total_pairs * 2 + 16);
    let mut node_keys: Vec<u64> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
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
    for v in &link_pairs {
        for &(a, b) in v {
            let ia = idx.get_or_insert(a, node_keys.len() as u64) as u32;
            if ia as usize == node_keys.len() {
                node_keys.push(a);
                parent.push(ia);
            }
            let ib = idx.get_or_insert(b, node_keys.len() as u64) as u32;
            if ib as usize == node_keys.len() {
                node_keys.push(b);
                parent.push(ib);
            }
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    let user_keys_snapshot = Keys { prefix: b'u', exotic: users.snapshot() };
    let n = node_keys.len();
    let mut name: Vec<u64> = vec![u64::MAX; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let k = node_keys[i];
        if name[r] == u64::MAX || user_keys_snapshot.cmp(k, name[r]) == std::cmp::Ordering::Less {
            name[r] = k;
        }
    }
    let mut link_map = U64Map::with_capacity(n + 16);
    for i in 0..n {
        let r = parent[i] as usize; // fully compressed by the loop above
        link_map.set(node_keys[i], name[r]);
    }
    drop(link_pairs);
    let link_map = &link_map;
    let resolve = |k: u64| -> u64 { link_map.get(k).unwrap_or(k) };
    lap("union-find");

    // ---- events + clicks parse
    let chunks = make_chunks(&files, &[0, 1]);
    let total_event_bytes: usize = files.iter().filter(|f| f.1 == 0).map(|f| f.0.len).sum();
    let est_events = total_event_bytes / 70 + 1024;
    let per_part = est_events / (NP * nthreads) * 5 / 4 + 16;
    let next = AtomicUsize::new(0);
    let ws_s = ws.div_euclid(1000);
    type Parsed = (Vec<Vec<EventRec>>, Vec<Vec<ClickRec>>);
    let parsed: Vec<Parsed> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut ev: Vec<Vec<EventRec>> = (0..NP).map(|_| Vec::with_capacity(per_part)).collect();
                    let mut cl: Vec<Vec<ClickRec>> = (0..NQ).map(|_| Vec::new()).collect();
                    let mut f7 = [(0u32, 0u32); 7];
                    let mut f4 = [(0u32, 0u32); 4];
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(c) = chunks.get(i) else { break };
                        let data = files[c.file as usize].0.bytes();
                        if c.kind == 0 {
                            for_lines(data, c.start, c.end, |line| {
                                if !split::<7>(line, &mut f7) {
                                    return;
                                }
                                let fld = |j: usize| &line[f7[j].0 as usize..f7[j].1 as usize];
                                let eid = fld(0);
                                if eid.is_empty() {
                                    return;
                                }
                                let ekey = pack(eid, b'e').unwrap_or_else(|| ekeys.intern(eid));
                                let ingest_ms = parse_ts(fld(3)).unwrap_or(i64::MAX / 2);
                                let ingest = (ingest_ms.div_euclid(1000) - ws_s).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                                let uid = fld(1);
                                let mut valid = !uid.is_empty();
                                let ukey = if valid { ukey_of(uid) } else { 0 };
                                let mut tsr = 0u32;
                                match parse_ts(fld(2)) {
                                    Some(t) if t >= ws && t < we => {
                                        tsr = (t - ws) as u32;
                                        // late
                                        if ingest_ms - t > DAY {
                                            valid = false;
                                        }
                                    }
                                    _ => valid = false,
                                }
                                let et = fld(4);
                                let ty: u64 = match et {
                                    b"view" => 0,
                                    b"cart" => 1,
                                    b"purchase" => 2,
                                    _ => {
                                        valid = false;
                                        3
                                    }
                                };
                                let pid = fld(5);
                                if pid.is_empty() {
                                    valid = false;
                                }
                                let mut price = 0u64;
                                let mut cat = 0u64;
                                if ty == 2 && valid {
                                    match parse_price(fld(6)) {
                                        Some(p) => {
                                            price = p as u64;
                                            cat = products.get(pid).unwrap_or(unknown_cat) as u64;
                                        }
                                        None => valid = false,
                                    }
                                }
                                let info = price | (cat << 24) | (ty << 40) | ((valid as u64) << 42);
                                ev[epart(ekey)].push(EventRec { ekey, ukey, ingest, ts: tsr, info });
                            });
                        } else {
                            for_lines(data, c.start, c.end, |line| {
                                if !split::<4>(line, &mut f4) {
                                    return;
                                }
                                let fld = |j: usize| &line[f4[j].0 as usize..f4[j].1 as usize];
                                let cid = fld(0);
                                if cid.is_empty() {
                                    return;
                                }
                                let uid = fld(1);
                                if uid.is_empty() {
                                    return;
                                }
                                let t = match parse_ts(fld(3)) {
                                    Some(t) if t >= ws - WEEK && t < we => t,
                                    _ => return,
                                };
                                let Some(camp) = campaigns.get(fld(2)) else { return };
                                let ckey = pack(cid, b'k').unwrap_or_else(|| ckeys.intern(cid));
                                let person = resolve(ukey_of(uid));
                                cl[pbucket(person)].push(ClickRec { person, ckey, ts: (t - ws) as i32, camp });
                            });
                        }
                    }
                    (ev, cl)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("events+clicks parsed");

    // users interned during parse are needed for resolve? No: resolve uses keys only.
    // ---- dedup events per partition, route survivors to person buckets
    let next = AtomicUsize::new(0);
    let routed: Vec<Vec<Vec<PEv>>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut out: Vec<Vec<PEv>> = (0..NQ).map(|_| Vec::new()).collect();
                    let mut buf: Vec<EventRec> = Vec::new();
                    loop {
                        let p = next.fetch_add(1, Ordering::Relaxed);
                        if p >= NP {
                            break;
                        }
                        buf.clear();
                        for t in &parsed {
                            buf.extend_from_slice(&t.0[p]);
                        }
                        buf.sort_unstable_by_key(|r| (r.ekey, r.ingest));
                        let mut i = 0;
                        while i < buf.len() {
                            let r = buf[i];
                            let mut j = i + 1;
                            while j < buf.len() && buf[j].ekey == r.ekey {
                                j += 1;
                            }
                            i = j;
                            if r.info >> 42 & 1 == 0 {
                                continue;
                            }
                            let person = resolve(r.ukey);
                            let ty = (r.info >> 40) & 3;
                            let price = if ty == 2 { (r.info & 0xFF_FFFF) as u32 } else { 0 };
                            out[pbucket(person)].push(PEv {
                                person,
                                ts: r.ts,
                                price,
                                cat: ((r.info >> 24) & 0xFFFF) as u32,
                            });
                        }
                    }
                    out
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
        camp: Vec<[i64; 4]>, // [(camp)*ndays + day] -> sessions, purchasing, revenue, assisted
    }
    let ncols = ncamp + 1; // last = organic
    let results: Vec<Out> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut o = Out { rows: Vec::new(), camp: vec![[0; 4]; ncols * ndays] };
                    let mut evs: Vec<PEv> = Vec::new();
                    let mut cls: Vec<ClickRec> = Vec::new();
                    let mut count: Vec<u32> = vec![0; ncamp];
                    let mut active: Vec<u32> = Vec::new();
                    let mut pos: Vec<u32> = vec![0; ncamp];
                    let mut catsum: Vec<(u32, i64)> = Vec::new();
                    loop {
                        let q = next.fetch_add(1, Ordering::Relaxed);
                        if q >= NQ {
                            break;
                        }
                        evs.clear();
                        for t in &routed {
                            evs.extend_from_slice(&t[q]);
                        }
                        evs.sort_unstable_by_key(|e| (e.person, e.ts));
                        cls.clear();
                        for t in &parsed {
                            cls.extend_from_slice(&t.1[q]);
                        }
                        cls.sort_unstable_by(|a, b| {
                            (a.person, a.ts)
                                .cmp(&(b.person, b.ts))
                                .then_with(|| ckeys_k.cmp(a.ckey, b.ckey))
                        });
                        cls.dedup_by(|b, a| a.ckey == b.ckey && a.person == b.person);
                        let mut ci = 0usize;
                        let mut i = 0usize;
                        while i < evs.len() {
                            let person = evs[i].person;
                            let mut j = i;
                            while j < evs.len() && evs[j].person == person {
                                j += 1;
                            }
                            // clicks of this person
                            while ci < cls.len() && cls[ci].person < person {
                                ci += 1;
                            }
                            let c0 = ci;
                            while ci < cls.len() && cls[ci].person == person {
                                ci += 1;
                            }
                            let pc = &cls[c0..ci];
                            let (mut lo, mut hi) = (0usize, 0usize);
                            // walk sessions
                            let mut k = i;
                            let mut cur_day: i32 = i32::MIN;
                            let mut row = UserRow { person, day: 0, sessions: 0, events: 0, revenue: 0, top: -1 };
                            catsum.clear();
                            while k < j {
                                let start = evs[k].ts as i64;
                                let day = ((ws + start).div_euclid(DAY) - first_day) as i32;
                                if day != cur_day {
                                    if cur_day != i32::MIN {
                                        o.rows.push(finish_row(&mut row, &mut catsum, cur_day, first_day));
                                        row = UserRow { person, day: 0, sessions: 0, events: 0, revenue: 0, top: -1 };
                                    }
                                    cur_day = day;
                                }
                                let mut prev = start;
                                let mut m = k;
                                let mut rev = 0i64;
                                let mut purchases = false;
                                while m < j && (evs[m].ts as i64) - prev <= GAP {
                                    let e = evs[m];
                                    prev = e.ts as i64;
                                    if e.price > 0 {
                                        purchases = true;
                                        rev += e.price as i64;
                                        match catsum.iter_mut().find(|x| x.0 == e.cat) {
                                            Some(x) => x.1 += e.price as i64,
                                            None => catsum.push((e.cat, e.price as i64)),
                                        }
                                    }
                                    m += 1;
                                }
                                let nev = (m - k) as u32;
                                row.sessions += 1;
                                row.events += nev;
                                row.revenue += rev;
                                // attribution window
                                while hi < pc.len() && (pc[hi].ts as i64) <= start {
                                    let c = pc[hi].camp as usize;
                                    if count[c] == 0 {
                                        pos[c] = active.len() as u32;
                                        active.push(c as u32);
                                    }
                                    count[c] += 1;
                                    hi += 1;
                                }
                                while lo < hi && (pc[lo].ts as i64) < start - WEEK {
                                    let c = pc[lo].camp as usize;
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
                                    let t = pc[hi - 1].ts;
                                    let mut x = hi - 1;
                                    while x > lo && pc[x - 1].ts == t {
                                        x -= 1;
                                    }
                                    pc[x].camp as usize
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
                                o.rows.push(finish_row(&mut row, &mut catsum, cur_day, first_day));
                            }
                            // reset campaign window state
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
                        values: Values::I64(rows.iter().map(|r| r.sessions as i64).collect()),
                        defined: None,
                        logical: Logical::None,
                    },
                    Column {
                        name: "events",
                        values: Values::I64(rows.iter().map(|r| r.events as i64).collect()),
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
        // campaign_daily
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
                    if c == ncamp { (b"organic", b"organic") } else { (&crows[c][0], &crows[c][1]) };
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
    // Skip destructors and unmapping.
    unsafe { libc::_exit(0) };
}

fn finish_row(row: &mut UserRow, catsum: &mut Vec<(u32, i64)>, day: i32, first_day: i64) -> UserRow {
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
