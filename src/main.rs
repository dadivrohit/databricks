//! Shopping Sessions: fast pipeline. See rules.md.
mod fast;
mod links;
mod parquet;
mod probe;
mod generator;

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

/// (offset, line) for every line (a trailing newline does not start a line).
fn line_slices(data: &[u8]) -> impl Iterator<Item = (usize, &[u8])> {
    let mut pos = 0usize;
    std::iter::from_fn(move || {
        if pos >= data.len() {
            return None;
        }
        let e = memchr::memchr(b'\n', &data[pos..]).map_or(data.len(), |i| pos + i);
        let r = (pos, &data[pos..e]);
        pos = e + 1;
        Some(r)
    })
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
    f: fs::File,
    len: usize,
    map: *const u8, // null when not mapped
}
unsafe impl Send for Mapped {}
unsafe impl Sync for Mapped {}

impl Mapped {
    fn open(path: &Path, mapped: bool) -> Mapped {
        use std::os::unix::io::AsRawFd;
        let f = fs::File::open(path).expect("open");
        let len = f.metadata().unwrap().len() as usize;
        let mut map = std::ptr::null();
        if mapped && len > 0 {
            let p = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0) };
            if p != libc::MAP_FAILED {
                map = p as *const u8;
            }
        }
        Mapped { f, len, map }
    }
    fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.map, self.len) }
    }
    /// Pre-load page tables for [start, end) (plus a tail) in one call.
    fn populate(&self, start: usize, end: usize) {
        let a = start & !4095;
        let e = (end + 8192).min(self.len);
        if e > a {
            unsafe { libc::madvise(self.map.add(a) as *mut libc::c_void, e - a, 22 /* MADV_POPULATE_READ */) };
        }
    }
    fn release(&self) {
        if !self.map.is_null() {
            unsafe { libc::munmap(self.map as *mut libc::c_void, self.len) };
        }
    }
}

/// Reads a chunk (plus the byte before it and the rest of its last line) into a reusable buffer.
struct ChunkReader {
    buf: Vec<u8>,
}

impl ChunkReader {
    fn new() -> Self {
        ChunkReader { buf: Vec::with_capacity(CHUNK + (64 << 10) + fast::SLACK) }
    }
    fn read_at(&mut self, m: &Mapped, at: usize, n: usize) {
        use std::os::unix::fs::FileExt;
        let old = self.buf.len();
        self.buf.resize(old + n, 0);
        let mut got = 0;
        while got < n {
            let r = m.f.read_at(&mut self.buf[old + got..old + n], (at + got) as u64).expect("read");
            if r == 0 {
                break;
            }
            got += r;
        }
        self.buf.truncate(old + got);
    }
    /// Returns (data, start, end) relative to data, for Scanner::scan.
    fn load(&mut self, m: &Mapped, start: usize, end: usize) -> (usize, usize) {
        let from = start.saturating_sub(1);
        self.buf.clear();
        let mut to = (end + 4096).min(m.len);
        self.read_at(m, from, to - from);
        // make sure the line that starts last before `end` is complete
        loop {
            let e_rel = end - from;
            let tail = &self.buf[e_rel.saturating_sub(1)..];
            if to >= m.len || memchr::memchr(b'\n', tail).is_some() {
                break;
            }
            let more = (64 << 10).min(m.len - to);
            self.read_at(m, to, more);
            to += more;
        }
        (start - from, end - from)
    }
    fn data(&self) -> &[u8] {
        &self.buf
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

const CHUNK: usize = 256 << 10;

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
    pc: u32, // price (low 24 bits, 0 = not a purchase) | category << 24
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
    // keys are already well spread (scrambled IDs, or interned indexes); one multiply is enough
    (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - P_BITS)) as usize
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

fn main() {
    if std::env::args().nth(1).as_deref() == Some("generate") {
        std::process::exit(match generator::main() {
            std::process::ExitCode::SUCCESS => 0,
            _ => 1,
        });
    }
    if std::env::var("PROBE").is_ok() {
        probe::run();
    }
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

    // products & categories (allocation-free per row)
    let prod_data = fs::read(data_dir.join("products.csv")).unwrap_or_default();
    let mut prod_rows: Vec<((usize, usize), (usize, usize))> = Vec::with_capacity(prod_data.len() / 16);
    {
        let mut fl = [(0usize, 0usize); 2];
        let mut first = true;
        for (off, line) in line_slices(&prod_data) {
            if first {
                first = false;
                continue;
            }
            if fast::split::<2>(line, &mut fl) {
                prod_rows.push(((off + fl[0].0, off + fl[0].1), (off + fl[1].0, off + fl[1].1)));
            }
        }
    }
    let pslice = |r: (usize, usize)| &prod_data[r.0..r.1];
    let mut cats: Vec<Vec<u8>> = Vec::new();
    {
        let mut seen: HashMap<&[u8], ()> = HashMap::new();
        for &(_, c) in &prod_rows {
            if seen.insert(pslice(c), ()).is_none() {
                cats.push(pslice(c).to_vec());
            }
        }
    }
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    assert!(cats.len() <= 256, "more than 256 categories are not supported");
    let cat_id = |s: &[u8]| cats.binary_search_by(|c| c.as_slice().cmp(s)).unwrap() as u32;
    let unknown_cat = cat_id(b"unknown");
    let mut products = Lookup::new(b"p");
    {
        let mut last: (&[u8], u32) = (&[], 0);
        let mut cache: HashMap<&[u8], u32> = HashMap::new();
        for &(pr, c) in &prod_rows {
            let cs = pslice(c);
            let id = if cs == last.0 {
                last.1
            } else {
                let i = *cache.entry(cs).or_insert_with(|| cat_id(cs));
                last = (cs, i);
                i
            };
            products.insert(pslice(pr), id);
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
    let use_mmap = std::env::var("MMAP").is_ok();
    let mut files: Vec<(Mapped, u8)> = Vec::new();
    for (sub, kind) in [("events", 0u8), ("clicks", 1u8), ("links", 2u8)] {
        for p in csv_files(&data_dir.join(sub)) {
            files.push((Mapped::open(&p, use_mmap && kind != 2), kind));
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
    let link_bytes: usize = files.iter().filter(|f| f.1 == 2).map(|f| f.0.len).sum();
    let lt = links::LinkTable::new(link_bytes / 12 + 1024);
    let next = AtomicUsize::new(nlink_chunks);
    let next_link = AtomicUsize::new(0);
    let barrier = std::sync::Barrier::new(nthreads);
    let exotic_slots: Mutex<Vec<u32>> = Mutex::new(Vec::new());
    struct Parsed {
        _arena: Arena,
        ev: Parts<EventRec>,
        cl: Parts<ClickRaw>,
    }
    let arena_cap = (total_event_bytes / nthreads) * 2 + (512 << 20);
    let parsed: Vec<Parsed> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|tid| {
                let (lt, barrier, exotic_slots, next_link, users) = (&lt, &barrier, &exotic_slots, &next_link, &users);
                let (chunks, files, next, cal, campaigns, products, ekeys, ckeys, t0) =
                    (&chunks, &files, &next, &cal, &campaigns, &products, &ekeys, &ckeys, &t0);
                s.spawn(move || {
                    // ---- links: insert keys and union them right away (lock-free); naming happens after parsing
                    let mut sc = Scanner::new();
                    let mut rd = ChunkReader::new();
                    let mut kpairs: Vec<(u64, u64)> = Vec::new();
                    loop {
                        let i = next_link.fetch_add(1, Ordering::Relaxed);
                        if i >= nlink_chunks {
                            break;
                        }
                        let c = chunks[i];
                        let (cs, ce) = rd.load(&files[c.file as usize].0, c.start, c.end);
                        sc.scan::<2>(rd.data(), cs, ce, |p, f| unsafe {
                            let (a, b) = (f[0], f[1]);
                            if a.0 == a.1 || b.0 == b.1 {
                                return;
                            }
                            let ka = key_of(p.add(a.0), a.1 - a.0, b'u', users);
                            let kb = key_of(p.add(b.0), b.1 - b.0, b'u', users);
                            kpairs.push((ka, kb));
                        });
                        for i in 0..kpairs.len() {
                            if i + 12 < kpairs.len() {
                                lt.prefetch(kpairs[i + 12].0);
                                lt.prefetch(kpairs[i + 12].1);
                            }
                            let (ka, kb) = kpairs[i];
                            lt.union(lt.insert(ka), lt.insert(kb));
                        }
                        kpairs.clear();
                    }
                    let _ = (barrier, exotic_slots, tid);
                    let mut arena = Arena::new(arena_cap);
                    let mut ev: WcParts<EventRec> = WcParts::new(NP);
                    let mut cl: Parts<ClickRaw> = Parts::new(1, fast::BLOCK_BYTES / std::mem::size_of::<ClickRaw>());
                    let mut ts = Ts::new(cal);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(c) = chunks.get(i) else { break };
                        let m = &files[c.file as usize].0;
                        let (data, cs, ce) = if m.map.is_null() {
                            let (cs, ce) = rd.load(m, c.start, c.end);
                            (rd.data(), cs, ce)
                        } else {
                            m.populate(c.start, c.end);
                            (m.bytes(), c.start, c.end)
                        };
                        match c.kind {
                            2 => unreachable!(),
                            1 => {
                                sc.scan::<4>(data, cs, ce, |p, f| unsafe {
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
                                    let ukey = key_of(p.add(fu.0), fu.1 - fu.0, b'u', users);
                                    cl.push(&mut arena, 0, ClickRaw { ukey, ckey, ts: (t - ws) as i32, camp });
                                });
                            }
                            _ => {
                                sc.scan::<7>(data, cs, ce, |p, f| unsafe {
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
                                    let ukey = if valid { key_of(p.add(f1.0), f1.1 - f1.0, b'u', users) } else { 0 };
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
                    ev.finish(&mut arena);
                    Parsed { _arena: arena, ev: ev.parts, cl }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("parsed (+union-find)");
    // ---- name each person (smallest ID) and point every linked ID at that name
    std::thread::scope(|s| {
        for tid in 0..nthreads {
            let (lt, barrier, exotic_slots, users) = (&lt, &barrier, &exotic_slots, &users);
            s.spawn(move || {
                let cap = lt.mask + 1;
                let (r0, r1) = (cap * tid / nthreads, cap * (tid + 1) / nthreads);
                let mut exo = Vec::new();
                for slot in r0..r1 {
                    let k = lt.slots[slot][0].load(Ordering::Relaxed);
                    if k == 0 {
                        continue;
                    }
                    if k & EXOTIC != 0 {
                        exo.push(slot as u32);
                    } else {
                        let r = lt.find(slot as u32) as usize;
                        lt.slots[r][1].fetch_max(!k, Ordering::Relaxed);
                    }
                }
                if !exo.is_empty() {
                    exotic_slots.lock().unwrap().extend(exo);
                }
                barrier.wait();
                if tid == 0 {
                    let ex = std::mem::take(&mut *exotic_slots.lock().unwrap());
                    if !ex.is_empty() {
                        let uk = Keys { prefix: b'u', exotic: users.snapshot() };
                        for slot in ex {
                            let k = lt.slots[slot as usize][0].load(Ordering::Relaxed);
                            let r = lt.find(slot) as usize;
                            let cur = !lt.slots[r][1].load(Ordering::Relaxed);
                            if cur == u64::MAX || uk.cmp(k, cur) == std::cmp::Ordering::Less {
                                lt.slots[r][1].store(!k, Ordering::Relaxed);
                            }
                        }
                    }
                }
                barrier.wait();
                for slot in r0..r1 {
                    if lt.slots[slot][0].load(Ordering::Relaxed) == 0 {
                        continue;
                    }
                    let r = lt.find(slot as u32) as usize;
                    if r != slot {
                        let name = lt.slots[r][1].load(Ordering::Relaxed);
                        lt.slots[slot][1].store(name, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    lap("named");

    // ---- dedup events per partition; route survivors and clicks to person buckets
    // each piece: 64 click blocks of one thread
    let click_pieces: Vec<(usize, usize, usize)> = {
        let mut v = Vec::new();
        for (t, p) in parsed.iter().enumerate() {
            let n = p.cl.block_ptrs(0).len();
            let mut s = 0;
            while s < n {
                let e = (s + 64).min(n);
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
                    let mut out: WcParts<PEv> = WcParts::new(NQ);
                    let mut outc: Parts<ClickRec> = Parts::new(NQ, fast::BLOCK_BYTES / std::mem::size_of::<ClickRec>());
                    let mut free: Vec<*mut u8> = Vec::new();
                    let mut buf: Vec<EventRec> = Vec::new();
                    let mut table: Vec<u32> = Vec::new();
                    let mut win: Vec<bool> = Vec::new();
                    let mut surv: Vec<u32> = Vec::new();
                    loop {
                        let w = next.fetch_add(1, Ordering::Relaxed);
                        // event partitions first (they free blocks), then click pieces (which reuse them)
                        if w >= NP && w - NP < click_pieces.len() {
                            let (t, a, b) = click_pieces[w - NP];
                            for src in parsed[t].cl.slices(0).skip(a).take(b - a) {
                            for (i, c) in src.iter().enumerate() {
                                if i + 16 < src.len() {
                                    lt.prefetch(src[i + 16].ukey);
                                }
                                let person = lt.get(c.ukey).unwrap_or(c.ukey);
                                outc.push_r(&mut arena, &mut free, pbucket(person), ClickRec { person, ckey: c.ckey, ts: c.ts, camp: c.camp });
                            }
                            }
                            continue;
                        }
                        if w >= NP {
                            break;
                        }
                        let p = w;
                        buf.clear();
                        for t in &parsed {
                            for sl in t.ev.slices(p) {
                                buf.extend_from_slice(sl);
                            }
                            free.extend(t.ev.block_ptrs(p).iter().map(|&b| b as *mut u8));
                        }
                        let n = buf.len();
                        if n == 0 {
                            continue;
                        }
                        let cap = (n * 2).next_power_of_two();
                        let mask = cap - 1;
                        table.clear();
                        table.resize(cap, 0);
                        win.clear();
                        win.resize(n, false);
                        for i in 0..n {
                            let r = unsafe { buf.get_unchecked(i) };
                            let mut h = (mix(r.ekey) as usize) & mask;
                            loop {
                                let slot = unsafe { table.get_unchecked_mut(h) };
                                if *slot == 0 {
                                    *slot = i as u32 + 1;
                                    win[i] = true;
                                    break;
                                }
                                let j = (*slot - 1) as usize;
                                if buf[j].ekey == r.ekey {
                                    if r.ingest < buf[j].ingest {
                                        win[j] = false;
                                        win[i] = true;
                                        *slot = i as u32 + 1;
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
                            if x + 16 < surv.len() {
                                lt.prefetch(buf[surv[x + 16] as usize].ukey);
                            }
                            let r = &buf[i as usize];
                            let person = lt.get(r.ukey).unwrap_or(r.ukey);
                            out.push_r(
                                &mut arena,
                                &mut free,
                                pbucket(person),
                                PEv { person, ts: r.ts, pc: r.price | ((r.info & 0xFF) << 24) },
                            );
                        }
                    }
                    out.finish(&mut arena);
                    Routed { _arena: arena, ev: out.parts, cl: outc }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("dedup+route");

    // ---- sessions & attribution per person bucket
    let ckeys_k = Keys { prefix: b'k', exotic: ckeys.snapshot() };
    // biggest buckets first, so a whale or shared-device person never runs last
    let mut order: Vec<(usize, usize)> = (0..NQ).map(|q| (routed.iter().map(|t| t.ev.len(q)).sum(), q)).collect();
    order.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let order = &order;
    let next = AtomicUsize::new(0);
    struct Out {
        rows: Vec<UserRow>,
        camp: Vec<[i64; 3]>, // [day * ncols + campaign]: sessions, purchasing, revenue
        assist: Vec<u32>,    // [day * ncols + campaign]
    }
    let ncols = ncamp + 1; // last = organic
    let results: Vec<Out> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut o = Out { rows: Vec::new(), camp: vec![[0; 3]; ncols * ndays], assist: vec![0; ncols * ndays] };
                    let mut evs: Vec<(u64, u32, u32)> = Vec::new(); // (idx<<32|ts, price, cat)
                    let mut cls: Vec<(u64, u64, u32)> = Vec::new(); // (idx<<32|ts', ckey, camp)
                    let mut pkeys: Vec<u64> = Vec::new();
                    let mut ltab: Vec<u32> = Vec::new();
                    let mut count: Vec<u32> = vec![0; ncamp];
                    let mut active: Vec<u32> = Vec::new();
                    let mut pos: Vec<u32> = vec![0; ncamp];
                    let mut catsum: Vec<(u32, i64)> = Vec::new();
                    loop {
                        let qi = next.fetch_add(1, Ordering::Relaxed);
                        if qi >= NQ {
                            break;
                        }
                        let (n, q) = order[qi];
                        if n == 0 {
                            continue;
                        }
                        let cap = (n * 2).next_power_of_two();
                        let mask = cap - 1;
                        ltab.clear();
                        ltab.resize(cap, 0);
                        pkeys.clear();
                        evs.clear();
                        for t in &routed {
                            for sl in t.ev.slices(q) {
                                for e in sl {
                                    let mut h = (mix(e.person) as usize) & mask;
                                    let idx = loop {
                                        let slot = unsafe { *ltab.get_unchecked(h) };
                                        if slot == 0 {
                                            ltab[h] = pkeys.len() as u32 + 1;
                                            pkeys.push(e.person);
                                            break pkeys.len() as u32 - 1;
                                        }
                                        if unsafe { *pkeys.get_unchecked(slot as usize - 1) } == e.person {
                                            break slot - 1;
                                        }
                                        h = (h + 1) & mask;
                                    };
                                    evs.push(((idx as u64) << 32 | e.ts as u64, e.pc & 0xFF_FFFF, e.pc >> 24));
                                }
                            }
                        }
                        evs.sort_unstable_by_key(|e| e.0);
                        cls.clear();
                        for t in &routed {
                            for sl in t.cl.slices(q) {
                                for c in sl {
                                    let mut h = (mix(c.person) as usize) & mask;
                                    let idx = loop {
                                        let slot = unsafe { *ltab.get_unchecked(h) };
                                        if slot == 0 {
                                            break -1i64;
                                        }
                                        if unsafe { *pkeys.get_unchecked(slot as usize - 1) } == c.person {
                                            break (slot - 1) as i64;
                                        }
                                        h = (h + 1) & mask;
                                    };
                                    if idx >= 0 {
                                        let tsu = (c.ts as u32) ^ 0x8000_0000;
                                        cls.push(((idx as u64) << 32 | tsu as u64, c.ckey, c.camp));
                                    }
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
                                let cell = &mut o.camp[dayu * ncols + credited];
                                cell[0] += 1;
                                cell[1] += purchases as i64;
                                cell[2] += rev;
                                let arow = &mut o.assist[dayu * ncols..(dayu + 1) * ncols];
                                for &a in &active {
                                    unsafe { *arow.get_unchecked_mut(a as usize) += 1 };
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
        for a in parsed.iter().map(|p| &p._arena).chain(routed.iter().map(|r| &r._arena)) {
            s.spawn(move || a.release());
        }
        let files = &files;
        for t in 0..nthreads {
            s.spawn(move || {
                for (i, f) in files.iter().enumerate() {
                    if i % nthreads == t {
                        f.0.release();
                    }
                }
            });
        }
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
                parquet::write_file(&ud_dir.join(format!("part-{ti}.parquet")), &cols, n).unwrap();
            });
        }
        let mut agg = vec![[0i64; 4]; ncols * ndays]; // [campaign * ndays + day]
        for o in &results {
            for d in 0..ndays {
                for c in 0..ncols {
                    let b = o.camp[d * ncols + c];
                    let a = &mut agg[c * ndays + d];
                    a[0] += b[0];
                    a[1] += b[1];
                    a[2] += b[2];
                    a[3] += o.assist[d * ncols + c] as i64;
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
        parquet::write_file(&cd_dir.join("part-0.parquet"), &cols, n).unwrap();
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
