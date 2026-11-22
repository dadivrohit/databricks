//! Probe mode (PROBE=1): measures the scoring machine and prints a SHORT report to stderr
//! (the platform only shows ~2 KB), then exits with an error on purpose so the dev run returns it.
use crate::fast::*;
use std::fmt::Write as _;
use std::time::Instant;

fn read(p: &str) -> String {
    std::fs::read_to_string(p).unwrap_or_default()
}
fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}
fn anon(bytes: usize, huge: bool) -> *mut u8 {
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED);
        if huge {
            libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
        }
        p as *mut u8
    }
}
fn touch(p: *mut u8, bytes: usize) {
    let mut i = 0;
    while i < bytes {
        unsafe { *p.add(i) = 1 };
        i += 4096;
    }
}
fn par<F: Fn(usize) + Sync>(n: usize, f: F) -> f64 {
    let t = Instant::now();
    std::thread::scope(|s| {
        for i in 0..n {
            let f = &f;
            s.spawn(move || f(i));
        }
    });
    ms(t)
}

/// Runs the pipeline as a child; returns "wall=start+parse+name+dedup+sess+write" in ms.
fn pipeline(data: &str, out: &str, n: usize) -> String {
    pipeline_env(data, out, n, &[])
}

fn pipeline_env(data: &str, out: &str, n: usize, extra: &[(&str, &str)]) -> String {
    let t = Instant::now();
    let mut cmd = std::process::Command::new("/proc/self/exe");
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let r = cmd
        .env_remove("PROBE")
        .env("TIMING", "1")
        .env("NCPU", n.to_string())
        .env("DATA_DIR", data)
        .env("OUTPUT_DIR", out)
        .output();
    let el = ms(t);
    let _ = std::fs::remove_dir_all(out);
    let Ok(o) = r else { return "ERR".into() };
    let err = String::from_utf8_lossy(&o.stderr);
    let at = |name: &str| -> f64 {
        err.lines()
            .find(|l| l.contains(name))
            .and_then(|l| l.split_whitespace().next())
            .and_then(|x| x.parse().ok())
            .unwrap_or(-1.0)
    };
    let (m, p, nm, d, s, w) = (at("mmap"), at("parsed"), at("named"), at("dedup"), at("sessions"), at("written"));
    format!("{el:.0}={m:.0}+{:.0}+{:.0}+{:.0}+{:.0}+{:.0}", p - m, nm - p, d - nm, s - d, w - s)
}

/// PROBE=3: generate a practice-sized dataset on this machine and time the pipeline on it.
fn run3() -> ! {
    let mut r = String::new();
    let cpu = read("/proc/cpuinfo");
    let model = cpu.lines().find(|l| l.starts_with("model name")).unwrap_or("").split(':').nth(1).unwrap_or("").trim().to_string();
    let _ = writeln!(r, "P3 cpu={model}");
    let scratch = std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into());
    let g = format!("{scratch}/g20");
    let _ = std::fs::remove_dir_all(&g);
    let t = Instant::now();
    let st = std::process::Command::new("/proc/self/exe")
        .env_remove("PROBE")
        .args(["generate", "--seed", "3", "--events", "20000000", "--clicks", "5000000", "--output", &g, "--threads", "8"])
        .output();
    let ok = st.as_ref().map(|o| o.status.success()).unwrap_or(false);
    let mut bytes = 0u64;
    for sub in ["events", "clicks", "links"] {
        if let Ok(rd) = std::fs::read_dir(format!("{g}/{sub}")) {
            bytes += rd.flatten().map(|e| e.metadata().map(|m| m.len()).unwrap_or(0)).sum::<u64>();
        }
    }
    let _ = writeln!(r, "gen20M ok={ok} {:.0}ms {}MB", ms(t), bytes >> 20);
    let out = format!("{scratch}/o3");
    let _ = writeln!(r, "ms wall=start+parse+name+dedup+sess+write");
    for (label, n, extra) in [
        ("base", 8usize, vec![]),
        ("pq11", 8, vec![("PBITS", "11"), ("QBITS", "11")]),
        ("lt12", 8, vec![("LTDIV", "12")]),
        ("c128", 8, vec![("CHUNKKB", "128")]),
        ("blwc", 8, vec![("BLWC", "1")]),
        ("base", 8, vec![]),
        ("pq11", 8, vec![("PBITS", "11"), ("QBITS", "11")]),
        ("lt12", 8, vec![("LTDIV", "12")]),
        ("c128", 8, vec![("CHUNKKB", "128")]),
        ("blwc", 8, vec![("BLWC", "1")]),
        ("pq9", 8, vec![("PBITS", "9"), ("QBITS", "9")]),
        ("base", 8, vec![]),
    ] {
        let _ = writeln!(r, "{label}:{}", pipeline_env(&g, &out, n, &extra));
    }
    let _ = writeln!(r, "END");
    eprint!("{r}");
    unsafe { libc::_exit(3) };
}

pub fn run() -> ! {
    if std::env::var("PROBE").as_deref() == Ok("3") {
        run3();
    }
    let mut r = String::new();
    let cpu = read("/proc/cpuinfo");
    let model = cpu.lines().find(|l| l.starts_with("model name")).unwrap_or("").split(':').nth(1).unwrap_or("").trim().to_string();
    let l3 = read("/sys/devices/system/cpu/cpu0/cache/index3/size");
    let thp = read("/sys/kernel/mm/transparent_hugepage/enabled");
    let thp = thp.split('[').nth(1).and_then(|x| x.split(']').next()).unwrap_or("?").to_string();
    let rel = read("/proc/sys/kernel/osrelease");
    let _ = writeln!(r, "P2 cpu={model} L3={} thp={thp} kern={}", l3.trim(), rel.trim());
    let data = std::env::var("DATA_DIR").unwrap_or("/data".into());
    let scratch = std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into());

    // 8x dataset from dev files (same IDs, so dedup removes 7/8; parse volume is realistic)
    let big = format!("{scratch}/big");
    let _ = std::fs::remove_dir_all(&big);
    let mut ev_files = Vec::new();
    for sub in ["events", "clicks", "links"] {
        let _ = std::fs::create_dir_all(format!("{big}/{sub}"));
        if let Ok(rd) = std::fs::read_dir(format!("{data}/{sub}")) {
            for e in rd.flatten() {
                if sub == "events" {
                    ev_files.push(e.path());
                }
                for k in 0..(if sub == "links" { 1 } else { 8 }) {
                    let _ = std::fs::copy(e.path(), format!("{big}/{sub}/r{k}_{}", e.file_name().to_string_lossy()));
                }
            }
        }
    }
    for f in ["products.csv", "campaigns.csv", "manifest.json"] {
        let _ = std::fs::copy(format!("{data}/{f}"), format!("{big}/{f}"));
    }
    let _ = pipeline(&big, &format!("{scratch}/o"), 8); // warm the cache
    let _ = write!(r, "BIG ms wall=start+parse+name+dedup+sess+write:");
    for n in [8usize, 8, 4, 2, 1] {
        let _ = write!(r, " n{n}:{}", pipeline(&big, &format!("{scratch}/o"), n));
    }
    let _ = writeln!(r);

    // fresh memory
    let gb = 1usize << 30;
    let _ = write!(r, "fresh1GB 8thr ms:");
    for huge in [false, true] {
        let per = gb / 8;
        let p = anon(gb, huge) as usize;
        let el = par(8, |i| touch((p + i * per) as *mut u8, per));
        let t = Instant::now();
        unsafe { libc::munmap(p as *mut _, gb) };
        let _ = write!(r, " huge={huge}:touch{el:.0}/unmap{:.0}", ms(t));
    }
    let _ = writeln!(r);

    // bandwidth + random lookups
    let buf = anon(gb, true) as usize;
    touch(buf as *mut u8, gb);
    let _ = write!(r, "readGB/s:");
    for n in [1usize, 4, 8] {
        let per = gb / n;
        let el = par(n, |i| {
            let p = (buf + i * per) as *const u64;
            let mut s = 0u64;
            for j in 0..per / 8 {
                s = s.wrapping_add(unsafe { *p.add(j) });
            }
            std::hint::black_box(s);
        });
        let _ = write!(r, " t{n}={:.1}", gb as f64 / el / 1e6);
    }
    let el = par(8, |i| unsafe {
        use std::arch::x86_64::*;
        let per = gb / 8;
        let p = (buf + i * per) as *mut __m512i;
        let v = _mm512_set1_epi64(7);
        for j in 0..per / 64 {
            _mm512_stream_si512(p.add(j) as *mut _, v);
        }
        _mm_sfence();
    });
    let _ = writeln!(r, " ntwrite8={:.1}", gb as f64 / el / 1e6);
    let _ = write!(r, "lookup ns(agg):");
    for (tbl, n) in [(8usize << 20, 8usize), (32 << 20, 8), (256 << 20, 8), (32 << 20, 1)] {
        let lookups = 4_000_000usize;
        let el = par(n, |i| {
            let p = buf as *const u64;
            let mask = tbl / 64 - 1;
            let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ i as u64;
            let mut s = 0u64;
            let mut idx = [0usize; 16];
            for k in 0..lookups / n {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let nxt = ((x >> 20) as usize & mask) * 8;
                unsafe {
                    std::arch::x86_64::_mm_prefetch(p.add(nxt) as *const i8, std::arch::x86_64::_MM_HINT_T0);
                    s = s.wrapping_add(*p.add(idx[k & 15]));
                }
                idx[k & 15] = nxt;
            }
            std::hint::black_box(s);
        });
        let _ = write!(r, " {}MB/t{n}={:.1}", tbl >> 20, el * 1e6 / lookups as f64);
    }
    let _ = writeln!(r);

    // input: pread copy vs mmap populate (8x event files, 8 threads)
    let mut big_ev: Vec<std::path::PathBuf> =
        std::fs::read_dir(format!("{big}/events")).map(|rd| rd.flatten().map(|e| e.path()).collect()).unwrap_or_default();
    big_ev.sort();
    let total: usize = big_ev.iter().map(|p| std::fs::metadata(p).map(|m| m.len() as usize).unwrap_or(0)).sum();
    let _ = write!(r, "input8 GB/s:");
    for mode in 0..3 {
        let next = std::sync::atomic::AtomicUsize::new(0);
        let el = par(8, |_| {
            let mut b = vec![0u8; 256 << 10];
            loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(path) = big_ev.get(i) else { break };
                let f = std::fs::File::open(path).unwrap();
                let len = f.metadata().unwrap().len() as usize;
                if mode == 0 {
                    use std::os::unix::fs::FileExt;
                    let mut off = 0;
                    while off < len {
                        let n = f.read_at(&mut b, off as u64).unwrap();
                        if n == 0 {
                            break;
                        }
                        std::hint::black_box(b[0]);
                        off += n;
                    }
                } else if len > 0 {
                    use std::os::unix::io::AsRawFd;
                    let flags = libc::MAP_PRIVATE | if mode == 2 { libc::MAP_POPULATE } else { 0 };
                    let p = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, flags, f.as_raw_fd(), 0) } as *const u8;
                    if mode == 1 {
                        unsafe { libc::madvise(p as *mut _, len, 22) };
                    }
                    let mut s = 0u64;
                    let mut j = 0;
                    while j < len {
                        s = s.wrapping_add(unsafe { *p.add(j) } as u64);
                        j += 4096;
                    }
                    std::hint::black_box(s);
                    unsafe { libc::munmap(p as *mut _, len) };
                }
            }
        });
        let _ = write!(r, " {}={:.1}", ["pread", "popread", "mappop"][mode], total as f64 / el / 1e6);
    }
    let _ = writeln!(r);

    // parse core (in memory)
    let blobs: Vec<Vec<u8>> = ev_files.iter().filter_map(|p| std::fs::read(p).ok()).filter(|b| b.len() > 1024).collect();
    let cal = Calendar::new();
    let _ = write!(r, "parse ns/line/thread:");
    for n in [1usize, 4, 8] {
        let lines = std::sync::atomic::AtomicUsize::new(0);
        let el = par(n, |_| {
            let mut sc = Scanner::new();
            let mut ts = Ts::new(&cal);
            let (mut acc, mut cnt) = (0u64, 0usize);
            for _ in 0..3 {
                for b in &blobs {
                    sc.scan::<7>(b, 0, b.len(), |p, f| unsafe {
                        let k = short_key(p.add(f[0].0), f[0].1 - f[0].0, b'e');
                        let u = short_key(p.add(f[1].0), f[1].1 - f[1].0, b'u');
                        let a = ts.ms(p.add(f[2].0), f[2].1 - f[2].0).unwrap_or(0);
                        let g = ts.ingest_secs(p.add(f[3].0), f[3].1 - f[3].0).unwrap_or(0);
                        acc = acc.wrapping_add(k ^ u ^ a as u64 ^ g as u64);
                        cnt += 1;
                    });
                }
            }
            std::hint::black_box(acc);
            lines.fetch_add(cnt, std::sync::atomic::Ordering::Relaxed);
        });
        let l = lines.load(std::sync::atomic::Ordering::Relaxed);
        let _ = write!(r, " t{n}={:.1}", el * 1e6 * n as f64 / l as f64);
    }
    let _ = writeln!(r, "\nEND");
    eprint!("{r}");
    unsafe { libc::_exit(3) };
}
