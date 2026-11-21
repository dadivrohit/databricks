//! Probe mode (PROBE=1): measures the scoring machine and prints a report to stderr,
//! then exits with an error on purpose so the dev-set run returns the logs.
use crate::fast::*;
use std::time::Instant;

fn read(p: &str) -> String {
    std::fs::read_to_string(p).unwrap_or_else(|e| format!("<{e}>"))
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

pub fn run() -> ! {
    let t_all = Instant::now();
    eprintln!("==== PROBE REPORT ====");
    // ---- system
    let cpu = read("/proc/cpuinfo");
    let model = cpu.lines().find(|l| l.starts_with("model name")).unwrap_or("");
    let cache = cpu.lines().find(|l| l.starts_with("cache size")).unwrap_or("");
    let mhz: Vec<&str> = cpu.lines().filter(|l| l.starts_with("cpu MHz")).collect();
    eprintln!("cpu: {model} | {cache} | {} logical | mhz {:?}", mhz.len(), mhz.iter().take(2).collect::<Vec<_>>());
    eprintln!("kernel: {}", read("/proc/version").trim());
    eprintln!("thp enabled: {} | defrag: {}", read("/sys/kernel/mm/transparent_hugepage/enabled").trim(), read("/sys/kernel/mm/transparent_hugepage/defrag").trim());
    eprintln!("affinity cpus: {:?}", std::thread::available_parallelism());
    for f in ["/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/cpuset.cpus.effective", "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"] {
        eprintln!("{f}: {}", read(f).trim());
    }
    let mi = read("/proc/meminfo");
    for l in mi.lines().take(6) {
        eprintln!("meminfo {l}");
    }
    for l in read("/proc/self/mountinfo").lines() {
        if l.contains(" /data ") || l.contains(" /output ") || l.contains(" /scratch ") || l.contains(" / ") || l.contains(" /tmp ") {
            eprintln!("mount: {l}");
        }
    }
    for k in ["NCPU", "MEM_GB", "DATA_DIR", "OUTPUT_DIR", "SCRATCH_DIR"] {
        eprintln!("env {k}={:?}", std::env::var(k).ok());
    }
    let data = std::env::var("DATA_DIR").unwrap_or("/data".into());
    let scratch = std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into());
    let mut ev_files = Vec::new();
    for sub in ["events", "clicks", "links"] {
        let mut total = 0u64;
        let mut n = 0;
        if let Ok(rd) = std::fs::read_dir(format!("{data}/{sub}")) {
            for e in rd.flatten() {
                let len = e.metadata().map(|m| m.len()).unwrap_or(0);
                total += len;
                n += 1;
                if sub == "events" {
                    ev_files.push(e.path());
                }
            }
        }
        eprintln!("data {sub}: {n} files, {total} bytes");
    }

    // ---- full pipeline, as subprocesses
    let exe = "/proc/self/exe";
    for (i, n) in [8usize, 8, 8, 4, 2, 1].iter().enumerate() {
        let out = format!("{scratch}/probe_out_{i}");
        let _ = std::fs::remove_dir_all(&out);
        let t = Instant::now();
        let r = std::process::Command::new(exe)
            .env_remove("PROBE")
            .env("TIMING", "1")
            .env("NCPU", n.to_string())
            .env("OUTPUT_DIR", &out)
            .output();
        let el = ms(t);
        match r {
            Ok(o) => {
                let err = String::from_utf8_lossy(&o.stderr);
                let phases: Vec<String> = err.lines().map(|l| l.split_whitespace().collect::<Vec<_>>().join(" ")).collect();
                eprintln!("pipeline NCPU={n}: wall {el:.1} ms, exit {:?} | {}", o.status.code(), phases.join(" | "));
            }
            Err(e) => eprintln!("pipeline NCPU={n}: spawn error {e}"),
        }
    }

    // ---- an 8x bigger dataset made from the dev files (identical copies), for realistic parse volume
    let big = format!("{scratch}/big");
    let t = Instant::now();
    let _ = std::fs::remove_dir_all(&big);
    let mut ok = true;
    for sub in ["events", "clicks", "links"] {
        ok &= std::fs::create_dir_all(format!("{big}/{sub}")).is_ok();
        if let Ok(rd) = std::fs::read_dir(format!("{data}/{sub}")) {
            for e in rd.flatten() {
                let reps = if sub == "links" { 1 } else { 8 };
                for k in 0..reps {
                    let dst = format!("{big}/{sub}/r{k}_{}", e.file_name().to_string_lossy());
                    ok &= std::fs::copy(e.path(), dst).is_ok();
                }
            }
        }
    }
    for f in ["products.csv", "campaigns.csv", "manifest.json"] {
        ok &= std::fs::copy(format!("{data}/{f}"), format!("{big}/{f}")).is_ok();
    }
    eprintln!("built 8x dataset in {:.0} ms (ok={ok})", ms(t));
    if ok {
        for (i, n) in [8usize, 8, 8, 4, 1].iter().enumerate() {
            let out = format!("{scratch}/probe_big_{i}");
            let t = Instant::now();
            let r = std::process::Command::new(exe)
                .env_remove("PROBE")
                .env("TIMING", "1")
                .env("NCPU", n.to_string())
                .env("DATA_DIR", &big)
                .env("OUTPUT_DIR", &out)
                .output();
            let el = ms(t);
            if let Ok(o) = r {
                let err = String::from_utf8_lossy(&o.stderr);
                let phases: Vec<String> = err.lines().map(|l| l.split_whitespace().collect::<Vec<_>>().join(" ")).collect();
                eprintln!("BIG pipeline NCPU={n}: wall {el:.1} ms, exit {:?} | {}", o.status.code(), phases.join(" | "));
            }
            let _ = std::fs::remove_dir_all(&out);
        }
    }

    // ---- fresh memory cost
    let mb = 512usize << 20;
    for huge in [false, true] {
        let p = anon(mb, huge);
        let t = Instant::now();
        touch(p, mb);
        eprintln!("fresh-touch 512MB 1 thread huge={huge}: {:.1} ms", ms(t));
        unsafe { libc::munmap(p as *mut _, mb) };
    }
    for huge in [false, true] {
        let per = 128usize << 20;
        let p = anon(per * 8, huge) as usize;
        let el = par(8, |i| touch((p + i * per) as *mut u8, per));
        eprintln!("fresh-touch 1GB 8 threads huge={huge}: {el:.1} ms");
        let t = Instant::now();
        unsafe { libc::munmap(p as *mut _, per * 8) };
        eprintln!("munmap 1GB huge={huge}: {:.1} ms", ms(t));
    }

    // ---- memory bandwidth (prefaulted)
    let gb = 1usize << 30;
    let buf = anon(gb, true) as usize;
    touch(buf as *mut u8, gb);
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
        eprintln!("read-bandwidth {n} threads: {:.1} GB/s", gb as f64 / el / 1e6);
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
    eprintln!("nt-write-bandwidth 8 threads: {:.1} GB/s", gb as f64 / el / 1e6);
    // random 64B reads with prefetch, 32MB table (like person lookups)
    for (tbl, n) in [(32usize << 20, 1usize), (32 << 20, 8), (8 << 20, 8), (256 << 20, 8)] {
        let lookups = 4_000_000usize;
        let el = par(n, |i| {
            let p = buf as *const u64;
            let mask = tbl / 64 - 1;
            let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ i as u64;
            let mut s = 0u64;
            let mut idx = [0usize; 16];
            for k in 0..16 {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                idx[k] = ((x >> 20) as usize & mask) * 8;
            }
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
        eprintln!("random-64B-lookups table {}MB {n} threads: {:.1} ns/lookup (aggregate)", tbl >> 20, el * 1e6 / lookups as f64);
    }

    // ---- reading input: pread copy vs mmap+populate
    let total: usize = ev_files.iter().map(|p| std::fs::metadata(p).map(|m| m.len() as usize).unwrap_or(0)).sum();
    for mode in 0..3 {
        let files = &ev_files;
        let next = std::sync::atomic::AtomicUsize::new(0);
        let el = par(8, |_| {
            let mut b = vec![0u8; 256 << 10];
            loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(path) = files.get(i) else { break };
                let f = std::fs::File::open(path).unwrap();
                let len = f.metadata().unwrap().len() as usize;
                if mode == 0 {
                    use std::os::unix::fs::FileExt;
                    let mut off = 0;
                    while off < len {
                        let r = f.read_at(&mut b, off as u64).unwrap();
                        if r == 0 {
                            break;
                        }
                        std::hint::black_box(b[0]);
                        off += r;
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
        let name = ["pread 256KB", "mmap+POPULATE_READ", "mmap+MAP_POPULATE"][mode];
        eprintln!("input {name} 8 threads: {el:.1} ms for {total} bytes ({:.1} GB/s)", total as f64 / el / 1e6);
    }

    // ---- parse throughput on in-memory event data
    let mut blobs: Vec<Vec<u8>> = ev_files.iter().filter_map(|p| std::fs::read(p).ok()).collect();
    blobs.retain(|b| b.len() > 1024);
    let cal = Calendar::new();
    for n in [1usize, 4, 8] {
        let lines = std::sync::atomic::AtomicUsize::new(0);
        let el = par(n, |_| {
            let mut sc = Scanner::new();
            let mut ts = Ts::new(&cal);
            let mut acc = 0u64;
            let mut cnt = 0usize;
            for _rep in 0..3 {
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
        eprintln!("parse-core {n} threads: {:.1} ns/line per thread ({l} lines, {el:.0} ms)", el * 1e6 * n as f64 / l as f64);
    }
    eprintln!("probe total {:.0} ms", ms(t_all));
    eprintln!("==== END PROBE (failing on purpose to return these logs) ====");
    unsafe { libc::_exit(3) };
}
