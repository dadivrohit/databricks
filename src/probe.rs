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
    let (m, p, nm, d, s, w) = (at("mmap"), at("parsed"), at("named"), at("dedup+"), at(" sessions"), at("written"));
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

/// Fraction (per mille) of a directory tree's file pages resident in the page cache, and its bytes.
fn resident(dir: &str) -> (u64, u64) {
    let (mut tot, mut res) = (0u64, 0u64);
    let mut stack = vec![std::path::PathBuf::from(dir)];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else { continue };
        for e in rd.flatten() {
            let path = e.path();
            let Ok(md) = e.metadata() else { continue };
            if md.is_dir() {
                stack.push(path);
                continue;
            }
            let len = md.len() as usize;
            if len == 0 {
                continue;
            }
            let Ok(f) = std::fs::File::open(&path) else { continue };
            use std::os::unix::io::AsRawFd;
            unsafe {
                let m = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, f.as_raw_fd(), 0);
                if m == libc::MAP_FAILED {
                    continue;
                }
                let pages = len.div_ceil(4096);
                let mut v = vec![0u8; pages];
                if libc::mincore(m, len, v.as_mut_ptr()) == 0 {
                    res += v.iter().filter(|&&b| b & 1 != 0).count() as u64 * 4096;
                }
                libc::munmap(m, len);
            }
            tot += len as u64;
        }
    }
    (tot, if tot == 0 { 0 } else { res.min(tot) * 1000 / tot })
}

fn first_line(path: &str) -> String {
    read(path).lines().next().unwrap_or("-").trim().to_string()
}

/// PROBE=6: what the (new) scoring setup looks like, and the pipeline on the given input.
fn run6() -> ! {
    let mut r = String::new();
    let cpu = read("/proc/cpuinfo");
    let model = cpu.lines().find(|l| l.starts_with("model name")).unwrap_or("").split(':').nth(1).unwrap_or("").trim().to_string();
    let ncpu_info = cpu.lines().filter(|l| l.starts_with("processor")).count();
    let par = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
    let thp = first_line("/sys/kernel/mm/transparent_hugepage/enabled");
    let defrag = first_line("/sys/kernel/mm/transparent_hugepage/defrag");
    let _ = writeln!(r, "P6 {model} cpus={ncpu_info} par={par} kern={}", first_line("/proc/sys/kernel/osrelease"));
    let _ = writeln!(r, "thp={thp} defrag={defrag}");
    let _ = writeln!(r, "cg cpu.max={} mem.max={} mem.cur={}", first_line("/sys/fs/cgroup/cpu.max"), first_line("/sys/fs/cgroup/memory.max"), first_line("/sys/fs/cgroup/memory.current"));
    let mi = read("/proc/meminfo");
    let get = |k: &str| mi.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) >> 10;
    let _ = writeln!(r, "memMB tot={} free={} avail={} cached={} dirty={}", get("MemTotal:"), get("MemFree:"), get("MemAvailable:"), get("Cached:"), get("Dirty:"));
    let data = std::env::var("DATA_DIR").unwrap_or("/data".into());
    let (tot, pm) = resident(&data);
    let mounts = read("/proc/mounts");
    let mnt = mounts.lines().filter(|l| { let m = l.split_whitespace().nth(1).unwrap_or(""); data.starts_with(m) }).max_by_key(|l| l.split_whitespace().nth(1).unwrap_or("").len()).unwrap_or("?").to_string();
    let _ = writeln!(r, "data={data} {}MB resident={pm}permil mnt={}", tot >> 20, mnt.split_whitespace().take(3).collect::<Vec<_>>().join(" "));
    let scratch = std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into());
    let out = format!("{scratch}/o6");
    let _ = writeln!(r, "ms wall=start+parse+name+dedup+sess+write");
    // the given input, first run decides cold vs warm
    for (label, extra) in [("dev1", vec![]), ("dev2", vec![]), ("devNH", vec![("NOHUGE", "1")]), ("dev3", vec![])] {
        let _ = writeln!(r, "{label}:{}", pipeline_env(&data, &out, 0, &extra));
    }
    let (_, pm2) = resident(&data);
    let _ = writeln!(r, "dev resident after={pm2}permil");
    let g = format!("{scratch}/g20");
    let _ = std::fs::remove_dir_all(&g);
    let t = Instant::now();
    let st = std::process::Command::new("/proc/self/exe")
        .env_remove("PROBE")
        .args(["generate", "--seed", "3", "--events", "20000000", "--clicks", "5000000", "--output", &g, "--threads", "8"])
        .output();
    let ok = st.as_ref().map(|o| o.status.success()).unwrap_or(false);
    let (gt, gpm) = resident(&g);
    let _ = writeln!(r, "gen20M ok={ok} {:.0}ms {}MB resident={gpm}permil", ms(t), gt >> 20);
    let mi = read("/proc/meminfo");
    let get = |k: &str| mi.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) >> 10;
    let _ = writeln!(r, "memMB free={} avail={} cached={} cg.cur={}", get("MemFree:"), get("MemAvailable:"), get("Cached:"), first_line("/sys/fs/cgroup/memory.current"));
    for (label, n, extra) in [
        ("g8", 0usize, vec![]),
        ("g8NH", 0, vec![("NOHUGE", "1")]),
        ("g8", 0, vec![]),
        ("g4", 4, vec![]),
        ("g8NH", 0, vec![("NOHUGE", "1")]),
        ("g8mm", 0, vec![("MMAP", "1")]),
    ] {
        let _ = writeln!(r, "{label}:{}", pipeline_env(&g, &out, n, &extra));
    }
    let (_, gpm2) = resident(&g);
    let _ = writeln!(r, "g resident after={gpm2}permil END");
    eprint!("{r}");
    unsafe { libc::_exit(3) };
}

/// Files (sorted, largest first) under data/{events,clicks,links}.
fn data_files(data: &str) -> Vec<(String, usize)> {
    let mut v = Vec::new();
    for sub in ["events", "clicks", "links"] {
        if let Ok(rd) = std::fs::read_dir(format!("{data}/{sub}")) {
            for e in rd.flatten() {
                let l = e.metadata().map(|m| m.len() as usize).unwrap_or(0);
                v.push((e.path().to_string_lossy().into_owned(), l));
            }
        }
    }
    v.sort_by_key(|x| std::cmp::Reverse(x.1));
    v
}

/// Reads every file with pread in `chunk`-byte pieces over `nt` threads; returns MB/s.
fn bw_pread(files: &[(String, usize)], chunk: usize, nt: usize) -> f64 {
    use std::os::unix::io::AsRawFd;
    let total: usize = files.iter().map(|f| f.1).sum();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let mut work = Vec::new();
    for (i, f) in files.iter().enumerate() {
        let mut o = 0;
        while o < f.1 {
            work.push((i, o, chunk.min(f.1 - o)));
            o += chunk;
        }
    }
    let fds: Vec<std::fs::File> = files.iter().map(|f| std::fs::File::open(&f.0).unwrap()).collect();
    let t = Instant::now();
    std::thread::scope(|sc| {
        for _ in 0..nt {
            sc.spawn(|| {
                let mut buf = vec![0u8; chunk];
                let mut x = 0u64;
                loop {
                    let k = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if k >= work.len() {
                        break;
                    }
                    let (i, o, n) = work[k];
                    unsafe { libc::pread(fds[i].as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, n, o as libc::off_t) };
                    x = x.wrapping_add(buf[0] as u64);
                }
                std::hint::black_box(x);
            });
        }
    });
    total as f64 / 1e6 / t.elapsed().as_secs_f64()
}

/// mmaps every file (populate or touch) over `nt` threads; returns MB/s.
fn bw_mmap(files: &[(String, usize)], populate: bool, nt: usize) -> f64 {
    use std::os::unix::io::AsRawFd;
    let total: usize = files.iter().map(|f| f.1).sum();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let t = Instant::now();
    std::thread::scope(|sc| {
        for _ in 0..nt {
            sc.spawn(|| {
                let mut x = 0u64;
                loop {
                    let k = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if k >= files.len() {
                        break;
                    }
                    let f = std::fs::File::open(&files[k].0).unwrap();
                    let n = files[k].1;
                    if n == 0 {
                        continue;
                    }
                    unsafe {
                        let fl = libc::MAP_PRIVATE | if populate { libc::MAP_POPULATE } else { 0 };
                        let m = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ, fl, f.as_raw_fd(), 0) as *const u8;
                        let mut o = 0;
                        while o < n {
                            x = x.wrapping_add(*m.add(o) as u64);
                            o += 4096;
                        }
                        libc::munmap(m as *mut libc::c_void, n);
                    }
                }
                std::hint::black_box(x);
            });
        }
    });
    total as f64 / 1e6 / t.elapsed().as_secs_f64()
}

/// PROBE=7: how fast the given input (/data) can be read, and the pipeline's read options on it.
fn run7() -> ! {
    let mut r = String::new();
    let data = std::env::var("DATA_DIR").unwrap_or("/data".into());
    let files = data_files(&data);
    let total: usize = files.iter().map(|f| f.1).sum();
    let _ = writeln!(r, "P7 data={data} files={} {}MB max={}KB", files.len(), total >> 20, files.first().map(|f| f.1 >> 10).unwrap_or(0));
    let _ = writeln!(r, "MB/s pread 256K: t1={:.0} t8={:.0} | 4M: t1={:.0} t8={:.0} | 64K t8={:.0}",
        bw_pread(&files, 256 << 10, 1), bw_pread(&files, 256 << 10, 8), bw_pread(&files, 4 << 20, 1), bw_pread(&files, 4 << 20, 8), bw_pread(&files, 64 << 10, 8));
    let _ = writeln!(r, "MB/s mmap touch: t1={:.0} t8={:.0} | populate: t1={:.0} t8={:.0}",
        bw_mmap(&files, false, 1), bw_mmap(&files, false, 8), bw_mmap(&files, true, 1), bw_mmap(&files, true, 8));
    let _ = writeln!(r, "MB/s again pread256K t8={:.0} mmap-touch t8={:.0}", bw_pread(&files, 256 << 10, 8), bw_mmap(&files, false, 8));
    let scratch = std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into());
    let out = format!("{scratch}/o7");
    let _ = writeln!(r, "ms wall=start+parse+name+dedup+sess+write");
    for (label, extra) in [
        ("base", vec![]),
        ("mmap", vec![("MMAP", "1")]),
        ("c2048", vec![("CHUNKKB", "2048")]),
        ("c64", vec![("CHUNKKB", "64")]),
        ("base", vec![]),
        ("mmap", vec![("MMAP", "1")]),
        ("c2048", vec![("CHUNKKB", "2048")]),
    ] {
        let _ = writeln!(r, "{label}:{}", pipeline_env(&data, &out, 0, &extra));
    }
    // copy the input to /scratch and time the pipeline there (page cache, not /data)
    let cp = format!("{scratch}/dcopy");
    let _ = std::fs::remove_dir_all(&cp);
    let t = Instant::now();
    for sub in ["events", "clicks", "links"] {
        let _ = std::fs::create_dir_all(format!("{cp}/{sub}"));
        if let Ok(rd) = std::fs::read_dir(format!("{data}/{sub}")) {
            for e in rd.flatten() {
                let _ = std::fs::copy(e.path(), format!("{cp}/{sub}/{}", e.file_name().to_string_lossy()));
            }
        }
    }
    for f in ["manifest.json", "products.csv", "campaigns.csv"] {
        let _ = std::fs::copy(format!("{data}/{f}"), format!("{cp}/{f}"));
    }
    let _ = writeln!(r, "copied to scratch {:.0}ms", ms(t));
    for (label, extra) in [("scr", vec![]), ("scrmm", vec![("MMAP", "1")]), ("scr", vec![])] {
        let _ = writeln!(r, "{label}:{}", pipeline_env(&cp, &out, 0, &extra));
    }
    let _ = writeln!(r, "END");
    eprint!("{r}");
    unsafe { libc::_exit(3) };
}

fn cpu_stat() -> (u64, u64, u64) {
    let s = read("/sys/fs/cgroup/cpu.stat");
    let g = |k: &str| s.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse().ok()).unwrap_or(0u64);
    (g("usage_usec"), g("nr_throttled"), g("throttled_usec"))
}

/// One pipeline run: wall, CPU used, cgroup throttling, and the parse threads' cycle split.
fn run_cpu(data: &str, out: &str, n: usize, extra: &[(&str, &str)]) -> String {
    let (u0, n0, t0) = cpu_stat();
    let mut ru0: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, &mut ru0) };
    let t = Instant::now();
    let mut cmd = std::process::Command::new("/proc/self/exe");
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let r = cmd.env_remove("PROBE").env("TIMING", "1").env("NCPU", n.to_string()).env("DATA_DIR", data).env("OUTPUT_DIR", out).output();
    let el = ms(t);
    let mut ru1: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, &mut ru1) };
    let (u1, n1, t1) = cpu_stat();
    let _ = std::fs::remove_dir_all(out);
    let tv = |a: libc::timeval| a.tv_sec as f64 * 1e3 + a.tv_usec as f64 / 1e3;
    let user = tv(ru1.ru_utime) - tv(ru0.ru_utime);
    let sys = tv(ru1.ru_stime) - tv(ru0.ru_stime);
    let Ok(o) = r else { return "ERR".into() };
    let err = String::from_utf8_lossy(&o.stderr);
    let at = |name: &str| -> f64 { err.lines().find(|l| l.contains(name)).and_then(|l| l.split_whitespace().next()).and_then(|x| x.parse().ok()).unwrap_or(-1.0) };
    let mut pk = [0u64; 4];
    for l in err.lines().filter(|l| l.starts_with("parse Mcyc")) {
        let v: Vec<u64> = l.split_whitespace().filter_map(|x| x.parse().ok()).collect();
        for i in 0..4.min(v.len()) {
            pk[i] += v[i];
        }
    }
    format!(
        "w{el:.0} p{:.0} u{user:.0} s{sys:.0} cg{} thr{}/{}ms pMcyc l{} c{} e{} r{}",
        at("parsed") - at("mmap"),
        (u1 - u0) / 1000,
        n1 - n0,
        (t1 - t0) / 1000,
        pk[0], pk[1], pk[2], pk[3]
    )
}

/// PROBE=8: CPU time, throttling and parse split on the given input.
fn run8() -> ! {
    let mut r = String::new();
    let data = std::env::var("DATA_DIR").unwrap_or("/data".into());
    let scratch = std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into());
    let out = format!("{scratch}/o8");
    let _ = writeln!(r, "P8 load={} cpu.max={}", first_line("/proc/loadavg"), first_line("/sys/fs/cgroup/cpu.max"));
    let _ = writeln!(r, "w=wall p=parse u/s=user/sys ms cg=cgroup cpu ms thr=throttled n/ms");
    for (label, n, extra) in [("d8", 0usize, vec![]), ("d8", 0, vec![]), ("d4", 4, vec![]), ("d2", 2, vec![]), ("d1", 1, vec![]), ("d8NH", 0, vec![("NOHUGE", "1")]), ("d8", 0, vec![])] {
        let _ = writeln!(r, "{label} {}", run_cpu(&data, &out, n, &extra));
    }
    let g = format!("{scratch}/g20");
    let _ = std::fs::remove_dir_all(&g);
    let _ = std::process::Command::new("/proc/self/exe")
        .env_remove("PROBE")
        .args(["generate", "--seed", "3", "--events", "20000000", "--clicks", "5000000", "--output", &g, "--threads", "8"])
        .output();
    for (label, n) in [("g8", 0usize), ("g4", 4), ("g8", 0)] {
        let _ = writeln!(r, "{label} {}", run_cpu(&g, &out, n, &[]));
    }
    let _ = writeln!(r, "load={} END", first_line("/proc/loadavg"));
    eprint!("{r}");
    unsafe { libc::_exit(3) };
}

pub fn run() -> ! {
    if std::env::var("PROBE").as_deref() == Ok("8") {
        run8();
    }
    if std::env::var("PROBE").as_deref() == Ok("7") {
        run7();
    }
    if std::env::var("PROBE").as_deref() == Ok("6") {
        run6();
    }
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
