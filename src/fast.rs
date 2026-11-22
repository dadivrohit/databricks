//! Fast field parsing: AVX-512 line masks, SWAR timestamps and digits.
use std::arch::x86_64::*;

#[inline(always)]
pub unsafe fn ld64(p: *const u8) -> u64 {
    unsafe { std::ptr::read_unaligned(p as *const u64) }
}
#[inline(always)]
pub unsafe fn ld32(p: *const u8) -> u32 {
    unsafe { std::ptr::read_unaligned(p as *const u32) }
}

/// Comma and quote masks for 128 bytes at p.
#[inline(always)]
pub unsafe fn masks128_cq(p: *const u8) -> (u128, u128) {
    unsafe {
        let a = _mm512_loadu_si512(p as *const _);
        let b = _mm512_loadu_si512(p.add(64) as *const _);
        let cm = _mm512_set1_epi8(b',' as i8);
        let qt = _mm512_set1_epi8(b'"' as i8);
        let c = (_mm512_cmpeq_epi8_mask(a, cm) as u128) | ((_mm512_cmpeq_epi8_mask(b, cm) as u128) << 64);
        let q = (_mm512_cmpeq_epi8_mask(a, qt) as u128) | ((_mm512_cmpeq_epi8_mask(b, qt) as u128) << 64);
        (c, q)
    }
}

/// Newline, comma and quote masks for 128 bytes at p.
#[inline(always)]
pub unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    unsafe {
        let a = _mm512_loadu_si512(p as *const _);
        let b = _mm512_loadu_si512(p.add(64) as *const _);
        let nl = _mm512_set1_epi8(b'\n' as i8);
        let cm = _mm512_set1_epi8(b',' as i8);
        let qt = _mm512_set1_epi8(b'"' as i8);
        let n = (_mm512_cmpeq_epi8_mask(a, nl) as u128) | ((_mm512_cmpeq_epi8_mask(b, nl) as u128) << 64);
        let c = (_mm512_cmpeq_epi8_mask(a, cm) as u128) | ((_mm512_cmpeq_epi8_mask(b, cm) as u128) << 64);
        let q = (_mm512_cmpeq_epi8_mask(a, qt) as u128) | ((_mm512_cmpeq_epi8_mask(b, qt) as u128) << 64);
        (n, c, q)
    }
}

/// Field bounds of a line given its comma and quote masks (already limited to the line).
/// Strips whole-field quotes. Returns false if malformed. Branch-free in the common case.
#[inline(always)]
pub unsafe fn fields<const N: usize>(p: *const u8, len: usize, cm: u128, qm: u128, f: &mut [(usize, usize); N]) -> bool {
    if cm.count_ones() as usize != N - 1 {
        return false;
    }
    let mut lo = cm as u64;
    let mut hi = (cm >> 64) as u64;
    let mut s = 0usize;
    for i in 0..N - 1 {
        let lz = lo == 0;
        let c = if lz { 64 + hi.trailing_zeros() } else { lo.trailing_zeros() } as usize;
        lo &= lo.wrapping_sub(1);
        hi &= hi.wrapping_sub(lz as u64);
        f[i] = (s, c);
        s = c + 1;
    }
    f[N - 1] = (s, len);
    {
        // Quotes may only wrap whole fields. Field starts FS, terminators FE (commas + line end),
        // last chars FL. For each quote at a field start, FE & !(FE - QS) is that field's
        // terminator; its last char must hold the matching quote.
        let fs = (cm << 1) | 1;
        let fe = cm | (1u128 << len);
        let fl = fe >> 1;
        let qs = qm & fs;
        let qe = qm & fl;
        let term = fe & !fe.wrapping_sub(qs);
        if qm != (qs | qe) || (qs & qe) != 0 || qe != (term >> 1) {
            return false;
        }
        // validated above: a field starts with a quote exactly when it is quoted
        for i in 0..N {
            let (s, e) = f[i];
            let q = (unsafe { *p.add(s) } == b'"') as usize;
            f[i] = (s + q, e - q);
        }
    }
    true
}

/// "<prefix>" + 1..8 ASCII non-NUL bytes -> order-preserving nonzero key with bit 63 clear.
/// 0 if the value does not have that shape.
#[inline(always)]
pub unsafe fn short_key(p: *const u8, n: usize, prefix: u8) -> u64 {
    if n < 2 || n > 9 || unsafe { *p } != prefix {
        return 0;
    }
    let k = n - 1;
    let mask = u64::MAX >> (64 - 8 * k);
    let x = unsafe { ld64(p.add(1)) } & mask;
    let y = x | !mask;
    if x & 0x8080_8080_8080_8080 != 0
        || (y.wrapping_sub(0x0101_0101_0101_0101) & !y & 0x8080_8080_8080_8080) != 0
    {
        return 0;
    }
    x.swap_bytes()
}

pub fn unpack_short(k: u64, prefix: u8, out: &mut Vec<u8>) {
    out.push(prefix);
    for b in k.to_be_bytes() {
        if b == 0 {
            break;
        }
        out.push(b);
    }
}

/// Bytes of a masked u64 that must be digits: returns digit values (0 elsewhere) or None.
#[inline(always)]
fn digits_of(v: u64, m: u64) -> Option<u64> {
    let d = (v ^ 0x3030_3030_3030_3030) & m;
    if (d & 0xF0F0_F0F0_F0F0_F0F0) | (d.wrapping_add(0x0606_0606_0606_0606) & 0x1010_1010_1010_1010 & m) != 0 {
        return None;
    }
    Some(d)
}

pub struct Calendar {
    /// days since epoch of the 1st of each month, from 1970-01
    first: Vec<i32>,
    len: Vec<u8>,
}

pub const CAL_Y0: i64 = 1970;
pub const CAL_YEARS: i64 = 160;

impl Calendar {
    pub fn new() -> Self {
        let mut first = Vec::new();
        let mut len = Vec::new();
        for y in CAL_Y0..CAL_Y0 + CAL_YEARS {
            for m in 1..=12 {
                first.push(crate::days_from_civil(y, m, 1) as i32);
                let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
                const MD: [u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
                len.push(if m == 2 && leap { 29 } else { MD[(m - 1) as usize] });
            }
        }
        Calendar { first, len }
    }

    /// "YYYY-MM-DDTHH:MM:SS" at p (19 bytes, p[19] not read) -> seconds since epoch.
    #[inline(always)]
    pub unsafe fn civil19(&self, p: *const u8) -> Option<i64> {
        let a = unsafe { ld64(p) }; // YYYY-MM-
        let b = unsafe { ld64(p.add(8)) }; // DDTHH:MM
        let c = unsafe { ld32(p.add(16)) } as u64 & 0x00FF_FFFF; // :SS
        if a & 0xFF00_00FF_0000_0000 != 0x2D00_002D_0000_0000
            || b & 0x0000_FF00_00FF_0000 != 0x0000_3A00_0054_0000
            || c & 0xFF != 0x3A
        {
            return None;
        }
        let da = digits_of(a, 0x00FF_FF00_FFFF_FFFF)?;
        let db = digits_of(b, 0xFFFF_00FF_FF00_FFFF)?;
        let dc = digits_of(c, 0x00FF_FF00)?;
        let y = (da & 0xFF) * 1000 + ((da >> 8) & 0xFF) * 100 + ((da >> 16) & 0xFF) * 10 + ((da >> 24) & 0xFF);
        let mo = ((da >> 40) & 0xFF) * 10 + ((da >> 48) & 0xFF);
        let d = (db & 0xFF) * 10 + ((db >> 8) & 0xFF);
        let h = ((db >> 24) & 0xFF) * 10 + ((db >> 32) & 0xFF);
        let mi = ((db >> 48) & 0xFF) * 10 + ((db >> 56) & 0xFF);
        let s = ((dc >> 8) & 0xFF) * 10 + ((dc >> 16) & 0xFF);
        if mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59 {
            return None;
        }
        let y = y as i64;
        let days = if (CAL_Y0..CAL_Y0 + CAL_YEARS).contains(&y) {
            let i = ((y - CAL_Y0) * 12 + mo as i64 - 1) as usize;
            if d > self.len[i] as u64 {
                return None;
            }
            self.first[i] as i64 + d as i64 - 1
        } else {
            let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
            const MD: [u64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
            let md = if mo == 2 && leap { 29 } else { MD[(mo - 1) as usize] };
            if d > md {
                return None;
            }
            crate::days_from_civil(y, mo as i64, d as i64)
        };
        Some(days * 86400 + (h * 3600 + mi * 60 + s) as i64)
    }

    /// Any ts form -> epoch ms, None if bad. p..p+n must be readable, plus 8 bytes slack.
    #[inline(always)]
    pub unsafe fn ts_ms(&self, p: *const u8, n: usize) -> Option<i64> {
        unsafe {
            if n == 20 && *p.add(19) == b'Z' {
                return Some(self.civil19(p)? * 1000);
            }
            if n == 25 && (*p.add(19) == b'+' || *p.add(19) == b'-') {
                let sign = *p.add(19);
                if *p.add(22) != b':' {
                    return None;
                }
                let base = self.civil19(p)?;
                let o = ld64(p.add(17)) >> 24; // bytes 20..25 -> HH:MM in low 5 bytes
                if o & 0xFF_0000 != 0x3A_0000 {
                    return None;
                }
                let d = digits_of(o, 0xFF_FF00_FFFF)?;
                let oh = (d & 0xFF) * 10 + ((d >> 8) & 0xFF);
                let om = ((d >> 24) & 0xFF) * 10 + ((d >> 32) & 0xFF);
                if oh > 23 || om > 59 {
                    return None;
                }
                let off = (oh * 60 + om) as i64 * 60;
                return Some((if sign == b'+' { base - off } else { base + off }) * 1000);
            }
            let v = digits_u64(p, n)?;
            if v > i64::MAX as u64 { None } else { Some(v as i64) }
        }
    }
}

#[inline(always)]
fn eight(v: u64) -> u64 {
    // v holds 8 digit values (0..9), first digit in the lowest byte
    let v = v.wrapping_mul(10).wrapping_add(v >> 8);
    (((v & 0x0000_00FF_0000_00FF).wrapping_mul(0x000F_4240_0000_0064))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(0x0000_2710_0000_0001)))
        >> 32
}

/// ASCII digits only (1+ of them) -> value; None on a non-digit or overflow of u64.
/// Up to 16 digits take no loop (8 readable bytes past p + n are assumed for n <= 8).
#[inline(always)]
pub unsafe fn digits_u64(p: *const u8, n: usize) -> Option<u64> {
    #[inline(always)]
    unsafe fn upto8(p: *const u8, k: usize) -> Option<u64> {
        // k in 1..=8 digits at p
        let sh = 8 * (8 - k) as u32;
        let raw = unsafe { ld64(p) } << sh; // digits in the high k bytes
        let fill = if k == 8 { 0 } else { 0x3030_3030_3030_3030u64 >> (8 * k) };
        let d = digits_of(raw | fill, u64::MAX)?;
        Some(eight(d))
    }
    if n == 0 {
        return None;
    }
    if n <= 8 {
        return unsafe { upto8(p, n) };
    }
    if n <= 16 {
        let lo = digits_of(unsafe { ld64(p.add(n - 8)) }, u64::MAX)?;
        let hi = unsafe { upto8(p, n - 8) }?;
        return Some(hi * 100_000_000 + eight(lo));
    }
    unsafe { digits_long(p, n) }
}

#[cold]
#[inline(never)]
unsafe fn digits_long(p: *const u8, n: usize) -> Option<u64> {
    // long: leading zeros, or overflow
    let mut v: u64 = 0;
    for i in 0..n {
        let d = unsafe { *p.add(i) }.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    Some(v)
}

/// digits only, value 1..=10_000_000
#[inline(always)]
pub unsafe fn price(p: *const u8, n: usize) -> Option<u32> {
    let v = unsafe { digits_u64(p, n) };
    match v {
        Some(v) if (1..=10_000_000).contains(&v) => Some(v as u32),
        Some(_) => None,
        None => {
            // overflowing all-digit strings: still digits-only but value too large
            None
        }
    }
}

/// Structural scanner: per 4 KB batch, flattens comma and newline positions with AVX-512,
/// then hands out lines with their field bounds. Lines are independent of each other,
/// so the CPU can overlap them.
/// Lines that took a slow path (128+ bytes, or near a buffer end): for XPROBE.
pub static ODD_LINES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Cycles parse threads spent reading vs in all: for XPROBE.
pub static READ_CYC: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PARSE_CYC: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub struct Scanner {
    nl: Vec<u32>,
    cm: Vec<u32>,
    qt: Vec<u64>,
    qpre: Vec<u32>,
    pad: Vec<u8>,
}

pub const BATCH: usize = 4096;
pub const SLACK: usize = 256;

#[inline(always)]
unsafe fn flatten(out: *mut u32, n: usize, base: u32, mut m: u64) -> usize {
    let cnt = m.count_ones() as usize;
    unsafe {
        let o = out.add(n);
        for i in 0..8 {
            *o.add(i) = base + m.trailing_zeros();
            m &= m.wrapping_sub(1);
        }
        if cnt > 8 {
            for i in 8..16 {
                *o.add(i) = base + m.trailing_zeros();
                m &= m.wrapping_sub(1);
            }
            let mut i = 16;
            while m != 0 {
                *o.add(i) = base + m.trailing_zeros();
                m &= m.wrapping_sub(1);
                i += 1;
            }
        }
    }
    n + cnt
}

impl Scanner {
    pub fn new() -> Self {
        Scanner {
            nl: vec![0; BATCH + 64],
            cm: vec![0; BATCH + 64],
            qt: vec![0; BATCH / 64 + 2],
            qpre: vec![0; BATCH / 64 + 2],
            pad: Vec::new(),
        }
    }

    /// Calls f(line_ptr, fields) for every well-formed line starting in [start, end)
    /// (the file's first line, the header, is skipped). line_ptr + field end + 64 is readable.
    #[inline(always)]
    pub fn scan<const N: usize>(
        &mut self,
        data: &[u8],
        start: usize,
        end: usize,
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
        let safe = len.saturating_sub(SLACK);
        let mut fl = [(0usize, 0usize); N];
        let nlp = self.nl.as_mut_ptr();
        'outer: while pos < end {
            let nb = (safe.saturating_sub(pos) / 64).min(BATCH / 64);
            if nb >= 2 {
                let r0 = pos;
                let mut nnl = 0usize;
                unsafe {
                    let vnl = _mm512_set1_epi8(b'\n' as i8);
                    for b in 0..nb {
                        let v = _mm512_loadu_si512(base.add(r0 + 64 * b) as *const _);
                        let mut m = _mm512_cmpeq_epi8_mask(v, vnl);
                        let cnt = m.count_ones() as usize;
                        let bb = (64 * b) as u32;
                        *nlp.add(nnl) = bb + m.trailing_zeros();
                        m &= m.wrapping_sub(1);
                        *nlp.add(nnl + 1) = bb + m.trailing_zeros();
                        m &= m.wrapping_sub(1);
                        let mut k = 2;
                        while m != 0 {
                            *nlp.add(nnl + k) = bb + m.trailing_zeros();
                            m &= m.wrapping_sub(1);
                            k += 1;
                        }
                        nnl += cnt;
                    }
                }
                if nnl == 0 {
                    pos = self.slow_line::<N>(data, pos, &mut fl, &mut f);
                    continue 'outer;
                }
                let mut ls = 0usize;
                for li in 0..nnl {
                    let le = unsafe { *nlp.add(li) } as usize;
                    if r0 + ls >= end {
                        return;
                    }
                    let l = le - ls;
                    let p = unsafe { base.add(r0 + ls) };
                    if l < 128 {
                        let (cm, qm) = unsafe { masks128_cq(p) };
                        let lm = (1u128 << l) - 1;
                        if unsafe { fields::<N>(p, l, cm & lm, qm & lm, &mut fl) } {
                            f(p, &fl);
                        }
                    } else {
                        ODD_LINES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let line = unsafe { std::slice::from_raw_parts(p, l) };
                        if split::<N>(line, &mut fl) {
                            f(p, &fl);
                        }
                    }
                    ls = le + 1;
                }
                pos = r0 + ls;
            } else {
                while pos < end {
                    pos = self.slow_line::<N>(data, pos, &mut fl, &mut f);
                }
            }
        }
    }

    #[cold]
    fn slow_line<const N: usize>(
        &mut self,
        data: &[u8],
        pos: usize,
        fl: &mut [(usize, usize); N],
        f: &mut impl FnMut(*const u8, &[(usize, usize); N]),
    ) -> usize {
        ODD_LINES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let e = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => data.len(),
        };
        let line = &data[pos..e];
        self.pad.clear();
        self.pad.extend_from_slice(line);
        self.pad.resize(line.len() + SLACK, 0);
        if split::<N>(&self.pad[..line.len()], fl) {
            f(self.pad.as_ptr(), fl);
        }
        e + 1
    }
}

/// Splits a line into exactly N fields, stripping whole-field quotes. false if malformed.
pub fn split<const N: usize>(line: &[u8], f: &mut [(usize, usize); N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut field = 0usize;
    let mut start = 0usize;
    let n = line.len();
    loop {
        let mut j = start;
        let mut quotes = 0u32;
        while j < n && line[j] != b',' {
            quotes += (line[j] == b'"') as u32;
            j += 1;
        }
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
        f[field] = (s, e);
        field += 1;
        if j >= n {
            break;
        }
        start = j + 1;
    }
    field == N
}

#[allow(dead_code)]
pub unsafe fn flatten_pub(out: *mut u32, n: usize, base: u32, m: u64) -> usize { unsafe { flatten(out, n, base, m) } }

/// Per-thread timestamp parser with a small date cache ("YYYY-MM-DD" -> day number).
pub struct Ts<'a> {
    cal: &'a Calendar,
    k1: [u64; 64],
    k2: [u16; 64],
    day: [i32; 64],
}

#[inline(always)]
unsafe fn ld16(p: *const u8) -> u16 {
    unsafe { std::ptr::read_unaligned(p as *const u16) }
}

impl<'a> Ts<'a> {
    pub fn new(cal: &'a Calendar) -> Self {
        Ts { cal, k1: [0; 64], k2: [0; 64], day: [0; 64] }
    }

    /// Validated "YYYY-MM-DD" at p -> days since epoch.
    #[inline(always)]
    unsafe fn date(&mut self, p: *const u8) -> Option<i64> {
        let a = unsafe { ld64(p) };
        let b = unsafe { ld16(p.add(8)) };
        let h = ((a ^ (b as u64).wrapping_mul(0x9E37_79B9)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 58) as usize;
        if self.k1[h] == a && self.k2[h] == b && a != 0 {
            return Some(self.day[h] as i64);
        }
        self.date_slow(p, a, b, h)
    }

    #[cold]
    unsafe fn date_slow(&mut self, p: *const u8, a: u64, b: u16, h: usize) -> Option<i64> {
        // reuse civil19 on a synthetic "YYYY-MM-DDT00:00:00"
        let mut buf = [0u8; 32];
        unsafe { std::ptr::copy_nonoverlapping(p, buf.as_mut_ptr(), 10) };
        buf[10..19].copy_from_slice(b"T00:00:00");
        let secs = unsafe { self.cal.civil19(buf.as_ptr()) }?;
        let d = secs / 86400;
        self.k1[h] = a;
        self.k2[h] = b;
        self.day[h] = d as i32;
        Some(d)
    }

    /// Validated "HH:MM:SS" at p (8 bytes) -> seconds of day.
    #[inline(always)]
    unsafe fn hms(p: *const u8) -> Option<i64> {
        let x = unsafe { ld64(p) };
        if x & 0x0000_FF00_00FF_0000 != 0x0000_3A00_003A_0000 {
            return None;
        }
        let d = digits_of(x, 0xFFFF_00FF_FF00_FFFF)?;
        let t = d.wrapping_mul(10).wrapping_add(d >> 8);
        let (h, m, s) = (t & 0xFF, (t >> 24) & 0xFF, (t >> 48) & 0xFF);
        if h > 23 || m > 59 || s > 59 {
            return None;
        }
        Some((h * 3600 + m * 60 + s) as i64)
    }

    /// "YYYY-MM-DDTHH:MM:SS" (19 bytes) -> epoch seconds, validated.
    #[inline(always)]
    unsafe fn civil(&mut self, p: *const u8) -> Option<i64> {
        unsafe {
            if *p.add(10) != b'T' {
                return None;
            }
            let d = self.date(p)?;
            let s = Self::hms(p.add(11))?;
            Some(d * 86400 + s)
        }
    }

    /// Any ts form -> epoch ms; None if bad.
    #[inline(always)]
    pub unsafe fn ms(&mut self, p: *const u8, n: usize) -> Option<i64> {
        unsafe {
            if n == 20 && *p.add(19) == b'Z' {
                return Some(self.civil(p)? * 1000);
            }
            if n == 25 && (*p.add(19) == b'+' || *p.add(19) == b'-') {
                let sign = *p.add(19);
                let base = self.civil(p)?;
                let o = ld64(p.add(17)) >> 24;
                if o & 0xFF_0000 != 0x3A_0000 {
                    return None;
                }
                let d = digits_of(o, 0xFF_FF00_FFFF)?;
                let oh = (d & 0xFF) * 10 + ((d >> 8) & 0xFF);
                let om = ((d >> 24) & 0xFF) * 10 + ((d >> 32) & 0xFF);
                if oh > 23 || om > 59 {
                    return None;
                }
                let off = (oh * 60 + om) as i64 * 60;
                return Some((if sign == b'+' { base - off } else { base + off }) * 1000);
            }
            let v = digits_u64(p, n)?;
            if v > i64::MAX as u64 { None } else { Some(v as i64) }
        }
    }

    /// ingest_ts: the rules guarantee a valid F1, so only the date goes through the cache.
    /// Falls back to the full parser if the field does not look like F1.
    #[inline(always)]
    pub unsafe fn ingest_secs(&mut self, p: *const u8, n: usize) -> Option<i64> {
        unsafe {
            if n == 20 {
                if let Some(d) = self.date(p) {
                    let x = ld64(p.add(11));
                    let d8 = x.wrapping_sub(0x3030_3030_3030_3030);
                    let t = d8.wrapping_mul(10).wrapping_add(d8 >> 8);
                    let s = (t & 0xFF) * 3600 + ((t >> 24) & 0xFF) * 60 + ((t >> 48) & 0xFF);
                    return Some(d * 86400 + s as i64);
                }
            }
            self.ms(p, n).map(|m| m.div_euclid(1000))
        }
    }
}

/// Bump allocator over one big anonymous mapping, backed by huge pages where possible.
pub struct Arena {
    base: *mut u8,
    cap: usize,
    used: usize,
}
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    /// Unmaps the whole arena (call once nothing reads it any more).
    pub fn release(&self) {
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.cap) };
    }
    pub fn new(cap: usize) -> Self {
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                cap,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        assert!(p != libc::MAP_FAILED, "arena mmap failed");
        if std::env::var_os("HUGE").is_some() {
            unsafe { libc::madvise(p, cap, libc::MADV_HUGEPAGE) };
        }
        if std::env::var("PREFAULT").is_ok() {
            let touch = cap.min(1 << 30);
            let mut i = 0;
            while i < touch {
                unsafe { *(p as *mut u8).add(i) = 0 };
                i += 4096;
            }
        }
        Arena { base: p as *mut u8, cap, used: 0 }
    }
    #[inline(always)]
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        let p = self.used;
        self.used += (bytes + 63) & !63;
        assert!(self.used <= self.cap, "arena full");
        unsafe { self.base.add(p) }
    }
}

/// N partitions of T, each a chain of fixed-size blocks from an arena.
pub const BLOCK_BYTES: usize = 8192;

pub struct Parts<T: Copy> {
    pub(crate) head: Vec<*mut T>,
    pub(crate) end: Vec<*mut T>,
    blocks: Vec<Vec<*mut T>>,
    blk: usize,
}
unsafe impl<T: Copy> Send for Parts<T> {}
unsafe impl<T: Copy> Sync for Parts<T> {}

impl<T: Copy> Parts<T> {
    pub fn new(n: usize, blk: usize) -> Self {
        Parts {
            head: vec![std::ptr::null_mut(); n],
            end: vec![std::ptr::null_mut(); n],
            blocks: (0..n).map(|_| Vec::new()).collect(),
            blk,
        }
    }
    #[inline(always)]
    pub fn push(&mut self, arena: &mut Arena, h: usize, v: T) {
        unsafe {
            let mut hp = *self.head.get_unchecked(h);
            if hp == *self.end.get_unchecked(h) {
                hp = self.grow(arena, h);
            }
            hp.write(v);
            *self.head.get_unchecked_mut(h) = hp.add(1);
        }
    }
    /// push, taking new blocks from `free` (8 KB blocks) before the arena.
    #[inline(always)]
    pub fn push_r(&mut self, arena: &mut Arena, free: &mut Vec<*mut u8>, h: usize, v: T) {
        unsafe {
            let mut hp = *self.head.get_unchecked(h);
            if hp == *self.end.get_unchecked(h) {
                hp = self.grow_r(arena, free, h);
            }
            hp.write(v);
            *self.head.get_unchecked_mut(h) = hp.add(1);
        }
    }
    #[cold]
    pub(crate) fn grow_r(&mut self, arena: &mut Arena, free: &mut Vec<*mut u8>, h: usize) -> *mut T {
        debug_assert!(self.blk * std::mem::size_of::<T>() <= BLOCK_BYTES);
        let p = match free.pop() {
            Some(b) => b as *mut T,
            None => arena.alloc(BLOCK_BYTES) as *mut T,
        };
        self.blocks[h].push(p);
        self.end[h] = unsafe { p.add(self.blk) };
        p
    }
    /// Raw block pointers of partition h (for recycling once consumed).
    pub fn block_ptrs(&self, h: usize) -> &[*mut T] {
        &self.blocks[h]
    }
    #[cold]
    pub(crate) fn grow(&mut self, arena: &mut Arena, h: usize) -> *mut T {
        let p = arena.alloc(self.blk * std::mem::size_of::<T>()) as *mut T;
        self.blocks[h].push(p);
        self.end[h] = unsafe { p.add(self.blk) };
        p
    }
    /// The slices of partition h, in order.
    pub fn slices(&self, h: usize) -> impl Iterator<Item = &[T]> + '_ {
        let n = self.blocks[h].len();
        self.blocks[h].iter().enumerate().map(move |(i, &b)| {
            let len = if i + 1 == n { unsafe { self.head[h].offset_from(b) as usize } } else { self.blk };
            unsafe { std::slice::from_raw_parts(b as *const T, len) }
        })
    }
    pub fn len(&self, h: usize) -> usize {
        self.slices(h).map(|s| s.len()).sum()
    }
}

/// Partitions with software write-combining: records are staged per partition in one
/// cache line and flushed as a full 64-byte non-temporal store.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct Line([u8; 64]);

/// Branch-free pushes for the parse scatter (env BLWC; off by default).
static BLWC: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(2);
#[inline(always)]
fn wc_branchless() -> bool {
    let v = BLWC.load(std::sync::atomic::Ordering::Relaxed);
    if v != 2 {
        return v == 1;
    }
    let r = std::env::var_os("BLWC").is_some();
    BLWC.store(r as u8, std::sync::atomic::Ordering::Relaxed);
    r
}

pub struct WcParts<T: Copy> {
    pub parts: Parts<T>,
    stage: Vec<Line>,
    fill: Vec<u8>,
}
unsafe impl<T: Copy> Send for WcParts<T> {}
unsafe impl<T: Copy> Sync for WcParts<T> {}

impl<T: Copy> WcParts<T> {
    const K: usize = 64 / std::mem::size_of::<T>();
    pub fn new(n: usize) -> Self {
        assert!(64 % std::mem::size_of::<T>() == 0);
        WcParts {
            parts: Parts::new(n, BLOCK_BYTES / std::mem::size_of::<T>()),
            stage: vec![Line([0; 64]); n],
            fill: vec![0; n],
        }
    }
    /// Branch-free push: streams the (possibly partial) staging line on every push and only
    /// moves the head once the line is full; a partial line is rewritten by the next push.
    /// (Which partitions fill up next is random, so a branch on it mispredicts often.)
    #[inline(always)]
    fn push_bl<const R: bool>(&mut self, arena: &mut Arena, free: &mut Vec<*mut u8>, h: usize, v: T) {
        unsafe {
            let f = *self.fill.get_unchecked(h) as usize;
            let line = self.stage.get_unchecked_mut(h);
            (line.0.as_mut_ptr() as *mut T).add(f).write(v);
            let p = &mut self.parts;
            let mut hp = *p.head.get_unchecked(h);
            if hp == *p.end.get_unchecked(h) {
                hp = if R { p.grow_r(arena, free, h) } else { p.grow(arena, h) };
            }
            let v512 = _mm512_load_si512(line.0.as_ptr() as *const _);
            _mm512_stream_si512(hp as *mut _, v512);
            let full = (f + 1 == Self::K) as usize;
            *self.fill.get_unchecked_mut(h) = ((f + 1) * (1 - full)) as u8;
            *p.head.get_unchecked_mut(h) = hp.add(Self::K * full);
        }
    }
    #[inline(always)]
    pub fn push(&mut self, arena: &mut Arena, h: usize, v: T) {
        if wc_branchless() {
            let mut none = Vec::new();
            return self.push_bl::<false>(arena, &mut none, h, v);
        }
        unsafe {
            let f = *self.fill.get_unchecked(h) as usize;
            let line = self.stage.get_unchecked_mut(h);
            (line.0.as_mut_ptr() as *mut T).add(f).write(v);
            if f + 1 < Self::K {
                *self.fill.get_unchecked_mut(h) = (f + 1) as u8;
                return;
            }
            *self.fill.get_unchecked_mut(h) = 0;
            let p = &mut self.parts;
            let mut hp = *p.head.get_unchecked(h);
            if hp == *p.end.get_unchecked(h) {
                hp = p.grow(arena, h);
            }
            let v512 = _mm512_load_si512(line.0.as_ptr() as *const _);
            _mm512_stream_si512(hp as *mut _, v512);
            *p.head.get_unchecked_mut(h) = hp.add(Self::K);
        }
    }
    /// push, taking new blocks from `free` before the arena.
    #[inline(always)]
    pub fn push_r(&mut self, arena: &mut Arena, free: &mut Vec<*mut u8>, h: usize, v: T) {
        unsafe {
            let f = *self.fill.get_unchecked(h) as usize;
            let line = self.stage.get_unchecked_mut(h);
            (line.0.as_mut_ptr() as *mut T).add(f).write(v);
            if f + 1 < Self::K {
                *self.fill.get_unchecked_mut(h) = (f + 1) as u8;
                return;
            }
            *self.fill.get_unchecked_mut(h) = 0;
            let p = &mut self.parts;
            let mut hp = *p.head.get_unchecked(h);
            if hp == *p.end.get_unchecked(h) {
                hp = p.grow_r(arena, free, h);
            }
            let v512 = _mm512_load_si512(line.0.as_ptr() as *const _);
            _mm512_stream_si512(hp as *mut _, v512);
            *p.head.get_unchecked_mut(h) = hp.add(Self::K);
        }
    }
    /// Writes out the partially filled lines and fences the streaming stores.
    pub fn finish(&mut self, arena: &mut Arena) {
        for h in 0..self.fill.len() {
            let f = self.fill[h] as usize;
            for i in 0..f {
                let v = unsafe { (self.stage[h].0.as_ptr() as *const T).add(i).read() };
                self.parts.push(arena, h, v);
            }
            self.fill[h] = 0;
        }
        unsafe { _mm_sfence() };
    }
}
