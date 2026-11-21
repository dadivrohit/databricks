//! Minimal Parquet writer: one row group, one PLAIN uncompressed data page per column.

pub enum Values {
    I32(Vec<i32>),
    I64(Vec<i64>),
    /// PLAIN byte arrays: already encoded as (u32 LE len, bytes)*
    Bytes(Vec<u8>),
}

pub struct Column {
    pub name: &'static str,
    pub values: Values,
    /// Some(bits) for an optional column: one bit per row (1 = present), LSB first.
    pub defined: Option<Vec<u8>>,
    pub logical: Logical,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Logical {
    None,
    String,
    Date,
}

struct Thrift {
    out: Vec<u8>,
    last: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Self {
        Thrift { out: Vec::with_capacity(512), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.out.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn header(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let delta = id - last;
        if delta > 0 && delta <= 15 {
            self.out.push(((delta as u8) << 4) | ty);
        } else {
            self.out.push(ty);
            let z = ((id as i64) << 1) ^ ((id as i64) >> 63);
            self.varint(z as u64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.header(id, T_I32);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.header(id, T_I64);
        self.zz(v);
    }
    fn binf(&mut self, id: i16, b: &[u8]) {
        self.header(id, T_BINARY);
        self.varint(b.len() as u64);
        self.out.extend_from_slice(b);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.header(id, T_LIST);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.out.push(((n as u8) << 4) | elem);
        } else {
            self.out.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn struct_begin_field(&mut self, id: i16) {
        self.header(id, T_STRUCT);
        self.last.push(0);
    }
    /// A struct as a list element.
    fn struct_begin(&mut self) {
        self.last.push(0);
    }
    fn struct_end(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
}

fn physical(v: &Values) -> i32 {
    match v {
        Values::I32(_) => 1,
        Values::I64(_) => 2,
        Values::Bytes(_) => 6,
    }
}

/// Encodes the file. `rows` is the row count of every column.
pub fn encode(columns: &[Column], rows: usize) -> Vec<u8> {
    let mut size_hint = 64;
    for c in columns {
        size_hint += match &c.values {
            Values::I32(v) => v.len() * 4,
            Values::I64(v) => v.len() * 8,
            Values::Bytes(b) => b.len(),
        } + rows / 8
            + 64;
    }
    let mut out = Vec::with_capacity(size_hint + 1024);
    out.extend_from_slice(b"PAR1");
    let mut metas = Vec::new(); // (offset, total size)
    for c in columns {
        let mut levels: Vec<u8> = Vec::new();
        if let Some(bits) = &c.defined {
            // RLE/bit-packed hybrid, bit width 1, one bit-packed run.
            let groups = rows.div_ceil(8);
            let mut run = Vec::with_capacity(groups + 10);
            let mut v = ((groups as u64) << 1) | 1;
            while v >= 0x80 {
                run.push((v as u8) | 0x80);
                v >>= 7;
            }
            run.push(v as u8);
            run.extend_from_slice(&bits[..groups]);
            levels.extend_from_slice(&(run.len() as u32).to_le_bytes());
            levels.extend_from_slice(&run);
        }
        let data_len = match &c.values {
            Values::I32(v) => v.len() * 4,
            Values::I64(v) => v.len() * 8,
            Values::Bytes(b) => b.len(),
        };
        let page_len = levels.len() + data_len;
        let mut h = Thrift::new();
        h.i32f(1, 0); // DATA_PAGE
        h.i32f(2, page_len as i32);
        h.i32f(3, page_len as i32);
        h.struct_begin_field(5);
        h.i32f(1, rows as i32);
        h.i32f(2, 0); // PLAIN
        h.i32f(3, 3); // RLE
        h.i32f(4, 3); // RLE
        h.struct_end();
        h.out.push(0); // end PageHeader
        let offset = out.len();
        out.extend_from_slice(&h.out);
        out.extend_from_slice(&levels);
        match &c.values {
            Values::I32(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Values::I64(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Values::Bytes(b) => out.extend_from_slice(b),
        }
        metas.push((offset, out.len() - offset));
    }
    // FileMetaData
    let mut t = Thrift::new();
    t.i32f(1, 1);
    t.list(2, T_STRUCT, columns.len() + 1);
    t.struct_begin();
    t.binf(4, b"schema");
    t.i32f(5, columns.len() as i32);
    t.struct_end();
    for c in columns {
        t.struct_begin();
        t.i32f(1, physical(&c.values));
        t.i32f(3, if c.defined.is_some() { 1 } else { 0 });
        t.binf(4, c.name.as_bytes());
        match c.logical {
            Logical::String => {
                t.i32f(6, 0);
                t.struct_begin_field(10);
                t.struct_begin_field(1);
                t.struct_end();
                t.struct_end();
            }
            Logical::Date => {
                t.i32f(6, 6);
                t.struct_begin_field(10);
                t.struct_begin_field(6);
                t.struct_end();
                t.struct_end();
            }
            Logical::None => {}
        }
        t.struct_end();
    }
    t.i64f(3, rows as i64);
    t.list(4, T_STRUCT, 1);
    t.struct_begin(); // RowGroup
    t.list(1, T_STRUCT, columns.len());
    let mut total = 0i64;
    for (c, &(offset, size)) in columns.iter().zip(&metas) {
        total += size as i64;
        t.struct_begin(); // ColumnChunk
        t.i64f(2, offset as i64);
        t.struct_begin_field(3); // ColumnMetaData
        t.i32f(1, physical(&c.values));
        t.list(2, T_I32, 2);
        t.zz(0);
        t.zz(3);
        t.list(3, T_BINARY, 1);
        t.varint(c.name.len() as u64);
        t.out.extend_from_slice(c.name.as_bytes());
        t.i32f(4, 0);
        t.i64f(5, rows as i64);
        t.i64f(6, size as i64);
        t.i64f(7, size as i64);
        t.i64f(9, offset as i64);
        t.struct_end();
        t.struct_end();
    }
    t.i64f(2, total);
    t.i64f(3, rows as i64);
    t.struct_end(); // RowGroup
    t.binf(6, b"shopping-sessions");
    t.out.push(0);
    out.extend_from_slice(&t.out);
    out.extend_from_slice(&(t.out.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    out
}
