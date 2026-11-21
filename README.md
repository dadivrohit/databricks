# Shopping Sessions: native Rust pipeline

A single static Rust binary implements `rules.md`: memory-mapped CSV parsing in parallel,
compact integer IDs, union-find over linked IDs, hash-partitioned event dedup, per-person
sessionization with last-click attribution and 7-day assists, and a minimal Parquet writer.

Build: `docker build -t shopping-sessions .` (multi-stage: Rust builder, `FROM scratch` runtime).
Run locally without Docker: `cargo build --release && target/release/shopping DATA_DIR OUTPUT_DIR`.
