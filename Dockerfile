# Shopping Sessions: native Rust pipeline, static binary on an empty image.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
RUN RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static" \
    cargo build --release --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/shopping /shopping

FROM scratch
COPY --from=build /shopping /shopping
ENV PROBE=6
USER 1000:1000
ENTRYPOINT ["/shopping"]
