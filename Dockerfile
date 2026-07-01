# =====================================
# Stage 1: Build dependencies
# =====================================
FROM rust:1.91.1-bookworm AS builder
WORKDIR /app

# Tambahkan protobuf-compiler di sini
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    pkg-config libssl-dev libz-dev openssl ca-certificates curl cmake make protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Batasi paralelisme build cargo agar tidak OOM saat compile
ENV CARGO_BUILD_JOBS=2
ENV CARGO_NET_RETRY=10

COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# =====================================
# Stage 2: Build application
# =====================================
COPY . .

# Pisahkan build worker dan http jadi 2 RUN terpisah
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin worker && \
    cp target/release/worker /app/worker-bin

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin http && \
    cp target/release/http /app/http-bin

# =====================================
# Stage 3: Runtime (Slim Image)
# =====================================
FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libssl3 ca-certificates libzstd1 tzdata curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m appuser

COPY --from=builder --chown=appuser:appuser /app/worker-bin /app/worker
COPY --from=builder --chown=appuser:appuser /app/http-bin /app/http

RUN chmod +x /app/worker /app/http

USER appuser

CMD ["./http"]
