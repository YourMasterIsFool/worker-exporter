# syntax=docker/dockerfile:1.7
# =====================================
# Stage 0: Chef (Base Image)
# =====================================
FROM rust:1.91.1-bookworm AS chef
WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    pkg-config libssl-dev libz-dev openssl ca-certificates curl cmake make protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*
# Install cargo-chef untuk caching dependencies
RUN cargo install cargo-chef --version ^0.1 --locked

# =====================================
# Stage 1: Planner (Membuat recipe)
# =====================================
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# =====================================
# Stage 2: Builder (Cook & Build)
# =====================================
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Batasi build jobs jika RAM terbatas agar tidak OOM
ENV CARGO_BUILD_JOBS=2
ENV CARGO_INCREMENTAL=0

# Cook dependencies (Cache layer ini stabil selama Cargo.toml/lock tidak berubah)
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json

# Salin source code dan build biner
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked --bin worker --bin http && \
    cp target/release/worker /app/worker-bin && \
    cp target/release/http /app/http-bin

# =====================================
# Stage 3: Runtime (Debian Slim - Stabil)
# =====================================
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# Install runtime libs
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libssl3 ca-certificates libzstd1 tzdata curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m appuser

RUN mkdir -p /app/exports && chown -R appuser:appuser /app/exports
# Copy biner dari stage builder
COPY --from=builder --chown=appuser:appuser --chmod=755 /app/worker-bin /app/worker
COPY --from=builder --chown=appuser:appuser --chmod=755 /app/http-bin /app/http

USER appuser
CMD ["./http"]
