# syntax=docker/dockerfile:1.7

# ============================================================
# Stage 0: Chef
# ============================================================
FROM rust:1.91.1-bookworm AS chef

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        pkg-config \
        cmake \
        make \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --version 0.1.73 --locked


# ============================================================
# Stage 1: Planner
# ============================================================
FROM chef AS planner

WORKDIR /app

COPY . .

RUN cargo chef prepare --recipe-path recipe.json


# ============================================================
# Stage 2: Builder
# ============================================================
FROM chef AS builder

WORKDIR /app

ENV CARGO_INCREMENTAL=0
ENV CARGO_BUILD_JOBS=2

COPY --from=planner /app/recipe.json ./recipe.json

# Compile production dependencies only.
# Production uses jemalloc; DHAT remains disabled.
RUN cargo chef cook \
    --release \
    --no-default-features \
    --features jemalloc \
    --locked \
    --recipe-path recipe.json

COPY . .

RUN cargo build \
        --release \
        --no-default-features \
        --features jemalloc \
        --locked \
        --bin worker \
        --bin http \
    && mkdir -p /out \
    && cp target/release/worker /out/worker \
    && cp target/release/http /out/http


# ============================================================
# Stage 3: Runtime
# ============================================================
FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && useradd \
        --create-home \
        --uid 10001 \
        --shell /usr/sbin/nologin \
        appuser \
    && mkdir -p /app/exports \
    && chown -R appuser:appuser /app

COPY --from=builder \
    --chown=appuser:appuser \
    --chmod=755 \
    /out/worker \
    /app/worker

COPY --from=builder \
    --chown=appuser:appuser \
    --chmod=755 \
    /out/http \
    /app/http

USER appuser

CMD ["/app/worker"]
