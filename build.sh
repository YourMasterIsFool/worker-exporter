#!/bin/bash
# build.sh - Turbo build script

set -e  # Exit on error

echo "🚀 TURBO BUILD MODE ACTIVATED"

# 1. Enable all optimizations
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
export BUILDKIT_PROGRESS=plain

# 2. Clean hanya yang perlu
if [ "$1" = "clean" ]; then
    echo "🧹 Cleaning only target directory..."
    docker buildx prune -f
    rm -rf target/* 2>/dev/null || true
fi

# 3. Build dengan semua cache
echo "📦 Building with maximum cache..."
time docker buildx build \
    --tag worker-exporter:latest \
    --cache-from type=local,src=.docker-cache \
    --cache-to type=local,dest=.docker-cache,mode=max \
    --build-arg CARGO_BUILD_JOBS=$(nproc) \
    --build-arg RUSTFLAGS="-C opt-level=z" \
    --progress=plain \
    .

echo "✅ Build completed!"
echo "⏱️  Next build will be 10-30 seconds (incremental)"
