# ---- Base image w/ Go toolchain and modules----
FROM golang:1.25-bookworm AS build-base

WORKDIR /app

# Download and cache Go modules
COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download


# ---- Production Image (build on top of base) ----
FROM build-base AS build-production

# Non-root user for security
RUN useradd -u 1001 nonroot

COPY . .

# Build the static binary from source code
RUN go build \
    -ldflags="-linkmode external -extldflags -static" \
    -tags netgo \
    -o go-redis ./app


# ---- Image for Benchmarking ----
FROM debian:bookworm-slim AS benchmark

# Download redis-tools (for redis-benchmark) and redis-server (official Redis)
RUN apt-get update \
    && apt-get install -y redis-tools redis-server \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary and benchmark script
COPY --from=build-production /app/go-redis /app/go-redis
COPY benchmark/benchmark.sh /benchmark.sh
RUN chmod +x /benchmark.sh

# Run the benchmark script
CMD ["/benchmark.sh"]


# ---- Final Image ----
FROM scratch

# Copy non-root user from build-production image
COPY --from=build-production /etc/passwd /etc/passwd

# Copy the binary from build-production image
COPY --from=build-production /app/go-redis go-redis

USER nonroot

EXPOSE 6379

# Run the binary to start the server
CMD ["./go-redis"]

