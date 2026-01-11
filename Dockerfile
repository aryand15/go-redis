# ---- Base Image ----
FROM golang:1.25-bookworm as build-base

WORKDIR /app

# Download and cache Go modules
COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

    
# ---- Production Image (build on top of base) ----
FROM build-base as build-production

# Non-root user for security
RUN useradd -u 1001 nonroot

COPY . .

# Build the static binary from source code
RUN go build \
    -ldflags="-linkmode external -extldflags -static" \
    -tags netgo \
    -o go-redis ./app


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

