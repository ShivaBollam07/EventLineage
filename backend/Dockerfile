# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy everything
COPY . .

# Tidy and download Go modules, then build
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /eventlineage-server ./cmd/server

# Final stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates wget

WORKDIR /app

# Copy binary from builder
COPY --from=builder /eventlineage-server /app/eventlineage-server

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q --spider http://localhost:8080/health || exit 1

# Run
ENTRYPOINT ["/app/eventlineage-server"]
