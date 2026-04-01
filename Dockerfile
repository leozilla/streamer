# Builder stage
FROM rust:latest AS builder

WORKDIR /usr/src/streamer

RUN apt-get update && apt-get install -y protobuf-compiler

COPY . .

RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /usr/src/streamer/target/release/streamer /app/streamer
COPY --from=builder /usr/src/streamer/config.yml /app/config.yml

CMD ["./streamer"]