FROM rust:1.83 AS builder

WORKDIR /app

COPY . .
RUN cargo build --release

FROM ubuntu:22.04

RUN apt update && apt install -y \
    libc6 \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/server .
COPY cassandra.json /app/

EXPOSE 9042 9043

CMD ["./server"]
