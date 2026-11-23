FROM rust:1.99-alpine AS build
RUN apk add --no-cache musl-dev cmake make g++
WORKDIR /app
COPY Cargo.toml ./
COPY common ./common
COPY producer ./producer
RUN cargo build --release -p producer

# для Rust
FROM alpine:3.22
RUN apk add --no-cache libstdc++
COPY --from=build /app/target/release/producer /usr/local/bin/producer
ENTRYPOINT ["producer"]