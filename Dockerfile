FROM rust:1.99-alpine AS build
RUN apk add --no-cache musl-dev cmake make g++
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY common ./common
COPY producer ./producer
COPY consumer ./consumer
RUN cargo build --release --locked -p producer -p consumer

# для Кафки
FROM alpine:3.22
RUN apk add --no-cache libstdc++
COPY --from=build /app/target/release/producer /usr/local/bin/producer
COPY --from=build /app/target/release/consumer /usr/local/bin/consumer