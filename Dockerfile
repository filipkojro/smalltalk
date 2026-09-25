FROM rust:alpine as builder
WORKDIR /usr/src/smalltalk
COPY . .
RUN cargo build -p server --release

FROM alpine:latest
COPY --from=builder /usr/src/smalltalk/target/release/server /usr/local/bin/smalltalk
CMD ["smalltalk"]
