# smalltalk

smalltalk is a lightweight, self-hosted voice chat app written in Rust

## How it works

When server is working participants connect from their terminals and choose a microphone and speakers from TUI created with [ratatui](https://docs.rs/ratatui/latest/ratatui/).
When a user speaks, [cpal](https://docs.rs/cpal/latest/cpal/) captures the microphone audio.
Next [rubato](https://docs.rs/rubato/latest/rubato/) converts it to 48kHz (same sample rate is used for every transmission), and [ropus](https://docs.rs/ropus/latest/ropus/) encodes it into Opus packet.
On the receiving end, the client do the same in reverse.
The server ralays voice packets over UDP for lower latency and network use.
A separate TCP connection scarries session and stream update

## Resource usage

In a one-one-one call, the client used about 1% CPU and about 20MB of RAM on an Apple M3 laptop.
During the same call, the server ran in Docker on an Intel Core i5-12600k and used under 1% CPU and under 2MB of RAM

***Voice traffic is currently unencrypted. Avoid using smalltalk for sensitive conversations.***

## Client

### Run with Cargo

```bash
cargo run -p client --release -- --server-ip <SERVER_IP>
```

Options:
```
-c, --connect-with-defaults <CONNECT_WITH_DEFAULTS>
-s, --server-ip <SERVER_IP>
-h, --help                                           Print help
-V, --version                                        Print version
```

Example

```bash
cargo run -p client --release -- --server-ip 192.168.14.95:6767
```

## Server

### Run with Docker

```bash
docker build -t smalltalk .
docker run -d --rm -p 6767:6767/tcp -p 6767:6767/udp --name smalltalk smalltalk
```

### Run with Cargo

before, open port `6767` tcp and udp

```bash
cargo run -p server --release -- [OPTIONS]
```

Options:

```
-s, --server-ip <SERVER_IP>  [default: 0.0.0.0:6767]
-h, --help                   Print help
-V, --version                Print version
```

Example

```bash
cargo run -p server --release
```

# Gallery

## Client's input and output device selection

![client device select](readme-imgs/client-device-select.png)

## Server logs

![server logs](readme-imgs/server-logs.png)
