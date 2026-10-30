# smalltalk

## running server

### using docker

```bash
docker build -t smalltalk .
docker run -d --rm -p 6767:6767/tcp -p 6767:6767/udp --name smalltalk smalltalk
```

### using cargo

before, open port `6767` tcp and udp

Usage:

```bash
cargo run -p server --release -- [OPTIONS]
```

Options:

```
-s, --server-ip <SERVER_IP>  [default: 0.0.0.0:6767]
-h, --help                   Print help
-V, --version                Print version
```

## build and run client

Usage: 

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
