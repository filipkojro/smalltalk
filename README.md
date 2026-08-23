# smalltalk

## build and run client

Usage: 

```bash
cargo run -p client --release -- --server-ip <SERVER_IP>
```

Options:

```
-s, --server-ip <SERVER_IP>  
-h, --help                   Print help
-V, --version                Print version
```

## build and run server

before, open port `8080` tcp/udp

Usage:

```bash
cargo run -p server --release -- [OPTIONS]
```

Options:

```
-s, --server-ip <SERVER_IP>  [default: 0.0.0.0:8080]
-h, --help                   Print help
-V, --version                Print version
```

---

I think it will be based on:

- `tokio`: async and network connection
- `cpal`: raw audio input and output
- `opus`: encoding and decoding audio stream
- `ffmpeg-next`: capturing, encoding and decoding video stream
- `egui` + `eframe`: ui
