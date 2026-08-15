# smalltalk

I think it will be based on:

- `tokio`: async and network connection
- `cpal`: raw audio input and output
- `opus`: encoding and decoding audio stream
- `ffmpeg-next`: capturing, encoding and decoding video stream
- `egui` + `eframe`: ui

## running client

```bash
cargo run -p client
```

## running server

open port `8080` tcp/udp and run

```bash
cargo run -p server
```
