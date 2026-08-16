use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::{
    HeapRb,
    traits::{Consumer, Observer, Producer, Split},
};
use shared::{ResponseStatus, TcpRequest, TcpResponse, TcpResponseData};
use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, UdpSocket},
};

#[tokio::main]
async fn main() {
    let server_address = "127.0.0.1:8080";

    let mut tcp_stream = TcpStream::connect(server_address)
        .await
        .expect("failed to connect using tcp");

    let _ = tcp_stream
        .write_all(
            serde_json::to_string(&TcpRequest {
                client_id: 0,
                data: shared::TcpRequestData::NewConnection,
            })
            .expect("failed to parse initial request")
            .as_bytes(),
        )
        .await
        .expect("failed to send initial message");

    tcp_stream.readable().await.expect("idk");
    let mut tcp_buf = [0; 4096];

    let mut session_client_id: u32 = 0;

    match tcp_stream.try_read(&mut tcp_buf) {
        Ok(n) => match serde_json::from_slice::<TcpResponse>(&tcp_buf[0..n]) {
            Ok(response) => {
                println!("connection Ok: {}", response.status == ResponseStatus::Ok);
                match response.data {
                    TcpResponseData::NewConnection { client_id } => {
                        session_client_id = client_id;
                        println!("new client id: {}", client_id)
                    }
                    _ => {
                        println!("wrong response")
                    }
                }
            }
            Err(e) => eprintln!("failed to parse tcp packet: {}", e),
        },
        Err(e) => eprintln!("failed to receive tcp packet: {}", e),
    }

    let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());

    let output_socket = socket.clone();
    let input_socket = socket.clone();

    let host = cpal::default_host();

    let input_device = host
        .default_input_device()
        .expect("no input device available");
    let output_device = host
        .default_output_device()
        .expect("no output device available");

    println!("input device: {}", input_device);
    println!("output device: {}", output_device);

    let config: cpal::StreamConfig = input_device
        .default_input_config()
        .expect("stream config init failed")
        .into();

    println!("stream config: {:?}", config);

    let ring_buffer_ms = 150.0;
    let ring_buffer_frames = (ring_buffer_ms / 1000.0) * config.sample_rate as f32;
    let ring_buffer_samples = ring_buffer_frames as usize * config.channels as usize;

    let ring_buffer_size = ring_buffer_samples;

    let microphone_ring_buffer = HeapRb::<f32>::new(ring_buffer_size);
    let (mut microphone_producer, mut microphone_consumer) = microphone_ring_buffer.split();

    let speaker_ring_buffer = HeapRb::<f32>::new(ring_buffer_size);
    let (mut speaker_producer, mut speaker_consumer) = speaker_ring_buffer.split();

    let input_data_fn = move |data: &[f32], _: &cpal::InputCallbackInfo| {
        for &sample in data {
            if microphone_producer.try_push(sample).is_err() {}
        }
    };

    let output_data_fn = move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
        for sample in data {
            *sample = match speaker_consumer.try_pop() {
                Some(s) => s,
                None => 0.0,
            };
        }
    };

    let err_fn = |err| eprintln!("stream error: {}", err);

    let input_stream = input_device
        .build_input_stream(config, input_data_fn, err_fn, None)
        .expect("building input stream failed");
    let output_stream = output_device
        .build_output_stream(config, output_data_fn, err_fn, None)
        .expect("building output stream failed");

    input_stream.play().expect("failed to play input stream");
    output_stream.play().expect("failed to play output stream");

    tokio::spawn(async move {
        let frames_needed = ((20.0 / 1000.0) * config.sample_rate as f32) as usize;
        let mut frame_buffer = vec![0.0; frames_needed];

        loop {
            if microphone_consumer.occupied_len() >= frames_needed {
                microphone_consumer.pop_slice(&mut frame_buffer);

                let mut payload = Vec::with_capacity(frames_needed * 4);

                payload.extend(session_client_id.to_be_bytes());

                for sample in &frame_buffer {
                    payload.extend_from_slice(&sample.to_le_bytes());
                }

                let _ = output_socket.send_to(&payload, server_address).await;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        }
    });

    tokio::spawn(async move {
        let mut receive_buffer = vec![0u8; 4096];

        loop {
            match input_socket.recv_from(&mut receive_buffer).await {
                Ok((len, _)) => {
                    if len < 4 {
                        continue;
                    }

                    let id_bytes: [u8; 4] = receive_buffer[0..4].try_into().unwrap();
                    let stream_id = u32::from_be_bytes(id_bytes);

                    let audio_packet = &receive_buffer[4..len];

                    let mut decoded_samples = Vec::with_capacity(audio_packet.len() / 4);

                    for chunk in audio_packet.chunks_exact(4) {
                        let bytes: [u8; 4] = chunk.try_into().unwrap();

                        decoded_samples.push(f32::from_le_bytes(bytes));
                    }

                    speaker_producer.push_slice(&decoded_samples);
                }
                Err(e) => eprintln!("failed to receive udp packet: {}", e),
            }
        }
    });

    tokio::signal::ctrl_c().await.expect("failed to close");
    println!("shutting down");
}
