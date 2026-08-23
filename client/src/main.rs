use cpal::{
    Device,
    traits::{DeviceTrait, HostTrait, StreamTrait},
};
use ringbuf::{
    HeapRb, consumer,
    traits::{Consumer, Observer, Producer, Split},
    wrap::caching::Caching,
};
use shared::{AudioStreamInfo, ResponseStatus, TcpRequest, TcpResponse, TcpResponseData};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
};

#[derive(Clone)]
enum ClientEvent {
    NewConnection,
    NewAudioStreamRequest(AudioStreamInfo),
    NewAudioStreamAccepted(u8),
}

type AudioStreamKey = (u32, u8);

type InputAudioStreamsProducers =
    Arc<Mutex<HashMap<AudioStreamKey, Caching<Arc<HeapRb<f32>>, true, false>>>>;
type InputAudioStreamsConsumers =
    Arc<Mutex<HashMap<AudioStreamKey, Caching<Arc<HeapRb<f32>>, false, true>>>>;

type InputAudioStreamsInfo = Arc<RwLock<HashMap<AudioStreamKey, AudioStreamInfo>>>;

struct ClientInfo {
    client_id: u32,
    server_address: &'static str,
    connected: bool,
}

#[tokio::main]
async fn main() {
    let client_info: Arc<RwLock<ClientInfo>> = Arc::new(RwLock::new(ClientInfo {
        client_id: 0,
        server_address: "127.0.0.1:8080",
        connected: false,
    }));

    let (global_event_tx, _) = broadcast::channel::<ClientEvent>(16);

    let active_input_audio_streams_producers: InputAudioStreamsProducers =
        Arc::new(Mutex::new(HashMap::new()));
    let active_input_audio_streams_consumers: InputAudioStreamsConsumers =
        Arc::new(Mutex::new(HashMap::new()));
    let active_input_audio_streams_info: InputAudioStreamsInfo =
        Arc::new(RwLock::new(HashMap::new()));

    let active_input_audio_streams_producers_tcp = active_input_audio_streams_producers.clone();
    let active_input_audio_streams_consumers_tcp = active_input_audio_streams_consumers.clone();
    let active_input_audio_streams_info_tcp = active_input_audio_streams_info.clone();

    let server_address = {
        let info = client_info.read().unwrap();
        info.server_address
    };

    let mut tcp_stream = TcpStream::connect(server_address)
        .await
        .expect("failed to connect using tcp");
    // tcp_stream.readable().await.expect("idk");
    //
    let client_info_tcp = client_info.clone();

    let mut global_event_rx = global_event_tx.subscribe();

    let global_event_tx_clone = global_event_tx.clone();

    // tcp connection
    tokio::spawn(async move {
        let mut tcp_buf = [0u8; 4096];
        loop {
            tokio::select! {
                socket_result = tcp_stream.read(&mut tcp_buf) => {

                    match socket_result {
                        Ok(0) => {
                            println!("server shut down");
                            break;
                        }
                        Ok(n) => {
                            let ssss = String::from_utf8(tcp_buf[0..n].to_vec()).unwrap();
                            println!("json data: {}", ssss);

                            match serde_json::from_slice::<TcpResponse>(&tcp_buf[0..n]) {
                            Ok(response) => {
                                println!("connection Ok: {}", response.status == ResponseStatus::Ok);
                                match response.data {
                                    TcpResponseData::NewConnection { client_id } => {
                                        let mut client_info_var = client_info_tcp.write().unwrap();
                                        client_info_var.client_id = client_id;
                                        client_info_var.connected = true;
                                        println!("fetched client id: {}", client_id);
                                    }
                                    TcpResponseData::UserConnected { user_id } => {
                                        println!("new user connected: {}", user_id);
                                    }
                                    TcpResponseData::UserDisconnected { user_id } => {
                                        println!("user disconnected: {}", user_id);
                                    }
                                    TcpResponseData::AudioStreamsFullUpdate { audio_stream_info } => {
                                        let mut producer_map = active_input_audio_streams_producers_tcp.lock().unwrap();
                                        producer_map.clear();
                                        let mut consumer_map = active_input_audio_streams_consumers_tcp.lock().unwrap();
                                        consumer_map.clear();

                                        let mut info_map = active_input_audio_streams_info_tcp.write().unwrap();
                                        info_map.clear();


                                        for audio_stream in audio_stream_info {
                                            let ring_buffer_ms = 150.0;
                                            let ring_buffer_frames = (ring_buffer_ms / 1000.0) * audio_stream.sample_rate as f32;
                                            let ring_buffer_samples = ring_buffer_frames as usize * audio_stream.channels as usize;
                                            let ring_buffer_size = ring_buffer_samples;
                                            let ring_buffer = HeapRb::<f32>::new(ring_buffer_size);
                                            let (producer, consumer) = ring_buffer.split();

                                            producer_map.insert(
                                                (audio_stream.client_id, audio_stream.stream_id),
                                                producer,
                                            );
                                            consumer_map.insert(
                                                (audio_stream.client_id, audio_stream.stream_id),
                                                consumer,
                                            );
                                            info_map.insert(
                                                (audio_stream.client_id, audio_stream.stream_id),
                                                audio_stream,
                                            );
                                        }
                                        println!("audio streams update");
                                    }
                                    TcpResponseData::NewAudioStreamAccepted { audio_stream_id } => {
                                        let _ = global_event_tx_clone.send(ClientEvent::NewAudioStreamAccepted(audio_stream_id));
                                    }
                                    _ => {
                                        println!("wrong response")
                                    }
                                }
                            }
                            Err(e) => eprintln!("failed to parse tcp packet: {}", e),}
                        },
                        Err(e) => eprintln!("failed to receive tcp packet: {}", e),
                    }
                }
                event_result = global_event_rx.recv() => {
                    match event_result {
                        Ok(event) => {
                            match event {
                                ClientEvent::NewConnection => {
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
                                }
                                ClientEvent::NewAudioStreamRequest(audio_stream_info) => {
                                    println!("new audio stream request");
                                    let session_client_id = client_info_tcp.read().unwrap().client_id;
                                    let _ = tcp_stream
                                        .write_all(
                                            serde_json::to_string(&TcpRequest {
                                                client_id: session_client_id,
                                                data: shared::TcpRequestData::NewAudioStream {
                                                    audio_stream_info
                                                },
                                            })
                                            .expect("failed to parse initial request")
                                            .as_bytes(),
                                        )
                                        .await
                                        .expect("failed to send initial message");
                                }
                                _ => {}
                            }
                        }
                        Err(e) => eprintln!("error: {}", e),
                    }
                }
            }
        }
    });

    let _ = global_event_tx.send(ClientEvent::NewConnection);

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

    let speaker_config: cpal::StreamConfig = output_device
        .default_output_config()
        .expect("stream config init failed")
        .into();

    println!("speaker config: {:?}", speaker_config);

    let active_input_audio_streams_consumer_cpal = active_input_audio_streams_consumers.clone();

    let output_data_fn = move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
        let mut streams = active_input_audio_streams_consumer_cpal.lock().unwrap();
        for sample in data {
            let mut mixed_value: f32 = 0.0;

            for (_key, consumer) in streams.iter_mut() {
                mixed_value += match consumer.try_pop() {
                    Some(s) => s,
                    None => 0.0,
                }
            }

            *sample = mixed_value;
        }
    };

    let err_fn = |err| eprintln!("stream error: {}", err);
    let output_stream = output_device
        .build_output_stream(speaker_config, output_data_fn, err_fn, None)
        .expect("building output stream failed");

    output_stream.play().expect("failed to play output stream");

    let active_input_audio_streams_producers_udp = active_input_audio_streams_producers.clone();
    let active_input_audio_streams_info_udp = active_input_audio_streams_info.clone();
    let speaker_config_clone = speaker_config.clone();

    tokio::spawn(async move {
        let mut receive_buffer = vec![0u8; 4096];

        loop {
            match input_socket.recv_from(&mut receive_buffer).await {
                Ok((len, _)) => {
                    if len < 4 {
                        continue;
                    }

                    let client_id_bytes: [u8; 4] = receive_buffer[0..4].try_into().unwrap();
                    let client_id = u32::from_be_bytes(client_id_bytes);

                    let stream_id_bytes = receive_buffer[4];
                    let stream_id = u8::from_be(stream_id_bytes);

                    let audio_packet = &receive_buffer[5..len];

                    let mut decoded_samples = Vec::with_capacity(audio_packet.len() / 4);

                    for chunk in audio_packet.chunks_exact(4) {
                        let bytes: [u8; 4] = chunk.try_into().unwrap();

                        decoded_samples.push(f32::from_le_bytes(bytes));
                    }

                    let mut active_streams_map =
                        active_input_audio_streams_producers_udp.lock().unwrap();

                    if let Some(producer) = active_streams_map.get_mut(&(client_id, stream_id)) {
                        if let Some(&stream_info) = active_input_audio_streams_info_udp
                            .read()
                            .unwrap()
                            .get(&(client_id, stream_id))
                        {
                            if stream_info.channels == speaker_config_clone.channels {
                                producer.push_slice(&decoded_samples);
                            } else {
                                let n = stream_info.channels;
                                let mut step = 0;
                                decoded_samples.retain(|_| {
                                    if step == 0 {
                                        step = n - 1;
                                        true
                                    } else {
                                        step -= 1;
                                        false
                                    }
                                });
                                if speaker_config_clone.channels == 1 {
                                    producer.push_slice(&decoded_samples);
                                } else {
                                    let mut out = vec![
                                        0f32;
                                        decoded_samples.len()
                                            * speaker_config_clone.channels as usize
                                    ];
                                    for (item, chunk) in
                                        decoded_samples.iter().zip(out.chunks_exact_mut(
                                            speaker_config_clone.channels as usize,
                                        ))
                                    {
                                        chunk.fill(*item);
                                    }
                                    producer.push_slice(&out);
                                }
                            }
                        }
                    }

                    // speaker_producer.push_slice(&decoded_samples);
                }
                Err(e) => eprintln!("failed to receive udp packet: {}", e),
            }
        }
    });

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let broadcast_transmitter = global_event_tx.clone();
    let broadcast_receiver = global_event_tx.subscribe();

    let client_info_audio_spawner = client_info.clone();

    // let _ = audio_stream_spawner(
    //     input_device,
    //     broadcast_transmitter,
    //     broadcast_receiver,
    //     output_socket,
    //     client_info_audio_spawner,
    // )
    // .await;

    tokio::spawn(audio_stream_spawner(
        input_device,
        broadcast_transmitter,
        broadcast_receiver,
        output_socket,
        client_info_audio_spawner,
    ));

    tokio::signal::ctrl_c().await.expect("failed to close");
    println!("shutting down");
}

async fn audio_stream_spawner(
    input_device: Device,
    broadcast_transmitter: Sender<ClientEvent>,
    mut broadcast_receiver: Receiver<ClientEvent>,
    udp_socket: Arc<UdpSocket>,
    clinet_info: Arc<RwLock<ClientInfo>>,
) {
    let config: cpal::StreamConfig = input_device
        .default_input_config()
        .expect("input stream config init failed")
        .into();

    let current_stream_id: u8 = rand::random();

    loop {
        let (connected, client_id) = {
            let info = clinet_info.read().unwrap();
            (info.connected, info.client_id)
        };
        if connected {
            let _ =
                broadcast_transmitter.send(ClientEvent::NewAudioStreamRequest(AudioStreamInfo {
                    client_id: client_id,
                    stream_id: current_stream_id,
                    channels: config.channels,
                    sample_rate: config.sample_rate,
                }));
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    loop {
        match broadcast_receiver.recv().await {
            Ok(ClientEvent::NewAudioStreamAccepted(stream_id))
                if stream_id == current_stream_id =>
            {
                println!("new stream accepted: {}", stream_id);
                break;
            }
            Ok(_) => {}
            Err(e) => eprintln!("error: {}", e),
        }
    }

    let ring_buffer_ms = 150.0;
    let ring_buffer_frames = (ring_buffer_ms / 1000.0) * config.sample_rate as f32;
    let ring_buffer_samples = ring_buffer_frames as usize * config.channels as usize;
    let ring_buffer_size = ring_buffer_samples;
    let ring_buffer = HeapRb::<f32>::new(ring_buffer_size);
    let (mut producer, mut consumer) = ring_buffer.split();

    let input_data_fn = move |data: &[f32], _: &cpal::InputCallbackInfo| {
        for &sample in data {
            if producer.try_push(sample).is_err() {}
        }
    };
    let err_fn = |err| eprintln!("stream error: {}", err);

    let input_stream = input_device
        .build_input_stream(config, input_data_fn, err_fn, None)
        .expect("building input stream failed");

    input_stream.play().expect("failed to play input stream");

    let frames_needed = ((20.0 / 1000.0) * config.sample_rate as f32) as usize;
    let mut frame_buffer = vec![0.0; frames_needed];

    loop {
        if consumer.occupied_len() >= frames_needed {
            consumer.pop_slice(&mut frame_buffer);

            let mut payload = Vec::with_capacity(4 + 1 + frames_needed * 4);

            let (server_address, client_id) = {
                let info = clinet_info.read().unwrap();
                (info.server_address, info.client_id)
            };

            payload.extend(client_id.to_be_bytes());
            payload.extend(current_stream_id.to_be_bytes());

            for sample in &frame_buffer {
                payload.extend_from_slice(&sample.to_le_bytes());
            }

            let _ = udp_socket.send_to(&payload, server_address).await;
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
}
