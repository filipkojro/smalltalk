use ringbuf::{HeapRb, traits::Split};
use ropus::Decoder;
use shared::{ResponseStatus, TcpRequest, TcpResponse, TcpResponseData};
use std::sync::{Arc, Mutex, RwLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::broadcast::{Receiver, Sender},
};

use crate::models::{
    ClientEvent, ClientInfo, InputAudioStreamsConsumers, InputAudioStreamsEncoders,
    InputAudioStreamsInfo, InputAudioStreamsProducers,
};

pub async fn tcp_connection(
    client_info: Arc<RwLock<ClientInfo>>,
    global_event_tx: Sender<ClientEvent>,
    mut global_event_rx: Receiver<ClientEvent>,
    active_input_audio_streams_producers: InputAudioStreamsProducers,
    active_input_audio_streams_consumers: InputAudioStreamsConsumers,
    active_input_audio_streams_encoders: InputAudioStreamsEncoders,
    active_input_audio_streams_info: InputAudioStreamsInfo,
) {
    let server_address = {
        let info = client_info.read().unwrap();
        info.server_address.clone()
    };

    let mut tcp_stream = TcpStream::connect(server_address)
        .await
        .expect("failed to connect using tcp");

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
                                    let mut client_info_var = client_info.write().unwrap();
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
                                    let mut producer_map = active_input_audio_streams_producers.lock().unwrap();
                                    producer_map.clear();
                                    let mut consumer_map = active_input_audio_streams_consumers.lock().unwrap();
                                    consumer_map.clear();

                                    let mut encoder_map = active_input_audio_streams_encoders.write().unwrap();
                                    encoder_map.clear();

                                    let mut info_map = active_input_audio_streams_info.write().unwrap();
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

                                        let channels: ropus::Channels = {
                                            if audio_stream.channels == 1 {
                                                ropus::Channels::Mono
                                            } else {
                                                ropus::Channels::Stereo
                                            }
                                        };

                                        let decoder = Decoder::new(audio_stream.sample_rate, channels).expect("failed to creat decoder");
                                        encoder_map.insert(
                                            (audio_stream.client_id, audio_stream.stream_id),
                                            Mutex::new(decoder),
                                        );

                                        info_map.insert(
                                            (audio_stream.client_id, audio_stream.stream_id),
                                            audio_stream,
                                        );
                                    }
                                    println!("audio streams update");
                                }
                                TcpResponseData::NewAudioStreamAccepted { audio_stream_id } => {
                                    let _ = global_event_tx.send(ClientEvent::NewAudioStreamAccepted(audio_stream_id));
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
                                let session_client_id = client_info.read().unwrap().client_id;
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
}
