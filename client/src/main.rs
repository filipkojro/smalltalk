mod audio_streams;
mod models;
mod tcp_connection;

use clap::Parser;
use cpal::traits::HostTrait;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use tokio::net::UdpSocket;
use tokio::sync::broadcast;

use crate::{
    audio_streams::{audio_stream_receiver, audio_stream_spawner},
    models::{
        CliArgs, ClientEvent, ClientInfo, InputAudioStreamsConsumers, InputAudioStreamsEncoders,
        InputAudioStreamsInfo, InputAudioStreamsProducers,
    },
    tcp_connection::tcp_connection,
};

#[tokio::main]
async fn main() {
    let cli = CliArgs::parse();

    let client_info: Arc<RwLock<ClientInfo>> = Arc::new(RwLock::new(ClientInfo {
        client_id: 0,
        server_address: cli.server_ip.unwrap(),
        connected: false,
    }));

    let (global_event_tx, _) = broadcast::channel::<ClientEvent>(16);

    let active_input_audio_streams_producers: InputAudioStreamsProducers =
        Arc::new(Mutex::new(HashMap::new()));
    let active_input_audio_streams_consumers: InputAudioStreamsConsumers =
        Arc::new(Mutex::new(HashMap::new()));

    let active_input_audio_streams_encoders: InputAudioStreamsEncoders =
        Arc::new(RwLock::new(HashMap::new()));

    let active_input_audio_streams_info: InputAudioStreamsInfo =
        Arc::new(RwLock::new(HashMap::new()));

    tokio::spawn(tcp_connection(
        client_info.clone(),
        global_event_tx.clone(),
        global_event_tx.subscribe(),
        active_input_audio_streams_producers.clone(),
        active_input_audio_streams_consumers.clone(),
        active_input_audio_streams_encoders.clone(),
        active_input_audio_streams_info.clone(),
    ));

    let _ = global_event_tx.send(ClientEvent::NewConnection);

    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());

    let host = cpal::default_host();

    let input_device = host
        .default_input_device()
        .expect("no input device available");
    let output_device = host
        .default_output_device()
        .expect("no output device available");

    println!("input device: {}", input_device);
    println!("output device: {}", output_device);

    tokio::spawn(audio_stream_receiver(
        output_device,
        active_input_audio_streams_producers.clone(),
        active_input_audio_streams_consumers.clone(),
        active_input_audio_streams_encoders.clone(),
        active_input_audio_streams_info.clone(),
        udp_socket.clone(),
    ));

    tokio::spawn(audio_stream_spawner(
        input_device,
        global_event_tx.clone(),
        global_event_tx.subscribe(),
        udp_socket.clone(),
        client_info.clone(),
    ));

    tokio::signal::ctrl_c().await.expect("failed to close");
    println!("shutting down");
}
