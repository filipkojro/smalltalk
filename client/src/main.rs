mod audio_streams;
mod models;
mod tcp_connection;
mod tui;

use clap::Parser;
use cpal::traits::{DeviceTrait, HostTrait};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use tokio::net::UdpSocket;
use tokio::sync::broadcast;

use crate::{
    audio_streams::{audio_stream_receiver, audio_stream_spawner},
    models::{
        AudioControl, CliArgs, ClientEvent, ClientInfo, InputAudioStreamsConsumers,
        InputAudioStreamsEncoders, InputAudioStreamsInfo, InputAudioStreamsProducers,
    },
    tcp_connection::tcp_connection,
    tui::Tui,
};

#[tokio::main]
async fn main() {
    let cli = CliArgs::parse();

    let (global_event_tx, _) = broadcast::channel::<ClientEvent>(16);

    let active_input_audio_streams_producers: InputAudioStreamsProducers =
        Arc::new(Mutex::new(HashMap::new()));
    let active_input_audio_streams_consumers: InputAudioStreamsConsumers =
        Arc::new(Mutex::new(HashMap::new()));

    let active_input_audio_streams_encoders: InputAudioStreamsEncoders =
        Arc::new(RwLock::new(HashMap::new()));

    let active_input_audio_streams_info: InputAudioStreamsInfo =
        Arc::new(RwLock::new(HashMap::new()));

    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());

    let audio_control: Arc<AudioControl> = Arc::new(AudioControl {
        active_input_audio_streams_producers: active_input_audio_streams_producers.clone(),
        active_input_audio_streams_consumers: active_input_audio_streams_consumers.clone(),
        active_input_audio_streams_encoders: active_input_audio_streams_encoders.clone(),
        active_input_audio_streams_info: active_input_audio_streams_info.clone(),
        udp_socket: udp_socket.clone(),
    });

    if let Some(server_ip) = cli.connect_with_defaults {
        let client_info: Arc<RwLock<ClientInfo>> = Arc::new(RwLock::new(ClientInfo {
            client_id: 0,
            server_address: server_ip,
            connected: false,
        }));

        tokio::spawn(tcp_connection(
            client_info.clone(),
            global_event_tx.clone(),
            global_event_tx.subscribe(),
            audio_control.clone(),
        ));

        let _ = global_event_tx.send(ClientEvent::NewConnection);

        let host = cpal::default_host();

        let input_device = host
            .default_input_device()
            .expect("no input device available");
        let input_device_config = input_device
            .default_input_config()
            .expect("failed to get default input config");

        let output_device = host
            .default_output_device()
            .expect("no output device available");
        let output_device_config = output_device
            .default_output_config()
            .expect("failed to get default output config");

        println!("input device: {}", input_device);
        println!("output device: {}", output_device);

        tokio::spawn(audio_stream_receiver(
            output_device,
            output_device_config.into(),
            active_input_audio_streams_producers.clone(),
            active_input_audio_streams_consumers.clone(),
            active_input_audio_streams_encoders.clone(),
            active_input_audio_streams_info.clone(),
            udp_socket.clone(),
        ));

        tokio::spawn(audio_stream_spawner(
            input_device,
            input_device_config.into(),
            global_event_tx.clone(),
            global_event_tx.subscribe(),
            udp_socket.clone(),
            client_info.clone(),
            1,
        ));

        tokio::signal::ctrl_c().await.expect("failed to close");
        println!("shutting down");
    }

    if let Some(server_ip) = cli.server_ip {
        let client_info: Arc<RwLock<ClientInfo>> = Arc::new(RwLock::new(ClientInfo {
            client_id: 0,
            server_address: server_ip,
            connected: false,
        }));

        tokio::spawn(tcp_connection(
            client_info.clone(),
            global_event_tx.clone(),
            global_event_tx.subscribe(),
            audio_control.clone(),
        ));

        let _ = global_event_tx.send(ClientEvent::NewConnection);

        ratatui::run(|terminal| {
            Tui::new().run(
                terminal,
                audio_control.clone(),
                global_event_tx.clone(),
                client_info.clone(),
            )
        });
    }
}
