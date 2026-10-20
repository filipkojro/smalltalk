use clap::Parser;
use ringbuf::{HeapRb, wrap::caching::Caching};
use ropus::Decoder;
use rubato::Fft;
use shared::AudioStreamInfo;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use tokio::net::UdpSocket;

#[derive(Parser)]
#[command(version, about)]
pub struct CliArgs {
    #[arg(short, long, required(false))]
    pub connect_with_defaults: Option<String>,

    #[arg(short, long, required(false))]
    pub server_ip: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum TuiScreen {
    DeviceSelect,
    Blank,
    Exit,
}

#[allow(dead_code)]
#[derive(Clone)]
pub enum ClientEvent {
    NewConnection,
    NewAudioStreamRequest(AudioStreamInfo),
    NewAudioStreamAccepted(u8),
}

pub struct ClientInfo {
    pub client_id: u32,
    pub server_address: String,
    pub connected: bool,
}

pub type AudioStreamKey = (u32, u8);

pub type InputAudioStreamsProducers =
    HashMap<AudioStreamKey, Caching<Arc<HeapRb<f32>>, true, false>>;
pub type InputAudioStreamsConsumers =
    Arc<Mutex<HashMap<AudioStreamKey, Caching<Arc<HeapRb<f32>>, false, true>>>>;

pub type InputAudioStreamsDecoders = HashMap<AudioStreamKey, Decoder>;
pub type InputAudioStreamsResamplers = HashMap<AudioStreamKey, Fft<f32>>;

pub type InputAudioStreamsInfo = Arc<RwLock<HashMap<AudioStreamKey, AudioStreamInfo>>>;

pub struct AudioControl {
    pub active_input_audio_streams_info: InputAudioStreamsInfo,
    pub udp_socket: Arc<UdpSocket>,
}
