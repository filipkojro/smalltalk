use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq)]
pub enum ResponseStatus {
    Ok,
    Err,
}

#[derive(Serialize, Deserialize)]
pub struct AudioStreamInfo {
    pub stream_id: u32,
    pub channels: u8,
    pub sample_rate: u32,
}

#[derive(Serialize, Deserialize)]
pub struct TcpRequest {
    pub client_id: u32,
    pub data: TcpRequestData,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TcpRequestData {
    NewConnection,
    DropConnection,
    NewAudioStream { audio_stream_info: AudioStreamInfo },
    DropAudioStream { audio_stream_id: u32 },
}

#[derive(Serialize, Deserialize)]
pub struct TcpResponse {
    pub status: ResponseStatus,
    pub data: TcpResponseData,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TcpResponseData {
    NewConnection {
        client_id: u32,
    },
    Updated,
    UserJoined {
        user_id: u32,
    },
    UserDisconnected {
        user_id: u32,
    },
    AudioStreamsUpdate {
        audio_stream_info: Vec<AudioStreamInfo>,
    },
}
