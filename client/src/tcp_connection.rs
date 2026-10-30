use futures::{SinkExt, StreamExt};
use shared::{TcpRequest, TcpResponse, TcpResponseData};
use std::sync::{Arc, RwLock};
use tokio::{
    net::TcpStream,
    sync::broadcast::{Receiver, Sender},
};
use tokio_util::{
    bytes::Bytes,
    codec::{Framed, LengthDelimitedCodec},
};

use crate::models::{AudioControl, ClientEvent, ClientInfo};

pub async fn tcp_connection(
    client_info: Arc<RwLock<ClientInfo>>,
    global_event_tx: Sender<ClientEvent>,
    mut global_event_rx: Receiver<ClientEvent>,
    audio_control: Arc<AudioControl>,
) {
    let server_address = {
        let info = client_info.read().unwrap();
        info.server_address.clone()
    };

    let tcp_stream = TcpStream::connect(server_address)
        .await
        .expect("failed to connect using tcp");

    let mut framed_tcp_stream = Framed::new(tcp_stream, LengthDelimitedCodec::new());

    loop {
        tokio::select! {
            frame_result = framed_tcp_stream.next() => {
                match frame_result {
                    None => {
                        // println!("server shut down");
                        break;
                    }
                    Some(Ok(bytes)) => {
                        match serde_json::from_slice::<TcpResponse>(&bytes) {
                            Ok(response) => {
                                // println!("connection Ok: {}", response.status == ResponseStatus::Ok);
                                match response.data {
                                    TcpResponseData::NewConnection { client_id } => {
                                        let mut client_info_var = client_info.write().unwrap();
                                        client_info_var.client_id = client_id;
                                        client_info_var.connected = true;
                                        // println!("fetched client id: {}", client_id);
                                    }
                                    TcpResponseData::UserConnected { .. } => {
                                        // println!("new user connected: {}", user_id);
                                    }
                                    TcpResponseData::UserDisconnected { .. } => {
                                        // println!("user disconnected: {}", user_id);
                                    }
                                    TcpResponseData::AudioStreamsFullUpdate { audio_stream_info } => {
                                        let mut info_map = audio_control.active_input_audio_streams_info.write().unwrap();
                                        info_map.clear();

                                        for audio_stream in audio_stream_info {
                                            info_map.insert(
                                                (audio_stream.client_id, audio_stream.stream_id),
                                                audio_stream,
                                            );
                                        }
                                        // println!("audio streams update");
                                    }
                                    TcpResponseData::NewAudioStreamAccepted { audio_stream_id } => {
                                        let _ = global_event_tx.send(ClientEvent::NewAudioStreamAccepted(audio_stream_id));
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("parsing error: {}", e);
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("socket read error: {}", e);
                            break;
                    }
                }
            }
            event_result = global_event_rx.recv() => {
                match event_result {
                    Ok(event) => {
                        match event {
                            ClientEvent::NewConnection => {
                                let request_json = serde_json::to_string(&TcpRequest {
                                    client_id: 0,
                                    data: shared::TcpRequestData::NewConnection,
                                })
                                .expect("failed to parse initial request");

                                framed_tcp_stream.send(Bytes::from(request_json))
                                .await
                                .expect("failed to send initial message");

                            }
                            ClientEvent::NewAudioStreamRequest(audio_stream_info) => {
                                // println!("new audio stream request");
                                let session_client_id = client_info.read().unwrap().client_id;

                                let request_json = serde_json::to_string(&TcpRequest {
                                    client_id: session_client_id,
                                    data: shared::TcpRequestData::NewAudioStream {
                                        audio_stream_info
                                    }
                                }).expect("failed to parse initial request");

                                framed_tcp_stream.send(Bytes::from(request_json))
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
