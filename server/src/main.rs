use clap::Parser;
use futures::{SinkExt, StreamExt};
use shared::{AudioStreamInfo, ResponseStatus, TcpRequest, TcpRequestData, TcpResponse};
use std::sync::Arc;
use std::{collections::HashMap, net::SocketAddr};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::{RwLock, broadcast},
};
use tokio_util::bytes::Bytes;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(short, long, default_value("0.0.0.0:6767"))]
    server_ip: Option<String>,
}

// TODO: add auth to this
struct Client {
    udp_addr: Option<SocketAddr>,
    audio_streams: Option<Vec<AudioStreamInfo>>,
}

#[allow(dead_code)]
#[derive(Clone)]
enum ServerEvent {
    NewAudioStream(AudioStreamInfo),
    SendAllStreams,
    SendAllOtherStreams(u32),
    UserConnected(u32),
    UserDisconnected(u32),
}

type State = Arc<RwLock<HashMap<u32, Client>>>;

fn scrape_all_client_streams_info(clients: &HashMap<u32, Client>) -> Vec<AudioStreamInfo> {
    let mut all_audio_streams_info: Vec<AudioStreamInfo> = Vec::new();
    for (_, data) in clients {
        if let Some(audio_streams_info) = &data.audio_streams {
            all_audio_streams_info.extend(audio_streams_info.iter().cloned());
        }
    }

    return all_audio_streams_info;
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let listen_address = cli.server_ip.unwrap();

    let (global_event_tx, _) = broadcast::channel::<ServerEvent>(16);

    let state: State = Arc::new(RwLock::new(HashMap::new()));

    let udp_state = state.clone();

    let tcp_listener = TcpListener::bind(listen_address.clone())
        .await
        .expect("cant make tcp listener");
    let udp_socket = UdpSocket::bind(listen_address.clone())
        .await
        .expect("cant udp socket");

    println!("listening on: {}", listen_address);

    tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];

        loop {
            let (len, src_adddress) = udp_socket.recv_from(&mut buf).await.expect("receive error");

            if len < 4 {
                continue;
            }

            let id_bytes: [u8; 4] = buf[0..4].try_into().unwrap();
            let client_id = u32::from_be_bytes(id_bytes);

            let mut clients = udp_state.write().await;
            if let Some(client) = clients.get_mut(&client_id) {
                client.udp_addr = Some(src_adddress);
            }

            for (&other_client_id, other_client) in clients.iter() {
                if other_client_id != client_id {
                    if let Some(destication_udp) = other_client.udp_addr {
                        let _ = udp_socket.send_to(&buf[..len], destication_udp).await;
                    }
                }
            }
        }
    });

    loop {
        let (tcp_stream, tcp_address) = tcp_listener.accept().await.expect("failed to accept tcp");

        let mut framed_tcp_stream = Framed::new(tcp_stream, LengthDelimitedCodec::new());

        let tcp_state = state.clone();

        let global_event_tx_clone = global_event_tx.clone();
        let mut global_event_rx = global_event_tx.subscribe();

        tokio::spawn(async move {
            let current_client_id: u32 = rand::random();

            loop {
                tokio::select! {
                    frame_result = framed_tcp_stream.next() => {
                        match frame_result {
                            None => {
                                let _ = global_event_tx_clone.send(ServerEvent::UserDisconnected(current_client_id));
                                println!("client disconnected: {}", tcp_address);
                                break;
                            }
                            Some(Ok(bytes)) => {
                                match serde_json::from_slice::<TcpRequest>(&bytes) {
                                    Ok(request) => match request.data {
                                        TcpRequestData::NewConnection => {
                                            tcp_state.write().await.insert(
                                                current_client_id,
                                                Client {
                                                    udp_addr: None,
                                                    audio_streams: None,
                                                },
                                            );

                                            println!("new client: {}, from: {}", current_client_id, tcp_address);

                                            let response_json = serde_json::to_string(&TcpResponse {
                                                status: ResponseStatus::Ok,
                                                data: shared::TcpResponseData::NewConnection {
                                                    client_id: current_client_id,
                                                },
                                            }).expect("failed to serialize TcpRequestData::NewAudioStreamAccepted");

                                            let _ = framed_tcp_stream.send(Bytes::from(response_json))
                                            .await
                                            .expect("failed to send message");

                                            let _ = global_event_tx_clone.send(ServerEvent::UserConnected(current_client_id));
                                        }
                                        TcpRequestData::NewAudioStream { audio_stream_info } => {
                                            if let Some(client) = tcp_state.write().await.get_mut(&current_client_id) {
                                                if let Some(audio_streams) = &mut client.audio_streams {
                                                    audio_streams.push(audio_stream_info.clone());
                                                } else {
                                                    client.audio_streams = Some(vec![audio_stream_info.clone()]);
                                                }
                                            }

                                            let response_json = serde_json::to_string(&TcpResponse {
                                                status: ResponseStatus::Ok,
                                                data: shared::TcpResponseData::NewAudioStreamAccepted {
                                                    audio_stream_id: audio_stream_info.stream_id,
                                                },
                                            }).expect("failed to serialize TcpRequestData::NewAudioStreamAccepted");

                                            let _ = framed_tcp_stream.send(Bytes::from(response_json)).await;

                                            let _ = global_event_tx_clone.send(ServerEvent::SendAllOtherStreams(current_client_id));
                                        }
                                        TcpRequestData::DropAudioStream { audio_stream_id } => {
                                            if let Some(client) = tcp_state.write().await.get_mut(&current_client_id) {
                                                if let Some(audio_streams) = &mut client.audio_streams {
                                                    if let Some(idx) = audio_streams.iter().position(|s| s.stream_id == audio_stream_id) {
                                                        audio_streams.remove(idx);
                                                    }
                                                }
                                            }
                                            let _ = global_event_tx_clone.send(ServerEvent::SendAllOtherStreams(current_client_id));
                                        }
                                    },
                                    Err(e) => eprintln!("failed to parse tcp packet: {}", e),
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
                                    ServerEvent::UserConnected(new_user_id) => {
                                        if new_user_id != current_client_id {
                                            let response_json = serde_json::to_string(&TcpResponse {
                                                status: ResponseStatus::Ok,
                                                data: shared::TcpResponseData::UserConnected {
                                                    user_id: new_user_id
                                                },
                                            }).expect("failed to serialize ServerEvent::UserConnected");

                                            let _ = framed_tcp_stream.send(Bytes::from(response_json)).await;

                                            let _ = global_event_tx_clone.send(ServerEvent::SendAllOtherStreams(current_client_id));
                                        }
                                    }
                                    ServerEvent::UserDisconnected(user_id) => {
                                        if user_id != current_client_id {
                                            let response_json = serde_json::to_string(&TcpResponse {
                                                status: ResponseStatus::Ok,
                                                data: shared::TcpResponseData::UserDisconnected {
                                                    user_id: user_id
                                                },
                                            }).expect("failed to serialize ServerEvent::UserDisconnected");

                                            let _ = framed_tcp_stream.send(Bytes::from(response_json)).await;

                                            let _ = global_event_tx_clone.send(ServerEvent::SendAllOtherStreams(current_client_id));
                                        }
                                    }
                                    ServerEvent::SendAllOtherStreams(id) => {
                                        if current_client_id != id {
                                            let clients = tcp_state.read().await;

                                            let response_json = serde_json::to_string(&TcpResponse {
                                                status: ResponseStatus::Ok,
                                                data: shared::TcpResponseData::AudioStreamsFullUpdate {
                                                    audio_stream_info: scrape_all_client_streams_info(&clients),
                                                },
                                            }).expect("failed to serialize ServerEvent::SendAllOtherStreams");

                                            let _ = framed_tcp_stream.send(Bytes::from(response_json)).await;
                                        }
                                    }
                                    ServerEvent::SendAllStreams => {
                                        let clients = tcp_state.read().await;

                                        let response_json = serde_json::to_string(&TcpResponse {
                                            status: ResponseStatus::Ok,
                                            data: shared::TcpResponseData::AudioStreamsFullUpdate {
                                                audio_stream_info: scrape_all_client_streams_info(&clients),
                                            },
                                        }).expect("failed to serialize ServerEvent::SendAllStreams");

                                        let _ = framed_tcp_stream.send(Bytes::from(response_json)).await;
                                    }
                                    _ => {
                                        println!("unhandled event");
                                    }
                                }
                            }
                            Err(e) => eprintln!("error: {}", e),
                        }
                    }
                }
            }
        });
    }
}
