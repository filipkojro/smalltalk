use clap::Parser;
use shared::{AudioStreamInfo, ResponseStatus, TcpRequest, TcpRequestData, TcpResponse};
use std::sync::Arc;
use std::{collections::HashMap, net::SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::{RwLock, broadcast},
};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(short, long, default_value("0.0.0.0:8080"))]
    server_ip: Option<String>,
}

// TODO: add auth to this
struct Client {
    udp_addr: Option<SocketAddr>,
    audio_streams: Option<Vec<AudioStreamInfo>>,
}

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
        let (mut tcp_stream, tcp_address) =
            tcp_listener.accept().await.expect("failed to accept tcp");

        let tcp_state = state.clone();

        let global_event_tx_clone = global_event_tx.clone();
        let mut global_event_rx = global_event_tx.subscribe();

        tokio::spawn(async move {
            let current_client_id: u32 = rand::random();
            let mut tcp_buf = [0; 4096];

            loop {
                tokio::select! {
                    socket_result = tcp_stream.read(&mut tcp_buf) => {
                        match socket_result {
                            Ok(0) => {
                                let _ = global_event_tx_clone.send(ServerEvent::UserDisconnected(current_client_id));
                                println!("client disconnected: {}", tcp_address);
                                break;
                            }
                            Ok(n) => match serde_json::from_slice::<TcpRequest>(&tcp_buf[0..n]) {
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
                                        let _ = tcp_stream
                                            .write_all(
                                                serde_json::to_string(&TcpResponse {
                                                    status: ResponseStatus::Ok,
                                                    data: shared::TcpResponseData::NewConnection {
                                                        client_id: current_client_id,
                                                    },
                                                })
                                                .expect("failed to parse response json TcpRequestData::NewConnection")
                                                .as_bytes(),
                                            )
                                            .await;

                                        let _ = global_event_tx_clone.send(ServerEvent::UserConnected(current_client_id));
                                    }
                                    TcpRequestData::NewAudioStream { audio_stream_info } => {
                                        if let Some(client) = tcp_state.write().await.get_mut(&current_client_id) {
                                            if let Some(audio_streams) = &mut client.audio_streams {
                                                audio_streams.push(audio_stream_info);
                                            } else {
                                                client.audio_streams = Some(vec![audio_stream_info]);
                                            }
                                        }
                                        let _ = tcp_stream
                                            .write_all(
                                                serde_json::to_string(&TcpResponse {
                                                    status: ResponseStatus::Ok,
                                                    data: shared::TcpResponseData::NewAudioStreamAccepted {
                                                        audio_stream_id: audio_stream_info.stream_id,
                                                    },
                                                })
                                                .expect("failed to parse response json TcpRequestData::NewConnection")
                                                .as_bytes(),
                                            )
                                            .await;

                                        let _ = global_event_tx_clone.send(ServerEvent::SendAllOtherStreams(current_client_id));
                                    }
                                    TcpRequestData::DropAudioStream { audio_stream_id } => {
                                        if let Some(client) = tcp_state.write().await.get_mut(&current_client_id) {
                                            if let Some(audio_streams) = &mut client.audio_streams {
                                                let mut maybe_idx: Option<usize> = None;
                                                for (i, audio_stream) in audio_streams.iter_mut().enumerate() {
                                                    if audio_stream.stream_id == audio_stream_id {
                                                        maybe_idx = Some(i);
                                                    }
                                                }
                                                if let Some(idx) = maybe_idx {
                                                    audio_streams.remove(idx);
                                                }
                                            }
                                        }
                                        let _ = global_event_tx_clone.send(ServerEvent::SendAllOtherStreams(current_client_id));
                                    }
                                    _ => eprintln!("i dont support other requests yet"),
                                },
                                Err(e) => eprintln!("failed to parse tcp packet: {}", e),
                            },
                            Err(e) => eprintln!("failed to receive tcp packet: {}", e),
                        }
                    }
                    event_result = global_event_rx.recv() => {
                        match event_result {
                            Ok(event) => {
                                match event {
                                    ServerEvent::UserConnected(new_user_id) => {
                                        if new_user_id != current_client_id {
                                            let _ = tcp_stream
                                                .write_all(
                                                    serde_json::to_string(&TcpResponse {
                                                        status: ResponseStatus::Ok,
                                                        data: shared::TcpResponseData::UserConnected {
                                                            user_id: new_user_id
                                                        },
                                                    })
                                                    .expect("failed to parse response json ServerEvent::UserConnected")
                                                    .as_bytes(),
                                                )
                                                .await;

                                            let _ = global_event_tx_clone.send(ServerEvent::SendAllOtherStreams(current_client_id));
                                        }
                                    }
                                    ServerEvent::UserDisconnected(user_id) => {
                                        if user_id != current_client_id {
                                            let _ = tcp_stream
                                                .write_all(
                                                    serde_json::to_string(&TcpResponse {
                                                        status: ResponseStatus::Ok,
                                                        data: shared::TcpResponseData::UserDisconnected {
                                                            user_id: user_id
                                                        },
                                                    })
                                                    .expect("failed to parse response json ServerEvent::UserConnected")
                                                    .as_bytes(),
                                                )
                                                .await;

                                            let _ = global_event_tx_clone.send(ServerEvent::SendAllOtherStreams(current_client_id));
                                        }
                                    }
                                    ServerEvent::SendAllOtherStreams(id) => {
                                        if current_client_id != id {
                                            let clients = tcp_state.read().await;

                                            let _ = tcp_stream
                                                .write_all(
                                                    serde_json::to_string(&TcpResponse {
                                                        status: ResponseStatus::Ok,
                                                        data: shared::TcpResponseData::AudioStreamsFullUpdate {
                                                            audio_stream_info: scrape_all_client_streams_info(&clients),
                                                        },
                                                    })
                                                    .expect("failed to parse response json TcpRequestData::NewConnection")
                                                    .as_bytes(),
                                                )
                                                .await;
                                        }
                                    }
                                    ServerEvent::SendAllStreams => {
                                        let clients = tcp_state.read().await;

                                        let _ = tcp_stream
                                            .write_all(
                                                serde_json::to_string(&TcpResponse {
                                                    status: ResponseStatus::Ok,
                                                    data: shared::TcpResponseData::AudioStreamsFullUpdate {
                                                        audio_stream_info: scrape_all_client_streams_info(&clients),
                                                    },
                                                })
                                                .expect("failed to parse response json TcpRequestData::NewConnection")
                                                .as_bytes(),
                                            )
                                            .await;
                                    }
                                    _ => {
                                        println!("lkdahjsf");
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
