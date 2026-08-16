use shared::{AudioStreamInfo, ResponseStatus, TcpRequest, TcpRequestData, TcpResponse};
use std::sync::Arc;
use std::{collections::HashMap, net::SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::{RwLock, broadcast},
};

struct Client {
    tcp_addr: SocketAddr,
    udp_addr: Option<SocketAddr>,
    audio_streams: Option<Vec<AudioStreamInfo>>,
}

#[derive(Clone, Debug)]
enum ServerEvent {
    NewStream(u32),
    UserConnected(u32),
    UserDisconnected(u32),
    ClosedStream(u32),
}

type State = Arc<RwLock<HashMap<u32, Client>>>;

#[tokio::main]
async fn main() {
    let listen_address = "0.0.0.0:8080";

    let (global_event_tx, _) = broadcast::channel::<ServerEvent>(16);

    let state: State = Arc::new(RwLock::new(HashMap::new()));

    let udp_state = state.clone();

    let tcp_listener = TcpListener::bind(listen_address)
        .await
        .expect("cant make tcp listener");
    let udp_socket = UdpSocket::bind(listen_address)
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

    let mut next_client_id: u32 = 1;

    loop {
        let (mut tcp_stream, tcp_address) =
            tcp_listener.accept().await.expect("failed to accept tcp");

        let tcp_state = state.clone();

        let global_event_tx_clone = global_event_tx.clone();
        let mut global_event_rx = global_event_tx.subscribe();

        tokio::spawn(async move {
            let mut current_client_id: u32 = 0;
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
                                        current_client_id = next_client_id;
                                        next_client_id += 1;
                                        tcp_state.write().await.insert(
                                            current_client_id,
                                            Client {
                                                tcp_addr: tcp_address,
                                                udp_addr: None,
                                                audio_streams: None,
                                            },
                                        );
                                        let _ = global_event_tx_clone.send(ServerEvent::UserConnected(current_client_id));
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
                                    }
                                    ServerEvent::UserDisconnected(user_id) => {
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
