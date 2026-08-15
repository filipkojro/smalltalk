use shared::{ConectionResponse, ResponseStatus};
use std::sync::Arc;
use std::{collections::HashMap, net::SocketAddr};
use tokio::io::AsyncWriteExt;
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::RwLock,
};

struct Client {
    tcp_addr: SocketAddr,
    udp_addr: Option<SocketAddr>,
}

type State = Arc<RwLock<HashMap<u32, Client>>>;

#[tokio::main]
async fn main() {
    let listen_address = "0.0.0.0:8080";

    let state: State = Arc::new(RwLock::new(HashMap::new()));

    let tcp_state = state.clone();
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

        let client_id = next_client_id;
        next_client_id += 1;

        tcp_state.write().await.insert(
            client_id,
            Client {
                tcp_addr: tcp_address,
                udp_addr: None,
            },
        );

        let welcome_msg = ConectionResponse {
            status: ResponseStatus::Ok,
            client_id: client_id,
        };

        let _ = tcp_stream
            .write_all(
                serde_json::to_string(&welcome_msg)
                    .expect("failed to parse json")
                    .as_bytes(),
            )
            .await;

        println!(
            "new client: {}, from: {}",
            client_id,
            tcp_state.read().await.get(&client_id).unwrap().tcp_addr
        );
    }
}
