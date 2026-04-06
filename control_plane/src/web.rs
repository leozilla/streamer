use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, ConnectInfo, State},
    response::Response,
    routing::get,
    Router,
};
use serde::Serialize;
use tower_http::services::ServeDir;
use tracing::{debug, trace, error};
use data_plane::DataPlane;

pub mod ws_api {
    tonic::include_proto!("ws_api");
}

pub struct WebServer {
    data_plane: Arc<DataPlane>,
}

impl WebServer {
    pub fn new(data_plane: Arc<DataPlane>) -> Self {
        Self {
            data_plane,
        }
    }

    pub async fn start(&self, addr: SocketAddr) -> tokio::io::Result<()> {
        let mgmt_service = ServeDir::new("assets");
        let app = Router::new()
            .nest_service("/mgmt", mgmt_service)
            .route("/ws", get(Self::ws_handler))
            .with_state(self.data_plane.clone());

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(
            listener, 
            app.into_make_service_with_connect_info::<SocketAddr>()).await?;

        Ok(())
    }

    async fn ws_handler(
        ws: WebSocketUpgrade,
        ConnectInfo(addr): ConnectInfo<SocketAddr>,
        State(data_plane): State<Arc<DataPlane>>
    ) -> Response {
        ws.on_upgrade(move |socket| Self::handle_socket(socket, addr, data_plane))
    }

    async fn handle_socket(mut socket: WebSocket, peer: SocketAddr, data_plane: Arc<DataPlane>) {
        debug!("Client connected {:}", peer);

        while let Some(result) = socket.recv().await {
            let msg = match result {
                Ok(msg) => msg,
                Err(e) => {
                    error!("Error receiving message: {}", e);
                    break; // Break the loop if the connection drops or errors out
                }
            };

            match msg {
                Message::Text(text) => {
                    trace!("Received text from client: {}", text);
                    
                    match serde_json::from_str::<ws_api::SubscribeStreamsRequest>(&text) {
                        Ok(request) => {
                            debug!("Handling {:?}", request);                            
                            Self::handle_subscribe_streams_req(&mut socket, request, &data_plane).await;
                        }
                        Err(e) => {
                            eprintln!("Failed to deserialize JSON to SubscribeStreamsRequest: {}", e);
                        }
                    }
                }
                Message::Close(_) => {
                    debug!("Client initiated a clean disconnect {:?}", socket);
                    break;
                }
                _ => {
                    trace!("Received non-text message type");
                }
            }
        }
    }

    async fn handle_subscribe_streams_req(socket: &mut WebSocket, _request: ws_api::SubscribeStreamsRequest, _data_plane: &DataPlane) {
        let reply = ws_api::SubscribeStreamsReply {
            status: 0,
            message: "Succees".to_string()
        };

        Self::send_json_reply(socket, reply).await;
    }

    async fn send_json_reply<T>(socket: &mut WebSocket, reply: T) 
    where T: Serialize, {
        match serde_json::to_string(&reply) {
            Ok(reply_json_string) => {
                trace!("Sending JSON: {}", reply_json_string);

                if socket.send(Message::Text(reply_json_string.into())).await.is_err() {
                    debug!("Client disconnected");
                }
            }
            Err(e) => {
                error!("Failed to serialize Protobuf message to JSON: {}", e);
            }
        }
    }
}