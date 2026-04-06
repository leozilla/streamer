use std::net::SocketAddr;

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, ConnectInfo},
    response::Response,
    routing::get,
    routing::get_service,
    Router,
};
use tower_http::services::ServeDir;
use tracing::{debug, trace, error};

pub mod ws_api {
    tonic::include_proto!("ws_api");
}

pub struct WebServer {
}

impl WebServer {
    pub fn new() -> Self {
        Self {
        }
    }

    pub async fn start(&self, addr: SocketAddr) -> tokio::io::Result<()> {
        let mgmt_service = ServeDir::new("assets");
        let app = Router::new()
            .nest_service("/mgmt", mgmt_service)
            .route("/ws", get(Self::ws_handler));

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(
            listener, 
            app.into_make_service_with_connect_info::<SocketAddr>()).await?;

        Ok(())
    }

    async fn ws_handler(ws: WebSocketUpgrade, ConnectInfo(addr): ConnectInfo<SocketAddr>) -> Response {
        ws.on_upgrade(move |socket| Self::handle_socket(socket, addr))
    }

    async fn handle_socket(mut socket: WebSocket, peer: SocketAddr) {
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
                    Self::handle_text_msg(&mut socket, text).await;
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

    async fn handle_text_msg(socket: &mut WebSocket, text: axum::extract::ws::Utf8Bytes) {
        trace!("Received text from client: {}", text);

        let reply = ws_api::SubscribeStreamsReply {
            status: 0,
            message: "Succees".to_string()
        };

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