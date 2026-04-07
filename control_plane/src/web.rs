use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router, extract::{ConnectInfo, State, ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade}}, response::Response, routing::get
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

        let mut event_rx = data_plane.subscribe_events();
        let mut is_subscribed = false;

        loop {
            tokio::select! {
                // 1. Handle incoming messages from the client
                result = socket.recv() => {
                    let msg = match result {
                        Some(Ok(msg)) => msg,
                        Some(Err(e)) => {
                            error!("Error receiving message: {}", e);
                            break; // Break the loop if the connection drops or errors out
                        }
                        None => break, // Client disconnected
                    };

                    match msg {
                        Message::Text(text) => {
                            Self::handle_text_message(&mut socket, data_plane.clone(), &mut is_subscribed, text).await;                            
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
                
                // 2. Handle outgoing event updates from the DataPlane
                Ok(data_event) = event_rx.recv(), if is_subscribed => {
                    Self::handle_evt(&mut socket, data_event).await;
                }
            }
        }
    }

    async fn handle_text_message(socket: &mut WebSocket, data_plane: Arc<DataPlane>, is_subscribed: &mut bool, text: Utf8Bytes) {
        trace!("Received text from client: {}", text);
        
        match serde_json::from_str::<ws_api::WsRx>(&text) {
            Ok(request) => {
                debug!("Handling {:?}", request);

                match request.payload {
                    Some(ws_api::ws_rx::Payload::SubscribeRequest(request)) => {                           
                        Self::handle_subscribe_streams_req(socket, request, &data_plane).await;
                        *is_subscribed = true;
                    },
                    None => {}
                }                                  
            }
            Err(e) => {
                eprintln!("Failed to deserialize JSON to SubscribeStreamsRequest: {}", e);
            }
        }
    }

    async fn handle_subscribe_streams_req(socket: &mut WebSocket, _request: ws_api::SubscribeStreamsRequest, data_plane: &DataPlane) {
        let reply = ws_api::SubscribeStreamsReply {
            status: 0,
            message: "Succees".to_string()
        };

        let tx = ws_api::WsTx {
            payload: Some(ws_api::ws_tx::Payload::SubscribeReply(reply))
        };
        Self::send_json(socket, tx).await;

        for stream in data_plane.list_provisioned_streams() {
            let evt = ws_api::StreamProvisionedEvent {
                id: stream.id,
                source_port: u32::from(stream.source),
                sink_port: u32::from(stream.sink),
            };
            let tx = ws_api::WsTx {
                payload: Some(ws_api::ws_tx::Payload::StreamProvisionedEvent(evt))
            };
            Self::send_json(socket, tx).await;
        }
    }

    async fn handle_evt(socket: &mut WebSocket, data_event: data_plane::DataPlaneEvent) {
        match data_event {
            data_plane::DataPlaneEvent::StreamProvisioned { id, source_port, sink_port } => {
                let evt = ws_api::StreamProvisionedEvent {
                    id,
                    source_port: u32::from(source_port),
                    sink_port: u32::from(sink_port),
                };
                let tx = ws_api::WsTx {
                    payload: Some(ws_api::ws_tx::Payload::StreamProvisionedEvent(evt))
                };
                Self::send_json(socket, tx).await;
            }
            data_plane::DataPlaneEvent::StreamRxActive { id, port } => {
                // Example: map to another ws_api event here
                // Self::send_json_reply(&mut socket, ws_api::StreamRemovedEvent { id }).await;
            }
            data_plane::DataPlaneEvent::StreamProcessingActive { id } => {
                // Example: map to another ws_api event here
                // Self::send_json_reply(&mut socket, ws_api::StreamRemovedEvent { id }).await;
            }
            data_plane::DataPlaneEvent::StreamTxActive { id, port } => {
                // Example: map to another ws_api event here
                // Self::send_json_reply(&mut socket, ws_api::StreamRemovedEvent { id }).await;
            }
        }
    }

    async fn send_json<T>(socket: &mut WebSocket, reply: T) 
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