use std::time::Duration;
use std::future::Future;
use std::pin::Pin;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::time::interval;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async,  MaybeTlsStream, WebSocketStream};
use url::Url;
use lazy_static::lazy_static;

use crate::handle::handle_tx;
use crate::decode;

lazy_static! {
    static ref WS: Mutex<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>> = Mutex::new(None);
    static ref SUBSCRIPTION_ID: Mutex<Option<serde_json::Value>> = Mutex::new(None);
}


const WALLET: &str = "wallet-to-monitor";

pub async fn syndica_stream() {
    connect().await;
}

fn connect() -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async {
        let ws_url = format!("pump-wss-url");
        let url = Url::parse(&ws_url).expect("Invalid WebSocket URL");
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

        {
            let mut ws_guard = WS.lock().await;
            *ws_guard = Some(ws_stream);
        }

        {
            let mut ws_guard = WS.lock().await;
            if let Some(ws) = ws_guard.as_mut() {
                let subscribe_message = json!({
                    "method": "subscribeAccountTrade",
                    "keys":  [WALLET],
                });

                ws.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_message.to_string()))
                    .await
                    .expect("Failed to send subscribe message");

                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    
                        let unsubscribe_message = json!({
                            "method": "unsubscribeTokenTrade",
                            "keys": [WALLET],
                        });
                    
                        let mut ws_guard = WS.lock().await;
                        if let Some(ws) = ws_guard.as_mut() {
                            ws.send(tokio_tungstenite::tungstenite::Message::Text(unsubscribe_message.to_string()))
                                .await
                                .ok();
                        }
                    });
                    
            }
        }

        let mut tx_count = 0;
        loop {
            let mut ws_guard = WS.lock().await;
            let ws = match ws_guard.as_mut() {
                Some(ws) => ws,
                None => break,
            };
        
            tokio::select! {
                // Incoming WebSocket message
                msg = ws.next() => {
                    match msg {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                            let tx: serde_json::Value = serde_json::from_str(&text).unwrap();
                
                            if tx.get("mint").is_some() {
                                let decoded: decode::DecodedTransaction = serde_json::from_value(tx.clone()).unwrap();
                                println!("{:?}", tx);
                                
                                // tokio::spawn(handle_tx(decoded, owner, "pump"));
                            } else {
                                println!("{:#?}", tx);
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            eprintln!("❌ WebSocket error: {:?}", e);
                        }
                        None => {
                            println!("🔌 Connection closed");
                        }
                    }
                }
                
        
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    if let Err(e) = ws.send(tokio_tungstenite::tungstenite::Message::Ping(vec![])).await {
                        eprintln!("❌ Failed to send Ping: {:?}", e);
                        break;
                    }
                }
            }
        }
        
    })
}
