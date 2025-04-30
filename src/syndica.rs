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

const TOKEN: &str = "syndica-token";
const WALLET: &str = "wallet-to-monitor";

pub async fn syndica_stream() {
    connect().await;
}

fn connect() -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async {
        let ws_url = format!("wss://api.syndica.io/api-token/{}", TOKEN);
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
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "chainstream.transactionsSubscribe",
                    "params": {
                        "network": "solana-mainnet",
                        "verified": false,
                        "filter": {
                            "commitment": "processed",
                            "accountKeys": {
                                "all": [WALLET],
                            },
                        },
                    }
                });

                ws.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_message.to_string()))
                    .await
                    .expect("Failed to send subscribe message");

                let mut ping_interval = interval(Duration::from_secs(30));
                tokio::spawn(async move {
                    loop {
                        ping_interval.tick().await;
                        let mut ws_guard = WS.lock().await;
                        if let Some(ws) = ws_guard.as_mut() {
                            ws.send(tokio_tungstenite::tungstenite::Message::Ping(vec![]))
                                .await
                                .ok();
                        }
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
                msg = ws.next() => {
                    match msg {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                            let json: serde_json::Value = serde_json::from_str(&text).unwrap();
                            if tx_count == 0 {
                                println!("🆗 Subscribed: {:?}", json);
                                let mut sub_id = SUBSCRIPTION_ID.lock().await;
                                *sub_id = Some(json["result"].clone());
                            } else if json.get("params")
                                .and_then(|p| p.get("result"))
                                .and_then(|r| r.get("value"))
                                .is_some()  
                            {
                                let tx = &json["params"]["result"]["value"];
                                if tx.get("meta")
                                    .and_then(|m| m.get("preTokenBalances"))
                                    .is_some()
                                {
                                    println!("\x1b[31mRecieved notification!\x1b[0m");
                                    let json = tx.to_string();
                                    let decoded: decode::DecodedTransaction = serde_json::from_str(&json).unwrap();
                                    let owner = WALLET;
                                    tokio::spawn(handle_tx(decoded, owner, "syndica"));
                                    
                                } else {
                                    println!("Skip: {:?}", tx);
                                }
                            }
                            tx_count += 1;
                        }
                        Some(Ok(_)) => {
                        }
                        Some(Err(e)) => {
                            eprintln!("❌ WebSocket error: {:?}", e);
                            break;
                        }
                        None => {
                            println!("🔌 Connection closed by server. Reconnecting...");
                            break;
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

// pub async fn unsubscribe_syndica() {
//     let mut ws_guard = WS.lock().await;
//     if let Some(ws) = ws_guard.as_mut() {
//         if let Some(sub_id) = SUBSCRIPTION_ID.lock().await.clone() {
//             let unsubscribe_message = json!({
//                 "jsonrpc": "2.0",
//                 "id": 99,
//                 "method": "chainstream.transactionsUnsubscribe",
//                 "params": [sub_id],
//             });

//             ws.send(tokio_tungstenite::tungstenite::Message::Text(unsubscribe_message.to_string()))
//                 .await
//                 .ok();
//             println!("🛑 Unsubscribed from Chainstream");
//         }
//     }
// }
