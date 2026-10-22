use futures_util::{SinkExt, StreamExt};
use std::time::Instant;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
struct Order {
    id: u64,
    side: Side,
    price: f64,
    quantity: f64,
}

struct OrderBook {
    bids: Vec<Order>,
    asks: Vec<Order>,
}

impl OrderBook {
    fn new() -> Self {
        OrderBook {
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    fn ensure_liquidity(&mut self, market_price: f64) {
        if self.asks.is_empty() {
            for i in 1..=3 {
                self.asks.push(Order {
                    id: 9000 + i,
                    side: Side::Sell,
                    price: market_price + (i as f64 * 0.50),
                    quantity: 5.0,
                });
            }
            self.asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
        }

        if self.bids.is_empty() {
            for i in 1..=3 {
                self.bids.push(Order {
                    id: 8000 + i,
                    side: Side::Buy,
                    price: market_price - (i as f64 * 0.50),
                    quantity: 5.0,
                });
            }
            self.bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
        }
    }

    fn broadcast_state(&self, tx: &broadcast::Sender<String>) {
        let top_bids: Vec<_> = self.bids.iter().take(5).map(|o| (o.price, o.quantity)).collect();
        let top_asks: Vec<_> = self.asks.iter().take(5).map(|o| (o.price, o.quantity)).collect();

        let json_msg = serde_json::json!({
            "type": "book_state",
            "bids": top_bids,
            "asks": top_asks
        });
        let _ = tx.send(json_msg.to_string());
    }

    fn process_incoming_order(&mut self, order: Order, tx: &broadcast::Sender<String>) {
        let start_time = Instant::now();
        let mut remaining_qty = order.quantity;

        match order.side {
            Side::Buy => {
                while remaining_qty > 0.0 {
                    if let Some(best_ask) = self.asks.first() {
                        if order.price >= best_ask.price {
                            let trade_qty = remaining_qty.min(best_ask.quantity);
                            let elapsed = start_time.elapsed();
                            let latency_str = format!("{:?}", elapsed);
                            
                            println!("⚡ [MATCH] Buy Order @ ${:.2} matched Ask @ ${:.2} | Qty: {:.2} | Latency: {}", 
                                order.price, best_ask.price, trade_qty, latency_str);

                            let json_msg = serde_json::json!({
                                "type": "match",
                                "price": best_ask.price,
                                "latency": latency_str,
                                "qty": trade_qty
                            });
                            let _ = tx.send(json_msg.to_string());

                            remaining_qty -= trade_qty;

                            if best_ask.quantity > trade_qty {
                                self.asks[0].quantity -= trade_qty;
                            } else {
                                self.asks.remove(0);
                            }
                        } else {
                            break; 
                        }
                    } else {
                        break; 
                    }
                }

                if remaining_qty > 0.0 {
                    let mut resting_order = order.clone();
                    resting_order.quantity = remaining_qty;

                    if let Some(best_ask) = self.asks.first() {
                        if resting_order.price >= best_ask.price {
                            resting_order.price = best_ask.price - 0.01;
                        }
                    }

                    println!("📦 [RESTING BID] Added to book @ ${:.2} | Qty: {:.2}", resting_order.price, resting_order.quantity);
                    self.bids.push(resting_order.clone());
                    self.bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());

                    // Broadcast resting order event to UI execution stream
                    let log_msg = serde_json::json!({
                        "type": "log",
                        "text": format!("📦 [RESTING BID] Added to book @ ${:.2} | Qty: {:.2}", resting_order.price, resting_order.quantity)
                    });
                    let _ = tx.send(log_msg.to_string());
                }
            }
            Side::Sell => {
                while remaining_qty > 0.0 {
                    if let Some(best_bid) = self.bids.first() {
                        if order.price <= best_bid.price {
                            let trade_qty = remaining_qty.min(best_bid.quantity);
                            let elapsed = start_time.elapsed();
                            let latency_str = format!("{:?}", elapsed);
                            
                            println!("⚡ [MATCH] Sell Order @ ${:.2} matched Bid @ ${:.2} | Qty: {:.2} | Latency: {}", 
                                order.price, best_bid.price, trade_qty, latency_str);

                            let json_msg = serde_json::json!({
                                "type": "match",
                                "price": best_bid.price,
                                "latency": latency_str,
                                "qty": trade_qty
                            });
                            let _ = tx.send(json_msg.to_string());

                            remaining_qty -= trade_qty;

                            if best_bid.quantity > trade_qty {
                                self.bids[0].quantity -= trade_qty;
                            } else {
                                self.bids.remove(0);
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                if remaining_qty > 0.0 {
                    let mut resting_order = order.clone();
                    resting_order.quantity = remaining_qty;

                    if let Some(best_bid) = self.bids.first() {
                        if resting_order.price <= best_bid.price {
                            resting_order.price = best_bid.price + 0.01;
                        }
                    }

                    println!("📦 [RESTING ASK] Added to book @ ${:.2} | Qty: {:.2}", resting_order.price, resting_order.quantity);
                    self.asks.push(resting_order.clone());
                    self.asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

                    // Broadcast resting order event to UI execution stream
                    let log_msg = serde_json::json!({
                        "type": "log",
                        "text": format!("📦 [RESTING ASK] Added to book @ ${:.2} | Qty: {:.2}", resting_order.price, resting_order.quantity)
                    });
                    let _ = tx.send(log_msg.to_string());
                }
            }
        }

        self.broadcast_state(tx);
    }
}

#[tokio::main]
async fn main() {
    let (tx, _): (broadcast::Sender<String>, broadcast::Receiver<String>) = broadcast::channel(100);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);
    
    let tx_server = tx.clone();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr).await.expect("Failed to bind port");
        println!("🚀 Server listening on {}", bind_addr);
        
        while let Ok((stream, _)) = listener.accept().await {
            let mut rx = tx_server.subscribe();
            tokio::spawn(async move {
                if let Ok(mut ws_stream) = tokio_tungstenite::accept_async(stream).await {
                    while let Ok(msg) = rx.recv().await {
                        if ws_stream.send(Message::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }
    });

    println!("Connecting to Coinbase Advanced Trade Live Feed...");
    let url = "wss://advanced-trade-ws.coinbase.com";
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect to Coinbase WebSocket");
    println!("Connected to Coinbase successfully!");

    let (mut write, mut read) = ws_stream.split();

    let subscribe_msg = serde_json::json!({
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channel": "ticker"
    });

    write.send(Message::Text(subscribe_msg.to_string().into()))
        .await
        .expect("Failed to send subscription message");

    println!("Subscribed to BTC-USD live stream. Engine active...\n");

    let mut book = OrderBook::new();
    let mut fake_order_id = 1;

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                    if json["channel"] == "ticker" {
                        if let Some(events) = json["events"].as_array() {
                            for event in events {
                                if let Some(tickers) = event["tickers"].as_array() {
                                    for ticker in tickers {
                                        if let Some(price_str) = ticker["price"].as_str() {
                                            if let Ok(market_price) = price_str.parse::<f64>() {
                                                book.ensure_liquidity(market_price);

                                                let test_order = if fake_order_id % 2 == 0 {
                                                    Order {
                                                        id: fake_order_id,
                                                        side: Side::Buy,
                                                        price: market_price + 1.00,
                                                        quantity: 1.0,
                                                    }
                                                } else {
                                                    Order {
                                                        id: fake_order_id,
                                                        side: Side::Buy,
                                                        price: market_price - 2.00,
                                                        quantity: 1.0,
                                                    }
                                                };

                                                fake_order_id += 1;
                                                book.process_incoming_order(test_order, &tx);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(Message::Ping(ping)) => {
                let _ = write.send(Message::Pong(ping)).await;
            }
            _ => {}
        }
    }
}