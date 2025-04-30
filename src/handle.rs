use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::sleep;

use crate::decode::{self, DecodedTransaction};

static EXECUTION: AtomicBool = AtomicBool::new(false);

pub async fn handle_tx(tx: DecodedTransaction, wallet: &str, who: &str) {
    if EXECUTION.swap(true, Ordering::SeqCst) {
        return; // already executing
    }

    println!("{}", who);

    let _ = decode::process_transaction(&tx, wallet);

    sleep(Duration::from_secs(5)).await;

    EXECUTION.store(false, Ordering::SeqCst);
}
