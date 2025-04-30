
use reqwest::Client;
use serde_json::Value;
use std::time::Instant;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use std::str::FromStr;
use solana_sdk::message::VersionedMessage;
use solana_sdk::signer::keypair::Keypair;
use base64::{engine::general_purpose, Engine as _};     
use bincode;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;
use rand::rngs::StdRng;
use rand::{SeedableRng}; 
use rand::seq::SliceRandom; // <---- IMPORTANT for .choose()


const NEXT_BLOCK_TIP_WALLETS: [&str; 17] = [
 "TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq",
"noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4",
"noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE",
"noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo",
"noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ",
"nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L",
"nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z",
"nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu",
"noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7",
"nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP",
"nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P",
"nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge",
"nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3",
"nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ",
"nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk",
"nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne",
"nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb"
];

const JITO_TIP_WALLETS: [&str; 8] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];


pub async fn execute_swap(
    privkey: &str,
    input_mint: &str,
    output_mint: &str,
    amount: u64,
    slippage_bps: u16
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now(); // start timer

    let wallet = Keypair::from_base58_string(privkey);
    let user_public_key = wallet.pubkey().to_string();
    let jito_fee = 1000000;   

    let output_mint_pubkey = Pubkey::from_str(output_mint)?;
    let wallet_pubkey = Pubkey::from_str(&user_public_key)?;
    let destination_ata = get_associated_token_address(&wallet_pubkey, &output_mint_pubkey);
    
    let quote_api = "https://lite-api.jup.ag/swap/v1/quote";
    let swap_api = "https://lite-api.jup.ag/swap/v1/swap";

    let url = format!(          
        "{}?inputMint={}&outputMint={}&amount={}&slippageBps={}",
        quote_api, input_mint, output_mint, amount, slippage_bps
    );

    let client = Client::new();


    let response = client.get(&url).send().await?;

    let quote: Value = response.json().await?;

    let payload = serde_json::json!({
        "userPublicKey": user_public_key,
        "prioritizationFeeLamports": { "jitoTipLamports": jito_fee },
        "dynamicComputeUnitLimit": true,
        "quoteResponse": quote,
        "wrapAndUnwrapSol": false,
        "destinationTokenAccount": destination_ata.to_string(),
    });
    

    let swap_response = client
    .post(swap_api)
    .header("Content-Type", "application/json")
    .json(&payload)
    .send()
    .await?;


let swap: Value = swap_response.json().await?;

// 1. Get the swapTransaction field from swap
let swap_transaction = swap["swapTransaction"]
    .as_str()
    .ok_or("Missing swapTransaction in response")?;



    let swap_tx_bytes = general_purpose::STANDARD.decode(swap_transaction)?;

    let mut transaction: VersionedTransaction = bincode::deserialize(&swap_tx_bytes)?;

  // Randomly pick a nextBlockValidatorPubkey
  let mut rng = StdRng::from_entropy(); // Use a `Send`-compatible RNG
  let random_wallet = NEXT_BLOCK_TIP_WALLETS.choose(&mut rng)
  .ok_or("Wallet list empty")?;
  let next_block_validator_pubkey = Pubkey::from_str(random_wallet)?;


  match &mut transaction.message {
    VersionedMessage::V0(msg) => {
        for key in &mut msg.account_keys {
            if JITO_TIP_WALLETS.iter().any(|tip| tip == &key.to_string()) {
                *key = next_block_validator_pubkey;
            }
        }

          // --- Insert ComputeUnitPrice Instruction ---
          let fee = 1_000_0000; // replace with your custom fee variable if needed
          let compute_ix = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(fee);
  
          // Find program_id index
          let program_id_index = msg.account_keys
              .iter()
              .position(|k| k == &compute_ix.program_id)
              .unwrap_or_else(|| {
                  msg.account_keys.push(compute_ix.program_id);
                  msg.account_keys.len() - 1
              });
  
          // Map account keys to indexes
          let account_key_indexes: Vec<u8> = compute_ix.accounts
              .iter()
              .map(|acc| {
                  msg.account_keys.iter().position(|k| k == &acc.pubkey)
                      .unwrap_or_else(|| {
                          msg.account_keys.push(acc.pubkey);
                          msg.account_keys.len() - 1
                      })
              })
              .map(|i| i as u8)
              .collect();
  
          let new_instruction = solana_sdk::instruction::CompiledInstruction {
              program_id_index: program_id_index as u8,
              accounts: account_key_indexes,
              data: compute_ix.data,
          };
  
          msg.instructions.insert(1, new_instruction);
    }
    _ => {
        println!("Unexpected message version. Not V0.");
    }
}

    

    let message_bytes = transaction.message.serialize();
    let signature = wallet.sign_message(&message_bytes);
    transaction.signatures = vec![signature];

    println!("{}", wallet.pubkey());

    let serialized = bincode::serialize(&transaction)?;
    let transaction_base64 = general_purpose::STANDARD.encode(&serialized);


    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendTransaction",
        "params": [
            transaction_base64,
            {
                "encoding": "base64"
            }
        ]
    });
    
    


    let response = client
    .post("http://fra2.nozomi.temporal.xyz/?c=your-api-key")  
    .header("Content-Type", "application/json")
    // .header("Authorization", token) 
    .json(&payload)
    .send()
    .await?;
    
    let send_result: serde_json::Value = response.json().await?;
    let elapsed = start.elapsed(); // end timer

    let pretty_send_result = serde_json::to_string_pretty(&send_result)?;
    println!("Response: \n{}", pretty_send_result);
    if let Some(signature) = send_result.get("result").and_then(|r| r.as_str()) {
        println!("\x1b[1m\x1b[32m✅ COPY BUY:\x1b[0m \x1b[36mhttps://solscan.io/tx/{}\x1b[0m", signature);
    } else {
        println!("Failed to extract transaction signature.");
    }
        println!("Swap request took: {:.2?}", elapsed);


    Ok(())
}
