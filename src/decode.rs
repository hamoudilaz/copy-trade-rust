use std::collections::{HashMap, HashSet};
use solana_sdk::pubkey::Pubkey;
use serde::Deserialize;
use crate::execute::execute_swap;


const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Deserialize)]
pub struct DecodedTransaction {
    pub meta: Option<TransactionMeta>,
}

#[derive(Debug, Deserialize)]
pub struct TransactionMeta {
    #[serde(rename = "preTokenBalances")]
    pub pre_token_balances: Option<Vec<TokenBalance>>,
    #[serde(rename = "postTokenBalances")]
    pub post_token_balances: Option<Vec<TokenBalance>>,
    #[serde(rename = "preBalances")]
    pub pre_balances: Vec<u64>,
    #[serde(rename = "postBalances")]
    pub post_balances: Vec<u64>,
    pub fee: u64,
}

#[derive(Debug, Deserialize)]
pub struct TokenBalance {
    pub owner: String,
    pub mint: String,
    #[serde(rename = "uiTokenAmount")]
    pub ui_token_amount: UiTokenAmount,
}

#[derive(Debug, Deserialize)]
pub struct UiTokenAmount {
    pub amount: String,
}



pub fn process_transaction(transaction: &DecodedTransaction, owner: &str) -> Option<()> {


    let meta = transaction.meta.as_ref()?;
    let pre_token_balances = meta.pre_token_balances.as_deref()?;
    if pre_token_balances.is_empty() {
        return None;
    }
    let post_token_balances = meta.post_token_balances.as_deref()?;
    if post_token_balances.is_empty() {
        return None;
    }

    let mut pre_balances = HashMap::new();
    for token in pre_token_balances {
        if token.owner == owner {
            let amount = token.ui_token_amount.amount.parse::<f64>().unwrap_or(0.0);  // Use amount
            *pre_balances.entry(token.mint.clone()).or_insert(0.0) += amount;
        }
    }

    let mut post_balances = HashMap::new();
    for token in post_token_balances {
        if token.owner == owner {
            let amount = token.ui_token_amount.amount.parse::<f64>().unwrap_or(0.0);  // Use amount
            *post_balances.entry(token.mint.clone()).or_insert(0.0) += amount;
        }
    }

    let all_mints: HashSet<_> = pre_balances.keys()
        .chain(post_balances.keys())
        .cloned()
        .collect();

    let mut input_mint = None;
    let mut output_mint = None;
    let mut input_amount = 0.0;
    let mut output_amount = 0.0;

    for mint in all_mints {
        let pre = *pre_balances.get(&mint).unwrap_or(&0.0);
        let post = *post_balances.get(&mint).unwrap_or(&0.0);
        let diff = post - pre;

        if diff < 0.0 {
            input_mint = Some(mint.clone());
            input_amount = (-diff) as f64;
        } else if diff > 0.0 {
            output_mint = Some(mint.clone());
            output_amount = diff as f64;
        }
    }

    let pre_sol = meta.pre_balances.first().copied().unwrap_or(0);
    let post_sol = meta.post_balances.first().copied().unwrap_or(0);
    let fee = meta.fee;

    let (mut sol_input, mut sol_output) = (0, 0);
    if post_sol < pre_sol {
        let net_spent = pre_sol - post_sol;
        if net_spent > fee {
            sol_input = net_spent - fee;
        }
    } else if post_sol > pre_sol {
        sol_output = post_sol - pre_sol + fee;
    }

    if input_mint.is_none() && sol_input > 0 {
        input_mint = Some(SOL_MINT.to_string());
        input_amount = sol_input as f64;
    }

    if output_mint.is_none() && sol_output > 0 {
        output_mint = Some(SOL_MINT.to_string());
        output_amount = sol_output as f64;
    }

    let transaction_type = if input_mint.as_deref() == Some(SOL_MINT) {
        "buy".to_string()
    } else {
        "sell".to_string()
    };

  let input_mint_clone = input_mint.clone().unwrap_or_default();
  let output_mint_clone = output_mint.clone().unwrap_or_default();

  let privkey = "Your-priv-key"; 
  let amount = 100000; // replace with swap amount
  let slippage_bps = 1000; // replace with slippage 

  tokio::spawn(async move {
      match execute_swap(privkey, &input_mint_clone, &output_mint_clone, amount, slippage_bps).await {
          Ok(_) => println!("Swap executed successfully."),
          Err(e) => eprintln!("Error executing swap: {}", e),
      }
  });

  Some(())

}
