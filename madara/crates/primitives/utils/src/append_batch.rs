use std::collections::HashMap;

use blockifier::state::cached_state::CommitmentStateDiff;
use blockifier::transaction::objects::TransactionExecutionInfo;
use mp_convert::Felt;
use serde::{Deserialize, Serialize};
use starknet_api::executable_transaction::AccountTransaction;

// TODO: copied from headers.rs
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GasPrices {
    pub eth_l1_gas_price: u128,
    pub strk_l1_gas_price: u128,
    pub eth_l1_data_gas_price: u128,
    pub strk_l1_data_gas_price: u128,
    pub eth_l2_gas_price: u128,
    pub strk_l2_gas_price: u128,
}

// TODO: move to some types file
#[derive(Debug, Deserialize, Serialize)]
pub struct AppendBatchParams {
    pub transactions: Vec<AccountTransaction>,
    pub transaction_results: Vec<(TransactionExecutionInfo, CommitmentStateDiff)>,
    pub initial_storage: HashMap<Felt, HashMap<Felt, Felt>>,
    pub current_storage: HashMap<Felt, HashMap<Felt, Felt>>,
    pub initial_nonces: HashMap<Felt, Felt>,
    pub current_nonces: HashMap<Felt, Felt>,
    pub gas_prices: GasPrices,
}
