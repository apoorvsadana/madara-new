use std::collections::HashMap;

use blockifier::state::cached_state::CommitmentStateDiff;
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
    pub transaction_results: Vec<(TransactionReceipt, CommitmentStateDiff)>,
    pub initial_storage: HashMap<Felt, HashMap<Felt, Felt>>,
    pub current_storage: HashMap<Felt, HashMap<Felt, Felt>>,
    pub initial_nonces: HashMap<Felt, Felt>,
    pub current_nonces: HashMap<Felt, Felt>,
    pub gas_prices: GasPrices,
}

// Copied this entire code from mp-receipt

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionReceipt {
    Invoke(InvokeTransactionReceipt),
    Declare(DeclareTransactionReceipt),
    Deploy(DeployTransactionReceipt),
    DeployAccount(DeployAccountTransactionReceipt),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvokeTransactionReceipt {
    pub transaction_hash: Felt, // This can be retrieved from the transaction itself.
    pub actual_fee: FeePayment,
    pub messages_sent: Vec<MsgToL1>,
    pub events: Vec<Event>,
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclareTransactionReceipt {
    pub transaction_hash: Felt,
    pub actual_fee: FeePayment,
    pub messages_sent: Vec<MsgToL1>,
    pub events: Vec<Event>,
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployTransactionReceipt {
    pub transaction_hash: Felt,
    pub actual_fee: FeePayment,
    pub messages_sent: Vec<MsgToL1>,
    pub events: Vec<Event>,
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
    pub contract_address: Felt,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployAccountTransactionReceipt {
    pub transaction_hash: Felt,
    pub actual_fee: FeePayment,
    pub messages_sent: Vec<MsgToL1>,
    pub events: Vec<Event>,
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
    pub contract_address: Felt,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeePayment {
    pub amount: Felt,
    pub unit: PriceUnit,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PriceUnit {
    #[default]
    Wei,
    Fri,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MsgToL1 {
    pub from_address: Felt,
    pub to_address: Felt,
    pub payload: Vec<Felt>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MsgToL2 {
    pub from_address: Felt,
    pub to_address: Felt,
    pub selector: Felt,
    pub payload: Vec<Felt>,
    pub nonce: Option<Felt>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GasVector {
    pub l1_gas: u128,
    pub l1_data_gas: u128,
    pub l2_gas: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ExecutionResources {
    pub steps: u64,
    pub memory_holes: u64,
    pub range_check_builtin_applications: u64,
    pub pedersen_builtin_applications: u64,
    pub poseidon_builtin_applications: u64,
    pub ec_op_builtin_applications: u64,
    pub ecdsa_builtin_applications: u64,
    pub bitwise_builtin_applications: u64,
    pub keccak_builtin_applications: u64,
    pub segment_arena_builtin: u64,
    pub data_availability: GasVector,
    pub total_gas_consumed: GasVector,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionResult {
    #[default]
    Succeeded,
    Reverted {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub from_address: Felt,
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
}
