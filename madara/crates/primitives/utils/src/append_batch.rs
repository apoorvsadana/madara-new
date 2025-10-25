use std::collections::HashMap;

use blockifier::state::cached_state::CommitmentStateDiff;
use blockifier::transaction::objects::TransactionExecutionInfo;
use mp_convert::Felt;
use serde::{Deserialize, Serialize};
use starknet_api::executable_transaction::AccountTransaction;

// TODO: move to some types file
#[derive(Debug, Deserialize, Serialize)]
pub struct AppendBatchParams {
    pub transactions: Vec<AccountTransaction>,
    pub transaction_results: Vec<(TransactionExecutionInfo, CommitmentStateDiff)>,
    pub initial_storage: HashMap<Felt, HashMap<Felt, Felt>>,
    pub current_storage: HashMap<Felt, HashMap<Felt, Felt>>,
    pub initial_nonces: HashMap<Felt, Felt>,
    pub current_nonces: HashMap<Felt, Felt>,
}
