use blockifier::execution::contract_class::RunnableCompiledClass;
use blockifier::state::cached_state::StateCache;
use blockifier::state::errors::StateError;
use blockifier::state::state_api::{StateReader, StateResult};
use mc_db::rocksdb::RocksDBStorage;
use mc_db::{MadaraStateView, MadaraStorageRead};
use mp_convert::ToFelt;
use starknet_api::core::{ClassHash, CompiledClassHash, ContractAddress, Nonce};
use starknet_api::state::StorageKey;
use starknet_types_core::felt::Felt;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Adapter for the db queries made by blockifier.
///
/// There is no actual mutable logic here - when using block production, the actual key value
/// changes in db are evaluated at the end only from the produced state diff.
pub struct BlockifierStateAdapter<D: MadaraStorageRead = RocksDBStorage> {
    pub view: MadaraStateView<D>,
    pub block_number: u64,
    pub storage_reads: Arc<Mutex<HashMap<ContractAddress, StorageKey>>>,
    pub nonce_reads: Arc<Mutex<HashMap<ContractAddress, Nonce>>>,
    pub cache: RefCell<StateCache>,
}

impl<D: MadaraStorageRead> BlockifierStateAdapter<D> {
    pub fn new(
        view: MadaraStateView<D>,
        block_number: u64,
        storage_reads: Arc<Mutex<HashMap<ContractAddress, StorageKey>>>,
        nonce_reads: Arc<Mutex<HashMap<ContractAddress, Nonce>>>,
        cache: RefCell<StateCache>,
    ) -> Self {
        Self { view, block_number, storage_reads, nonce_reads, cache }
    }

    pub fn is_l1_to_l2_message_nonce_consumed(&self, nonce: u64) -> StateResult<bool> {
        let value = if let Some(tx_hash) =
            self.view.backend().get_l1_handler_txn_hash_by_nonce(nonce).map_err(|err| {
                StateError::StateReadError(format!(
                    "Failed to l1 handler txn hash by core contract nonce: on={}, nonce={nonce}: {err:#}",
                    self.view
                ))
            })? {
            self.view
                .find_transaction_by_hash(&tx_hash)
                .map_err(|err| {
                    StateError::StateReadError(format!(
                        "Failed to l1 handler txn by hash: on={}, nonce={nonce}: {err:#}",
                        self.view
                    ))
                })?
                .is_some()
        } else {
            false
        };

        tracing::debug!("is_l1_to_l2_message_nonce_consumed: on={}, nonce={nonce} => {value}", self.view);
        Ok(value)
    }
}

// TODO: mapping StateErrors InternalServerError in execution RPC endpoints is not properly handled.
// It is however properly handled for transaction validator.
impl<D: MadaraStorageRead> StateReader for BlockifierStateAdapter<D> {
    fn get_storage_at(&self, contract_address: ContractAddress, key: StorageKey) -> StateResult<Felt> {
        let cache = self.cache.borrow_mut();
        if let Some(value) = cache.get_storage_at(contract_address, key) {
            self.storage_reads.lock().unwrap().insert(contract_address, key);
            // No need to update the cache here because blockifier is still internally used CachedState.
            return Ok(*value);
        }

        let value = self
            .view
            .get_contract_storage(&contract_address.to_felt(), &key.to_felt())
            .map_err(|err| {
                StateError::StateReadError(format!(
                    "Failed to retrieve storage value: on={}, contract_address={:#x} key={:#x}: {err:#}",
                    self.view,
                    contract_address.to_felt(),
                    key.to_felt(),
                ))
            })?
            .unwrap_or(Felt::ZERO);

        self.storage_reads.lock().unwrap().insert(contract_address, key);

        tracing::debug!(
            "get_storage_at: on={}, contract_address={:#x} key={:#x} => {value:#x}",
            self.view,
            contract_address.to_felt(),
            key.to_felt(),
        );

        Ok(value)
    }

    fn get_nonce_at(&self, contract_address: ContractAddress) -> StateResult<Nonce> {
        let cache = self.cache.borrow_mut();
        if let Some(value) = cache.get_nonce_at(contract_address) {
            self.nonce_reads.lock().unwrap().insert(contract_address, *value);
            // No need to update the cache here because blockifier is still internally used CachedState.
            return Ok(*value);
        }

        let value = self
            .view
            .get_contract_nonce(&contract_address.to_felt())
            .map_err(|err| {
                StateError::StateReadError(format!(
                    "Failed to retrieve nonce: on={}, contract_address={:#x}: {err:#}",
                    self.view,
                    contract_address.to_felt(),
                ))
            })?
            .unwrap_or(Felt::ZERO);
        self.nonce_reads.lock().unwrap().insert(contract_address, Nonce(value));

        tracing::debug!(
            "get_nonce_at: on={}, contract_address={:#x} => {value:#x}",
            self.view,
            contract_address.to_felt(),
        );

        Ok(Nonce(value))
    }

    /// Blockifier expects us to return 0x0 if the contract is not deployed.
    fn get_class_hash_at(&self, contract_address: ContractAddress) -> StateResult<ClassHash> {
        let cache = self.cache.borrow_mut();
        if let Some(value) = cache.get_class_hash_at(contract_address) {
            // No need to update the cache here because blockifier is still internally used CachedState.
            return Ok(*value);
        }

        let value = self
            .view
            .get_contract_class_hash(&contract_address.to_felt())
            .map_err(|err| {
                StateError::StateReadError(format!(
                    "Failed to retrieve class_hash: on={}, contract_address={:#x}: {err:#}",
                    self.view,
                    contract_address.to_felt(),
                ))
            })?
            .unwrap_or(Felt::ZERO);

        tracing::debug!(
            "get_class_hash_at: on={}, contract_address={:#x} => {value:#x}",
            self.view,
            contract_address.to_felt(),
        );

        Ok(ClassHash(value))
    }

    fn get_compiled_class(&self, class_hash: ClassHash) -> StateResult<RunnableCompiledClass> {
        let cache = self.cache.borrow_mut();
        // TODO: what do we do here for cache?

        let value = self.view.get_class_info_and_compiled(&class_hash.to_felt()).map_err(|err| {
            StateError::StateReadError(format!(
                "Failed to retrieve class: on={}, class_hash={:#x}: {err:#}",
                self.view,
                class_hash.to_felt(),
            ))
        })?;

        let converted_class = value.ok_or(StateError::UndeclaredClassHash(class_hash))?;

        tracing::debug!("get_compiled_contract_class: on={}, class_hash={:#x}", self.view, class_hash.to_felt());

        (&converted_class).try_into().map_err(|err| {
            tracing::error!("Failed to convert class {class_hash:#} to blockifier format: {err:#}");
            StateError::StateReadError(format!("Failed to convert class {class_hash:#}"))
        })
    }

    fn get_compiled_class_hash(&self, class_hash: ClassHash) -> StateResult<CompiledClassHash> {
        let cache = self.cache.borrow_mut();
        if let Some(value) = cache.get_compiled_class_hash(class_hash) {
            // No need to update the cache here because blockifier is still internally used CachedState.
            return Ok(*value);
        }

        let value = self.view.get_class_info(&class_hash.to_felt()).map_err(|err| {
            StateError::StateReadError(format!(
                "Failed to retrieve class_hash: on={}, class_hash={:#x}: {err:#}",
                self.view,
                class_hash.to_felt(),
            ))
        })?;

        let value = value.and_then(|c| c.compiled_class_hash()).ok_or_else(|| {
            StateError::StateReadError(format!(
                "Class does not have a compiled class hash: on={}, class_hash={:#x}",
                self.view,
                class_hash.to_felt(),
            ))
        })?;

        tracing::debug!(
            "get_compiled_class_hash: on={}, class_hash={:#x} => {value:#x}",
            self.view,
            class_hash.to_felt(),
        );

        Ok(CompiledClassHash(value))
    }
}
