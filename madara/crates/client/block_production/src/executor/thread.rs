//! Executor thread internal logic.

use crate::util::{create_execution_context, AdditionalTxInfo, BatchToExecute, BlockExecutionContext, ExecutionStats};
use anyhow::Context;
use blockifier::{
    blockifier::transaction_executor::TransactionExecutor,
    state::{
        cached_state::{CommitmentStateDiff, StateMaps},
        state_api::State,
    },
    transaction::{account_transaction::ExecutionFlags, objects::TransactionExecutionInfo},
};
use futures::future::OptionFuture;
use mc_db::MadaraBackend;
use mc_exec::{execution::TxInfo, LayeredStateAdapter, MadaraBackendExecutionExt};
use mp_block::header::GasPrices;
use mp_convert::{Felt, ToFelt};
use mp_utils::append_batch::AppendBatchParams;
use rayon::prelude::*;
use starknet_api::{contract_class::ContractClass, core::ContractAddress, hash::StarkHash, state::StorageKey};
use starknet_api::{
    core::{ClassHash, Nonce},
    executable_transaction::AccountTransaction,
};
use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Arc,
};
use tokio::{sync::mpsc, time::Instant};
use tracing::info;

struct ExecutorStateExecuting {
    exec_ctx: BlockExecutionContext,
    /// Note: We have a special StateAdaptor here. This is because saving the block to the database can actually lag a
    /// bit behind our execution. As such, any change that we make will need to be cached in our state adaptor so that
    /// we can be sure the state of the last block is always visible to the new one.
    executor: TransactionExecutor<LayeredStateAdapter>,
    declared_classes: HashMap<ClassHash, ContractClass>,
    consumed_l1_to_l2_nonces: HashSet<u64>,
}

struct ExecutorStateNewBlock {
    /// Keep the cached adaptor around to keep the cache around.
    state_adaptor: LayeredStateAdapter,
    consumed_l1_to_l2_nonces: HashSet<u64>,
}

/// Note: The reason this exists is because we want to create the new block execution context (meaning, the block header) as late as possible, as to have
/// the best gas prices. This is especially important when the no_empty_block configuration is enabled, as otherwise we would end up:
/// - Creating a new execution context, using the current gas prices.
/// - Waiting for a transaction to arrive.... potentially for a very, very long time..
/// - Transaction arrives, we execute it and close the block, as the block_time is reached.
///
/// At that point, the gas prices would be all wrong! In order to support no_empty_block correctly, we have to delay execution context creation
/// until the first transaction has arrived.
#[allow(clippy::large_enum_variant)]
enum ExecutorThreadState {
    /// A block has been started.
    Executing(ExecutorStateExecuting),
    /// Intermediate state, we do not have initialized the execution yet.
    NewBlock(ExecutorStateNewBlock),
}

struct AppendBatchState {
    pub transactions: Vec<AccountTransaction>,
    pub transaction_results: Vec<(TransactionExecutionInfo, CommitmentStateDiff)>,
}

struct ExecutorInitialCache {
    pub initial_storage: HashMap<Felt, HashMap<Felt, Felt>>,
    pub current_storage: HashMap<Felt, HashMap<Felt, Felt>>,
    pub initial_nonces: HashMap<Felt, Felt>,
    pub current_nonces: HashMap<Felt, Felt>,
}

impl ExecutorThreadState {
    fn consumed_l1_to_l2_nonces(&mut self) -> &mut HashSet<u64> {
        match self {
            ExecutorThreadState::Executing(s) => &mut s.consumed_l1_to_l2_nonces,
            ExecutorThreadState::NewBlock(s) => &mut s.consumed_l1_to_l2_nonces,
        }
    }
    /// Returns a mutable reference to the state adapter.
    fn layered_state_adapter_mut(&mut self) -> &mut LayeredStateAdapter {
        match self {
            ExecutorThreadState::Executing(s) => {
                &mut s.executor.block_state.as_mut().expect("State already taken").state
            }
            ExecutorThreadState::NewBlock(s) => &mut s.state_adaptor,
        }
    }
}

/// Executor runs on a separate thread, as to avoid having tx popping, block closing etc. take precious time away that could
/// be spent executing the next tick instead.
/// This thread becomes the blockifier executor scheduler thread (via TransactionExecutor), which will internally spawn worker threads.
pub struct ExecutorThread {
    backend: Arc<MadaraBackend>,

    incoming_batches: mpsc::Receiver<super::BatchToExecute>,
    replies_sender: mpsc::Sender<super::ExecutorMessage>,
    commands: mpsc::UnboundedReceiver<super::ExecutorCommand>,

    /// See `take_tx_batch`. When the mempool is empty, we will not be getting transactions.
    /// We still potentially want to emit empty blocks based on the block_time deadline.
    wait_rt: tokio::runtime::Runtime,
}

enum WaitTxBatchOutcome {
    /// Batch channel closed.
    Exit,
    /// Got a command to execute.
    Command(super::ExecutorCommand),
    /// Batch
    Batch(BatchToExecute),
}

impl ExecutorThread {
    pub fn new(
        backend: Arc<MadaraBackend>,
        incoming_batches: mpsc::Receiver<super::BatchToExecute>,
        replies_sender: mpsc::Sender<super::ExecutorMessage>,
        commands: mpsc::UnboundedReceiver<super::ExecutorCommand>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            backend,
            incoming_batches,
            replies_sender,
            commands,
            wait_rt: tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .context("Building tokio runtime")?,
        })
    }
    /// Returns None when the channel is closed.
    /// We want to close down the thread in that case.
    fn wait_take_tx_batch(&mut self, deadline: Option<Instant>, should_wait: bool) -> WaitTxBatchOutcome {
        if let Ok(batch) = self.incoming_batches.try_recv() {
            return WaitTxBatchOutcome::Batch(batch);
        }

        if let Ok(cmd) = self.commands.try_recv() {
            return WaitTxBatchOutcome::Command(cmd);
        }

        if !should_wait {
            return WaitTxBatchOutcome::Batch(Default::default());
        }

        tracing::debug!("Waiting for batch. until_block_time_deadline={}", deadline.is_some());

        // nb: tokio has blocking_recv, but no blocking_recv_timeout? this kinda sucks :(
        // especially because they do have it implemented in send_timeout and internally, they just have not exposed the
        // function.
        // Should be fine, as we optimistically try_recv above and we should only hit this when we actually have to wait.
        // nb.2: use an async block here, as timeout_at needs a runtime to be available on creation.
        self.wait_rt.block_on(async {
            tokio::select! {
                Some(cmd) = self.commands.recv() => {
                    tracing::debug!("Got cmd {cmd:?}.");
                    WaitTxBatchOutcome::Command(cmd)
                }
                _ = OptionFuture::from(deadline.map(tokio::time::sleep_until)) => {
                    tracing::debug!("Waiting for batch timed out.");
                    WaitTxBatchOutcome::Batch(Default::default())
                }
                el = self.incoming_batches.recv() => match el {
                    Some(el) => {
                        tracing::debug!("Got new batch with {} transactions.", el.len());
                        WaitTxBatchOutcome::Batch(el)
                    }
                    None => {
                        tracing::debug!("Batch channel closed.");
                        WaitTxBatchOutcome::Exit
                    }
                }
            }
        })
    }

    /// We are making a new block - we need to put the hash of current_block_n-10 into the state diff.
    /// current_block_n-10 however might not be saved into the database yet. In that case, we have to wait.
    /// This shouldn't create a deadlock (cyclic wait) unless the database is in a weird state (?)
    ///
    /// https://docs.starknet.io/architecture-and-concepts/network-architecture/starknet-state/#address_0x1
    fn wait_for_hash_of_block_min_10(&self, block_n: u64) -> anyhow::Result<Option<(u64, Felt)>> {
        let Some(block_n_min_10) = block_n.checked_sub(10) else { return Ok(None) };

        let get_hash_from_db = || {
            if let Some(view) = self.backend.block_view_on_confirmed(block_n_min_10) {
                // block exists
                anyhow::Ok(Some(view.get_block_info().context("Getting block hash of block_n - 10")?.block_hash))
            } else {
                Ok(None)
            }
        };

        // Optimistically get the hash from database without subscribing to the closed_blocks channel.
        if let Some(block_hash) = get_hash_from_db()? {
            Ok(Some((block_n_min_10, block_hash)))
        } else {
            tracing::debug!("Waiting on block_n={} to get closed. (current={})", block_n_min_10, block_n);
            loop {
                let mut receiver = self.backend.watch_chain_tip();
                // We need to re-query the DB here since the it is possible for the block hash to have arrived just in between.
                if let Some(block_hash) = get_hash_from_db()? {
                    break Ok(Some((block_n_min_10, block_hash)));
                }
                tracing::debug!("Waiting for hash of block_n-10.");
                self.wait_rt.block_on(async { receiver.recv().await });
            }
        }
    }

    /// End the current block.
    /// `extend_state_diffs` are passed when appending a batch. We want the new block state adapter to include the state diffs created by the batch.
    fn end_block(&mut self, state: &mut ExecutorStateExecuting) -> anyhow::Result<ExecutorThreadState> {
        let mut cached_state = state.executor.block_state.take().expect("Executor block state already taken");

        let state_diff = cached_state.to_state_diff().context("Cannot make state diff")?.state_maps;
        let mut cached_adapter = cached_state.state;
        cached_adapter.finish_block(
            state_diff,
            mem::take(&mut state.declared_classes),
            mem::take(&mut state.consumed_l1_to_l2_nonces),
        )?;

        Ok(ExecutorThreadState::NewBlock(ExecutorStateNewBlock {
            state_adaptor: cached_adapter,
            consumed_l1_to_l2_nonces: HashSet::new(),
        }))
    }

    /// Returns the initial state diff storage too. It is used to create the StartNewBlock message and transition to ExecutorState::Executing.
    fn create_execution_state(
        &mut self,
        state: ExecutorStateNewBlock,
        previous_l2_gas_used: u128,
        executor_initial_cache: Option<ExecutorInitialCache>,
        override_gas_price: Option<GasPrices>,
    ) -> anyhow::Result<ExecutorStateExecuting> {
        let previous_l2_gas_price = state.state_adaptor.latest_gas_prices().strk_l2_gas_price;
        let mut exec_ctx = create_execution_context(
            &self.backend,
            state.state_adaptor.block_n(),
            previous_l2_gas_price,
            previous_l2_gas_used,
        )?;

        if let Some(override_gas_price) = override_gas_price {
            exec_ctx.gas_prices = override_gas_price;
        }

        // Create the TransactionExecution, but reuse the layered_state_adapter.
        let mut executor =
            self.backend.new_executor_for_block_production(state.state_adaptor, exec_ctx.to_blockifier()?)?;

        if let Some(append_batch_params) = executor_initial_cache {
            let state = executor.block_state.as_ref().unwrap();
            let mut cache = state.cache.borrow_mut();

            // set initial storages
            append_batch_params.initial_storage.into_iter().for_each(|(contract_address, storage_map)| {
                storage_map.into_iter().for_each(|(key, value)| {
                    cache.set_storage_initial_value(
                        convert_felt_to_contract_address(contract_address),
                        convert_felt_to_storage_key(key),
                        value,
                    );
                });
            });
            // set current storages
            append_batch_params.current_storage.into_iter().for_each(|(contract_address, storage_map)| {
                storage_map.into_iter().for_each(|(key, value)| {
                    cache.set_storage_value(
                        convert_felt_to_contract_address(contract_address),
                        convert_felt_to_storage_key(key),
                        value,
                    );
                });
            });
            // set initial nonces
            append_batch_params.initial_nonces.into_iter().for_each(|(contract_address, nonce)| {
                cache.set_nonce_initial_value(convert_felt_to_contract_address(contract_address), Nonce(nonce));
            });
            // set current nonces
            append_batch_params.current_nonces.into_iter().for_each(|(contract_address, nonce)| {
                cache.set_nonce_value(convert_felt_to_contract_address(contract_address), Nonce(nonce));
            });
        }

        // Prepare the block_n-10 state diff entry on the 0x1 contract.
        if let Some((block_n_min_10, block_hash_n_min_10)) =
            self.wait_for_hash_of_block_min_10(exec_ctx.block_number)?
        {
            let contract_address = 1u64.into();
            let key = block_n_min_10.into();
            executor
                .block_state
                .as_mut()
                .expect("Blockifier block context has been taken")
                .set_storage_at(contract_address, key, block_hash_n_min_10)
                .context("Cannot set storage value in cache")?;

            tracing::debug!(
                "State diff inserted {:#x} {:#x} => {block_hash_n_min_10:#x}",
                contract_address.to_felt(),
                key.to_felt()
            );
        }
        Ok(ExecutorStateExecuting {
            exec_ctx,
            executor,
            consumed_l1_to_l2_nonces: state.consumed_l1_to_l2_nonces,
            declared_classes: HashMap::new(),
        })
    }

    fn initial_state(&self) -> anyhow::Result<ExecutorThreadState> {
        Ok(ExecutorThreadState::NewBlock(ExecutorStateNewBlock {
            state_adaptor: LayeredStateAdapter::new(Arc::clone(&self.backend))?,
            consumed_l1_to_l2_nonces: HashSet::new(),
        }))
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        let batch_size = self.backend.chain_config().block_production_concurrency.batch_size;
        let block_time = self.backend.chain_config().block_time;
        let no_empty_blocks = self.backend.chain_config().no_empty_blocks;

        // Initial state is ExecutorState::NewBlock, we don't yet have an execution state.
        let mut state = self.initial_state().context("Creating executor initial state")?;

        // The batch of transactions to execute.
        let mut to_exec = BatchToExecute::with_capacity(batch_size);

        let mut next_block_deadline = Instant::now() + block_time;
        let mut force_close = false;
        let mut block_empty = true;
        let mut l2_gas_consumed_block = 0;
        let mut append_batch_state: Option<AppendBatchState> = None;
        let mut executor_initial_cache: Option<ExecutorInitialCache> = None;
        let mut override_gas_price: Option<GasPrices> = None;

        tracing::debug!("Starting executor thread.");

        // The goal here is to do the least possible between batches, as to maximize CPU usage. Any millisecond spent
        //  outside of `TransactionExecutor::execute_txs` is a millisecond where we could have used every CPU cores, but are using only one.
        // `blockifier` isn't really well optimized in this regard, but since we can't easily change its code (maybe we should?) we're
        //  still optimizing everything we have a hand on here in madara.
        loop {
            // Take transactions to execute.
            if to_exec.len() < batch_size {
                let wait_deadline = if block_empty && no_empty_blocks { None } else { Some(next_block_deadline) };
                // should_wait: We don't want to wait if we already have transactions to process - but we would still like to fill up our batch if possible.

                let taken = match self.wait_take_tx_batch(wait_deadline, /* should_wait */ to_exec.is_empty()) {
                    // Got a batch
                    WaitTxBatchOutcome::Batch(batch_to_execute) => batch_to_execute,
                    // Got a command
                    WaitTxBatchOutcome::Command(executor_command) => {
                        match executor_command {
                            super::ExecutorCommand::CloseBlock(callback) => {
                                force_close = true;
                                let _ = callback.send(Ok(()));
                                Default::default()
                            }
                            super::ExecutorCommand::AppendBatch(append_batch_params, callback) => {
                                // validate if initial reads by the batch are correct in parallel
                                let preconfirmed_view = self.backend.view_on_latest();

                                // Run storage and nonce validation in parallel
                                let (storage_result, nonce_result) = rayon::join(
                                    || -> anyhow::Result<()> {
                                        // Validate storage in parallel
                                        append_batch_params.initial_storage.par_iter().try_for_each(
                                            |(contract_address, storage)| {
                                                storage.par_iter().try_for_each(|(key, value)| {
                                                    let stored_value = preconfirmed_view
                                                        .get_contract_storage(contract_address, key)?;
                                                    if stored_value.unwrap_or_default() != *value {
                                                        // Err(anyhow::anyhow!(
                                                        //     "Initial storage value mismatch for contract {:#x} key {:#x}: expected {:#x} but got {:?}",
                                                        //     contract_address, key, value, stored_value
                                                        // ))
                                                        anyhow::Ok(())
                                                    } else {
                                                        Ok(())
                                                    }
                                                })
                                            },
                                        )?;
                                        Ok(())
                                    },
                                    || -> anyhow::Result<()> {
                                        // Validate nonces in parallel
                                        append_batch_params.initial_nonces.par_iter().try_for_each(
                                            |(contract_address, nonce)| {
                                                let stored_nonce =
                                                    preconfirmed_view.get_contract_nonce(contract_address)?;
                                                if stored_nonce.unwrap_or_default() != *nonce {
                                                    // Err(anyhow::anyhow!(
                                                    //     "Initial nonce mismatch for contract {:#x}: expected {:#x} but got {:?}",
                                                    //     contract_address, nonce, stored_nonce
                                                    // ))
                                                    anyhow::Ok(())
                                                } else {
                                                    Ok(())
                                                }
                                            },
                                        )?;
                                        Ok(())
                                    },
                                );

                                // Handle both results
                                storage_result?;
                                nonce_result?;

                                // If we're in executing state, close the current block
                                if let ExecutorThreadState::Executing(ref mut execution_state) = state {
                                    info!(
                                        "Closing current block before appending batch at block {}",
                                        execution_state.exec_ctx.block_number
                                    );
                                    let block_exec_summary = execution_state.executor.finalize()?;
                                    if self
                                        .replies_sender
                                        .blocking_send(super::ExecutorMessage::EndBlock(block_exec_summary))
                                        .is_err()
                                    {
                                        // Receiver closed
                                        return Ok(());
                                    }
                                    next_block_deadline = Instant::now() + block_time;

                                    state = self.end_block(execution_state).context("Ending block")?;
                                    block_empty = true;
                                }

                                // setting force close to true as we don't want append batch txs to overlap with other txs for now
                                // when we've a better way to understand resources used inside an append batch, we can ignore closing the
                                // block
                                force_close = true;
                                info!(
                                    "Preparing to execute append batch with {} transactions",
                                    append_batch_params.transactions.len()
                                );
                                append_batch_state = Some(AppendBatchState {
                                    transactions: append_batch_params.transactions,
                                    transaction_results: append_batch_params.transaction_results,
                                });
                                executor_initial_cache = Some(ExecutorInitialCache {
                                    initial_storage: append_batch_params.initial_storage,
                                    current_storage: append_batch_params.current_storage,
                                    initial_nonces: append_batch_params.initial_nonces,
                                    current_nonces: append_batch_params.current_nonces,
                                });
                                override_gas_price = Some(GasPrices {
                                    eth_l1_gas_price: append_batch_params.gas_prices.eth_l1_gas_price,
                                    strk_l1_gas_price: append_batch_params.gas_prices.strk_l1_gas_price,
                                    eth_l1_data_gas_price: append_batch_params.gas_prices.eth_l1_data_gas_price,
                                    strk_l1_data_gas_price: append_batch_params.gas_prices.strk_l1_data_gas_price,
                                    eth_l2_gas_price: append_batch_params.gas_prices.eth_l2_gas_price,
                                    strk_l2_gas_price: append_batch_params.gas_prices.strk_l2_gas_price,
                                });

                                let _ = callback.send(Ok(()));
                                Default::default()
                            }
                        }
                    }
                    // Channel closed. Exit gracefully.
                    WaitTxBatchOutcome::Exit => return Ok(()),
                };

                for (tx, additional_info) in taken {
                    // Remove duplicate l1handlertxs. We want to be absolutely sure we're not duplicating them.
                    if let Some(nonce) = tx.l1_handler_tx_nonce() {
                        let nonce: u64 = nonce.to_felt().try_into().context("Converting nonce from felt to u64")?;

                        if state
                            .layered_state_adapter_mut()
                            .is_l1_to_l2_message_nonce_consumed(nonce)
                            .context("Checking is l1 to l2 message nonce is already consumed")?
                            || !state.consumed_l1_to_l2_nonces().insert(nonce)
                        // insert: Returns true if it was already consumed in the current state.
                        {
                            tracing::debug!("L1 Core Contract nonce already consumed: {nonce}");
                            continue;
                        }
                    }
                    to_exec.push(tx, additional_info)
                }
            }

            // Create a new execution state (new block) if it does not already exist.
            // This transitions the state machine from ExecutorState::NewBlock to ExecutorState::Executing, and
            // creates the blockifier TransactionExecutor.
            let execution_state = match state {
                ExecutorThreadState::Executing(ref mut executor_state_executing) => executor_state_executing,
                ExecutorThreadState::NewBlock(state_new_block) => {
                    // Create new execution state.
                    let execution_state = self
                        .create_execution_state(
                            state_new_block,
                            l2_gas_consumed_block,
                            executor_initial_cache.take(),
                            override_gas_price.take(),
                        )
                        .context("Creating execution state")?;
                    l2_gas_consumed_block = 0;

                    info!(
                        "Starting new block {} with previous gas used {}",
                        execution_state.exec_ctx.block_number, l2_gas_consumed_block
                    );
                    if self
                        .replies_sender
                        .blocking_send(super::ExecutorMessage::StartNewBlock {
                            exec_ctx: execution_state.exec_ctx.clone(),
                        })
                        .is_err()
                    {
                        // Receiver closed
                        break Ok(());
                    }

                    // Replace the state with ExecutorState::Executing while returning a mutable reference to it.
                    // I wish rust had a better way to do that :/
                    state = ExecutorThreadState::Executing(execution_state);
                    let ExecutorThreadState::Executing(execution_state) = &mut state else { unreachable!() };
                    execution_state
                }
            };

            let exec_start_time = Instant::now();

            let (blockifier_results, block_full, executed_txs) = if append_batch_state.is_some() {
                let append_batch_state = append_batch_state.take().unwrap();
                info!("Starting append batch execution with {} transactions", append_batch_state.transactions.len());
                let blockifier_results = append_batch_state
                    .transaction_results
                    .into_par_iter()
                    .map(|(info, state_diff)| {
                        let state_maps = StateMaps {
                            nonces: state_diff.address_to_nonce.into_iter().collect(),
                            class_hashes: state_diff.address_to_class_hash.into_iter().collect(),
                            storage: state_diff
                                .storage_updates
                                .into_iter()
                                .flat_map(|(addr, storage_map)| {
                                    storage_map.into_iter().map(move |(key, value)| ((addr, key), value))
                                })
                                .collect(),
                            compiled_class_hashes: state_diff.class_hash_to_compiled_class_hash.into_iter().collect(),
                            declared_contracts: HashMap::new(), // Assuming we don't have this for now
                        };
                        Ok((info, state_maps))
                    })
                    .collect::<Vec<_>>();

                // we don't want to mix batch txs with other txs
                let block_full = true;

                let mut batch_to_execute = BatchToExecute::default();
                append_batch_state.transactions.into_iter().for_each(|tx| {
                    let blockifier_tx = blockifier::transaction::transaction_execution::Transaction::Account(
                        blockifier::transaction::account_transaction::AccountTransaction {
                            tx,
                            execution_flags: ExecutionFlags {
                                only_query: false,
                                charge_fee: true,
                                validate: true,
                                strict_nonce_check: true,
                            },
                        },
                    ); // Assuming From trait is implemented
                    let additional_info = AdditionalTxInfo::default(); // We can add declared class if needed
                    batch_to_execute.push(blockifier_tx, additional_info);
                });
                (blockifier_results, block_full, batch_to_execute)
            } else {
                info!("Executing transactions without append batch");
                // TODO: we should use the execution deadline option
                // Execute the transactions.
                let blockifier_results =
                    execution_state.executor.execute_txs(&to_exec.txs, /* execution_deadline */ None);
                // When the bouncer cap is reached, blockifier will return fewer results than what we asked for.
                let block_full = blockifier_results.len() < to_exec.len();

                // Remove the used txs.
                let executed_txs = to_exec.remove_n_front(blockifier_results.len());
                (blockifier_results, block_full, executed_txs)
            };
            let exec_duration = exec_start_time.elapsed();

            let mut stats = ExecutionStats::default();
            stats.n_batches += 1;
            stats.n_executed += executed_txs.len();
            stats.exec_duration += exec_duration;

            // Doesn't process the results, it just inspects them for logging stats, and figures out which classes were declared.
            // Results are processed async, outside of the executor.
            for (btx, res) in executed_txs.txs.iter().zip(blockifier_results.iter()) {
                match res {
                    Ok((execution_info, _state_diff)) => {
                        tracing::trace!("Successful execution of transaction {:#x}", btx.tx_hash().to_felt());

                        stats.n_added_to_block += 1;
                        stats.l2_gas_consumed += u128::from(execution_info.receipt.gas.l2_gas.0);
                        block_empty = false;
                        if execution_info.is_reverted() {
                            stats.n_reverted += 1;
                        } else if let Some((class_hash, contract_class)) = btx.declared_contract_class() {
                            tracing::debug!("Declared class_hash={:#x}", class_hash.to_felt());
                            stats.declared_classes += 1;
                            execution_state.declared_classes.insert(class_hash, contract_class);
                        }
                    }
                    Err(err) => {
                        // These are the transactions that have errored but we can't revert them. It can be because of an internal server error, but
                        // errors during the execution of Declare and DeployAccount also appear here as they cannot be reverted.
                        // We reject them.
                        // Note that this is a big DoS vector.
                        tracing::error!(
                            "Rejected transaction {:#x} for unexpected error: {err:#}",
                            btx.tx_hash().to_felt()
                        );
                        stats.n_rejected += 1;
                    }
                }
            }
            l2_gas_consumed_block += stats.l2_gas_consumed;

            tracing::debug!("Finished batch execution.");
            info!(
                "Execution stats: executed={}, added={}, reverted={}, rejected={}, gas={}, declared_classes={}",
                stats.n_executed,
                stats.n_added_to_block,
                stats.n_reverted,
                stats.n_rejected,
                stats.l2_gas_consumed,
                stats.declared_classes
            );
            tracing::debug!(
                "Weights: {:?}",
                execution_state.executor.bouncer.lock().expect("Bouncer lock poisoned").get_bouncer_weights()
            );
            info!("Block status: full={}, empty={}, force_close={}", block_full, block_empty, force_close);

            let exec_result = super::BatchExecutionResult { executed_txs, blockifier_results, stats };
            if exec_result.stats.n_executed > 0
                && self.replies_sender.blocking_send(super::ExecutorMessage::BatchExecuted(exec_result)).is_err()
            {
                // Receiver closed
                break Ok(());
            }

            // End a block once we reached the block closing condition.
            // This transitions the state machine from ExecutorState::Executing to ExecutorState::NewBlock.

            let now = Instant::now();
            let block_time_deadline_reached = now >= next_block_deadline;
            if force_close || block_full || block_time_deadline_reached {
                info!(
                    "Ending block {} (force_close={}, block_full={}, block_time_deadline_reached={}, gas_used={})",
                    execution_state.exec_ctx.block_number,
                    force_close,
                    block_full,
                    block_time_deadline_reached,
                    l2_gas_consumed_block
                );
                let block_exec_summary = execution_state.executor.finalize()?;

                if self.replies_sender.blocking_send(super::ExecutorMessage::EndBlock(block_exec_summary)).is_err() {
                    // Receiver closed
                    break Ok(());
                }
                next_block_deadline = Instant::now() + block_time;
                state = self.end_block(execution_state).context("Ending block")?;
                block_empty = true;
                force_close = false;
            }
        }
    }
}

pub fn convert_felt_to_contract_address(felt: Felt) -> ContractAddress {
    let stark_hash = StarkHash::from(felt);
    ContractAddress(stark_hash.try_into().expect("Failed to convert contract address to Patricia Key"))
}

pub fn convert_felt_to_storage_key(felt: Felt) -> StorageKey {
    StorageKey(felt.try_into().expect("Failed to convert storage key to Patricia Key"))
}
