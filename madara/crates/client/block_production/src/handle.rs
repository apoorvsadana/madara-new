use crate::executor::{self, ExecutorCommand, ExecutorCommandError};
use crate::util::{AdditionalTxInfo, BatchToExecute, ExecutionStats};
use crate::BatchExecutionResult;
use async_trait::async_trait;
use blockifier::blockifier::transaction_executor::{TransactionExecutionOutput, TransactionExecutorResult};
use blockifier::state::cached_state::{CommitmentStateDiff, StateMaps};
use blockifier::transaction::account_transaction::ExecutionFlags;
use blockifier::transaction::objects::TransactionExecutionInfo;
use mc_db::MadaraBackend;
use mc_submit_tx::{
    SubmitTransaction, SubmitTransactionError, SubmitValidatedTransaction, TransactionValidator,
    TransactionValidatorConfig,
};
use mp_rpc::admin::BroadcastedDeclareTxnV0;
use mp_rpc::v0_9_0::{
    AddInvokeTransactionResult, BroadcastedDeclareTxn, BroadcastedDeployAccountTxn, BroadcastedInvokeTxn,
    ClassAndTxnHash, ContractAndTxnHash,
};
use mp_transactions::validated::ValidatedTransaction;
use mp_utils::append_batch::AppendBatchParams;
use starknet_api::executable_transaction::AccountTransaction;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

struct BypassInput(mpsc::Sender<ValidatedTransaction>);

#[async_trait]
impl SubmitValidatedTransaction for BypassInput {
    async fn submit_validated_transaction(&self, tx: ValidatedTransaction) -> Result<(), SubmitTransactionError> {
        self.0.send(tx).await.map_err(|e| SubmitTransactionError::Internal(anyhow::anyhow!(e)))
    }
    async fn received_transaction(&self, _hash: starknet_types_core::felt::Felt) -> Option<bool> {
        None
    }
    async fn subscribe_new_transactions(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<starknet_types_core::felt::Felt>> {
        None
    }
}

#[derive(Clone, Debug)]
/// Remotely control block production.
pub struct BlockProductionHandle {
    /// Commands to executor task.
    executor_commands: mpsc::UnboundedSender<executor::ExecutorCommand>,
    bypass_input: mpsc::Sender<ValidatedTransaction>,
    /// We use TransactionValidator to handle conversion to blockifier, class compilation etc. Mostly for convenience.
    tx_converter: Arc<TransactionValidator>,
}

impl BlockProductionHandle {
    pub(crate) fn new(
        backend: Arc<MadaraBackend>,
        executor_commands: mpsc::UnboundedSender<executor::ExecutorCommand>,
        bypass_input: mpsc::Sender<ValidatedTransaction>,
    ) -> Self {
        Self {
            executor_commands,
            bypass_input: bypass_input.clone(),
            tx_converter: TransactionValidator::new(
                Arc::new(BypassInput(bypass_input)),
                backend,
                TransactionValidatorConfig::default().with_disable_validation(true),
            )
            .into(),
        }
    }

    /// Force the current block to close without waiting for block time.
    pub async fn close_block(&self) -> Result<(), ExecutorCommandError> {
        let (sender, recv) = oneshot::channel();
        self.executor_commands
            .send(ExecutorCommand::CloseBlock(sender))
            .map_err(|_| ExecutorCommandError::ChannelClosed)?;
        recv.await.map_err(|_| ExecutorCommandError::ChannelClosed)?
    }

    /// Append a batch executed outside of Madara (assuming third party is trusted)
    pub async fn append_batch(&self, append_batch: AppendBatchParams) -> Result<(), ExecutorCommandError> {
        let start = std::time::Instant::now();
        let (sender, recv) = oneshot::channel();

        let send_start = std::time::Instant::now();
        self.executor_commands.send(ExecutorCommand::AppendBatch(append_batch, sender)).map_err(|e| {
            tracing::error!("Error sending append batch command: {:?}", e);
            ExecutorCommandError::ChannelClosed
        })?;
        tracing::info!("Channel send took {:.3}ms", send_start.elapsed().as_secs_f64() * 1000.0);

        let recv_start = std::time::Instant::now();
        let result = recv.await.map_err(|e| {
            tracing::error!("Error receiving append batch result: {:?}", e);
            ExecutorCommandError::ChannelClosed
        })?;
        tracing::info!("Channel recv took {:.3}ms", recv_start.elapsed().as_secs_f64() * 1000.0);
        tracing::info!("Total handle.append_batch took {:.3}ms", start.elapsed().as_secs_f64() * 1000.0);

        result
    }

    /// Send a transaction through the bypass channel to bypass mempool and validation.
    pub async fn send_tx_raw(&self, tx: ValidatedTransaction) -> Result<(), ExecutorCommandError> {
        self.bypass_input.send(tx).await.map_err(|_| ExecutorCommandError::ChannelClosed)
    }
}

// For convenience, we proxy the submit tx traits.

#[async_trait]
impl SubmitTransaction for BlockProductionHandle {
    async fn submit_declare_v0_transaction(
        &self,
        tx: BroadcastedDeclareTxnV0,
    ) -> Result<ClassAndTxnHash, SubmitTransactionError> {
        self.tx_converter.submit_declare_v0_transaction(tx).await
    }
    async fn submit_declare_transaction(
        &self,
        tx: BroadcastedDeclareTxn,
    ) -> Result<ClassAndTxnHash, SubmitTransactionError> {
        self.tx_converter.submit_declare_transaction(tx).await
    }
    async fn submit_deploy_account_transaction(
        &self,
        tx: BroadcastedDeployAccountTxn,
    ) -> Result<ContractAndTxnHash, SubmitTransactionError> {
        self.tx_converter.submit_deploy_account_transaction(tx).await
    }
    async fn submit_invoke_transaction(
        &self,
        tx: BroadcastedInvokeTxn,
    ) -> Result<AddInvokeTransactionResult, SubmitTransactionError> {
        self.tx_converter.submit_invoke_transaction(tx).await
    }
    async fn received_transaction(&self, _hash: starknet_types_core::felt::Felt) -> Option<bool> {
        None
    }
    async fn subscribe_new_transactions(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<starknet_types_core::felt::Felt>> {
        None
    }
}

#[async_trait]
impl SubmitValidatedTransaction for BlockProductionHandle {
    async fn submit_validated_transaction(&self, tx: ValidatedTransaction) -> Result<(), SubmitTransactionError> {
        self.send_tx_raw(tx).await.map_err(|e| SubmitTransactionError::Internal(anyhow::anyhow!(e)))
    }
    async fn received_transaction(&self, _hash: starknet_types_core::felt::Felt) -> Option<bool> {
        None
    }
    async fn subscribe_new_transactions(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<starknet_types_core::felt::Felt>> {
        None
    }
}
