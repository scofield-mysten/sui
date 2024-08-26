// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use sui_types::transaction::InputObjectKind;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    dependency_controller::DependencyController,
    executor::{ExecutableTransaction, ExecutionEffects, Executor, TransactionWithTimestamp},
};

pub type ProxyId = usize;

/// A proxy is responsible for pre-executing transactions.
pub struct Proxy<E: Executor> {
    /// The ID of the proxy.
    id: ProxyId,
    /// The executor for the transactions.
    executor: E,
    /// The object store.
    store: Arc<E::Store>,
    /// The receiver for transactions.
    rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
    /// The sender for transactions with results.
    tx_results: Sender<ExecutionEffects<E::StateChanges>>,
    /// The dependency controller for multi-core tx execution.
    dependency_controller: DependencyController,
}

impl<E: Executor> Proxy<E> {
    /// Create a new proxy.
    pub fn new(
        id: ProxyId,
        executor: E,
        store: E::Store,
        rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
        tx_results: Sender<ExecutionEffects<E::StateChanges>>,
    ) -> Self {
        Self {
            id,
            executor,
            store: Arc::new(store),
            rx_transactions,
            tx_results,
            dependency_controller: DependencyController::new(),
        }
    }

    /// Run the proxy.
    pub async fn run(&mut self)
    where
        E: Send + 'static,
        <E as Executor>::Store: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::StateChanges: Send,
    {
        tracing::info!("Proxy {} started", self.id);

        let mut task_id = 0;
        let ctx = self.executor.get_context();
        while let Some(transaction) = self.rx_transactions.recv().await {
            task_id += 1;
            let obj_ids = transaction
                .input_objects()
                .into_iter()
                .filter_map(|kind| {
                    match kind {
                        InputObjectKind::ImmOrOwnedMoveObject((obj_id, _, _)) => Some(obj_id),
                        InputObjectKind::SharedMoveObject {
                            id: obj_id,
                            initial_shared_version: _,
                            mutable: _,
                        } => Some(obj_id),
                        _ => None, // filter out move package
                    }
                })
                .collect::<Vec<_>>();

            let (prior_handles, current_handles) = self
                .dependency_controller
                .get_dependencies(task_id, obj_ids);

            let store = self.store.clone();
            let id = self.id;
            let tx_results = self.tx_results.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                for prior_notify in prior_handles {
                    prior_notify.notified().await;
                }

                let execution_result = E::exec_on_ctx(ctx, store, transaction).await;

                for notify in current_handles {
                    notify.notify_one();
                }

                if tx_results.send(execution_result).await.is_err() {
                    tracing::warn!("Failed to send execution result, stopping proxy {}", id);
                }
            });
        }
    }

    /// Spawn the proxy in a new task.
    pub fn spawn(mut self) -> JoinHandle<()>
    where
        E: Send + 'static,
        <E as Executor>::Store: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::StateChanges: Send,
    {
        tokio::spawn(async move {
            self.run().await;
        })
    }
}

#[cfg(test)]
mod tests {

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkConfig,
        executor::SuiTransactionWithTimestamp,
        executor::{generate_transactions, SuiExecutor},
        proxy::Proxy,
    };

    #[tokio::test]
    async fn pre_execute() {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);

        let config = BenchmarkConfig::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        let store = executor.create_in_memory_store();
        let transactions = generate_transactions(&config).await;
        let proxy = Proxy::new(0, executor, store, rx_proxy, tx_results);

        // Send transactions to the proxy.
        for tx in transactions {
            let transaction = SuiTransactionWithTimestamp::new_for_tests(tx);
            tx_proxy.send(transaction).await.unwrap();
        }

        // Spawn the proxy.
        proxy.spawn();

        // Receive the results.
        let results = rx_results.recv().await.unwrap();
        assert!(results.success());
    }
}
