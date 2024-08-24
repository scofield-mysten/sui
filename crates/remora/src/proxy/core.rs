// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    executor::api::{ExecutionEffects, Executor, TransactionWithTimestamp},
    metrics::Metrics,
};

pub type ProxyId = String;

/// A proxy is responsible for pre-executing transactions.
pub struct ProxyCore<E: Executor> {
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
    /// The  metrics for the proxy
    metrics: Arc<Metrics>,
}

impl<E: Executor> ProxyCore<E> {
    /// Create a new proxy.
    pub fn new(
        id: ProxyId,
        executor: E,
        store: E::Store,
        rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
        tx_results: Sender<ExecutionEffects<E::StateChanges>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            id,
            executor,
            store: Arc::new(store),
            rx_transactions,
            tx_results,
            metrics,
        }
    }

    /// Pre-execute a transaction.
    // TODO: Naive single-threaded execution.
    async fn pre_execute(
        &mut self,
        transaction: &TransactionWithTimestamp<E::Transaction>,
    ) -> ExecutionEffects<E::StateChanges> {
        self.executor.execute(&self.store, transaction).await
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

        while let Some(transaction) = self.rx_transactions.recv().await {
            self.metrics.increase_proxy_load(&self.id);
            let execution_result = self.pre_execute(&transaction).await;
            self.metrics.decrease_proxy_load(&self.id);
            if self.tx_results.send(execution_result).await.is_err() {
                tracing::warn!(
                    "Failed to send execution result, stopping proxy {}",
                    self.id
                );
                break;
            }
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

    use std::sync::Arc;

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkParameters,
        executor::sui::{generate_transactions, SuiExecutor, SuiTransactionWithTimestamp},
        metrics::Metrics,
        proxy::core::ProxyCore,
    };

    #[tokio::test]
    async fn pre_execute() {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);

        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        let store = executor.create_in_memory_store();
        let metrics = Arc::new(Metrics::new_for_tests());
        let proxy_id = "0".to_string();
        let proxy = ProxyCore::new(proxy_id, executor, store, rx_proxy, tx_results, metrics);

        // Send transactions to the proxy.
        let transactions = generate_transactions(&config).await;
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
