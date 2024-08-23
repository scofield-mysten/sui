// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use std::collections::HashMap;

use crate::benchmark_context::BenchmarkContext;
use crate::command::WorkloadKind;
use crate::tx_generator::{
    counter_tx_generator::CounterTxGenerator, MoveTxGenerator, NonMoveTxGenerator,
    PackagePublishTxGenerator, TxGenerator,
};
use std::path::PathBuf;
use std::sync::Arc;
use sui_test_transaction_builder::PublishData;
use sui_types::base_types::SuiAddress;

#[derive(Clone)]
pub struct Workload {
    pub tx_count: u64,
    pub workload_kind: WorkloadKind,
}

impl Workload {
    pub fn new(tx_count: u64, workload_kind: WorkloadKind) -> Self {
        Self {
            tx_count,
            workload_kind,
        }
    }

    pub fn num_accounts(&self) -> u64 {
        match self.workload_kind {
            WorkloadKind::Counter { txs_per_counter } => self.tx_count / txs_per_counter,
            _ => self.tx_count,
        }
    }

    pub(crate) fn gas_object_num_per_account(&self) -> u64 {
        self.workload_kind.gas_object_num_per_account()
    }

    pub async fn create_tx_generator(&self, ctx: &mut BenchmarkContext) -> Arc<dyn TxGenerator> {
        match &self.workload_kind {
            WorkloadKind::NoMove => Arc::new(NonMoveTxGenerator::new()),
            WorkloadKind::PTB {
                num_transfers,
                use_native_transfer,
                num_dynamic_fields,
                computation,
                num_shared_objects,
                num_mints,
                nft_size,
                use_batch_mint,
            } => {
                let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                path.extend(["move_package"]);
                let move_package = ctx.publish_package(PublishData::Source(path, false)).await;
                let root_objects = ctx
                    .preparing_dynamic_fields(move_package.0, *num_dynamic_fields)
                    .await;
                let shared_objects = ctx
                    .prepare_shared_objects(move_package.0, *num_shared_objects)
                    .await;
                Arc::new(MoveTxGenerator::new(
                    move_package.0,
                    *num_transfers,
                    *use_native_transfer,
                    *computation,
                    root_objects,
                    shared_objects,
                    *num_mints,
                    *nft_size,
                    *use_batch_mint,
                ))
            }
            WorkloadKind::Publish {
                manifest_file: manifest_path,
            } => Arc::new(PackagePublishTxGenerator::new(ctx, manifest_path.clone()).await),
            WorkloadKind::Counter { txs_per_counter } => {
                let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                path.extend(["move_package"]);
                let move_package = ctx.publish_package(PublishData::Source(path, false)).await;

                // generate counter objects
                let counter_objects = ctx
                    .prepare_shared_objects(move_package.0, self.num_accounts() as usize)
                    .await;

                let mut account_orders: HashMap<SuiAddress, usize> = HashMap::new();

                // Iterate over the values and assign a unique index to each
                for (idx, value) in ctx.get_accounts().keys().enumerate() {
                    account_orders.insert(*value, idx);
                }
                println!("finish preparing shared objects {}", counter_objects.len());

                Arc::new(CounterTxGenerator::new(
                    move_package.0,
                    counter_objects,
                    account_orders,
                    *txs_per_counter,
                ))
            }
        }
    }
}
