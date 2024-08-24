// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use super::TxGenerator;
use crate::mock_account::Account;
use sui_test_transaction_builder::TestTransactionBuilder;
use sui_types::{
    base_types::{ObjectID, SequenceNumber, SuiAddress},
    transaction::{CallArg, ObjectArg, Transaction, DEFAULT_VALIDATOR_GAS_PRICE},
};

pub struct CounterTxGenerator {
    move_package: ObjectID,
    counter_objects: Vec<(ObjectID, SequenceNumber)>,
    account_orders: HashMap<SuiAddress, usize>,
    txs_per_counter: u64,
}

impl CounterTxGenerator {
    pub fn new(
        move_package: ObjectID,
        counter_objects: Vec<(ObjectID, SequenceNumber)>,
        account_orders: HashMap<SuiAddress, usize>,
        txs_per_counter: u64,
    ) -> Self {
        Self {
            move_package,
            counter_objects,
            account_orders,
            txs_per_counter,
        }
    }
}

impl TxGenerator for CounterTxGenerator {
    fn generate_txs(&self, account: Account) -> Vec<Transaction> {
        // dirty tricks to ensure there's exclusive account used by a shared object
        let index = self.account_orders.get(&account.sender).unwrap();
        let counter = self.counter_objects[*index];
        let mut txs = Vec::with_capacity(self.txs_per_counter as usize);

        for i in 0..self.txs_per_counter {
            let tx = TestTransactionBuilder::new(
                account.sender,
                account.gas_objects[i as usize],
                DEFAULT_VALIDATOR_GAS_PRICE,
            )
            .move_call(
                self.move_package,
                "benchmark",
                "increment_shared_counter",
                vec![CallArg::Object(ObjectArg::SharedObject {
                    id: counter.0,
                    initial_shared_version: counter.1,
                    mutable: true,
                })],
            )
            .build_and_sign(account.keypair.as_ref());

            txs.push(tx.clone());
        }
        txs
    }

    fn name(&self) -> &'static str {
        "Counter Increment Transaction Generator"
    }
}
