// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, path::PathBuf};

use anyhow::Context;
use clap::Parser;
use remora::{
    config::{BenchmarkConfig, ImportExport, WorkloadType},
    executor::{export_to_files, init_workload, LOG_DIR},
};

use sui_single_node_benchmark::{benchmark_context::BenchmarkContext, command::Component};

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Remora log generator", long_about = None)]
struct Args {
    /// The configuration for the benchmark.
    #[clap(long, value_name = "FILE")]
    benchmark_config: Option<PathBuf>,
}

/// The main function for the load generator.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = match args.benchmark_config {
        Some(path) => BenchmarkConfig::load(path).context("Failed to load benchmark config")?,
        None => BenchmarkConfig::default(),
    };
    assert_eq!(config.workload, WorkloadType::Contention);

    let working_directory = LOG_DIR;
    fs::create_dir_all(&working_directory).expect(&format!(
        "Failed to create directory '{}'",
        working_directory
    ));

    // generate txs and export to files
    let workload = init_workload(&config);
    let mut ctx = BenchmarkContext::new(workload.clone(), Component::PipeTxsToChannel, true).await;
    let tx_generator = workload.create_tx_generator(&mut ctx).await;
    let txs = ctx.generate_transactions(tx_generator).await;
    let txs = ctx.certify_transactions(txs, false).await;

    export_to_files(ctx.get_accounts(), &txs, working_directory.into());

    Ok(())
}
