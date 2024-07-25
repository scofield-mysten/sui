// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::Parser;
use remora::{
    config::{BenchmarkConfig, ImportExport, ValidatorConfig},
    executor::SuiExecutor,
    metrics::Metrics,
    validator::SingleMachineValidator,
};

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Remora load generator", long_about = None)]
struct Args {
    /// The configuration for the benchmark.
    #[clap(long, value_name = "FILE")]
    benchmark_config: Option<PathBuf>,
    /// The configuration for the validator.
    #[clap(long, value_name = "FILE")]
    validator_config: Option<PathBuf>,
}

/// The main function for the load generator.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let benchmark_config = match args.benchmark_config {
        Some(path) => BenchmarkConfig::load(path).context("Failed to load benchmark config")?,
        None => BenchmarkConfig::default(),
    };
    let validator_config = match args.validator_config {
        Some(path) => ValidatorConfig::load(path).context("Failed to load validator config")?,
        None => ValidatorConfig::default(),
    };

    let _ = tracing_subscriber::fmt::try_init();
    let registry = mysten_metrics::start_prometheus_server(validator_config.metrics_address);
    let metrics = Arc::new(Metrics::new(&registry.default_registry()));

    //
    tracing::info!("Loading executor");
    let executor = SuiExecutor::new(&benchmark_config).await;

    //
    tracing::info!(
        "Starting validator on {}",
        validator_config.validator_address
    );
    tracing::info!("Exposing metrics on {}", validator_config.metrics_address);
    SingleMachineValidator::start(executor, &validator_config, metrics)
        .await
        .collect_results()
        .await;

    Ok(())
}