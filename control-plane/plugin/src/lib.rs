#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate lazy_static;

use operations::{Label, SetProperty};
use resources::LabelResources;

use crate::{
    operations::{
        Cordoning, Drain, Get, GetBlockDevices, GetSnapshots, List, ListExt, Operations,
        PluginResult, RebuildHistory, ReplicaTopology, Scale,
    },
    resources::{
        blockdevice, cordon, drain, node, pool, snapshot, volume, CordonResources, DrainResources,
        GetCordonArgs, GetDrainArgs, GetResources, ScaleResources, SetPropertyResources,
        UnCordonResources,
    },
};

pub mod operations;
pub mod resources;
pub mod rest_wrapper;

/// Flush traces on `Drop`.
pub struct TracingFlusher {}
impl Drop for TracingFlusher {
    fn drop(&mut self) {
        utils::tracing_telemetry::flush_traces();
    }
}

/// Every plugin operation must implement this trait to become composable.
#[async_trait::async_trait(?Send)]
pub trait ExecuteOperation {
    type Args;
    type Error;
    async fn execute(&self, cli_args: &Self::Args) -> Result<(), Self::Error>;
}

#[derive(clap::Parser, Debug)]
pub struct CliArgs {
    /// The Output, viz yaml, json.
    #[clap(global = true, default_value = resources::utils::OutputFormat::None.as_ref(), short, long)]
    pub output: resources::utils::OutputFormat,

    /// Trace rest requests to the Jaeger endpoint agent.
    #[clap(global = true, long, short)]
    pub jaeger: Option<String>,

    /// Timeout for the REST operations.
    #[clap(long, short, default_value = "10s")]
    pub timeout: humantime::Duration,
}

impl CliArgs {
    /// Initialize tracing (including opentelemetry).
    pub fn init_tracing(&self) -> TracingFlusher {
        let git_version = option_env!("GIT_VERSION").unwrap_or_else(utils::raw_version_str);
        let tags =
            utils::tracing_telemetry::default_tracing_tags(git_version, env!("CARGO_PKG_VERSION"));

        let fmt_layer = match std::env::var("RUST_LOG") {
            Ok(_) => utils::tracing_telemetry::FmtLayer::Stderr,
            Err(_) => utils::tracing_telemetry::FmtLayer::None,
        };

        utils::tracing_telemetry::init_tracing_ext(
            env!("CARGO_PKG_NAME"),
            tags,
            self.jaeger.as_ref(),
            fmt_layer,
            None,
        );

        TracingFlusher {}
    }
}

#[async_trait::async_trait(?Send)]
impl ExecuteOperation for Operations {
    type Args = CliArgs;
    type Error = crate::resources::Error;
    async fn execute(&self, cli_args: &CliArgs) -> PluginResult {
        match self {
            Operations::Drain(resource) => resource.execute(cli_args).await,
            Operations::Get(resource) => resource.execute(cli_args).await,
            Operations::Scale(resource) => resource.execute(cli_args).await,
            Operations::Set(resource) => resource.execute(cli_args).await,
            Operations::Cordon(resource) => resource.execute(cli_args).await,
            Operations::Uncordon(resource) => resource.execute(cli_args).await,
            Operations::Label(resource) => resource.execute(cli_args).await,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ExecuteOperation for DrainResources {
    type Args = CliArgs;
    type Error = crate::resources::Error;
    async fn execute(&self, cli_args: &CliArgs) -> PluginResult {
        match self {
            DrainResources::Node(drain_node_args) => {
                node::Node::drain(
                    &drain_node_args.node_id(),
                    drain_node_args.label(),
                    drain_node_args.drain_timeout(),
                    &cli_args.output,
                )
                .await
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ExecuteOperation for GetResources {
    type Args = CliArgs;
    type Error = crate::resources::Error;
    async fn execute(&self, cli_args: &CliArgs) -> PluginResult {
        match self {
            GetResources::Cordon(get_cordon_resource) => match get_cordon_resource {
                GetCordonArgs::Node { id: node_id } => {
                    cordon::NodeCordon::get(node_id, &cli_args.output).await
                }
                GetCordonArgs::Nodes => cordon::NodeCordons::list(&cli_args.output).await,
            },
            GetResources::Drain(get_drain_resource) => match get_drain_resource {
                GetDrainArgs::Node { id: node_id } => {
                    drain::NodeDrain::get(node_id, &cli_args.output).await
                }
                GetDrainArgs::Nodes => drain::NodeDrains::list(&cli_args.output).await,
            },
            GetResources::Volumes(vol_args) => {
                volume::Volumes::list(&cli_args.output, vol_args).await
            }
            GetResources::Volume { id } => volume::Volume::get(id, &cli_args.output).await,
            GetResources::RebuildHistory { id } => {
                volume::Volume::rebuild_history(id, &cli_args.output).await
            }
            GetResources::VolumeReplicaTopologies(vol_args) => {
                volume::Volume::topologies(&cli_args.output, vol_args).await
            }
            GetResources::VolumeReplicaTopology { id } => {
                volume::Volume::topology(id, &cli_args.output).await
            }
            GetResources::Pools => pool::Pools::list(&cli_args.output).await,
            GetResources::Pool { id } => pool::Pool::get(id, &cli_args.output).await,
            GetResources::Nodes => node::Nodes::list(&cli_args.output).await,
            GetResources::Node(args) => node::Node::get(&args.node_id(), &cli_args.output).await,
            GetResources::BlockDevices(bdargs) => {
                blockdevice::BlockDevice::get_blockdevices(
                    &bdargs.node_id(),
                    &bdargs.all(),
                    &cli_args.output,
                )
                .await
            }
            GetResources::VolumeSnapshots(snapargs) => {
                snapshot::VolumeSnapshots::get_snapshots(
                    &snapargs.volume(),
                    &snapargs.snapshot(),
                    &cli_args.output,
                )
                .await
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ExecuteOperation for ScaleResources {
    type Args = CliArgs;
    type Error = crate::resources::Error;
    async fn execute(&self, cli_args: &CliArgs) -> PluginResult {
        match self {
            ScaleResources::Volume { id, replica_count } => {
                volume::Volume::scale(id, *replica_count, &cli_args.output).await
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ExecuteOperation for SetPropertyResources {
    type Args = CliArgs;
    type Error = crate::resources::Error;
    async fn execute(&self, cli_args: &CliArgs) -> PluginResult {
        match self {
            SetPropertyResources::Volume { id, properties } => {
                volume::Volume::set_property(id, properties, &cli_args.output).await
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ExecuteOperation for CordonResources {
    type Args = CliArgs;
    type Error = crate::resources::Error;
    async fn execute(&self, cli_args: &CliArgs) -> PluginResult {
        match self {
            CordonResources::Node { id, label } => {
                node::Node::cordon(id, label, &cli_args.output).await
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ExecuteOperation for UnCordonResources {
    type Args = CliArgs;
    type Error = crate::resources::Error;
    async fn execute(&self, cli_args: &CliArgs) -> PluginResult {
        match self {
            UnCordonResources::Node { id, label } => {
                node::Node::uncordon(id, label, &cli_args.output).await
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ExecuteOperation for LabelResources {
    type Args = CliArgs;
    type Error = crate::resources::Error;
    async fn execute(&self, cli_args: &CliArgs) -> PluginResult {
        match self {
            LabelResources::Node {
                id,
                label,
                overwrite,
            } => node::Node::label(id, label.to_string(), *overwrite, &cli_args.output).await,
        }
    }
}
