use anyhow::anyhow;
use tracing::info;

use clap::{Arg, ArgMatches};
mod config;
mod controller;
mod identity;
mod pvwatcher;
mod server;

use config::CsiControllerConfig;
use csi_driver::client::{RestApiClient, REST_CLIENT};
use stor_port::types::v0::openapi::clients;

const CSI_SOCKET: &str = "/var/tmp/csi.sock";
const CONCURRENCY_LIMIT: usize = 10;
const REST_TIMEOUT: &str = "30s";

/// Initialize API client instance. Must be called prior to
/// obtaining the client instance.
pub(crate) fn initialize_rest_api() -> anyhow::Result<()> {
    if REST_CLIENT.get().is_some() {
        return Err(anyhow!("API client already initialized"));
    }

    let cfg = CsiControllerConfig::get_config();
    let endpoint = cfg.rest_endpoint();

    let url = clients::tower::Url::parse(endpoint)
        .map_err(|error| anyhow!("Invalid API endpoint URL {}: {:?}", endpoint, error))?;
    let concurrency_limit = cfg.create_volume_limit() * 2;
    let tower = clients::tower::Configuration::builder()
        .with_timeout(cfg.io_timeout())
        .with_concurrency_limit(Some(concurrency_limit))
        .build_url(url)
        .map_err(|error| {
            anyhow::anyhow!(
                "Failed to create openapi configuration, Error: '{:?}'",
                error
            )
        })?;

    REST_CLIENT.get_or_init(|| RestApiClient {
        rest_client: clients::tower::ApiClient::new(tower.clone()),
    });

    info!(
        "API client is initialized with endpoint {}, I/O timeout = {:?}",
        endpoint,
        cfg.io_timeout(),
    );
    Ok(())
}

/// Initialize all components before starting the CSI controller.
fn initialize_controller(args: &ArgMatches) -> anyhow::Result<()> {
    CsiControllerConfig::initialize(args)?;
    initialize_rest_api()
        .map_err(|error| anyhow::anyhow!("Failed to initialize API client, error = {error}"))?;
    Ok(())
}

#[tracing::instrument]
async fn ping_rest_api() {
    info!("Checking REST API endpoint accessibility ...");

    match RestApiClient::get_client().list_nodes().await {
        Err(error) => tracing::error!(?error, "REST API endpoint is not accessible"),
        Ok(nodes) => {
            let names: Vec<String> = nodes.into_iter().map(|n| n.id).collect();
            info!(
                "REST API endpoint available, {len} IoEngine node(s) reported: {names:?}",
                len = names.len(),
            );
        }
    }
}

#[tokio::main(worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let args = clap::Command::new(utils::package_description!())
        .version(utils::version_info_str!())
        .arg(
            Arg::new("endpoint")
                .long("rest-endpoint")
                .short('r')
                .env("ENDPOINT")
                .default_value("http://ksnode-1:30011")
                .help("A URL endpoint to the control plane's rest endpoint"),
        )
        .arg(
            Arg::new("socket")
                .long("csi-socket")
                .short('c')
                .env("CSI_SOCKET")
                .default_value(CSI_SOCKET)
                .help("The CSI socket path"),
        )
        .arg(
            Arg::new("jaeger")
                .short('j')
                .long("jaeger")
                .env("JAEGER_ENDPOINT")
                .help("Enable open telemetry and forward to jaeger"),
        )
        .arg(
            Arg::new("timeout")
                .short('t')
                .long("rest-timeout")
                .env("REST_TIMEOUT")
                .default_value(REST_TIMEOUT),
        )
        .arg(
            Arg::new("node-selector")
                .long("node-selector")
                .action(clap::ArgAction::Append)
                .num_args(1)
                .allow_hyphen_values(true)
                .default_value(csi_driver::csi_node_selector())
                .help(
                    "The node selector label which this plugin will report as part of its topology.\n\
                    Example:\n --node-selector key=value --node-selector key2=value2",
                ),
        )
        .arg(
            Arg::new("create-volume-limit")
                .long("create-volume-limit")
                .value_parser(clap::value_parser!(usize))
                .default_value(CONCURRENCY_LIMIT.to_string())
                .help(
                    "The number of worker threads that process requests"
                ),
        )
        .arg(
            Arg::new("orphan-vol-gc-period")
                .long("orphan-vol-gc-period")
                .default_value("10m")
                .help(
                    "How often to check and delete orphaned volumes. \n\
                        An orphan volume is a volume with no corresponding PV",
                )
        )
        .get_matches();

    utils::print_package_info!();

    let tags = utils::tracing_telemetry::default_tracing_tags(
        utils::raw_version_str(),
        env!("CARGO_PKG_VERSION"),
    );
    utils::tracing_telemetry::init_tracing(
        "csi-controller",
        tags,
        args.get_one::<String>("jaeger").cloned(),
    );
    let orphan_period = args
        .get_one::<String>("orphan-vol-gc-period")
        .map(|p| p.parse::<humantime::Duration>())
        .transpose()?;
    let csi_socket = args
        .get_one::<String>("socket")
        .expect("CSI socket must be specified");

    initialize_controller(&args)?;

    info!(
        "Starting IoEngine CSI Controller, REST endpoint = {}",
        CsiControllerConfig::get_config().rest_endpoint()
    );

    // Try to detect REST API endpoint to debug the accessibility status.
    ping_rest_api().await;

    // Starts PV Garbage Collector if platform type is k8s
    if stor_port::platform::current_platform_type() == stor_port::platform::PlatformType::K8s {
        let gc_instance = pvwatcher::PvGarbageCollector::new(orphan_period).await?;
        tokio::spawn(async move { gc_instance.run_watcher().await });
    }

    let result = server::CsiServer::run(csi_socket).await;
    utils::tracing_telemetry::flush_traces();
    result
}
