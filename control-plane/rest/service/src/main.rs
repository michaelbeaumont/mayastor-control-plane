mod authentication;
mod v0;

use crate::v0::{CORE_CLIENT, JSON_GRPC_CLIENT};
use actix_service::ServiceFactory;
use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware, App, HttpServer,
};
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, rsa_private_keys};
use std::{fs::File, io::BufReader};
use utils::{nats_connection::NatsConnectionSpec, DEFAULT_GRPC_CLIENT_ADDR};

#[derive(Debug, Parser)]
#[structopt(name = utils::package_description!(), version = utils::version_info_str!())]
pub(crate) struct CliArgs {
    /// The bind address for the REST interface (with HTTPS)
    /// Default: 0.0.0.0:8080
    #[clap(long, default_value = "0.0.0.0:8080")]
    https: String,
    /// The bind address for the REST interface (with HTTP)
    #[clap(long)]
    http: Option<String>,

    /// The CORE gRPC Server URL or address to connect to the services.
    #[clap(long, short = 'z', default_value = DEFAULT_GRPC_CLIENT_ADDR)]
    core_grpc: Uri,

    /// The json gRPC Server URL or address to connect to the service.
    #[clap(long, short = 'J')]
    json_grpc: Option<Uri>,

    /// Path to the certificate file
    #[clap(long, short, required_unless_present = "dummy_certificates")]
    cert_file: Option<String>,
    /// Path to the key file
    #[clap(long, short, required_unless_present = "dummy_certificates")]
    key_file: Option<String>,

    /// Use dummy HTTPS certificates (for testing)
    #[clap(long, short, required_unless_present = "cert_file")]
    dummy_certificates: bool,

    /// Trace rest requests to the Jaeger endpoint agent
    #[clap(long, short)]
    jaeger: Option<String>,

    /// Path to JSON Web KEY file used for authenticating REST requests
    #[clap(long, required_unless_present = "no_auth")]
    jwk: Option<String>,

    /// Don't authenticate REST requests
    #[clap(long, required_unless_present = "jwk")]
    no_auth: bool,

    /// The default timeout for backend requests issued by the REST Server
    #[clap(long, short, default_value = utils::DEFAULT_REQ_TIMEOUT)]
    request_timeout: humantime::Duration,

    /// Add process service tags to the traces
    #[clap(short, long, env = "TRACING_TAGS", value_delimiter=',', value_parser = utils::tracing_telemetry::parse_key_value)]
    tracing_tags: Vec<KeyValue>,

    /// Don't use minimum timeouts for specific requests
    #[clap(long)]
    no_min_timeouts: bool,

    /// Set number of workers to start.
    /// The value 0 means the number of available physical CPUs is used.
    #[clap(long, short, default_value = physical())]
    workers: usize,

    /// Set the max number of workers to start.
    /// The value 0 means the number of available physical CPUs is used.
    #[clap(long, short, default_value = utils::DEFAULT_REST_MAX_WORKER_THREADS)]
    max_workers: usize,
}
impl CliArgs {
    fn args() -> Self {
        CliArgs::parse()
    }
}

/// Return the number of physical cpus.
fn physical() -> &'static str {
    Box::leak(num_cpus::get_physical().to_string().into_boxed_str())
}

/// default timeout options for every bus request
fn timeout_opts() -> TimeoutOptions {
    let timeout_opts =
        TimeoutOptions::new_no_retries().with_req_timeout(CliArgs::args().request_timeout.into());

    if CliArgs::args().no_min_timeouts {
        timeout_opts.with_min_req_timeout(None)
    } else {
        timeout_opts.with_min_req_timeout(RequestMinTimeout::default())
    }
}

use actix_web_opentelemetry::RequestTracing;
use clap::Parser;
use grpc::{client::CoreClient, operations::jsongrpc::client::JsonGrpcClient};
use http::Uri;
use stor_port::transport_api::{RequestMinTimeout, TimeoutOptions};
use utils::tracing_telemetry::KeyValue;

/// Extension trait for actix-web applications.
pub trait OpenApiExt<T> {
    /// configures the App with this version's handlers and openapi generation
    fn configure_api(
        self,
        config: &dyn Fn(actix_web::App<T>) -> actix_web::App<T>,
    ) -> actix_web::App<T>;
}

impl<T, B> OpenApiExt<T> for actix_web::App<T>
where
    B: MessageBody,
    T: ServiceFactory<
        ServiceRequest,
        Config = (),
        Response = ServiceResponse<B>,
        Error = actix_web::Error,
        InitError = (),
    >,
{
    fn configure_api(
        self,
        config: &dyn Fn(actix_web::App<T>) -> actix_web::App<T>,
    ) -> actix_web::App<T> {
        config(self)
    }
}

fn get_certificates() -> anyhow::Result<ServerConfig> {
    if CliArgs::args().dummy_certificates {
        get_dummy_certificates()
    } else {
        // guaranteed to be `Some` by the require_unless attribute
        let cert_file = CliArgs::args().cert_file.expect("cert_file is required");
        let key_file = CliArgs::args().key_file.expect("key_file is required");
        let cert_file = &mut BufReader::new(File::open(cert_file)?);
        let key_file = &mut BufReader::new(File::open(key_file)?);
        load_certificates(cert_file, key_file)
    }
}

fn get_dummy_certificates() -> anyhow::Result<ServerConfig> {
    let cert_file = &mut BufReader::new(&std::include_bytes!("../../certs/rsa/user.chain")[..]);
    let key_file = &mut BufReader::new(&std::include_bytes!("../../certs/rsa/user.rsa")[..]);

    load_certificates(cert_file, key_file)
}

fn load_certificates<R: std::io::Read>(
    cert_file: &mut BufReader<R>,
    key_file: &mut BufReader<R>,
) -> anyhow::Result<ServerConfig> {
    let config = ServerConfig::builder().with_safe_defaults();
    let cert_chain = certs(cert_file).map_err(|_| {
        anyhow::anyhow!("Failed to retrieve certificates from the certificate file",)
    })?;
    let mut keys = rsa_private_keys(key_file).map_err(|_| {
        anyhow::anyhow!("Failed to retrieve the rsa private keys from the key file",)
    })?;
    if keys.is_empty() {
        anyhow::bail!("No keys found in the keys file");
    }
    let config = config.with_no_client_auth().with_single_cert(
        cert_chain.into_iter().map(Certificate).collect(),
        PrivateKey(keys.remove(0)),
    )?;
    Ok(config)
}

fn get_jwk_path() -> Option<String> {
    match CliArgs::args().jwk {
        Some(path) => Some(path),
        None => match CliArgs::args().no_auth {
            true => None,
            false => panic!("Cannot authenticate without a JWK file"),
        },
    }
}

fn workers(args: &CliArgs) -> usize {
    let max_workers = match args.max_workers {
        0 => num_cpus::get_physical(),
        max => max,
    };

    let workers = match args.workers {
        0 => num_cpus::get_physical(),
        workers => workers,
    };

    workers.clamp(1, max_workers)
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    utils::print_package_info!();
    let cli_args = CliArgs::args();
    println!("Using options: {:?}", &cli_args);

    let nats = NatsConnectionSpec::from_url("nats://my-nats:4222")
        .unwrap()
        .connect()
        .await
        .unwrap();
    utils::tracing_telemetry::init_tracing(
        "rest-server",
        cli_args.tracing_tags.clone(),
        cli_args.jaeger.clone(),
        nats,
    );

    let app = move || {
        App::new()
            .wrap(RequestTracing::new())
            .wrap(middleware::Logger::default())
            .app_data(authentication::init(get_jwk_path()))
            .configure_api(&v0::configure_api)
    };

    // Initialise the core client to be used in rest
    CORE_CLIENT
        .set(CoreClient::new(CliArgs::args().core_grpc, timeout_opts()).await)
        .ok()
        .expect("Expect to be initialised only once");

    // Initialise the json grpc client to be used in rest
    if CliArgs::args().json_grpc.is_some() {
        JSON_GRPC_CLIENT
            .set(JsonGrpcClient::new(CliArgs::args().json_grpc.unwrap(), timeout_opts()).await)
            .ok()
            .expect("Expect to be initialised only once");
    }

    let server = HttpServer::new(app).bind_rustls(CliArgs::args().https, get_certificates()?)?;
    let result = if let Some(http) = CliArgs::args().http {
        server.bind(http).map_err(anyhow::Error::from)?
    } else {
        server
    }
    .workers(workers(&CliArgs::args()))
    .run()
    .await
    .map_err(|e| e.into());

    utils::tracing_telemetry::flush_traces();

    result
}
