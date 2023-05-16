mod operations;
mod registry;
/// Node Service
pub(super) mod service;
mod specs;
/// node watchdog to keep track of a node's liveness
pub(crate) mod watchdog;
pub(crate) mod wrapper;

use super::{controller::registry::Registry, CliArgs};
use agents::Service;
use grpc::operations::{node::server::NodeServer, registration::server::RegistrationServer};
use std::sync::Arc;
use stor_port::{
    transport_api::{v0::*, *},
    types::v0::transport::{GetBlockDevices, GetNodes},
};

/// Configure the Service and return the builder.
pub(crate) async fn configure(builder: agents::ServiceEmpty) -> Service {
    let node_service = create_node_service(&builder).await;
    let node_grpc_service = NodeServer::new(Arc::new(node_service.clone()));
    let registration_service = RegistrationServer::new(Arc::new(node_service));

    builder
        .with_service(node_grpc_service.into_grpc_server())
        .with_service(registration_service.clone().into_v1_grpc_server())
        .with_service(registration_service.into_v1_alpha_grpc_server())
}

async fn create_node_service<S>(builder: &Service<S>) -> service::Service {
    let registry = builder.shared_state::<Registry>().clone();
    let deadline = CliArgs::args().deadline.into();
    let request = CliArgs::args().request_timeout.into();
    let connect = CliArgs::args().connect_timeout.into();
    let no_min = CliArgs::args().no_min_timeouts;

    service::Service::new(registry.clone(), deadline, request, connect, no_min).await
}
