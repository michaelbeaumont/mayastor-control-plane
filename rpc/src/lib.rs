extern crate bytes;
extern crate prost;
extern crate prost_derive;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate tonic;
#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::unit_arg)]
#[allow(clippy::redundant_closure)]
#[allow(clippy::upper_case_acronyms)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub mod io_engine {
    use std::{future::Future, net::SocketAddr, str::FromStr, time::Duration};
    use strum_macros::{Display, EnumString};
    use tonic::{transport::Channel, Status};

    use crate::v1::pb::{
        CreateReplicaSnapshotResponse, ListSnapshotsResponse, NexusCreateSnapshotResponse,
    };
    /// AutoGenerated Io Engine Client V0.
    pub use mayastor_client::MayastorClient as IoEngineClientV0;

    /// Io Engine Client V1, with its components.
    #[derive(Clone)]
    struct IoEngineClientV1<Channel> {
        /// AutoGenerated Io Engine V1 Nexus Client.
        nexus: super::v1::nexus::nexus_rpc_client::NexusRpcClient<Channel>,
        /// AutoGenerated Io Engine V1 Nexus Client.
        replica: super::v1::replica::replica_rpc_client::ReplicaRpcClient<Channel>,
        /// AutoGenerated Io Engine V1 Snapshot Client.
        snapshot: super::v1::snapshot::snapshot_rpc_client::SnapshotRpcClient<Channel>,
    }
    impl IoEngineClientV1<Channel> {
        fn new(channel: Channel) -> Self {
            Self {
                nexus: super::v1::nexus::nexus_rpc_client::NexusRpcClient::new(channel.clone()),
                replica: super::v1::replica::replica_rpc_client::ReplicaRpcClient::new(
                    channel.clone(),
                ),
                snapshot: super::v1::snapshot::snapshot_rpc_client::SnapshotRpcClient::new(channel),
            }
        }
    }

    /// Nvme ANA Parse Error.
    #[derive(Debug)]
    pub enum Error {
        ParseError,
    }

    impl From<()> for Null {
        fn from(_: ()) -> Self {
            Self {}
        }
    }

    impl FromStr for NvmeAnaState {
        type Err = Error;
        fn from_str(state: &str) -> Result<Self, Self::Err> {
            match state {
                "optimized" => Ok(Self::NvmeAnaOptimizedState),
                "non_optimized" => Ok(Self::NvmeAnaNonOptimizedState),
                "inaccessible" => Ok(Self::NvmeAnaInaccessibleState),
                _ => Err(Error::ParseError),
            }
        }
    }

    include!(concat!(env!("OUT_DIR"), "/mayastor.rs"));

    /// The IoEngine grpc api versions.
    #[derive(Default, Debug, EnumString, Display, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
    #[strum(ascii_case_insensitive)]
    pub enum IoEngineApiVersion {
        #[default]
        V0,
        V1,
    }
    impl IoEngineApiVersion {
        /// Convert a list of `Self` to a `String` argument.
        pub fn vec_to_str(vec: Vec<Self>) -> String {
            vec.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    /// A versioned IoEngine client
    #[derive(Clone)]
    enum IoEngineClient {
        V0(IoEngineClientV0<Channel>),
        V1(IoEngineClientV1<Channel>),
    }

    /// Test Rpc Handle to connect to an io-engine instance via an endpoint.
    /// Gives access to the io-engine client and the bdev client.
    #[derive(Clone)]
    pub struct RpcHandle {
        name: String,
        endpoint: SocketAddr,
        io_engine: IoEngineClient,
    }

    trait Rpc: RpcHealth + RpcNexus + RpcReplica {}
    #[tonic::async_trait]
    trait RpcHealth {
        /// Ping the Io Engine for responsiveness.
        async fn ping(&mut self) -> Result<(), tonic::Status>;
    }
    #[tonic::async_trait]
    trait RpcNexus {
        /// List all Nexuses.
        async fn list_nexuses(&mut self) -> Result<String, tonic::Status>;
        /// Fault a Nexus Child.
        async fn fault_child(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status>;
        /// Add a Nexus Child.
        async fn add_child(
            &mut self,
            uuid: &str,
            uri: &str,
            norebuild: bool,
        ) -> Result<String, tonic::Status>;
        /// Remove a Nexus Child.
        async fn remove_child(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status>;
        /// Pause a rebuild.
        async fn pause_rebuild(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status>;
        /// Resume a rebuild.
        async fn resume_rebuild(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status>;
    }
    #[tonic::async_trait]
    trait RpcReplica {
        /// Share a pool replica.
        async fn share(
            &mut self,
            uuid: &str,
            allowed_hosts: Vec<&str>,
        ) -> Result<String, tonic::Status>;
    }
    #[tonic::async_trait]
    trait RpcSnapshot {
        /// Create a nexus snapshot.
        async fn create_nexus_snap(
            &mut self,
            nexus_uuid: &str,
            snapshot_name: &str,
            entity_id: &str,
            txn_id: &str,
            replica_uuid: &str,
            snapshot_uuid: &str,
        ) -> Result<NexusCreateSnapshotResponse, tonic::Status>;
        /// Create a replica snapshot.
        async fn create_replica_snap(
            &mut self,
            snapshot_name: &str,
            entity_id: &str,
            txn_id: &str,
            replica_uuid: &str,
            snapshot_uuid: &str,
        ) -> Result<CreateReplicaSnapshotResponse, tonic::Status>;
        /// List replica snapshots.
        async fn list_replica_snaps(
            &mut self,
            source_uuid: Option<&str>,
            snapshot_uuid: Option<&str>,
        ) -> Result<ListSnapshotsResponse, tonic::Status>;
    }
    #[tonic::async_trait]
    impl RpcHealth for IoEngineClientV0<Channel> {
        async fn ping(&mut self) -> Result<(), tonic::Status> {
            self.list_nexus_v2(Null {}).await?;
            Ok(())
        }
    }
    #[tonic::async_trait]
    impl RpcNexus for IoEngineClientV0<Channel> {
        async fn list_nexuses(&mut self) -> Result<String, tonic::Status> {
            let nexuses = self.list_nexus_v2(Null {}).await?;
            Ok(format!("{:?}", nexuses.into_inner().nexus_list))
        }

        async fn fault_child(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status> {
            self.fault_nexus_child(FaultNexusChildRequest {
                uuid: uuid.to_string(),
                uri: uri.to_string(),
            })
            .await?;
            Ok(())
        }

        async fn add_child(
            &mut self,
            uuid: &str,
            uri: &str,
            norebuild: bool,
        ) -> Result<String, tonic::Status> {
            let child = self
                .add_child_nexus(AddChildNexusRequest {
                    uuid: uuid.to_string(),
                    uri: uri.to_string(),
                    norebuild,
                })
                .await?;
            Ok(format!("{:?}", child.into_inner()))
        }

        async fn remove_child(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status> {
            self.remove_child_nexus(RemoveChildNexusRequest {
                uuid: uuid.to_string(),
                uri: uri.to_string(),
            })
            .await?;
            Ok(())
        }

        async fn pause_rebuild(&mut self, uuid: &str, uri: &str) -> Result<(), Status> {
            self.pause_rebuild(PauseRebuildRequest {
                uuid: uuid.to_string(),
                uri: uri.to_string(),
            })
            .await?;
            Ok(())
        }

        async fn resume_rebuild(&mut self, uuid: &str, uri: &str) -> Result<(), Status> {
            self.resume_rebuild(ResumeRebuildRequest {
                uuid: uuid.to_string(),
                uri: uri.to_string(),
            })
            .await?;
            Ok(())
        }
    }
    #[tonic::async_trait]
    impl RpcReplica for IoEngineClientV0<Channel> {
        async fn share(
            &mut self,
            uuid: &str,
            allowed_hosts: Vec<&str>,
        ) -> Result<String, tonic::Status> {
            let result = self
                .share_replica(ShareReplicaRequest {
                    uuid: uuid.to_string(),
                    share: ShareProtocolReplica::ReplicaNvmf as i32,
                    allowed_hosts: allowed_hosts.iter().map(ToString::to_string).collect(),
                })
                .await?;
            Ok(result.into_inner().uri)
        }
    }
    #[tonic::async_trait]
    impl RpcHealth for IoEngineClientV1<Channel> {
        async fn ping(&mut self) -> Result<(), tonic::Status> {
            self.nexus
                .list_nexus(super::v1::nexus::ListNexusOptions::default())
                .await?;
            Ok(())
        }
    }
    #[tonic::async_trait]
    impl RpcNexus for IoEngineClientV1<Channel> {
        async fn list_nexuses(&mut self) -> Result<String, tonic::Status> {
            let nexuses = self
                .nexus
                .list_nexus(super::v1::nexus::ListNexusOptions::default())
                .await?;
            Ok(format!("{:?}", nexuses.into_inner().nexus_list))
        }

        async fn fault_child(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status> {
            self.nexus
                .fault_nexus_child(super::v1::nexus::FaultNexusChildRequest {
                    uuid: uuid.to_string(),
                    uri: uri.to_string(),
                })
                .await?;
            Ok(())
        }

        async fn add_child(
            &mut self,
            uuid: &str,
            uri: &str,
            norebuild: bool,
        ) -> Result<String, tonic::Status> {
            let child = self
                .nexus
                .add_child_nexus(super::v1::nexus::AddChildNexusRequest {
                    uuid: uuid.to_string(),
                    uri: uri.to_string(),
                    norebuild,
                })
                .await?;
            Ok(format!("{:?}", child.into_inner()))
        }

        async fn remove_child(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status> {
            self.nexus
                .remove_child_nexus(super::v1::nexus::RemoveChildNexusRequest {
                    uuid: uuid.to_string(),
                    uri: uri.to_string(),
                })
                .await?;
            Ok(())
        }

        async fn pause_rebuild(&mut self, uuid: &str, uri: &str) -> Result<(), Status> {
            self.nexus
                .pause_rebuild(super::v1::pb::PauseRebuildRequest {
                    nexus_uuid: uuid.to_string(),
                    uri: uri.to_string(),
                })
                .await?;
            Ok(())
        }

        async fn resume_rebuild(&mut self, uuid: &str, uri: &str) -> Result<(), Status> {
            self.nexus
                .resume_rebuild(super::v1::pb::ResumeRebuildRequest {
                    nexus_uuid: uuid.to_string(),
                    uri: uri.to_string(),
                })
                .await?;
            Ok(())
        }
    }
    #[tonic::async_trait]
    impl RpcSnapshot for IoEngineClientV1<Channel> {
        async fn create_nexus_snap(
            &mut self,
            nexus_uuid: &str,
            snapshot_name: &str,
            entity_id: &str,
            txn_id: &str,
            replica_uuid: &str,
            snapshot_uuid: &str,
        ) -> Result<NexusCreateSnapshotResponse, Status> {
            Ok(self
                .snapshot
                .create_nexus_snapshot(super::v1::pb::NexusCreateSnapshotRequest {
                    nexus_uuid: nexus_uuid.to_string(),
                    entity_id: entity_id.to_string(),
                    txn_id: txn_id.to_string(),
                    snapshot_name: snapshot_name.to_string(),
                    replicas: vec![super::v1::pb::NexusCreateSnapshotReplicaDescriptor {
                        replica_uuid: replica_uuid.to_string(),
                        snapshot_uuid: Some(snapshot_uuid.to_string()),
                        skip: false,
                    }],
                })
                .await?
                .into_inner())
        }

        async fn create_replica_snap(
            &mut self,
            snapshot_name: &str,
            entity_id: &str,
            txn_id: &str,
            replica_uuid: &str,
            snapshot_uuid: &str,
        ) -> Result<CreateReplicaSnapshotResponse, Status> {
            Ok(self
                .snapshot
                .create_replica_snapshot(super::v1::pb::CreateReplicaSnapshotRequest {
                    replica_uuid: replica_uuid.to_string(),
                    entity_id: entity_id.to_string(),
                    txn_id: txn_id.to_string(),
                    snapshot_name: snapshot_name.to_string(),
                    snapshot_uuid: snapshot_uuid.to_string(),
                })
                .await?
                .into_inner())
        }

        async fn list_replica_snaps(
            &mut self,
            source_uuid: Option<&str>,
            snapshot_uuid: Option<&str>,
        ) -> Result<ListSnapshotsResponse, Status> {
            Ok(self
                .snapshot
                .list_snapshot(super::v1::pb::ListSnapshotsRequest {
                    source_uuid: source_uuid.map(|uuid| uuid.to_string()),
                    snapshot_uuid: snapshot_uuid.map(|uuid| uuid.to_string()),
                    query: None,
                })
                .await?
                .into_inner())
        }
    }
    #[tonic::async_trait]
    impl RpcReplica for IoEngineClientV1<Channel> {
        async fn share(
            &mut self,
            uuid: &str,
            allowed_hosts: Vec<&str>,
        ) -> Result<String, tonic::Status> {
            let result = self
                .replica
                .share_replica(super::v1::replica::ShareReplicaRequest {
                    uuid: uuid.to_string(),
                    share: ShareProtocolReplica::ReplicaNvmf as i32,
                    allowed_hosts: allowed_hosts.iter().map(ToString::to_string).collect(),
                })
                .await?;
            Ok(result.into_inner().uri)
        }
    }

    impl RpcHandle {
        /// Ping the Io Engine for responsiveness.
        pub async fn ping(&mut self) -> Result<(), tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(cli) => cli.ping().await,
                IoEngineClient::V1(cli) => cli.ping().await,
            }
        }
        /// Pause a Rebuild.
        pub async fn pause_rebuild(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(cli) => RpcNexus::pause_rebuild(cli, uuid, uri).await,
                IoEngineClient::V1(cli) => RpcNexus::pause_rebuild(cli, uuid, uri).await,
            }
        }
        /// Resume a Rebuild.
        pub async fn resume_rebuild(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(cli) => RpcNexus::resume_rebuild(cli, uuid, uri).await,
                IoEngineClient::V1(cli) => RpcNexus::resume_rebuild(cli, uuid, uri).await,
            }
        }
        /// List all Nexuses.
        pub async fn list_nexuses(&mut self) -> Result<String, tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(cli) => cli.list_nexuses().await,
                IoEngineClient::V1(cli) => cli.list_nexuses().await,
            }
        }
        /// Fault a Nexus Child.
        pub async fn fault_child(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(cli) => cli.fault_child(uuid, uri).await,
                IoEngineClient::V1(cli) => cli.fault_child(uuid, uri).await,
            }
        }
        /// Add a Nexus Child.
        pub async fn add_child(
            &mut self,
            uuid: &str,
            uri: &str,
            norebuild: bool,
        ) -> Result<String, tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(cli) => cli.add_child(uuid, uri, norebuild).await,
                IoEngineClient::V1(cli) => cli.add_child(uuid, uri, norebuild).await,
            }
        }
        /// Remove a Nexus Child.
        pub async fn remove_child(&mut self, uuid: &str, uri: &str) -> Result<(), tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(cli) => cli.remove_child(uuid, uri).await,
                IoEngineClient::V1(cli) => cli.remove_child(uuid, uri).await,
            }
        }
        /// Share a pool replica.
        pub async fn share_replica<T: AsRef<str>>(
            &mut self,
            uuid: &str,
            allowed_hosts: Vec<T>,
        ) -> Result<String, tonic::Status> {
            let allowed_hosts = allowed_hosts.iter().map(AsRef::as_ref).collect();
            match &mut self.io_engine {
                IoEngineClient::V0(cli) => cli.share(uuid, allowed_hosts).await,
                IoEngineClient::V1(cli) => cli.share(uuid, allowed_hosts).await,
            }
        }

        /// Create a nexus snapshot.
        pub async fn create_nexus_snap(
            &mut self,
            nexus_uuid: &str,
            snapshot_name: &str,
            entity_id: &str,
            txn_id: &str,
            replica_uuid: &str,
            snapshot_uuid: &str,
        ) -> Result<NexusCreateSnapshotResponse, tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(_cli) => unimplemented!(),
                IoEngineClient::V1(cli) => {
                    cli.create_nexus_snap(
                        nexus_uuid,
                        snapshot_name,
                        entity_id,
                        txn_id,
                        replica_uuid,
                        snapshot_uuid,
                    )
                    .await
                }
            }
        }

        /// Create a replica snapshot.
        pub async fn create_replica_snap(
            &mut self,
            snapshot_name: &str,
            entity_id: &str,
            txn_id: &str,
            replica_uuid: &str,
            snapshot_uuid: &str,
        ) -> Result<CreateReplicaSnapshotResponse, tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(_cli) => unimplemented!(),
                IoEngineClient::V1(cli) => {
                    cli.create_replica_snap(
                        snapshot_name,
                        entity_id,
                        txn_id,
                        replica_uuid,
                        snapshot_uuid,
                    )
                    .await
                }
            }
        }

        /// List replica snapshots.
        pub async fn list_replica_snaps(
            &mut self,
            source_uuid: Option<&str>,
            snapshot_uuid: Option<&str>,
        ) -> Result<ListSnapshotsResponse, tonic::Status> {
            match &mut self.io_engine {
                IoEngineClient::V0(_cli) => unimplemented!(),
                IoEngineClient::V1(cli) => cli.list_replica_snaps(source_uuid, snapshot_uuid).await,
            }
        }

        /// Connect to the container and return a handle to `Self`
        /// Note: The initial connection with a timeout is using blocking calls
        pub async fn connect<S: Fn(Duration) -> F, F: Future<Output = ()>>(
            version: IoEngineApiVersion,
            name: &str,
            endpoint: SocketAddr,
            sleep: S,
        ) -> Result<Self, String> {
            let endpoint_str = format!("http://{endpoint}");
            let mut attempts = 20;
            let channel = loop {
                match tonic::transport::Endpoint::new(endpoint_str.clone())
                    .map_err(|e| e.to_string())?
                    .connect_timeout(Duration::from_millis(100))
                    .connect()
                    .await
                {
                    Ok(channel) => break channel,
                    Err(_) => {
                        sleep(Duration::from_millis(50)).await;
                        attempts -= 1;
                        if attempts == 0 {
                            return Err(format!("Failed to connect to {name}/{endpoint}"));
                        }
                        continue;
                    }
                }
            };

            let client = match version {
                IoEngineApiVersion::V0 => IoEngineClient::V0(IoEngineClientV0::new(channel)),
                IoEngineApiVersion::V1 => IoEngineClient::V1(IoEngineClientV1::new(channel)),
            };

            Ok(Self {
                name: name.to_string(),
                io_engine: client,
                endpoint,
            })
        }
    }
}

pub mod csi {
    #![allow(clippy::derive_partial_eq_without_eq)]
    include!(concat!(env!("OUT_DIR"), "/csi.v1.rs"));
}

pub mod v1 {
    /// The raw protobuf types.
    pub mod pb {
        #![allow(clippy::derive_partial_eq_without_eq)]
        include!(concat!(env!("OUT_DIR"), "/mayastor.v1.rs"));
    }

    /// V1 Registration autogenerated grpc code.
    pub mod registration {
        pub use super::pb::{
            registration_client, registration_server, ApiVersion, DeregisterRequest,
            RegisterRequest,
        };
    }

    /// V1 Host autogenerated grpc code.
    pub mod host {
        pub use super::pb::{
            block_device::{Filesystem, Partition},
            host_rpc_client, BlockDevice, ListBlockDevicesRequest,
        };
    }

    /// V1 Replica autogenerated grpc code.
    pub mod replica {
        pub use super::pb::{
            destroy_replica_request, list_replica_options, replica_rpc_client,
            CreateReplicaRequest, DestroyReplicaRequest, ListReplicaOptions, ListReplicasResponse,
            Replica, ReplicaSpaceUsage, ShareReplicaRequest, UnshareReplicaRequest,
        };
    }

    /// V1 Nexus autogenerated grpc code.
    pub mod nexus {
        pub use super::pb::{
            nexus_rpc_client, AddChildNexusRequest, AddChildNexusResponse, Child, ChildAction,
            ChildOperationRequest, ChildState, ChildStateReason, CreateNexusRequest,
            CreateNexusResponse, DestroyNexusRequest, FaultNexusChildRequest, ListNexusOptions,
            ListNexusResponse, ListRebuildHistoryRequest, ListRebuildHistoryResponse, Nexus,
            NexusNvmePreemption, NexusState, NvmeAnaState, NvmeReservation, PublishNexusRequest,
            PublishNexusResponse, RebuildHistoryRecord, RebuildHistoryRequest,
            RebuildHistoryResponse, RebuildJobState, RemoveChildNexusRequest,
            RemoveChildNexusResponse, ShutdownNexusRequest, UnpublishNexusRequest,
            UnpublishNexusResponse,
        };
    }

    pub mod snapshot {
        pub use super::pb::{
            destroy_snapshot_request, list_snapshots_request, snapshot_rpc_client,
            CreateReplicaSnapshotRequest, CreateReplicaSnapshotResponse,
            CreateSnapshotCloneRequest, DestroySnapshotRequest, ListSnapshotCloneRequest,
            ListSnapshotCloneResponse, ListSnapshotsRequest, ListSnapshotsResponse, Nexus,
            NexusCreateSnapshotReplicaDescriptor, NexusCreateSnapshotReplicaStatus,
            NexusCreateSnapshotRequest, NexusCreateSnapshotResponse, SnapshotInfo,
        };
    }

    /// V1 Pool autogenerated grpc code.
    pub mod pool {
        pub use super::pb::{
            pool_rpc_client, CreatePoolRequest, DestroyPoolRequest, ImportPoolRequest,
            ListPoolOptions, ListPoolsResponse, Pool, PoolType,
        };
    }

    /// V1 JsonRpc autogenerated grpc code.
    pub mod json {
        pub use super::pb::{
            json_rpc_client, json_rpc_client::JsonRpcClient, JsonRpcRequest, JsonRpcResponse,
        };
    }
}

/// V1 Alpha api version.
pub mod v1_alpha {
    /// V1 alpha registration autogenerated grpc code.
    pub mod registration {
        #![allow(clippy::derive_partial_eq_without_eq)]
        include!(concat!(env!("OUT_DIR"), "/v1.registration.rs"));
    }
}
