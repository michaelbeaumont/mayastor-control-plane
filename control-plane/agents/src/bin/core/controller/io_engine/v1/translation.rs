use crate::controller::io_engine::translation::{IoEngineToAgent, TryIoEngineToAgent};
use agents::errors::SvcError;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        openapi::apis::IntoVec,
        transport::{
            self, ChildState, ChildStateReason, Nexus, NexusId, NexusNvmePreemption,
            NexusNvmfConfig, NexusStatus, NodeId, NvmeReservation, PoolState, PoolUuid, Protocol,
            Replica, ReplicaId, ReplicaName, ReplicaStatus,
        },
    },
};

use rpc::v1;
use std::convert::TryFrom;

/// Trait for converting agent messages to io-engine messages.
pub(super) trait AgentToIoEngine {
    /// RpcIoEngine message type.
    type IoEngineMessage;
    /// Conversion of agent message to io-engine message.
    fn to_rpc(&self) -> Self::IoEngineMessage;
}

impl IoEngineToAgent for v1::host::Partition {
    type AgentMessage = transport::Partition;
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            parent: self.parent.clone(),
            number: self.number,
            name: self.name.clone(),
            scheme: self.scheme.clone(),
            typeid: self.typeid.clone(),
            uuid: self.uuid.clone(),
        }
    }
}

impl IoEngineToAgent for v1::host::Filesystem {
    type AgentMessage = transport::Filesystem;
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            fstype: self.fstype.clone(),
            label: self.label.clone(),
            uuid: self.uuid.clone(),
            mountpoint: self.mountpoints.get(0).cloned().unwrap_or_default(),
        }
    }
}

impl IoEngineToAgent for v1::host::BlockDevice {
    type AgentMessage = transport::BlockDevice;
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            devname: self.devname.clone(),
            devtype: self.devtype.clone(),
            devmajor: self.devmajor,
            devminor: self.devminor,
            model: self.model.clone(),
            devpath: self.devpath.clone(),
            devlinks: self.devlinks.clone(),
            size: self.size,
            partition: match &self.partition {
                Some(partition) => partition.to_agent(),
                None => transport::Partition {
                    ..Default::default()
                },
            },
            filesystem: match &self.filesystem {
                Some(filesystem) => filesystem.to_agent(),
                None => transport::Filesystem {
                    ..Default::default()
                },
            },
            available: self.available,
        }
    }
}

impl TryIoEngineToAgent for v1::replica::Replica {
    type AgentMessage = transport::Replica;
    fn try_to_agent(&self) -> Result<Self::AgentMessage, SvcError> {
        Ok(transport::Replica {
            node: Default::default(),
            name: self.name.clone().into(),
            uuid: ReplicaId::try_from(self.uuid.as_str()).map_err(|_| SvcError::InvalidUuid {
                uuid: self.uuid.to_owned(),
                kind: ResourceKind::Replica,
            })?,
            pool_id: self.poolname.clone().into(),
            pool_uuid: Some(PoolUuid::try_from(self.pooluuid.clone()).map_err(|_| {
                SvcError::InvalidUuid {
                    uuid: self.pooluuid.to_owned(),
                    kind: ResourceKind::Replica,
                }
            })?),
            thin: self.thin,
            size: self.size,
            space: self.usage.as_ref().map(IoEngineToAgent::to_agent),
            share: self.share.into(),
            uri: self.uri.clone(),
            status: ReplicaStatus::Online,
            allowed_hosts: self
                .allowed_hosts
                .clone()
                .into_iter()
                .map(|nqn| {
                    // should we allow for invalid here since it comes directly from the dataplane?
                    transport::HostNqn::try_from(&nqn)
                        .unwrap_or(transport::HostNqn::Invalid { nqn })
                })
                .collect(),
        })
    }
}
impl IoEngineToAgent for v1::replica::ReplicaSpaceUsage {
    type AgentMessage = transport::ReplicaSpaceUsage;
    fn to_agent(&self) -> Self::AgentMessage {
        transport::ReplicaSpaceUsage {
            capacity_bytes: self.capacity_bytes,
            allocated_bytes: self.allocated_bytes,
            cluster_size: self.cluster_size,
            clusters: self.num_clusters,
            allocated_clusters: self.num_allocated_clusters,
        }
    }
}

impl AgentToIoEngine for transport::CreateReplica {
    type IoEngineMessage = v1::replica::CreateReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            uuid: self.uuid.clone().into(),
            pooluuid: match self.pool_uuid.clone() {
                Some(uuid) => uuid.into(),
                // TODO: implement a getter function to fetch the uuid of the pool from the given
                //       name
                None => self.pool_id.clone().into(),
            },
            thin: self.thin,
            size: self.size,
            share: self.share as i32,
            allowed_hosts: self.allowed_hosts.clone().into_vec(),
        }
    }
}

impl AgentToIoEngine for transport::ShareReplica {
    type IoEngineMessage = v1::replica::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            share: self.protocol as i32,
            allowed_hosts: self.allowed_hosts.clone().into_vec(),
        }
    }
}

impl AgentToIoEngine for transport::UnshareReplica {
    type IoEngineMessage = v1::replica::UnshareReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
        }
    }
}

impl AgentToIoEngine for transport::DestroyReplica {
    type IoEngineMessage = v1::replica::DestroyReplicaRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        let pool_ref = match &self.pool_uuid {
            Some(uuid) => v1::replica::destroy_replica_request::Pool::PoolUuid(uuid.to_string()),
            None => v1::replica::destroy_replica_request::Pool::PoolName(self.pool_id.to_string()),
        };
        v1::replica::DestroyReplicaRequest {
            uuid: ReplicaName::from_opt_uuid(self.name.as_ref(), &self.uuid).into(),
            pool: Some(pool_ref),
        }
    }
}

/// Convert rpc replica to an agent replica.
pub(super) fn rpc_replica_to_agent(
    rpc_replica: &v1::replica::Replica,
    id: &NodeId,
) -> Result<Replica, SvcError> {
    let mut replica = rpc_replica.try_to_agent()?;
    replica.node = id.clone();
    Ok(replica)
}

impl TryIoEngineToAgent for v1::nexus::Nexus {
    type AgentMessage = transport::Nexus;

    fn try_to_agent(&self) -> Result<Self::AgentMessage, SvcError> {
        Ok(Self::AgentMessage {
            node: Default::default(),
            name: self.name.clone(),
            uuid: NexusId::try_from(self.uuid.as_str()).map_err(|_| SvcError::InvalidUuid {
                uuid: self.uuid.to_owned(),
                kind: ResourceKind::Nexus,
            })?,
            size: self.size,
            status: NexusStatus::from(self.state),
            children: self.children.iter().map(|c| c.to_agent()).collect(),
            device_uri: self.device_uri.clone(),
            rebuilds: self.rebuilds,
            // todo: do we need an "other" Protocol variant in case we don't recognise it?
            share: Protocol::try_from(self.device_uri.as_str()).unwrap_or(Protocol::None),
            allowed_hosts: self
                .allowed_hosts
                .iter()
                .map(|n| {
                    // should we allow for invalid here since it comes directly from the dataplane?
                    transport::HostNqn::try_from(n)
                        .unwrap_or(transport::HostNqn::Invalid { nqn: n.to_string() })
                })
                .collect(),
        })
    }
}

/// New-type wrapper for external types.
/// Allows us to convert from external types which would otherwise not be allowed.
struct ExternalType<T>(T);
impl From<ExternalType<v1::nexus::ChildState>> for ChildState {
    fn from(src: ExternalType<v1::nexus::ChildState>) -> Self {
        match src.0 {
            v1::nexus::ChildState::Unknown => ChildState::Unknown,
            v1::nexus::ChildState::Online => ChildState::Online,
            v1::nexus::ChildState::Degraded => ChildState::Degraded,
            v1::nexus::ChildState::Faulted => ChildState::Faulted,
        }
    }
}
impl From<ExternalType<v1::nexus::ChildStateReason>> for ChildStateReason {
    fn from(src: ExternalType<v1::nexus::ChildStateReason>) -> Self {
        match src.0 {
            v1::nexus::ChildStateReason::None => Self::Unknown,
            v1::nexus::ChildStateReason::Init => Self::Init,
            v1::nexus::ChildStateReason::Closed => Self::Closed,
            v1::nexus::ChildStateReason::CannotOpen => Self::CantOpen,
            v1::nexus::ChildStateReason::ConfigInvalid => Self::ConfigInvalid,
            v1::nexus::ChildStateReason::RebuildFailed => Self::RebuildFailed,
            v1::nexus::ChildStateReason::IoFailure => Self::IoError,
            v1::nexus::ChildStateReason::ByClient => Self::ByClient,
            v1::nexus::ChildStateReason::OutOfSync => Self::OutOfSync,
            v1::nexus::ChildStateReason::NoSpace => Self::NoSpace,
            v1::nexus::ChildStateReason::TimedOut => Self::TimedOut,
            v1::nexus::ChildStateReason::AdminFailed => Self::AdminCommandFailed,
        }
    }
}

impl IoEngineToAgent for v1::nexus::Child {
    type AgentMessage = transport::Child;

    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            uri: self.uri.clone().into(),
            state: ChildState::from(ExternalType(
                v1::nexus::ChildState::from_i32(self.state)
                    .unwrap_or(v1::nexus::ChildState::Unknown),
            )),
            rebuild_progress: u8::try_from(self.rebuild_progress).ok(),
            state_reason: v1::nexus::ChildStateReason::from_i32(self.state_reason)
                .map(|f| From::from(ExternalType(f)))
                .unwrap_or(ChildStateReason::Unknown),
            faulted_at: self
                .fault_timestamp
                .clone()
                .and_then(|t| std::time::SystemTime::try_from(t).ok()),
        }
    }
}

impl AgentToIoEngine for transport::CreateNexus {
    type IoEngineMessage = v1::nexus::CreateNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        let nexus_config = self
            .config
            .clone()
            .unwrap_or_else(|| NexusNvmfConfig::default().with_no_resv());
        Self::IoEngineMessage {
            name: self.name(),
            uuid: self.uuid.clone().into(),
            size: self.size,
            min_cntl_id: nexus_config.min_cntl_id() as u32,
            max_cntl_id: nexus_config.max_cntl_id() as u32,
            resv_key: nexus_config.resv_key(),
            preempt_key: nexus_config.preempt_key(),
            children: self.children.clone().into_vec(),
            nexus_info_key: self.nexus_info_key(),
            resv_type: Some(
                v1::nexus::NvmeReservation::from(ExternalType(nexus_config.resv_type())) as i32,
            ),
            preempt_policy: v1::nexus::NexusNvmePreemption::from(ExternalType(
                nexus_config.preempt_policy(),
            )) as i32,
        }
    }
}

impl AgentToIoEngine for transport::DestroyNexus {
    type IoEngineMessage = v1::nexus::DestroyNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::ShareNexus {
    type IoEngineMessage = v1::nexus::PublishNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
            key: self.key.clone().unwrap_or_default(),
            share: self.protocol as i32,
            allowed_hosts: self.allowed_hosts.clone().into_vec(),
        }
    }
}

impl AgentToIoEngine for transport::UnshareNexus {
    type IoEngineMessage = v1::nexus::UnpublishNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::AddNexusChild {
    type IoEngineMessage = v1::nexus::AddChildNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
            norebuild: !self.auto_rebuild,
        }
    }
}

impl AgentToIoEngine for transport::RemoveNexusChild {
    type IoEngineMessage = v1::nexus::RemoveChildNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::FaultNexusChild {
    type IoEngineMessage = v1::nexus::FaultNexusChildRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
        }
    }
}

impl AgentToIoEngine for transport::NexusChildAction {
    type IoEngineMessage = v1::nexus::ChildOperationRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        v1::nexus::ChildOperationRequest {
            nexus_uuid: self.nexus().to_string(),
            uri: self.uri().to_string(),
            action: self.action().to_rpc().into(),
        }
    }
}
impl AgentToIoEngine for transport::NexusChildActionKind {
    type IoEngineMessage = v1::nexus::ChildAction;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        match self {
            transport::NexusChildActionKind::Offline => v1::nexus::ChildAction::Offline,
            transport::NexusChildActionKind::Online => v1::nexus::ChildAction::Online,
            transport::NexusChildActionKind::Retire => v1::nexus::ChildAction::Retire,
        }
    }
}

impl AgentToIoEngine for transport::ShutdownNexus {
    type IoEngineMessage = v1::nexus::ShutdownNexusRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            uuid: self.uuid().into(),
        }
    }
}

/// convert rpc nexus to a agent nexus
pub(super) fn rpc_nexus_to_agent(
    rpc_nexus: &v1::nexus::Nexus,
    id: &NodeId,
) -> Result<Nexus, SvcError> {
    let mut nexus = rpc_nexus.try_to_agent()?;
    nexus.node = id.clone();
    Ok(nexus)
}

impl IoEngineToAgent for v1::pool::Pool {
    type AgentMessage = transport::PoolState;
    /// This converts gRPC pool object into Control plane Pool state.
    fn to_agent(&self) -> Self::AgentMessage {
        Self::AgentMessage {
            node: Default::default(),
            id: self.name.clone().into(),
            disks: self.disks.clone().into_vec(),
            capacity: self.capacity,
            used: self.used,
            status: self.state.into(),
        }
    }
}

impl AgentToIoEngine for transport::CreatePool {
    type IoEngineMessage = v1::pool::CreatePoolRequest;
    /// This converts Control plane CreatePool struct to IO Engine gRPC message.
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: self.id.clone().into(),
            disks: self.disks.iter().map(|d| d.to_string()).collect(),
            uuid: None,
            pooltype: v1::pool::PoolType::Lvs as i32,
        }
    }
}

impl AgentToIoEngine for transport::DestroyPool {
    type IoEngineMessage = v1::pool::DestroyPoolRequest;
    /// This converts Control plane DeletePool struct to IO Engine gRPC message.
    fn to_rpc(&self) -> Self::IoEngineMessage {
        Self::IoEngineMessage {
            name: self.id.clone().into(),
            uuid: None,
        }
    }
}

impl AgentToIoEngine for transport::ImportPool {
    type IoEngineMessage = v1::pool::ImportPoolRequest;
    fn to_rpc(&self) -> Self::IoEngineMessage {
        v1::pool::ImportPoolRequest {
            name: self.id.clone().into(),
            uuid: None,
            disks: self.disks.clone().into_vec(),
            pooltype: v1::pool::PoolType::Lvs as i32,
        }
    }
}

/// Converts rpc pool to an agent pool.
pub(super) fn rpc_pool_to_agent(rpc_pool: &rpc::v1::pool::Pool, id: &NodeId) -> PoolState {
    let mut pool = rpc_pool.to_agent();
    pool.node = id.clone();
    pool
}

impl From<ExternalType<NvmeReservation>> for v1::nexus::NvmeReservation {
    fn from(value: ExternalType<NvmeReservation>) -> Self {
        match value.0 {
            NvmeReservation::Reserved => Self::Reserved,
            NvmeReservation::WriteExclusive => Self::WriteExclusive,
            NvmeReservation::ExclusiveAccess => Self::ExclusiveAccess,
            NvmeReservation::WriteExclusiveRegsOnly => Self::WriteExclusiveRegsOnly,
            NvmeReservation::ExclusiveAccessRegsOnly => Self::ExclusiveAccessRegsOnly,
            NvmeReservation::WriteExclusiveAllRegs => Self::WriteExclusiveAllRegs,
            NvmeReservation::ExclusiveAccessAllRegs => Self::ExclusiveAccessAllRegs,
        }
    }
}
impl From<ExternalType<NexusNvmePreemption>> for v1::nexus::NexusNvmePreemption {
    fn from(value: ExternalType<NexusNvmePreemption>) -> Self {
        match value.0 {
            NexusNvmePreemption::ArgKey(_) => Self::ArgKey,
            NexusNvmePreemption::Holder => Self::Holder,
        }
    }
}
