use crate::controller::{
    io_engine::NexusChildApi,
    registry::Registry,
    resources::{
        operations::ResourceOffspring,
        operations_helper::{GuardedOperationsHelper, OperationSequenceGuard},
        OperationGuardArc,
    },
};
use agents::errors::SvcError;
use tracing::info;

use crate::controller::resources::operations::{ResourceOwnerUpdate, ResourceSharing};
use stor_port::types::v0::{
    store::{
        nexus::{NexusOperation, NexusSpec, ReplicaUri},
        nexus_child::NexusChild,
        volume::VolumeSpec,
    },
    transport::{
        AddNexusReplica, Child, ChildUri, Nexus, NodeId, RemoveNexusChild, RemoveNexusReplica,
        Replica, ReplicaOwners, ShareReplica, UnshareReplica, VolumeId,
    },
};

impl OperationGuardArc<NexusSpec> {
    /// Attach the specified replica to the volume nexus
    /// The replica might need to be shared/unshared so it can be opened by the nexus
    pub(crate) async fn attach_replica(
        &mut self,
        registry: &Registry,
        replica: &Replica,
    ) -> Result<(), SvcError> {
        // Adding a replica to a nexus will initiate a rebuild.
        // First check that we are able to start a rebuild.
        registry.rebuild_allowed().await?;
        let uri = self
            .make_me_replica_accessible(registry, replica, None)
            .await?;
        let request = AddNexusReplica {
            node: self.as_ref().node.clone(),
            nexus: self.as_ref().uuid.clone(),
            replica: ReplicaUri::new(&replica.uuid, &uri),
            auto_rebuild: true,
        };
        self.add_replica(registry, &request).await?;
        Ok(())
    }

    /// Remove the given replica from the nexus.
    pub(crate) async fn remove_replica(
        &mut self,
        registry: &Registry,
        replica: &ReplicaUri,
    ) -> Result<(), SvcError> {
        let request = RemoveNexusReplica::new(&self.as_ref().node, self.uuid(), replica);
        let node = registry.node_wrapper(&request.node).await?;

        let status = registry.nexus(&request.nexus).await?;
        let spec_clone = self
            .start_update(
                registry,
                &status,
                NexusOperation::RemoveChild(NexusChild::from(&request.replica)),
            )
            .await?;

        let result = node.remove_child(&RemoveNexusChild::from(&request)).await;
        self.on_remove_disown_replica(registry, &request, &result)
            .await;

        self.complete_update(registry, result, spec_clone).await
    }

    /// Remove a nexus child uri and disown it from the nexus.
    /// If the volume is present, also attempts to remove the replica from the volume, if no
    /// longer required.
    pub(crate) async fn remove_vol_child_by_uri(
        &mut self,
        volume: &mut Option<&mut OperationGuardArc<VolumeSpec>>,
        registry: &Registry,
        nexus: &Nexus,
        uri: &ChildUri,
    ) -> Result<(), SvcError> {
        let nexus_children = &self.as_ref().children;
        match nexus_children.iter().find(|c| &c.uri() == uri).cloned() {
            Some(NexusChild::Replica(replica)) => {
                self.remove_replica(registry, &replica).await?;
                if let Some(volume) = volume {
                    volume
                        .remove_unused_volume_replica(registry, replica.uuid())
                        .await
                        .ok();
                }
                Ok(())
            }
            Some(NexusChild::Uri(uri)) => {
                let request = RemoveNexusChild::new(&nexus.node, &nexus.uuid, &uri);
                self.remove_child(registry, &request).await
            }
            None => {
                let request = RemoveNexusChild::new(&nexus.node, &nexus.uuid, uri);
                self.remove_child(registry, &request).await
            }
        }
    }

    /// Remove a nexus child uri and disown it from the nexus.
    pub(crate) async fn remove_child_by_uri(
        &mut self,
        registry: &Registry,
        nexus: &Nexus,
        uri: &ChildUri,
    ) -> Result<(), SvcError> {
        self.remove_vol_child_by_uri(&mut None, registry, nexus, uri)
            .await
    }

    /// Make the replica accessible on the specified `NodeId`.
    /// This means the replica might have to be shared/unshared so it can be open through
    /// the correct protocol (loopback locally, and nvmf remotely).
    pub(crate) async fn make_me_replica_accessible(
        &self,
        registry: &Registry,
        replica_state: &Replica,
        vol_id: Option<VolumeId>,
    ) -> Result<ChildUri, SvcError> {
        Self::make_replica_accessible(registry, replica_state, &self.as_ref().node, vol_id).await
    }

    /// Make the replica accessible on the specified `NodeId`.
    /// This means the replica might have to be shared/unshared so it can be open through
    /// the correct protocol (loopback locally, and nvmf remotely).
    pub(crate) async fn make_replica_accessible(
        registry: &Registry,
        replica_state: &Replica,
        nexus_node: &NodeId,
        vol_id: Option<VolumeId>,
    ) -> Result<ChildUri, SvcError> {
        if nexus_node == &replica_state.node {
            // on the same node, so connect via the loopback bdev
            let mut replica = registry.specs().replica(&replica_state.uuid).await?;
            let request: UnshareReplica =
                UnshareReplica::from(replica_state).with_volume_id(vol_id.clone());
            info!("unshare request is {:?}", request);
            info!("Sharing {:?} of {:?} over bdev", replica_state.uuid, vol_id);
            match replica.unshare(registry, &request).await {
                Ok(uri) => Ok(uri.into()),
                Err(SvcError::NotShared { .. }) => {
                    info!("Not shared error arm");
                    Ok(replica_state.uri.clone().into())
                }
                Err(error) => Err(error),
            }
        } else {
            // on a different node, so connect via an nvmf target
            let mut replica = registry.specs().replica(&replica_state.uuid).await?;
            let allowed_hosts = registry.node_nqn(nexus_node).await?;
            let request = ShareReplica::from(replica_state)
                .with_hosts(allowed_hosts)
                .with_vol_id(vol_id.clone());
            info!("Sharing {:?} of {:?} over nvmf", replica_state.uuid, vol_id);
            match replica.share(registry, &request).await {
                Ok(uri) => Ok(ChildUri::from(uri)),
                Err(SvcError::AlreadyShared { .. }) => Ok(replica_state.uri.clone().into()),
                Err(error) => Err(error),
            }
        }
    }

    /// Add the given request replica to the nexus.
    pub(super) async fn add_replica(
        &mut self,
        registry: &Registry,
        request: &AddNexusReplica,
    ) -> Result<Child, SvcError> {
        let node = registry.node_wrapper(&request.node).await?;
        let replica = registry.specs().replica(request.replica.uuid()).await?;
        // we don't persist nexus owners to pstor anymore, instead we rebuild at startup
        replica.lock().owners.add_owner(&request.nexus);

        let status = registry.nexus(&request.nexus).await?;
        let spec_clone = self
            .start_update(
                registry,
                &status,
                NexusOperation::AddChild(NexusChild::from(&request.replica)),
            )
            .await?;

        let result = node.add_child(&request.into()).await;
        self.complete_update(registry, result, spec_clone).await
    }

    /// On successful removal of replica from nexus, disown the replica from the nexus.
    pub(super) async fn on_remove_disown_replica(
        &self,
        registry: &Registry,
        request: &RemoveNexusReplica,
        result: &Result<(), SvcError>,
    ) {
        if result.is_ok() {
            if let Some(replica) = registry.specs().replica_rsc(request.replica.uuid()) {
                if let Ok(mut replica) = replica.operation_guard() {
                    let disowner = ReplicaOwners::new(None, vec![request.nexus.clone()]);
                    let _ = replica.remove_owners(registry, &disowner, true).await;
                }
            }
        }
    }
}
