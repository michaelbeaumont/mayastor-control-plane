use crate::controller::{
    registry::Registry,
    resources::{
        operations_helper::{OperationSequenceGuard, ResourceSpecsLocked},
        OperationGuardArc, ResourceMutex,
    },
};
use agents::errors::{NodeNotFound, SvcError};
use snafu::OptionExt;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::{
        store::{
            node::{NodeLabels, NodeOperation, NodeSpec},
            SpecStatus, SpecTransaction,
        },
        transport::{NodeId, Register, VolumeId},
    },
};

use crate::controller::resources::operations_helper::{
    GuardedOperationsHelper, SpecOperationsHelper,
};

use std::{collections::HashSet, time::SystemTime};

impl ResourceSpecsLocked {
    /// Create a node spec for the register request
    pub(crate) async fn register_node(
        &self,
        registry: &Registry,
        node: &Register,
    ) -> Result<NodeSpec, SvcError> {
        let (changed, node) = {
            let mut specs = self.write();
            match specs.nodes.get(&node.id) {
                Some(node_spec) => {
                    let mut node_spec = node_spec.lock();
                    let changed = node_spec.endpoint() != node.grpc_endpoint
                        || node_spec.node_nqn() != &node.node_nqn;

                    node_spec.set_endpoint(node.grpc_endpoint);
                    node_spec.set_nqn(node.node_nqn.clone());
                    (changed, node_spec.clone())
                }
                None => {
                    let node = NodeSpec::new(
                        node.id.clone(),
                        node.grpc_endpoint,
                        NodeLabels::new(),
                        None,
                        node.node_nqn.clone(),
                    );
                    specs.nodes.insert(node.clone());
                    (true, node)
                }
            }
        };
        if changed {
            registry.store_obj(&node).await?;
        }
        Ok(node)
    }

    /// Get node spec by its `NodeId`
    pub(crate) fn node_rsc(&self, node_id: &NodeId) -> Result<ResourceMutex<NodeSpec>, SvcError> {
        self.read()
            .nodes
            .get(node_id)
            .cloned()
            .context(NodeNotFound {
                node_id: node_id.to_owned(),
            })
    }

    /// Get cloned node spec by its `NodeId`
    pub(crate) fn node(&self, node_id: &NodeId) -> Result<NodeSpec, SvcError> {
        self.node_rsc(node_id).map(|n| n.lock().clone())
    }

    /// Get guarded cloned node spec by its `NodeId`
    pub(crate) async fn guarded_node(
        &self,
        node_id: &NodeId,
    ) -> Result<OperationGuardArc<NodeSpec>, SvcError> {
        let node = self.node_rsc(node_id)?;
        let guarded_node = node.operation_guard_wait().await?;
        Ok(guarded_node)
    }

    /// Get all locked node specs
    pub(crate) fn nodes_rsc(&self) -> Vec<ResourceMutex<NodeSpec>> {
        self.read().nodes.to_vec()
    }

    /// Get all node specs cloned
    pub(crate) fn nodes(&self) -> Vec<NodeSpec> {
        self.nodes_rsc()
            .into_iter()
            .map(|n| n.lock().clone())
            .collect()
    }

    /// Get all cordoned nodes.
    pub(crate) fn cordoned_nodes(&self) -> Vec<NodeSpec> {
        self.read()
            .nodes
            .to_vec()
            .into_iter()
            .filter_map(|node_spec| {
                if node_spec.lock().cordoned() {
                    Some(node_spec.lock().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Set the NodeSpec to the drained state.
    pub(crate) async fn set_node_drained(
        &self,
        registry: &Registry,
        node_id: &NodeId,
    ) -> Result<NodeSpec, SvcError> {
        let node = self.node_rsc(node_id)?;
        let guarded_node = node.operation_guard_wait().await?;
        let drained_node_spec = {
            let mut locked_node = guarded_node.lock();
            locked_node.set_drained();
            locked_node.clone()
        };
        registry.store_obj(&drained_node_spec).await?;
        Ok(drained_node_spec)
    }

    /// Add the draining volume to the node spec for checking shutdown nexuses.
    pub(crate) async fn add_node_draining_volumes(
        &self,
        registry: &Registry,
        node_id: &NodeId,
        draining_volumes: HashSet<VolumeId>,
    ) -> Result<NodeSpec, SvcError> {
        let node = self.node_rsc(node_id)?;
        let guarded_node = node.operation_guard_wait().await?;
        let drained_node_spec = {
            let mut locked_node = guarded_node.lock();
            locked_node.add_draining_volumes(draining_volumes);
            locked_node.clone()
        };
        registry.store_obj(&drained_node_spec).await?;
        Ok(drained_node_spec)
    }

    /// Get the draining volumes on this node.
    pub(crate) async fn node_draining_volumes(
        &self,
        node_id: &NodeId,
    ) -> Result<HashSet<VolumeId>, SvcError> {
        let node = self.node_rsc(node_id)?;
        let locked_node = node.lock();
        Ok(locked_node.draining_volumes())
    }

    /// Get the number of draining volumes on this node.
    pub(crate) async fn node_draining_volume_count(
        &self,
        node_id: &NodeId,
    ) -> Result<usize, SvcError> {
        let node = self.node_rsc(node_id)?;
        let locked_node = node.lock();
        Ok(locked_node.draining_volume_count())
    }

    /// Remove the given volume from this node.
    pub(crate) async fn remove_node_draining_volumes(
        &self,
        registry: &Registry,
        node_id: &NodeId,
        draining_volumes: HashSet<VolumeId>,
    ) -> Result<NodeSpec, SvcError> {
        let node = self.node_rsc(node_id)?;
        let guarded_node = node.operation_guard_wait().await?;
        let drained_node_spec = {
            let mut locked_node = guarded_node.lock();
            locked_node.remove_draining_volumes(draining_volumes);
            locked_node.clone()
        };
        registry.store_obj(&drained_node_spec).await?;
        Ok(drained_node_spec)
    }
    /// Remove all volumes from this node.
    pub(crate) async fn remove_all_node_draining_volumes(
        &self,
        registry: &Registry,
        node_id: &NodeId,
    ) -> Result<NodeSpec, SvcError> {
        let node = self.node_rsc(node_id)?;
        let guarded_node = node.operation_guard_wait().await?;
        let drained_node_spec = {
            let mut locked_node = guarded_node.lock();
            locked_node.remove_all_draining_volumes();
            locked_node.clone()
        };
        registry.store_obj(&drained_node_spec).await?;
        Ok(drained_node_spec)
    }

    /// Set the draining timestamp on this node.
    pub(crate) async fn set_draining_timestamp_if_none(
        &self,
        node_id: &NodeId,
    ) -> Result<(), SvcError> {
        let node = self.node_rsc(node_id)?;
        let guarded_node = node.operation_guard_wait().await?;
        let mut locked_node = guarded_node.lock();
        locked_node.set_draining_timestamp_if_none();
        Ok(())
    }

    /// Get the draining timestamp on this node.
    pub(crate) async fn node_draining_timestamp(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<SystemTime>, SvcError> {
        let node = self.node_rsc(node_id)?;
        let locked_node = node.lock();
        Ok(locked_node.draining_timestamp())
    }
}

#[async_trait::async_trait]
impl GuardedOperationsHelper for OperationGuardArc<NodeSpec> {
    type Create = Register;
    type Owners = ();
    type Status = ();
    type State = NodeSpec; // must equal Inner
    type UpdateOp = NodeOperation;
    type Inner = NodeSpec;

    fn remove_spec(&self, _registry: &Registry) {
        //let uuid = self.lock().uuid.clone();
        //registry.specs().remove_volume(&uuid);
        //let ag_info = self.lock().affinity_group.clone();
        //if let Some(ag) = ag_info {
        //    registry.specs().remove_affinity_group(&uuid, ag.id())
        //}
    }
}

#[async_trait::async_trait]
impl SpecOperationsHelper for NodeSpec {
    type Create = Register;
    type Owners = ();
    type Status = ();
    type State = NodeSpec; // used to compare current state with requested operation
    type UpdateOp = NodeOperation;

    async fn start_update_op(
        &mut self,
        _: &Registry,
        _state: &Self::State,
        op: Self::UpdateOp,
    ) -> Result<(), SvcError> {
        match op.clone() {
            NodeOperation::Cordon(label) => {
                // Do not allow the same label to be applied more than once.
                if self.has_cordon_label(&label) {
                    Err(SvcError::CordonLabel {
                        node_id: self.id().to_string(),
                        label,
                    })
                } else {
                    self.start_op(op);
                    Ok(())
                }
            }
            NodeOperation::Uncordon(label) => {
                // Check that the label is present.
                if !self.has_cordon_label(&label) {
                    Err(SvcError::CordonLabel {
                        node_id: self.id().to_string(),
                        label,
                    })
                } else {
                    self.start_op(op);
                    Ok(())
                }
            }
            NodeOperation::Drain(label) => {
                // Do not allow the same label to be applied more than once.
                if self.has_cordon_label(&label) {
                    Err(SvcError::CordonLabel {
                        node_id: self.id().to_string(),
                        label,
                    })
                } else {
                    self.start_op(op);
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
    fn start_create_op(&mut self) {
        self.start_op(NodeOperation::Create);
    }
    fn start_destroy_op(&mut self) {
        self.start_op(NodeOperation::Destroy);
    }

    fn dirty(&self) -> bool {
        self.pending_op()
    }
    fn kind(&self) -> ResourceKind {
        ResourceKind::Node
    }
    fn uuid_str(&self) -> String {
        self.id().to_string()
    }
    fn status(&self) -> SpecStatus<Self::Status> {
        self.status.clone()
    }
    fn set_status(&mut self, status: SpecStatus<Self::Status>) {
        self.status = status
    }
    fn operation_result(&self) -> Option<Option<bool>> {
        self.operation.as_ref().map(|r| r.result)
    }
}
