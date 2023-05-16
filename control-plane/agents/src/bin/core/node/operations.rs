/// in here node register operation
use agents::errors::SvcError;

use stor_port::types::v0::store::node::{NodeLabels, NodeOperation, NodeSpec};

use crate::controller::{
    registry::Registry,
    resources::{
        operations::{ResourceCordon, ResourceDrain, ResourceLifecycle},
        operations_helper::{GuardedOperationsHelper, OnCreateFail},
        OperationGuardArc,
    },
};

use stor_port::types::v0::transport::Register;

#[async_trait::async_trait]
impl ResourceLifecycle for OperationGuardArc<NodeSpec> {
    type Create = Register;
    type CreateOutput = Self;
    type Destroy = ();

    async fn create(
        registry: &Registry,
        request: &Register,
    ) -> Result<Self::CreateOutput, SvcError> {
        // create a guarded spec

        // this must return an OperationGuard object, and call start_create() etc here

        let _node2 = {
            let mut specs = registry.specs().write();
            match specs.nodes.get(&request.id) {
                Some(node_spec) => {
                    let mut node_spec = node_spec.lock();
                    node_spec.set_endpoint(request.grpc_endpoint);
                    node_spec.set_nqn(request.node_nqn.clone());
                    node_spec.clone()
                }
                None => {
                    let node = NodeSpec::new(
                        request.id.clone(),
                        request.grpc_endpoint,
                        NodeLabels::new(),
                        None,
                        request.node_nqn.clone(),
                    );
                    specs.nodes.insert(node.clone());
                    node
                }
            }
        };
        let guarded_node = registry.specs().guarded_node(&request.id).await?;

        let result = guarded_node.start_create(registry, request).await;

        let _node_state = guarded_node
            .complete_create(result, registry, OnCreateFail::SetDeleting)
            .await?;

        Ok(guarded_node)
    }

    async fn destroy(
        &mut self,
        _registry: &Registry,
        _request: &Self::Destroy,
    ) -> Result<(), SvcError> {
        unimplemented!()
    }
}

/// Resource Cordon Operations.
#[async_trait::async_trait]
impl ResourceCordon for OperationGuardArc<NodeSpec> {
    type CordonOutput = NodeSpec;
    type UncordonOutput = NodeSpec;

    async fn cordon(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::CordonOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &cloned_node_spec, NodeOperation::Cordon(label))
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await?;
        registry.specs().node(self.as_ref().id())
    }

    async fn uncordon(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::UncordonOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &cloned_node_spec, NodeOperation::Uncordon(label))
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await?;
        registry.specs().node(self.as_ref().id())
    }
}

/// Resource Cordon Operations.
#[async_trait::async_trait]
impl ResourceDrain for OperationGuardArc<NodeSpec> {
    type DrainOutput = NodeSpec;

    async fn drain(
        &mut self,
        registry: &Registry,
        label: String,
    ) -> Result<Self::DrainOutput, SvcError> {
        let cloned_node_spec = self.lock().clone();
        let spec_clone = self
            .start_update(registry, &cloned_node_spec, NodeOperation::Drain(label))
            .await?;

        self.complete_update(registry, Ok(self.as_ref().clone()), spec_clone)
            .await?;
        registry.specs().node(self.as_ref().id())
    }
}
