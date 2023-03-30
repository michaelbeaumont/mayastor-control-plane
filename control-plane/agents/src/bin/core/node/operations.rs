/// in here node register operation
use agents::errors::SvcError;

use stor_port::types::v0::store::node::{NodeOperation, NodeSpec};

use crate::controller::{
    registry::Registry,
    resources::{
        operations::{ResourceCordon, ResourceDrain},
        operations_helper::GuardedOperationsHelper,
        OperationGuardArc,
    },
};

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
