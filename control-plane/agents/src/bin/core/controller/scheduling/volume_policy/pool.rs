use crate::controller::scheduling::{
    resources::{ChildItem, PoolItem},
    volume::{GetSuitablePoolsContext, ReplicaResizePoolsContext},
};
use std::collections::HashMap;
use stor_port::types::v0::transport::{PoolStatus, PoolTopology};

/// Filter pools used for replica creation.
pub(crate) struct PoolBaseFilters {}
impl PoolBaseFilters {
    /// The minimum free space in a pool for it to be eligible for thin provisioned replicas.
    fn free_space_watermark() -> u64 {
        16 * 1024 * 1024
    }
    /// Should only attempt to use pools with capacity bigger than the requested replica size.
    pub(crate) fn capacity(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        item.pool.capacity > request.size
    }
    /// Should only attempt to use pools with capacity bigger than the requested replica size.
    pub(crate) fn overcommit(
        request: &GetSuitablePoolsContext,
        item: &PoolItem,
        allowed_commit_percent: u64,
    ) -> bool {
        match request.as_thin() {
            true => request.overcommit(allowed_commit_percent, item.pool()),
            false => true,
        }
    }
    /// Should only attempt to use pools with capacity bigger than the requested size
    /// for replica expand.
    pub(crate) fn overcommit_repl_resize(
        request: &ReplicaResizePoolsContext,
        item: &ChildItem,
        allowed_commit_percent: u64,
    ) -> bool {
        match request.spec().as_thin() {
            true => request.overcommit(allowed_commit_percent, item.pool()),
            false => true,
        }
    }
    /// Should only attempt to use pools with sufficient free space.
    pub(crate) fn min_free_space(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        match request.as_thin() {
            true => item.pool.free_space() > Self::free_space_watermark(),
            false => item.pool.free_space() > request.size,
        }
    }
    /// Return true if the pool has enough capacity to resize the replica by the requested
    /// value.
    pub(crate) fn min_free_space_repl_resize(
        request: &ReplicaResizePoolsContext,
        item: &ChildItem,
    ) -> bool {
        match request.spec().as_thin() {
            true => item.pool().free_space() > Self::free_space_watermark(),
            false => item.pool().free_space() > request.required_capacity(),
        }
    }
    /// Should only attempt to use pools with sufficient free space for a full rebuild.
    /// Currently the data-plane fully rebuilds a volume, meaning a thin provisioned volume
    /// becomes fully allocated.
    pub(crate) fn min_free_space_full_rebuild(
        request: &GetSuitablePoolsContext,
        item: &PoolItem,
    ) -> bool {
        match request.as_thin() && request.config().is_none() {
            true => item.pool.free_space() > Self::free_space_watermark(),
            false => item.pool.free_space() > request.size,
        }
    }
    /// Should only attempt to use usable (not faulted) pools.
    pub(crate) fn usable(_: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        item.pool.status != PoolStatus::Faulted && item.pool.status != PoolStatus::Unknown
    }

    /// Should only attempt to use pools having specific creation label if topology has it.
    pub(crate) fn topology(request: &GetSuitablePoolsContext, item: &PoolItem) -> bool {
        let mut volume_pool_topology_inclusion_labels: HashMap<String, String>;
        let volume_pool_topology_exclusion_labels: HashMap<String, String>;
        match request.topology.clone() {
            None => return true,
            Some(topology) => match topology.pool {
                None => return true,
                Some(pool_topology) => match pool_topology {
                    PoolTopology::Labelled(labelled_topology) => {
                        // Return false if the exclusion and incluson labels has any common key.
                        if labelled_topology
                            .inclusion
                            .keys()
                            .any(|key| labelled_topology.exclusion.contains_key(key))
                        {
                            return false;
                        }

                        if !labelled_topology.inclusion.is_empty()
                            || !labelled_topology.exclusion.is_empty()
                        {
                            volume_pool_topology_inclusion_labels = labelled_topology.inclusion;
                            volume_pool_topology_exclusion_labels = labelled_topology.exclusion;
                        } else {
                            return true;
                        }
                    }
                },
            },
        };

        // We will reach this part of code only if the volume has inclusion/exclusion labels.
        match request.registry().specs().pool(&item.pool.id) {
            Ok(spec) => {
                match spec.labels {
                    None => false,
                    Some(label) => {
                        // The inclusion labes of volume must match the labels in pool
                        // for the specified pool to qualify for volume provisioning.
                        // i.e both the maps volume_pool_topology_inclusion_labels and label must be
                        // equal
                        let inclusion_match = label.iter().all(|(key, value)| {
                            volume_pool_topology_inclusion_labels.get(key) == Some(value)
                        }) && volume_pool_topology_inclusion_labels
                            .iter()
                            .all(|(key, value)| label.get(key) == Some(value));

                        // The exclusion labels value in volume should not match the pool
                        // labels value
                        let common_keys: Vec<_> = volume_pool_topology_exclusion_labels
                            .keys()
                            .filter(|&key| label.contains_key(key))
                            .collect();
                        let exclusion_match = common_keys.iter().any(|&key| {
                            volume_pool_topology_exclusion_labels.get(key) != label.get(key)
                        });
                        // removing the auto added key so that match can be done on user provided
                        // values
                        volume_pool_topology_inclusion_labels.remove("openebs.io/created-by");

                        match (
                            volume_pool_topology_inclusion_labels.is_empty(),
                            volume_pool_topology_exclusion_labels.is_empty(),
                        ) {
                            (false, false) => {
                                return inclusion_match && exclusion_match;
                            }
                            (false, true) => {
                                return inclusion_match;
                            }
                            (true, false) => {
                                return exclusion_match;
                            }
                            _ => {
                                return true;
                            }
                        }
                    }
                }
            }
            Err(_) => false,
        }
    }
}
