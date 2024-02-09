#![cfg(test)]

use deployer_cluster::ClusterBuilder;
use std::{collections::HashMap, time::Duration};
use stor_port::types::v0::openapi::{models, models::PublishVolumeBody};

struct DeviceDisconnect(nvmeadm::NvmeTarget);
impl Drop for DeviceDisconnect {
    fn drop(&mut self) {
        if self.0.disconnect().is_err() {
            std::process::Command::new("sudo")
                .args(["nvme", "disconnect-all"])
                .status()
                .unwrap();
        }
    }
}
#[tokio::test]
async fn fs_consistent_snapshot() {
    let cache_period = Duration::from_millis(250);
    let reconcile_period = Duration::from_millis(3000);
    let cluster = ClusterBuilder::builder()
        .with_rest(true)
        .with_io_engines(1)
        .with_pool(0, "malloc:///p1?size_mb=100")
        .with_csi(true, true)
        .with_csi_registration(true)
        .with_cache_period(&humantime::Duration::from(cache_period).to_string())
        .with_reconcile_period(reconcile_period, reconcile_period)
        .build()
        .await
        .unwrap();

    let api_client = cluster.rest_v00();
    let volumes_api = api_client.volumes_api();

    let volume_size = 80u64 * 1024 * 1024;
    let mut volume = volumes_api
        .put_volume(
            &"ec4e66fd-3b33-4439-b504-d49aba53da26".parse().unwrap(),
            models::CreateVolumeBody::new(models::VolumePolicy::new(true), 1, volume_size, true),
        )
        .await
        .unwrap();

    volume = volumes_api
        .put_volume_target(
            &volume.spec.uuid,
            PublishVolumeBody::new_all(
                HashMap::new(),
                None,
                cluster.node(0).to_string(),
                models::VolumeShareProtocol::Nvmf,
                None,
                cluster.csi_node(0),
            ),
        )
        .await
        .unwrap();
    let uri = volume.state.target.as_ref().unwrap().device_uri.as_str();
    //let _drop_target = DeviceDisconnect(nvmeadm::NvmeTarget::try_from(uri).unwrap());

    let mut node = cluster.csi_node_client(0).await.unwrap();
    node.node_stage_volume_fs(&volume).await.unwrap();

    volumes_api.del_volume_target(&volume.spec.uuid, None).await.unwrap();

    let mut controller = cluster.csi_controller_client().await.unwrap();
    let req = rpc::csi::CreateSnapshotRequest {
        source_volume_id: "ec4e66fd-3b33-4439-b504-d49aba53da26".to_string(),
        name: "snapshot-41ad0274-fedd-4f41-96db-2afc9178669a".to_string(),
        secrets: Default::default(),
        parameters: HashMap::new(),
    };
    controller.csi().create_snapshot(req).await.unwrap();
}
