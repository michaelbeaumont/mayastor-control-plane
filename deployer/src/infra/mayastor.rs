use super::*;

#[async_trait]
impl ComponentAction for Mayastor {
    fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
        let mut cfg = cfg;
        for i in 0 .. options.mayastors {
            let mayastor_socket =
                format!("{}:10124", cfg.next_ip_for_name(&Self::name(i, options))?);
            let mut bin = Binary::from_path("mayastor")
                .with_nats("-n")
                .with_args(vec!["-N", &Self::name(i, options)])
                .with_args(vec!["-g", &mayastor_socket]);

            if options.developer_delayed {
                bin = bin.with_env("DEVELOPER_DELAYED", "1");
            }

            if !options.no_etcd {
                let etcd = format!("etcd.{}:2379", options.cluster_label.name());
                bin = bin.with_args(vec!["-p", &etcd]);
            }
            cfg = cfg.add_container_bin(&Self::name(i, options), bin)
        }
        Ok(cfg)
    }
    async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        for i in 0 .. options.mayastors {
            cfg.start(&Self::name(i, options)).await?;
        }
        Ok(())
    }
    async fn wait_on(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
        for i in 0 .. options.mayastors {
            let mut hdl = cfg.grpc_handle(&Self::name(i, options)).await.unwrap();
            hdl.mayastor
                .list_nexus(rpc::mayastor::Null {})
                .await
                .unwrap();
        }
        Ok(())
    }
}

impl Mayastor {
    pub fn name(i: u32, options: &StartOptions) -> String {
        if options.mayastors == 1 {
            "mayastor".into()
        } else {
            format!("mayastor-{}", i + 1)
        }
    }
}
