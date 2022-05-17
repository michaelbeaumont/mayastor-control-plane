variable "registry" {
  type        = string
  description = "The docker registry to pull from"
  default     = "mayadata"
}

variable "tag" {
  type        = string
  description = "The default docker image tag to use when pulling images, this applies to mayadata images only"
  default     = "develop"
}

variable "etcd_image" {
  type        = string
  default     = "docker.io/bitnami/etcd:3.4.15-debian-10-r43"
  description = "etcd image to use"
}

variable "control_node" {
  type        = string
  default     = "ksnode-1"
  description = "The node on which control plane components are scheduled - soft requirement"
}

variable "control_resource_limits" {
  type = map(any)
  default = {
    "cpu"    = "1000m"
    "memory" = "1Gi"
  }
}
variable "control_resource_requests" {
  type = map(any)
  default = {
    "cpu"    = "250m"
    "memory" = "500Mi"
  }
}

variable "dsp_operator_image" {
  type        = string
  description = "operator image to use for managing disk pools"
  default     = "mayastor-operator-diskpool"
}

variable "rest_image" {
  type    = string
  default = "mayastor-api-rest"
}

variable "core_image" {
  type    = string
  default = "mayastor-agent-core"
}

variable "io-engine_image" {
  type        = string
  description = "io engine image to use"
  default     = "mayastor-io-engine"
}

variable "io-engine_hugepages_2Mi" {
  type        = string
  description = "amount of hugepages to allocate for io-engine"
  default     = "2Gi"
}

variable "io-engine_cpus" {
  type        = string
  description = "number of CPUs to use"
  default     = 2
}
variable "io-engine_cpu_list" {
  type        = string
  description = "List of cores to run on, eg: 2,3"
  default     = "2,3"
}

variable "io-engine_memory" {
  type        = string
  description = "amount of memory to request for io-engine"
  default     = "4Gi"
}

variable "io-engine_rust_log" {
  type        = string
  description = "The RUST_LOG environment filter for io-engine"
  default     = "debug,h2=info,hyper=info,tower_buffer=info,tower=info,rustls=info,reqwest=info,tokio_util=info,async_io=info,polling=info,tonic=info,want=info,mio=info"
}

variable "csi_node_image" {
  type        = string
  description = "CSI agent image to use"
  default     = "mayastor-csi-node"
}

variable "csi_node_grace_period" {
  type        = string
  description = "termination grace period in seconds for the CSI pod"
  default     = 30
}

variable "csi_registar_image" {
  type        = string
  default     = "k8s.gcr.io/sig-storage/csi-node-driver-registrar:v2.5.0"
  description = "CSI sidecars to use"
}

variable "csi_attacher_image" {
  type        = string
  default     = "quay.io/k8scsi/csi-attacher:v3.1.0"
  description = "csi-attacher to use"
}

variable "csi_provisioner" {
  type        = string
  default     = "quay.io/k8scsi/csi-provisioner:v2.1.1"
  description = "csi-provisioner to use"
}

variable "csi_controller_image" {
  type        = string
  description = "CSI controller image to use"
  default     = "mayastor-csi-controller"
}

variable "control_request_timeout" {
  type        = string
  description = "default request timeout for any NATS or GRPC request"
  default     = "5s"
}

variable "control_cache_period" {
  type        = string
  description = "the period at which a component updates its resource cache"
  default     = "30s"
}

variable "with_jaeger" {
  type        = bool
  description = "enables or disables the jaegertracing-operator"
  default     = true
}

variable "control_rust_log" {
  type        = string
  description = "The RUST_LOG environment filter for all control-plane components"
  default     = "info,core=debug,rest=debug,csi_controller=debug,csi_node=debug,diskpool_operator=debug,common_lib=debug,grpc=debug"
}
