variable "name" {
  description = "Name of the cluster; prefixes every resource."
  type        = string
  default     = "dkv-bench"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-west-1"
}

variable "availability_zones" {
  description = "Zones the nodes are spread over, one subnet each; the load generator goes in the first."
  type        = list(string)
  default     = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]
}

variable "vpc_cidr" {
  description = "CIDR of the VPC; each zone gets a /24 inside it."
  type        = string
  default     = "10.42.0.0/16"
}

variable "allowed_cidr" {
  description = "Where the public API (7070), the admin API (7200) and SSH may be reached from: your address, as a /32."
  type        = string
}

variable "ssh_key_name" {
  description = "An existing EC2 key pair, for SSH to every instance."
  type        = string
}

variable "node_count" {
  description = "Nodes in the cluster; the first three bootstrap it, the others join."
  type        = number
  default     = 3
  validation {
    condition     = var.node_count >= 3
    error_message = "Three nodes at least: the first three bootstrap the cluster and its meta group."
  }
}

variable "node_instance_type" {
  description = "Instance type of the nodes. Not burstable (t*): CPU credits distort benchmark results."
  type        = string
  default     = "c6i.large"
}

variable "loadgen_instance_type" {
  description = "Instance type of the load generator."
  type        = string
  default     = "c6i.xlarge"
}

variable "volume_type" {
  description = "EBS volume type of every instance; fsync latency, and so write latency, depends on it."
  type        = string
  default     = "gp3"
}

variable "volume_size_gb" {
  description = "Root volume size, which holds the data directory."
  type        = number
  default     = 50
}

variable "volume_iops" {
  description = "Provisioned IOPS (gp3: 3,000 to 16,000)."
  type        = number
  default     = 3000
}

variable "volume_throughput_mbps" {
  description = "Provisioned throughput in MiB/s (gp3: 125 to 1,000)."
  type        = number
  default     = 125
}

variable "shards" {
  description = "cluster.shards; fixed when the cluster is bootstrapped."
  type        = number
  default     = 64
}

variable "replication_factor" {
  description = "cluster.replication_factor."
  type        = number
  default     = 3
}

variable "storage_sync" {
  description = "storage.sync: always, interval or never."
  type        = string
  default     = "always"
}

variable "binaries_dir" {
  description = "Directory with linux/amd64 builds of dkv-server, dkvctl and dkv-bench (up.sh builds them)."
  type        = string
  default     = "../../bench/bin/linux-amd64"
}
