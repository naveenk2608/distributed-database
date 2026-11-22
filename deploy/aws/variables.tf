variable "name" {
  description = "Name of the cluster; prefixes every resource."
  type        = string
  default     = "dkv-bench"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "ap-south-1"
}

variable "availability_zones" {
  description = "Zones the nodes are spread over, one subnet each; the load generator goes in the first. Empty: the first three zones of the region, by name, that offer both instance types."
  type        = list(string)
  default     = []
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

variable "ssh_public_key_file" {
  description = "An OpenSSH public key file. Terraform registers it as the instances' key pair and deletes the key pair on destroy; the private key is the same path without .pub."
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

variable "free_tier_only" {
  description = "Refuse instance types that are not free-tier eligible for this account in the region: an AWS free plan account can launch only those."
  type        = bool
  default     = true
}

variable "node_instance_type" {
  description = "Instance type of the nodes. The default is free-tier eligible and not a t* type: flex instances have no CPU credits, and run at full speed most of the time over a 40% baseline."
  type        = string
  default     = "c7i-flex.large"
}

variable "loadgen_instance_type" {
  description = "Instance type of the load generator, which also runs dkv-console."
  type        = string
  default     = "c7i-flex.large"
}

variable "volume_type" {
  description = "EBS volume type of every instance; fsync latency, and so write latency, depends on it."
  type        = string
  default     = "gp3"
}

variable "volume_size_gb" {
  description = "Root volume size of every instance, which holds the data directory. 8 GiB is the size of the Ubuntu image, the smallest allowed."
  type        = number
  default     = 8
}

variable "volume_iops" {
  description = "Provisioned IOPS (gp3: 3,000, the baseline included in its price, to 16,000)."
  type        = number
  default     = 3000
}

variable "volume_throughput_mbps" {
  description = "Provisioned throughput in MiB/s (gp3: 125, the baseline included in its price, to 1,000)."
  type        = number
  default     = 125
}

variable "shards" {
  description = "cluster.shards; fixed when the cluster is bootstrapped. 16, as in make bench-local."
  type        = number
  default     = 16
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

variable "log_level" {
  description = "log.level of the nodes and the console (warn, as in make bench-local)."
  type        = string
  default     = "warn"
}

variable "console_port" {
  description = "Port dkv-console serves the dashboard on, on the load generator's loopback only: reach it through an SSH tunnel (output console_tunnel)."
  type        = number
  default     = 8080
}

variable "binaries_dir" {
  description = "Directory with linux/amd64 builds of dkv-server, dkvctl, dkv-bench and dkv-console, and web.tar.gz, the built dashboard (up.sh makes them)."
  type        = string
  default     = "../../bench/bin/linux-amd64"
}
