output "region" {
  value = var.region
}

output "node_public_ips" {
  description = "Public addresses of the nodes, in order."
  value       = aws_instance.node[*].public_ip
}

output "node_private_ips" {
  description = "Private addresses; the public API is on port 7070, the admin API on 7200."
  value       = local.node_ip
}

output "loadgen_public_ip" {
  value = aws_instance.loadgen.public_ip
}

output "bench_addr" {
  description = "--addr for dkv-bench and dkvctl on the load generator."
  value       = join(",", [for ip in local.node_ip : "${ip}:7070"])
}

output "bench_admin" {
  description = "--admin for dkv-bench on the load generator."
  value       = local.node_admins
}

output "ssh_key_file" {
  description = "Private key for SSH to every instance, as user ubuntu."
  value       = trimsuffix(pathexpand(var.ssh_public_key_file), ".pub")
}

output "console_port" {
  value = var.console_port
}

output "console_tunnel" {
  description = "Run this, then open http://localhost:<console_port> for the dashboard."
  value = format("ssh -i %s -N -L %d:127.0.0.1:%d ubuntu@%s", trimsuffix(pathexpand(var.ssh_public_key_file), ".pub"),
  var.console_port, var.console_port, aws_instance.loadgen.public_ip)
}

output "environment" {
  description = "What the benchmark report should record about this deployment."
  value = format("AWS %s: %d x %s nodes in %d zones (%s), load generator %s in %s; root %s volumes of %d GiB, %d IOPS, %d MiB/s; %s; %d shards, RF %d, storage.sync %s",
    var.region, var.node_count, var.node_instance_type, local.zones, join(", ", local.availability_zones),
    var.loadgen_instance_type, local.availability_zones[0], var.volume_type, var.volume_size_gb, var.volume_iops,
  var.volume_throughput_mbps, data.aws_ami.ubuntu.name, var.shards, var.replication_factor, var.storage_sync)
}

output "deployment" {
  description = "What deploy/aws/bench.sh records in bench/results/aws/meta/deployment.json."
  value = {
    region = var.region
    zones  = local.availability_zones
    ami    = { id = data.aws_ami.ubuntu.id, name = data.aws_ami.ubuntu.name }
    nodes = [for n in concat(aws_instance.node, [aws_instance.loadgen]) : {
      name              = n.tags["Name"]
      instance_id       = n.id
      instance_type     = n.instance_type
      availability_zone = n.availability_zone
      private_ip        = n.private_ip
    }]
    instance_types = { for role, t in { node = data.aws_ec2_instance_type.node, loadgen = data.aws_ec2_instance_type.loadgen } : role => {
      instance_type       = t.instance_type
      vcpus               = t.default_vcpus
      memory_mib          = t.memory_size
      network_performance = t.network_performance
      free_tier_eligible  = t.free_tier_eligible
      ebs_baseline_iops   = t.ebs_performance_baseline_iops
      ebs_baseline_mbps   = t.ebs_performance_baseline_throughput
    } }
    volume = {
      type            = var.volume_type
      size_gb         = var.volume_size_gb
      iops            = var.volume_iops
      throughput_mbps = var.volume_throughput_mbps
      encrypted       = true
    }
    cluster = {
      shards             = var.shards
      replication_factor = var.replication_factor
      storage_sync       = var.storage_sync
      log_level          = var.log_level
    }
  }
}
