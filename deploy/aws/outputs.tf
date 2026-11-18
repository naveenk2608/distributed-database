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

output "bench_addrs" {
  description = "--addr and --admin for dkv-bench on the load generator."
  value = {
    addr  = join(",", [for ip in local.node_ip : "${ip}:7070"])
    admin = join(",", [for ip in local.node_ip : "${ip}:7200"])
  }
}

output "environment" {
  description = "What the benchmark report should record about this deployment."
  value = format("AWS %s: %d x %s nodes in %d zones, load generator %s; %s volumes, %d GiB, %d IOPS, %d MiB/s",
    var.region, var.node_count, var.node_instance_type, length(var.availability_zones), var.loadgen_instance_type,
  var.volume_type, var.volume_size_gb, var.volume_iops, var.volume_throughput_mbps)
}
