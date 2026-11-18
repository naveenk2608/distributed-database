# A dkv cluster for benchmarks: nodes spread over availability zones in a
# VPC of their own, and one load-generator instance. deploy/aws/up.sh
# builds the binaries and applies this; deploy/aws/down.sh destroys it.

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  tags                 = { Name = var.name }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
}

resource "aws_subnet" "zone" {
  count                   = length(var.availability_zones)
  vpc_id                  = aws_vpc.this.id
  availability_zone       = var.availability_zones[count.index]
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.name}-${var.availability_zones[count.index]}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
}

resource "aws_route_table_association" "zone" {
  count          = length(aws_subnet.zone)
  subnet_id      = aws_subnet.zone[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "dkv" {
  name        = var.name
  description = "dkv nodes and load generator"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "everything between the cluster's own instances"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  ingress {
    description = "public API from the operator"
    from_port   = 7070
    to_port     = 7070
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  ingress {
    description = "admin API (status, metrics) from the operator"
    from_port   = 7200
    to_port     = 7200
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  ingress {
    description = "SSH from the operator"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# The binaries travel through a private bucket that only the instances'
# role can read.
resource "aws_s3_bucket" "artifacts" {
  bucket_prefix = "${var.name}-artifacts-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "binary" {
  for_each = toset(["dkv-server", "dkvctl", "dkv-bench"])
  bucket   = aws_s3_bucket.artifacts.id
  key      = "bin/${each.key}"
  source   = "${var.binaries_dir}/${each.key}"
  etag     = filemd5("${var.binaries_dir}/${each.key}")
}

resource "aws_iam_role" "instance" {
  name_prefix = "${var.name}-"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "read_artifacts" {
  role = aws_iam_role.instance.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.artifacts.arn}/*" }]
  })
}

resource "aws_iam_instance_profile" "instance" {
  name_prefix = "${var.name}-"
  role        = aws_iam_role.instance.name
}

locals {
  zones = length(var.availability_zones)
  # Fixed private addresses, so every node's peer list is known before any
  # instance starts: node i is host 10+i of its zone's subnet.
  node_ip = [for i in range(var.node_count) : cidrhost(aws_subnet.zone[i % local.zones].cidr_block, 10 + i)]
  peers   = join(",", [for ip in slice(local.node_ip, 0, 3) : "${ip}:7100"])
}

resource "aws_instance" "node" {
  count                  = var.node_count
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.node_instance_type
  subnet_id              = aws_subnet.zone[count.index % local.zones].id
  private_ip             = local.node_ip[count.index]
  vpc_security_group_ids = [aws_security_group.dkv.id]
  key_name               = var.ssh_key_name
  iam_instance_profile   = aws_iam_instance_profile.instance.name
  root_block_device {
    volume_type = var.volume_type
    volume_size = var.volume_size_gb
    iops        = var.volume_type == "gp3" || var.volume_type == "io2" ? var.volume_iops : null
    throughput  = var.volume_type == "gp3" ? var.volume_throughput_mbps : null
  }
  user_data = templatefile("${path.module}/node-user-data.sh.tftpl", {
    bucket             = aws_s3_bucket.artifacts.id
    internal_addr      = "${local.node_ip[count.index]}:7100"
    bootstrap          = count.index < 3
    peers              = local.peers
    shards             = var.shards
    replication_factor = var.replication_factor
    storage_sync       = var.storage_sync
  })
  user_data_replace_on_change = true
  depends_on                  = [aws_s3_object.binary]
  tags                        = { Name = "${var.name}-node-${count.index + 1}" }
}

resource "aws_instance" "loadgen" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.loadgen_instance_type
  subnet_id              = aws_subnet.zone[0].id
  vpc_security_group_ids = [aws_security_group.dkv.id]
  key_name               = var.ssh_key_name
  iam_instance_profile   = aws_iam_instance_profile.instance.name
  root_block_device {
    volume_type = "gp3"
    volume_size = 20
  }
  user_data = templatefile("${path.module}/loadgen-user-data.sh.tftpl", {
    bucket = aws_s3_bucket.artifacts.id
  })
  depends_on = [aws_s3_object.binary]
  tags       = { Name = "${var.name}-loadgen" }
}
