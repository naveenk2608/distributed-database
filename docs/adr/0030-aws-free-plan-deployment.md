# ADR 0030: The AWS deployment fits an AWS free plan account

Status: Accepted (Phase 10 follow-up; revises the instance types, region and network of the Phase 10 `deploy/aws`)

## Context

Phase 10 wrote `deploy/aws` for a paid account: `c6i.large` nodes and a
`c6i.xlarge` load generator in `eu-west-1`, 50 GiB volumes, a key pair made
by hand. It was validated in CI but never applied.

The one deployment that will run is on an AWS free plan account (accounts
created after 15 July 2025). Such an account spends credits instead of
money, can launch only instance types that `DescribeInstanceTypes` reports
as free-tier eligible for it, and closes when the plan ends or the credits
run out. The run is one-off: create the cluster in `ap-south-1`, show
failover on the dashboard, run a short benchmark from a load generator in
the same region, destroy everything, and prove nothing is left.

## Decision

- **Instance types.** Three `c7i-flex.large` nodes and a `c7i-flex.large`
  load generator (2 vCPUs, 4 GiB). A precondition on each instance refuses a
  type that is not free-tier eligible for the account (`free_tier_only`,
  default true), so a plan fails before anything is created.
- **No NAT gateway, no Elastic IPs.** Every instance is in a public subnet
  with an auto-assigned public IPv4 address, released when the instance
  terminates, and reaches the internet (the snap store, S3) through the
  internet gateway. The security group admits SSH, the public API and the
  admin API only from the operator's address, and everything between the
  cluster's own instances.
- **Three zones**, the first three of the region (by name) that offer both
  instance types, unless `availability_zones` names them.
- **Volumes.** One 8 GiB gp3 root volume per instance (the Ubuntu image's
  size, the smallest allowed) at gp3's baseline 3,000 IOPS and 125 MiB/s,
  encrypted with the AWS-managed key; the data directory is on it.
- **The console runs on the load generator**, with its history in memory
  (ADR 0014 allows it), listening on loopback only; the dashboard is reached
  through an SSH tunnel.
- **The key pair** is the operator's local public key, registered by
  Terraform and deleted on destroy.
- **Tags.** The provider's default tags put `project=dkv` and
  `cluster=<name>` on every resource that takes tags, root volumes included.
  `make aws-down` runs `check-empty.sh` after `terraform destroy`: it lists
  what is still tagged `project=dkv` in the region (instances not
  terminated, volumes, Elastic IPs, NAT gateways, VPCs, subnets, internet
  gateways, security groups, key pairs) and the S3 buckets, IAM roles and
  instance profiles named after the cluster, and fails if anything is left.
- **Failover runs keep the killed node down.** The unit keeps
  `Restart=on-failure`; during the failover experiment a runtime drop-in
  (`/run/systemd/system/dkv-server.service.d`, gone at reboot) sets
  `Restart=no`, so a SIGKILLed node stays down until the script starts it.

## Alternatives

- **`m7i-flex.large`** (2 vCPUs, 8 GiB, also eligible) costs more per hour,
  and a node with 16 shards needs far less than 4 GiB. **`t3.micro`,
  `t3.small`, `t4g.*`** are eligible too but burstable: throughput would
  depend on each instance's CPU credit balance, and `t4g` is arm64. Flex
  instances have no credits, but they are not fixed-performance either: they
  run at full speed most of the time over a 40% baseline, which the results
  must state.
- **Private nodes with an S3 gateway endpoint** (free) and only the load
  generator public would save three public addresses. The nodes could then
  not reach the snap store to install the AWS CLI, so the binaries would
  have to come through the load generator or a hand-signed S3 request: more
  moving parts to save a few cents in a run of an hour or two.
- **A NAT gateway** costs more per hour than a node, plus a charge per GB,
  for no benefit here.
- **One zone** has no cross-zone transfer charge and lower latency, but a
  cluster spread over zones is the one worth measuring: replication and
  elections cross zones as they would in production. Cross-zone transfer
  for a short benchmark costs cents.
- **PostgreSQL** for the console's history (on the load generator, or RDS)
  adds boot steps or a paid service and keeps nothing worth keeping after a
  one-hour run. **The console on the operator's machine**, polling the
  nodes' admin ports across the internet, would add that latency to every
  poll; the tunnel carries only the dashboard.
- **A separate data volume** per node: more resources to tag and clean up,
  no gain at this size.

## Consequences

- The deployment refuses to start on a type the account cannot launch,
  instead of failing in the middle of an apply.
- Its cost per hour is the four instances, their public addresses and the
  volumes; docs/PROGRESS.md records the estimate taken from the AWS Price
  List API before the run.
- Results from flex instances on gp3 describe this configuration only;
  docs/benchmarks.md says so with them.
- The dashboard is never exposed to the internet.
