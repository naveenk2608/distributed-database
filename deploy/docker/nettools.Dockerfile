# Network fault tools for the chaos tests (test/chaos, make chaos):
# iptables for partitions and tc (iproute2) for netem latency, loss and
# duplication. The harness runs this image inside a node container's
# network namespace (--network container:<node> --cap-add NET_ADMIN), so
# the node image itself stays distroless and unprivileged.
#   docker build -f deploy/docker/nettools.Dockerfile -t dkv-nettools:dev deploy/docker
FROM alpine:3.20
RUN apk add --no-cache iptables iproute2
