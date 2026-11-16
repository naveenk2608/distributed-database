# syntax=docker/dockerfile:1
#
# dkv node image: static binaries on a distroless base, running as nonroot.
# Build from the repository root:
#   docker build -f deploy/docker/Dockerfile -t dkv:dev .

FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dkv-server ./cmd/dkv-server && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dkvctl ./cmd/dkvctl && \
    mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/dkv-server /out/dkvctl /usr/local/bin/
# A named volume mounted here starts out owned by the image's directory, so
# the nonroot user can write to it.
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot:nonroot
VOLUME ["/data"]
EXPOSE 7070 7100
ENTRYPOINT ["/usr/local/bin/dkv-server"]
