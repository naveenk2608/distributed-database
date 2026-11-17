# syntax=docker/dockerfile:1
#
# dkv-console image: the dashboard backend with the built dashboard, on a
# distroless base, running as nonroot. Build from the repository root:
#   docker build -f deploy/docker/console.Dockerfile -t dkv-console:dev .

FROM node:22.12.0-bookworm-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dkv-console ./cmd/dkv-console

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/dkv-console /usr/local/bin/
COPY --from=web /web/dist /web
ENV DKV_CONSOLE_WEB_DIR=/web
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/dkv-console"]
