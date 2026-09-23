# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS build
WORKDIR /src
# Override for regions where proxy.golang.org is unreachable, e.g.
# docker build --build-arg GOPROXY=https://goproxy.cn,direct .
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/replicron .     && mkdir -p /var/lib/replicron     && chown 65534:65534 /var/lib/replicron

# Pure-Go static binary: no OS is needed beyond the binary itself and the
# CA bundle (the build stage ships one for `go mod download` over TLS).
# 65534 is the conventional unprivileged uid (nobody).
FROM scratch
COPY --from=build /out/replicron /replicron
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# Pre-owned data dir so named volumes initialize writable for the runtime uid.
COPY --from=build --chown=65534:65534 /var/lib/replicron /var/lib/replicron
USER 65534:65534
ENTRYPOINT ["/replicron"]
CMD ["--help"]
