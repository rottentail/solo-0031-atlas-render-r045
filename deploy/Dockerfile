# Build stage: the code tree is only needed here, never at runtime.
FROM golang:1.22-bookworm AS build
WORKDIR /src
# The service uses only the Go standard library, so no dependency downloads occur.
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
ENV CGO_ENABLED=0
RUN go build -trimpath -buildvcs=false -ldflags='-s -w' -o /out/atlas ./cmd/atlas
# Prepare an empty directory with the runtime uid/gid so that a freshly created
# named volume inherits non-root ownership instead of root-only ownership.
RUN mkdir -p /out/state

# Runtime stage: distroless base, no shell, no package manager, no toolchain.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/atlas /usr/local/bin/atlas
COPY --from=build --chown=65532:65532 /out/state /var/lib/atlas

# State lives outside the image. The process runs as the distroless nonroot
# user (65532); the volume must be writable by that uid.
ENV ATLAS_ADDR=0.0.0.0:8763 \
    ATLAS_STATE=/var/lib/atlas/state.json
VOLUME ["/var/lib/atlas"]
USER 65532:65532
EXPOSE 8763

# Liveness only. Readiness (data volume writable, single owner) is a separate
# probe so a volume problem never causes a restart loop.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/atlas", "check", "-mode", "live"]

ENTRYPOINT ["/usr/local/bin/atlas"]
