# syntax=docker/dockerfile:1
# Build official, commit-pinned sources: the upstream container images are unavailable.
FROM golang:1.24.8-alpine@sha256:3d78beb141d98f42337f1252ecf2a5f20374109929a4c3f6817f9e4179cc0ae5 AS toolchain
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
RUN apk add --no-cache ca-certificates git

FROM toolchain AS server-build
WORKDIR /source
# RELEASE.2025-10-15T17-29-55Z includes the final upstream security release.
RUN git init . && git remote add origin https://github.com/minio/minio.git && \
    git fetch --depth=1 origin 9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a && \
    git checkout --detach FETCH_HEAD
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags "$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go 2025-10-15T17:29:55Z)" \
    -o /out/minio .

FROM toolchain AS client-build
WORKDIR /source
# Keep the previously configured mc release, resolved to its immutable commit.
RUN git init . && git remote add origin https://github.com/minio/mc.git && \
    git fetch --depth=1 origin 7394ce0dd2a80935aded936b09fa12cbb3cb8096 && \
    git checkout --detach FETCH_HEAD
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags "$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go 2025-08-13T08:35:41Z)" \
    -o /out/mc .

FROM alpine:3.22.3@sha256:55ae5d250caebc548793f321534bc6a8ef1d116f334f18f4ada1b2daad3251b2 AS runtime
RUN apk add --no-cache ca-certificates curl

FROM runtime AS client
LABEL org.opencontainers.image.source="https://github.com/minio/mc" \
    org.opencontainers.image.revision="7394ce0dd2a80935aded936b09fa12cbb3cb8096" \
    org.opencontainers.image.version="RELEASE.2025-08-13T08-35-41Z" \
    org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=client-build /out/mc /usr/bin/mc
COPY --from=client-build /source/LICENSE /source/CREDITS /licenses/
ENTRYPOINT ["mc"]

FROM runtime AS server
LABEL org.opencontainers.image.source="https://github.com/minio/minio" \
    org.opencontainers.image.revision="9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a" \
    org.opencontainers.image.version="RELEASE.2025-10-15T17-29-55Z" \
    org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=server-build /out/minio /usr/bin/minio
COPY --from=server-build /source/LICENSE /source/CREDITS /licenses/
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
