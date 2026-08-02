FROM postgres:18-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296

LABEL org.opencontainers.image.source="https://github.com/prv-ctech/tmdb-mirror"

RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
        pgbackrest=2.59.0-1.pgdg12+1 \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY --chmod=0755 initdb/10-bootstrap.sh /docker-entrypoint-initdb.d/10-bootstrap.sh
COPY --chmod=0755 initdb/20-pgbackrest-bootstrap.sh /docker-entrypoint-initdb.d/20-pgbackrest-bootstrap.sh
COPY --chmod=0755 pgbackrest-runner.sh /usr/local/bin/tmdb-pgbackrest
COPY --chmod=0755 entrypoint.sh /usr/local/bin/tmdb-postgres-entrypoint

ENTRYPOINT ["/usr/local/bin/tmdb-postgres-entrypoint"]
