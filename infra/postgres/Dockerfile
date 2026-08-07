FROM postgres:18-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296

LABEL org.opencontainers.image.source="https://github.com/prv-ctech/tmdb-mirror"

RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
        jq \
        pgbackrest=2.59.0-1.pgdg12+1 \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY --chmod=0755 infra/postgres/initdb/10-bootstrap.sh /docker-entrypoint-initdb.d/10-bootstrap.sh
COPY --chmod=0755 infra/postgres/initdb/20-pgbackrest-bootstrap.sh /docker-entrypoint-initdb.d/20-pgbackrest-bootstrap.sh
COPY --chmod=0755 infra/postgres/pgbackrest-runner.sh /usr/local/bin/tmdb-pgbackrest
COPY --chmod=0755 infra/postgres/entrypoint.sh /usr/local/bin/tmdb-postgres-entrypoint
COPY --chmod=0755 infra/runtime/tmdb-log-run /usr/local/bin/tmdb-log-run

ENTRYPOINT ["/usr/local/bin/tmdb-postgres-entrypoint"]
