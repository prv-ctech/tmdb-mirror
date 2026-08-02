FROM postgres:18-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296

COPY --chmod=0755 initdb/10-bootstrap.sh /docker-entrypoint-initdb.d/10-bootstrap.sh
