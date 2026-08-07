# Production Rust image for the TMDB API and workers.
#
# The builder and runtime references are immutable. Update the digest only as
# part of a reviewed dependency/image upgrade, then rebuild every service.
FROM rust:1.97-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa AS builder

WORKDIR /workspace
ENV CARGO_TERM_COLOR=always

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY apps ./apps

RUN cargo build --locked --release --bins

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS runtime

LABEL org.opencontainers.image.source="https://github.com/prv-ctech/tmdb-mirror"

RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates curl jq \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 tmdb \
    && useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent \
        --shell /usr/sbin/nologin tmdb

COPY --from=builder /workspace/target/release/tmdb-api /usr/local/bin/tmdb-api
COPY --from=builder /workspace/target/release/tmdb-images /usr/local/bin/tmdb-images
COPY --from=builder /workspace/target/release/tmdb-ingest /usr/local/bin/tmdb-ingest
COPY --from=builder /workspace/target/release/tmdb-worker /usr/local/bin/tmdb-worker
COPY infra/runtime/tmdb-log-run /usr/local/bin/tmdb-log-run
COPY infra/runtime/tmdb-runtime /usr/local/bin/tmdb-runtime
RUN chmod 0755 /usr/local/bin/tmdb-log-run /usr/local/bin/tmdb-runtime

USER 10001:10001
WORKDIR /nonexistent
ENTRYPOINT ["/usr/local/bin/tmdb-api"]
